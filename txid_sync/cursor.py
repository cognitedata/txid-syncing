from marshmallow import Schema, fields, ValidationError, validates_schema
import marshmallow
import hashlib
import base64
from cryptography.fernet import Fernet
import enum



FERNET_NOT_REALLY_A_KEY = base64.urlsafe_b64encode(hashlib.sha256(b"This key is not really for security, but to make cursors opaque and un-editable by consumers. angry-ACQUIRE-benefice-anthill").digest())

class Priority(enum.IntEnum):
    EXCEEDING_BATCH_SIZE = 0
    PAST_XIPS = 1
    SCROLL = 2


class Cursor:
    """ Cursor enables reliably scrolling through all the data in a tracked Postgres table,
    with no state or long-running transactions on the server, given a previous
    (which could be an empty "start from scratch") cursor, the current visibility snapshot, and 
    the rows returned by the previous cursor.

    It will emit the _current_ data as of the cursor passing through some data. It is not a server
    side cursor (that's the point!), so if something changes before the cursor has got there, it will
    see the data as of when the cursor gets to a row, and not what it was at the creation of the initial
    cursor. This is a feature. Server side cursors are tied to a particular connection (and thus not very
    stable as far as external clients go), and prevents the xmin horizon of a cluster from progressing.
    Following this, data that gets updated will be pushed towards the end of the "queue". 


    A cursor is made up of the following components. Names are similar to how they appear in Postgres
    documentation and source code, with "xid" meaning "transaction id" and "xip" meaning
    "transaction in progress".

    - `batch_size´, which limits how many objects will be fetched in a batch. There is a round trip per
    batch.
    - Where to continue from, `xid_next`. If everything committed in order and if a transaction couldn't
    change more than batch_size objects, this would be all we'd need. 
    - Since a transaction can change more objects than we can fit in batch, if `xid_at` is set, then so
    must `xid_at_id`. When set, it means we couldn't know if we finished everything that changed within
    a particular transaction, so we need to proceed from `xid_at_id`. ("seq" here is a tiebreaker within
    a version of which there is only one. This could for example by the (surrogate) id of the row) If
    `xid_at` is set, then `xid_next` will be `xid_at + 1`.
    - Now able to handle more changes than `batch_size` within a single transaction, if transactions always
    committed in order, we wouldn't need more. But since they can, we also need the list of active transactions,
    the `xip_list`, which has the IDs of other transactions that could possibly change everything. Until they
    commit or abort, we need to keep track of them. We only need to keep track of XIPs less than `xid_next`,
    because if they're past `xid_next`, we'll get to them anyway. If an XIP hasn't completed by the time `xid_next`
    progresses past it, then it'll remain in `xip_list` until it has. The length of the xip list is limited
    by the number of active connections to Postgres. A healthy workload will not have a large amount of xips
    that remain around for long.

    A cursor might have a mix of all three cases, and the priority in which we deal with them is as follows:

    1. `xid_at` and `xid_at_id`, e.g. (1234, 1000) meaning that within tx 1234, we got to the row with id 1000, and
    since we can't know how many more got touched within the same tx, we continue there.
    2. When `xip_list` isn't empty, we then query for transactions that were still in progress when `xid_next`
    progressed past them. We proceed in ascending order. If we can't know whether we saw all the changes of a
    now-commited xip, it'll turn into case (1), with `xid_at` and `xid_at_id` provided.
    3. If we know we're done with everything within an `xid_at` and everything from a past xip, then we can proceed
    with a simple case of looking for changes whose `txid >= xid_next`.

    We know we're "done" with everything within a version since we query for batch_size + 1 to peek into what the
    next batch would be. Within a priority, everything is sorted on txid ascending, and then id ascending. (In most
    query plans, "sorted on" will likely mean following a sorted index on (txid, seq))
    Thus, if the first item of the next batch has a different version, then we've seen everything from that version.

    If we make it to case (3) (as in cases 1+2 did not produce enough rows to fill up the batch), we know that any
    XIP we've been carrying must either have completed (and appeared, if it changed anything relevant to us, and
    possibly become case (1) if it changed more than could fit in the remainder of the batch), still be in the
    xip-list from a current snapshot (i.e. it hasn't completed yet, and we can't know when it will), or we can
    drop it. 
    """

    # xip_list is Postgres lingo for "list of transactions in progress", and "xid" transaction id.
    def __init__(self, batch_size, xid_at=None, xid_at_id=None, xip_list=None, xid_next=None,
                partition_id=0, number_of_partitions=1,
                # XXX: Short term hack. Raw SQL should not be in the cursor, or the key 
                # really needs to be treated as a key. Instead, they should contain filters from
                # the query DSL that the property graph will handle            
                additional_froms=None, additional_wheres=None,
                # For debugging
                previous=None, type=None):
        self.batch_size = batch_size
        self.xid_at = xid_at
        self.xid_at_id = xid_at_id
        self.xip_list = xip_list or []
        self.xid_next = xid_next

        # Temporary hack
        self.additional_froms = additional_froms
        self.additional_wheres = additional_wheres

        # These are just for debugging
        self._previous = previous
        self.type = type

    def to_token(self) -> str:
        f = Fernet(FERNET_NOT_REALLY_A_KEY)
        return f.encrypt(CursorSchema().dumps(self).encode('utf-8')).decode('utf-8')

    @classmethod
    def from_token(cls, token: bytes) -> 'Cursor':
        t = Fernet(FERNET_NOT_REALLY_A_KEY).decrypt(token)
        return CursorSchema().loads(t)

    @classmethod
    def empty(cls, batch_size=1000, **kw) -> 'Cursor':
        return cls(batch_size=batch_size, xid_next=0, **kw)

    def get_changes_cte(self) -> str:
        """ Return SQL that defines a `changes` table expression that can be selected
        from by subsequent SQL that figures out how to produce the objects to emit.

        with changes as (
            select last_modified_txid, id from (

                select * from (
                        -- We need to wrap this in a subquery to be able to
                        -- order and limit prior to the union all
                        select 0, last_modified_txid, id from version_info
                        where last_modified_txid = 1 and id > 1
                        order by id asc
                        limit 101
                    ) as _0(priority, last_modified_txid, id)

                -- We do ALL to skip a duplicate-removal step (there will be none)
                -- and to enable Postgres to terminate once 101 records have been found
                union all
                
                select * from (
                    select 1, last_modified_txid, id from version_info
                    where last_modified_txid = ANY(ARRAY[2])
                    order by last_modified_txid asc, id asc
                    limit 101
                ) as _2(priority, last_modified_txid, id)
                
                union all
                
                select * from (
                    select 2, last_modified_txid, id from version_info
                    where last_modified_txid > 3
                    order by last_modified_txid asc, id asc
                    limit 101
                ) as _5(priority, last_modified_txid, id)
                
                -- This applies to the UNION ALL
                -- If the first SELECT produces 101 records, then no more
                -- SELECTs at the union level will execute
                limit 101
                
                ) _ 
                -- We order OUTSIDE of the UNION-level, or Postgres would need to fully execute
                -- all of the lower-priority SELECTs to produce a total order
                order by priority asc, last_modified_txid asc, id asc
        )
        select * from changes;

        
        That somewhat long (but conceptually simple) query will produce a query plan like the following.
        Note the "never executed" detail on the last Limit nodes.

        Subquery Scan on changes  (cost=78.20..79.47 rows=101 width=16) (actual time=0.342..0.378 rows=101 loops=1)
        ->  Sort  (cost=78.20..78.46 rows=101 width=20) (actual time=0.341..0.354 rows=101 loops=1)
                Sort Key: _.priority, _.last_modified_txid, _.id
                Sort Method: quicksort  Memory: 32kB
                ->  Subquery Scan on _  (cost=0.42..74.84 rows=101 width=20) (actual time=0.044..0.292 rows=101 loops=1)
                    ->  Limit  (cost=0.42..73.83 rows=101 width=20) (actual time=0.044..0.264 rows=101 loops=1)
                            ->  Append  (cost=0.42..91.27 rows=125 width=20) (actual time=0.042..0.246 rows=101 loops=1)
                                ->  Limit  (cost=0.42..36.16 rows=12 width=20) (actual time=0.012..0.013 rows=0 loops=1)
                                        ->  Index Only Scan using version_info_last_modified_txid_id_idx on version_info  (cost=0.42..36.16 rows=12 width=20) (actual time=0.012..0.012 rows=0 loops=1)
                                            Index Cond: ((last_modified_txid = 1) AND (id > 1))
                                            Heap Fetches: 0
                                ->  Limit  (cost=0.42..36.13 rows=12 width=20) (actual time=0.029..0.214 rows=101 loops=1)
                                        ->  Index Only Scan using version_info_last_modified_txid_id_idx on version_info version_info_1  (cost=0.42..36.13 rows=12 width=20) (actual time=0.029..0.198 rows=101 loops=1)
                                            Index Cond: (last_modified_txid = ANY ('{2}'::integer[]))
                                            Heap Fetches: 101
                                ->  Limit  (cost=0.42..17.11 rows=101 width=20) (never executed)
                                        ->  Index Only Scan using version_info_last_modified_txid_id_idx on version_info version_info_2  (cost=0.42..169838.48 rows=1028038 width=20) (never executed)
                                            Index Cond: (last_modified_txid > 3)
                                            Heap Fetches: 0

        """

        selects = []
        # We add one, because we use the last row (which will be the first of the *next* batch)
        # to derive a cursor. The last plus-one record will not be emitted
        batch_plus_one = self.batch_size + 1

        additional_froms = ("\n" + "\n".join(self.additional_froms)) if self.additional_froms else ""
        additional_wheres = ("and \n({})\n".format(") and \n(\n".join(self.additional_wheres))) if self.additional_wheres else ""

        params = dict(txid=self.xid_at, id=self.xid_at_id, xip_list=self.xip_list, xid_next=self.xid_next,
                batch_size=batch_plus_one, additional_wheres=additional_wheres, additional_froms=additional_froms
        )

        if self.xid_at_id is not None:
            # Where we left off, we were unable to fit everything in the version
            # within the batch, and we _know_ there is more since we peek at the would-be
            # next item so we continue from xid_at_node_id. This gets the higher priority,
            # since we can only keep track of how deep we're in one version at the time.
            # (Or the cursor would potentially become huge)
            # Even if an XIP commits and is thus older, we finish here first, and do the
            # potentially-completed XIPs next.
            selects.append(
                """
select {prio}, last_modified_txid, id, version
from version_info {additional_froms}
where last_modified_txid = {txid} and id > {id} {additional_wheres}
order by last_modified_txid asc, id asc
limit {batch_size}
                """.format(prio=Priority.EXCEEDING_BATCH_SIZE, **params)
            )

        # Before we get to the "proceed with a simple scroll >= xid_next" part, we check if xids that
        # appeared as XIPs before might have completed. These XIPs will necessarily be < xid_next, or
        # we wouldn't have kept track of them in the first place. If they don't produce anything, we
        # can drop them unless they're still active according to the snapshot we used to pull the rows
        # from
        if self.xip_list:
            selects.append(
                """
select {prio}, last_modified_txid, id, version
from version_info {additional_froms}
where last_modified_txid = ANY(ARRAY{xip_list}) {additional_wheres}
order by last_modified_txid asc, id asc
limit {batch_size}
                """.format(prio=Priority.PAST_XIPS, **params)
            )

        if self.xid_next is not None:
            selects.append(
                """
select {prio}, last_modified_txid, id, version
from version_info {additional_froms}
where last_modified_txid >= {xid_next} {additional_wheres}
order by last_modified_txid asc, id asc
limit {batch_size}
                """.format(prio=Priority.SCROLL, **params)
            )

        return """
/* {self} (previous: {previous}) */
with changes as (
    select priority, last_modified_txid, id, version from (
        {unions}
        limit {batch_size}
    ) _
    order by priority asc, last_modified_txid asc, id asc
)""".format(
            # We union ALL because we know that one row cannot appear multiple times (as the last change will be
            # by exactly one transaction),
            # and we'd also like to NOT pull from lower priority sub-queries if the higher priority ones
            # produce enough to fill up the batch. (If you EXPLAIN ANALYZE a query where
            # that is the case, you'll see "never executed" in the sub-plan.) Postgres
            # cannot do this optimisation for union (without "all"), nor if there is an
            # ORDER BY on the union-level SELECT. The orders we use here are either wrapped
            # outside the union-level select, or within the sub-select that's being 
            # unioned. What we achieve with the careful placement of sub-selects and orders
            # is that the 2nd and 3rd sub-selects will only be pull-ed *if* required, and
            # only for however many rows are necessary to fill the batch.
            self=self,
            previous=self._previous,
            unions="\n\nunion all\n\n".join(
                "select * from (\n\t{}\n) as _{}(priority, last_modified_txid, id, version)".format(
                    select, i
                ) for i, select in enumerate(selects)
            ),
            batch_size=batch_plus_one
        )

    def __str__(self):
        return '<{}: {}>'.format(
            type(self).__name__,
            ', '.join(
                '{}={}'.format(key, getattr(self, key))
                for key in ('batch_size', 'xid_at', 'xid_at_id', 'xip_list', 'xid_next', 'type', 'number_of_partitions', 'partition_id')
                if getattr(self, key) is not None
            )
        )

    def advance(self, snapshot, rows, new_batch_size=None) -> "Cursor":
        if not rows:
            # If we're done, then past xips must have finished or be in the active_xip_list,
            # since we've just queried for them
            xid_next = self.xid_next + 1
            xip_list = [xip for xip in snapshot["xip_list"] if xip < xid_next]
            cursor_type = 'empty'
            return type(self)(
                new_batch_size or self.batch_size, xid_next=xid_next, xip_list=xip_list,
                partition_id=self.partition_id, number_of_partitions=self.number_of_partitions,
                previous=self, type=cursor_type
            )

        xid_at = None
        xid_at_id = None
        cursor_type = None

        # We know we have rows, or we would've returned earlier
        if len(rows) <= self.batch_size:
            # We fetch batch size + 1, so here we're done with anything committed (or we'd have the +1), but
            # we need to carry forward xip-list of uncommitted transactions.
            xid_next = rows[-1]["last_modified_txid"] + 1
            cursor_type = 'complete'

        else:
            assert len(rows) == self.batch_size + 1
            # We know for sure that there's more committed than we've seen so far
            cursor_type = 'partial'

            # The second last is the last of *this* batch
            last_of_this_batch = rows[-2]
            # ... and this is a peek into the next batch
            peek_at_next_batch = rows[-1]

            if last_of_this_batch['last_modified_txid'] == peek_at_next_batch['last_modified_txid']:
                # Their txids are the same, so we need to continue plowing through things on the same 
                # txid. We continue based on seq-progress within a version
                xid_at_id = last_of_this_batch['id']
                xid_at = last_of_this_batch['last_modified_txid']
                # All we know here is that the end of this batch is not all that's on the same version.
                # We might be going through what was an XIP that committed and changed a lot, so we
                # make sure that xid_next does not go backwards. 
                xid_next = xid_at + 1
                cursor_type = 'partial'
            else:
                # The first item of the next batch has a higher version than the last item of this batch, so
                # we know we're done with everything for that version. Thus, we can continue based on version
                # progress.
                assert last_of_this_batch['last_modified_txid'] < peek_at_next_batch['last_modified_txid']
                xid_next = peek_at_next_batch['last_modified_txid']
                cursor_type = 'scroll'

        # xid_next should never go backwards. The last item in a batch might have been contributed by
        # an old xip (which could in turn have become an xid_at)
        xid_next=max(self.xid_next, xid_next)

        # Now to figure out which XIPs we need to keep track of
        if rows[-1]['priority'] == Priority.SCROLL:
            # We got past xid_at and any committed xips, so anything else to carry forward must
            # be active (i.e. not yet committed/aborted) and less than where we scroll from next,
            # or we'll just scroll past it.
            xip_list = [xip for xip in snapshot["xip_list"] if xip < xid_next]
        else:
            # We're in the middle of a version with a lot of changes or handling of old xips.
            # So bring forward any old xips, as we haven't gotten to or past them yet. We also
            # need to consider active xips here (that weren't part of xips we brought forward),
            # because xid_next might have progressed past them at this point.
            xip_list = sorted([
                xip for xip in
                (set(self.xip_list) | set(snapshot["xip_list"])) - set(
                    # Anything we've seen in this batch we don't need to carry forward. We might not have
                    # been able to process all within that version, but that'll turn into an 
                    # xid_at+seq if so, and we'll scroll through them that way
                    r["last_modified_txid"] for r in rows
                )
                # Keep any xips that we're not going to based on comparing to >= xid_next
                if xip < xid_next
            ])

        self._previous = None # or we'd memleak as we'd keep the chain forever
        return type(self)(
            self.batch_size, xid_next=xid_next, xip_list=xip_list,
            xid_at=xid_at, xid_at_id=xid_at_id,
            additional_froms=self.additional_froms, additional_wheres=self.additional_wheres,
            type=cursor_type, previous=self)


class CursorSchema(Schema):
    # offset - we should not need offset, since all xids have them baked in.
    batch_size = fields.Integer(required=True)

    # Where are we currently at?
    xid_at = fields.Integer(allow_none=True)

    # If provided, then we're not done with everything at xid_at, but scrolling through
    # I.e we couldn't fit everything within batch size
    xid_at_id = fields.Integer(allow_none=True)

    # If provided, then there are transacions less than xid_next that we need to do
    # when we're done with xid_at, and before we go to xid_next.
    # When done with xid_at, we move through the lowest ids in xip-list that are committed.
    # Presumably, xip_list can never be longer than the max allowed number of connections to
    # Postgres. (Or maybe it can with prepared transactions? We should not be using them anyway)
    xip_list = fields.List(fields.Integer(), allow_none=True)

    # We know we have done everything except what's in xip-list up to this version    
    xid_next = fields.Integer(allow_none=True)

    @validates_schema
    def validate(self, data, **kw):
        if not 1 <= data['batch_size'] <= 10000:
            raise ValidationError("'batch_size' must be [1, 10000]")

        if data['xid_at'] and not(data.get('xid_next') and data.get('xid_at_id')):
            raise ValidationError("if xid_at is provided, both xid_next and xid_at_id must be provided too")

    @marshmallow.post_load
    def make_token(self, data, **kw):
        return Cursor(**data)

    # TODO: version and a "do not modify-hint", fernet has timestamp
