#!/usr/bin/env python3
import asyncio, datetime, os
import aiopg

from psycopg2.extras import RealDictCursor

from txid_sync.cursor import Cursor


import time, random
crash_after = time.time() + (120 * random.random())
def maybe_crash():
    # Force the occasional crash, which over time should occur a bit of everywhere.
    # Just used while "fuzzing" for concurrency bugs and not intended (as is, but proper
    # chaos engineering should be able to target us) as is.
    if time.time() > crash_after:
        if random.random() < .2:
            print('Suddenly the Dungeon collapses!! - You die...')
            # Very rudely die, not allowing anything to clean up or finish whatever it's doing
            # (which sys.exit might)
            import os
            os.kill(os.getpid(), 9)


class SyncClient:

    def __init__(self, postgres_config, batch_size=1000, initial_cursor=None):
        self._postgres_config = postgres_config
        self._current_cursor = initial_cursor or Cursor.empty(batch_size)
        self._connection = None
        self.batch_size = batch_size

        self._keep_running = False
        self._is_listening = False
        self.indexed = 0
        self.errors = 0

    async def connect(self):
        self._connection = await aiopg.connect(self._postgres_config)

    async def _listen_to_changes(self):
        if self._is_listening:
            return

        async with self._connection.cursor() as cur:
            await cur.execute("LISTEN table_changed")
            self._is_listening = True

    def pg_cursor(self):
        return self._connection.cursor(cursor_factory=RealDictCursor)

    async def _maybe_unlisten_to_changes(self):
        if not self._is_listening:
            return

        async with self._connection.cursor() as cur:
            await cur.execute("UNLISTEN table_changed")
            self._is_listening = False

    async def initialise(self):
        async with self.pg_cursor() as cur:
            # In case the database was just pg_restore-d
            await cur.execute("select configure_txid_offset()")

    async def run_forever(self):
        if not self._connection:
            await self.connect()

        await self.initialise()

        self._keep_running = True
        try:
            while self._keep_running:
                maybe_crash()
                    
                # If we're listening, stop listening, in case we're about to get a big batch, so
                # the notify queue doesn't grow. 
                await self._maybe_unlisten_to_changes()

                # Process everything from the initial cursor, until we're fully caught up.
                await self.process_batches()
                maybe_crash()

                # When we're caught up, start listening.
                await self._listen_to_changes()
                maybe_crash()

                # Some more changes might have happened in the brief window we weren't listening, so
                # double check, or we're not going to catch them until something else changes. It's
                # possible that this very brief window happens to be when an update touching everything
                # completes, but that's unlikely, and Postgres will handle that by spilling to disk
                # if required. Furthermore, the unique channel+payload combos is kept low.
                # This process batch call will probably have 0 or very few items most of the time.
                await self.process_batches()
                maybe_crash()

                # Wait for a notification. We started listening before we checked the second time,
                # and notifications are delivered between transactions.
                while self._keep_running:
                    try:
                        msg = await asyncio.wait_for(self._connection.notifies.get(), timeout=10)
                    except asyncio.TimeoutError:
                        # This allows for either picking up changes that for some reason didn't
                        # cause a notify (which a trigger should do), or to cheaply advance a cursor
                        # over a longer period of no changes.
                        break
                    else:
                        if msg.channel == 'table_changed' and msg.payload == 'version_info':
                            # Something might have changed, so stop waiting for now.
                            # The notification says nothing about what or whether something change,
                            # just that there's a possibility. The cursor we hold knows how to get
                            # at whatever might have changed

                            # If a bunch of notifies made it, clear them, as one is enough to make us
                            # look, and we're going to see the result of all the changes anyway.
                            # The first thing we do before starting on a (potentially very large) batch
                            # is to unlisten, so we make it less likely we get a lot of redundant pending
                            # notifications.
                            self._connection.notifies._queue.clear()
                            break

        except KeyboardInterrupt:
            pass
        finally:
            await self._connection.close()

    def stop(self):
        self._keep_running = False

    def get_sql_for_batch(self):
        return "{} select * from changes".format(self._current_cursor.get_changes_cte())

    async def process_batches(self):
        while True:
            async with self.pg_cursor() as cur:
                # Every batch is up to `batch_size` items.
                # A batch fetch is a single read-only transaction in repeatable read mode.
                # It's in repeatable read, because we want the same MVCC snapshot that we determine

                t = await cur.begin()
                # TODO: while aiopg defaults to repeatable read, we should probably set it explicitly,
                # as it's an important assumption
                assert 'Repeatable' in str(t._isolation)

                # The current cursor, the snapshot, and the last row will determine what the next
                # cursor will look like. With repeatable read, this snapshot will be the same
                # that's in use in the next query. (With a default read committed every statement
                # has its own snapshot)
                await cur.execute("select adjusted_txid_snapshot_as_json()")
                snapshot = (await cur.fetchone())['adjusted_txid_snapshot_as_json']

                # We get the batch with the same snapshot that we now have a copy of.
                sql = self.get_sql_for_batch()
                await cur.execute(sql)
                rows = await cur.fetchall()

                # End the transaction as early as we can. We have everything we need, and what follows
                # might involve external systems that might block forever for all we know.
                await t.rollback()

                if rows:
                    # We might have batch_size + 1 rows, this makes sure we don't process the peek
                    # of the first row of the next batch
                    await self.process_batch(rows[:self._current_cursor.batch_size])

                maybe_crash()

                # _current_cursor got us the just-processed batch. To pick up from here,
                # continue from the advanced cursor. (So we can save it prior to using it)
                # If we crash right after saving it, we'll pick up from where we left.
                # If we crash prior to saving it, we'll redo whatever we just processed.
                # Should the client change the batch size, that'll take effect for new
                # cursors
                self._current_cursor = self._current_cursor.advance(snapshot, rows, new_batch_size=self.batch_size)
                await self.save_cursor()

                maybe_crash()

                if not rows:
                    return

    async def save_cursor(self):
        t = self._current_cursor.to_token()
        open('cursor.token', 'w').write(t)                
        print('Saved token', self._current_cursor)

    async def process_batch(self, batch):
        """ Do something with the batch of objects to handle.

        This handler needs to do its own retries and reason about how to achieve
        atomicity if external systems are involved. If it returns, the cursor will
        progress, which means we consider everything in this batch to have been properly
        dealt with.

        There is no way to indicate that a single item failed. If you have a permanent
        error for a single object in the batch, you'll probably want to track that somewhere,
        *before* returning here.

        Exception raised here will cause the entire batch to be re-tried, and the cursor
        will never progress past this batch if it keeps raising.
        """
        print('Would process batch of ', len(batch))
        pass


# Example ES client follows. It's missing a lot of stuff, but should give the general idea

import elasticsearch
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk


class ElasticsearchSyncClient(SyncClient):

    def __init__(self, *a, **kw):
        super(ElasticsearchSyncClient, self).__init__(*a, **kw)
        self.es_client = AsyncElasticsearch()

        self.index_name = kw.pop("es_index_name", "some-index")
        self.cursor_doc_id = kw.pop("cursor_doc_id", "_internal_cursor")

        # Don't necessarily save the cursor for every batch, since it's an operation we serialize on
        # Saving it all the time would slow down bulk indexing. Not saving it means we do a little
        # bit of extra work when resuming, which is fine. We also piggyback the previous cursor into
        # every bulk, so duplicate work on crash should be very manageable
        self.cursor_save_delay_in_seconds = 10
        self.previous_cursor_saved = datetime.datetime.utcfromtimestamp(0)
        self._persistable_cursor = None

        self.indexed = 0
        self.errors = 0

    def get_sql_for_batch(self):
        changes_cte = self._current_cursor.get_changes_cte()
        # This is just a stub, but we'd put together the object graph here. In a property graph
        # implementation, this could be a query generated from a graphql query, which is a pretty
        # reasonable way of expressing how to put together a desired object graph.
        # The lateral is pointless here, but would be useful if making a json object graph in one go
        return changes_cte + """

        select changes.*, json_strip_nulls(json_build_object('data', data)) as data
        from changes
        left join lateral (
            select * from some_table
            --left join metadata using(id)
            where id=changes.id
        ) as data on(true)
        order by last_modified_txid asc, id asc     
        """.format(changes_cte)

    async def process_batch(self, batch):
        if not batch:
            return

        maybe_crash()
        print('Got docs to index', len(batch))

        bulk = [
            {
                "_index": self.index_name,
                "_id": doc["id"],
                "_version": doc["version"],
                "_version_type": "external",
                "id": doc["id"],
                "doc_version": doc["version"],
                "txid": doc["last_modified_txid"],
                **doc["data"]
            }
            for doc in batch
        ]
        if self._persistable_cursor:
            # This will have been the cursor for the _previous_ batch. We piggy back on this bulk
            # request to update the cursor. (The update is cheap,)
            bulk.append({
                "_index": self.index_name,
                "_id": self.cursor_doc_id,
                "cursor_token": self._current_cursor.to_token(),
                "timestamp": datetime.datetime.utcnow().isoformat()
            })

        # We're NOT saving the cursor in this bulk. We need to assess the errors first.
        _, errors = await async_bulk(self.es_client, bulk, chunk_size=self.batch_size, raise_on_error=False)

        maybe_crash()

        for error in errors:
            # version_conflict_engine_exception are not a problem, as it means the document
            # we sent is either already indexed (most likely) or a newer version exists (should
            # not happen if everything goes through us) Documents indexed prior to persisting 
            # the cursor will cause this herror if a crash+restart happens after indexing of the
            # docs, but before the persisting of the cursor
            if error['index']['error']['type'] != 'version_conflict_engine_exception':
                # TODO: Proper retrying if the queue is full (back pressure), indexing if it's a
                # mapping error (no amount of retrying will fix it), etc. would happen here
                print('***', error)
                raise NotImplementedError() # We're a POC for now

        self._persistable_cursor = self._current_cursor.to_token()

        self.errors += len(errors)
        self.indexed += len(batch) - len(errors)

        if self.indexed:
            print('Errors/Indexed/%: ', self.errors, self.indexed, self.errors * 100.0/self.indexed)

        if errors:
            print('There were multiple index operations for the same doc+version combo', len(errors), errors[0])

    async def save_cursor(self):
        if (datetime.datetime.utcnow() - self.previous_cursor_saved).total_seconds() < self.cursor_save_delay_in_seconds:
            # We recently updated the saved cursor, so don't do it again yet
            # 
            return

        maybe_crash()

        now = datetime.datetime.utcnow()
        await self.es_client.index(self.index_name, {
            "cursor_token": self._current_cursor.to_token(),
            "timestamp": now.isoformat()
        }, id=self.cursor_doc_id)
        self.previous_cursor_saved = now
        print("Persisted {} at {}".format(self._current_cursor, now))

    async def initialise(self):
        await super(ElasticsearchSyncClient, self).initialise()
        try:
            doc = await self.es_client.get(self.index_name, self.cursor_doc_id, realtime=True)
        except elasticsearch.NotFoundError:
            print("Starting from scratch")
            additional_froms=['left outer join node using(node_id)', 'left outer join edge using(edge_id)']
            additional_wheres=['node.project_id IN (1,2) OR edge.project_id IN (1,2)']
            # node and edge not part of this demo
            self._current_cursor = Cursor.empty(batch_size=self.batch_size) # additional_froms=additional_froms, additional_wheres=additional_wheres)
        else:
            self._current_cursor = Cursor.from_token(doc["_source"]["cursor_token"].encode("utf8"))
            print("Resuming from", self._current_cursor)


dsn = os.environ.get("POSTGRES_CONFIG", "dbname=sync-test fallback_application_name=pysync client_encoding=utf-8")
async def main():
    c = ElasticsearchSyncClient(dsn, batch_size=1000*10)
    import os
    print('Starting up', os.getpid())
    await c.run_forever()



loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    pass