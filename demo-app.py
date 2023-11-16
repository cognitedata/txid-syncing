# Copyright 2022. Cognite AS.
# This is the MIT license:
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import json
import os
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from hashlib import sha256
from typing import List, Optional, Tuple, TypeVar, cast

import asyncpg
from aiohttp import web
from cryptography.fernet import Fernet, InvalidSignature, InvalidToken  # type: ignore
from lz4.frame import compress, decompress, get_frame_info  # type: ignore
from orjson import dumps, loads
from pydantic import BaseModel, Field, validator
from pydantic.types import ConstrainedInt

BATCH_SIZE_MAX = 2000


T = TypeVar("T")


def coalesce(*args: Optional[T]) -> Optional[T]:
    """Get the first non-None argument, if any, which may be falsy as long as it's not None

    >>> coalesce(0, 1, 2)
    0
    >>> coalesce(None, 0, 1)
    0
    >>> coalesce(None, None, 'foo')
    'foo'
    >>> coalesce(False, True)
    False
    """
    for el in args:
        if el is not None:
            return el
    return None


class BatchSize(ConstrainedInt):
    # As far as the syncing code is concerned, a batch of 1 works
    # We validate in `TableExpression` that more reasonable batch sizes are used
    ge = 1
    le = BATCH_SIZE_MAX


# TODO: Make this configurable, even though it's not a security-focused key.
default_cursor_key_not_for_security = urlsafe_b64encode(
    sha256(
        b"This key is not really for security, but to make cursors opaque and discourage direct editing by consumers"
    ).digest()
)

# Since the key isn't for security, it wouldn't be hard to make a decompression bomb,
# so bail if decompression would take excessive memory.
# In practice, we shouldn't see cursors beyond a few kilobytes
MAX_UNCOMPRESSED_CURSOR_SIZE = 4 * 1024 * 1024  # 4 MiB


class Cursor(BaseModel):
    version: int = 1

    batch_size: BatchSize = cast(BatchSize, BATCH_SIZE_MAX)

    # xid_at* get populated if the previous cursor produces a full batch, i.e.
    # number of results == batch_size
    # xid_at is the txid of the last row we saw, which since the batch was full,
    # we can't know if we're done with
    xid_at: Optional[int] = Field(allow_null=True)
    # ... and this is how far we got _within_ the txid of xid_at:
    xid_at_id: Optional[int] = Field(allow_null=True)

    # These two are always part of the cursor.
    # xip_list holds the transactions in progress, which may or may not produce
    # relevant results for future batches
    xip_list: List[int]
    # xid_next is where to continue from when done with the batch(es) produced
    # by this cursor
    xid_next: int

    # Watermark before which deleted are skipped. If empty, all deletes are skipped,
    # which should only be the case for the first cursor in a chain, i.e. one where
    # xid_next = 0
    xid_skip_deletes_before: Optional[int]

    # When was the cursor created? We don't use this today, but we need this to
    # be able to go "Hey, your cursor is actually so old that soft-deleted data
    # has been fully cleaned up, so your cursor isn't going to catch old data"
    issued_at: Optional[int]

    @validator("issued_at", pre=True, always=True)
    def set_issued_at(cls, v: int) -> int:
        return v or int(time.time())

    def to_opaque_cursor(self, key: bytes = default_cursor_key_not_for_security) -> "OpaqueCursor":
        return OpaqueCursor(urlsafe_b64encode(Fernet(key).encrypt(compress(dumps(self.dict())))))

    def progress_to(
        self,
        xid_next: int,
        xip_list: List[int],
        xid_at: Optional[int] = None,
        xid_at_id: Optional[int] = None,
        xid_skip_deletes_before: Optional[int] = None,
    ) -> "Cursor":
        # No point in holding on to XIPs we'll get to via >= xid_next anyway
        xip_list = [xip for xip in xip_list if xip < xid_next]
        xid_skip_deletes_before = coalesce(xid_skip_deletes_before, self.xid_skip_deletes_before)

        if xid_skip_deletes_before:
            # If everything is past xid_skip_deletes_before, we've synced past it, and will forever
            # need deletes. We set it to 0, which lets cursors converge when they catch up
            if (
                xid_skip_deletes_before < xid_next
                and (xid_at is None or xid_skip_deletes_before < xid_at)
                and all(xid_skip_deletes_before < xip for xip in xip_list)
            ):
                xid_skip_deletes_before = 0

        return Cursor(
            version=self.version,
            batch_size=self.batch_size,
            issued_at=int(time.time()),
            xid_next=xid_next,
            xid_at=xid_at,
            xid_at_id=xid_at_id,
            xip_list=xip_list,
            xid_skip_deletes_before=xid_skip_deletes_before,
        )


class Snapshot(BaseModel):
    """Wrap the MVCC snapshot used for the transactions involved with syncing queries.

    To learn more about MVCC:
        - [Postgres docs on concurrency control](https://www.postgresql.org/docs/current/mvcc-intro.html)
        - [MVCC in PostgreSQL — 4. Snapshots](https://postgrespro.com/blog/pgsql/5967899)
        - [MVCC unmasked](https://momjian.us/main/writings/pgsql/mvcc.pdf)
    """

    # xmin is the lowest visible txid
    xmin: int
    # ... while xmax is the highest
    xmax: int
    # ... and "xip" is "transactions in progress"
    xip_list: List[int]

    # In general, the modifications of a transaction is visible iff
    # xmin <= txid < xmax and txid not in xip_list


class CursorContext(BaseModel):
    """Combines information on a cursor and what it produced to make a _new_ cursor."""

    snapshot: Snapshot
    count: int
    batch_size: int
    last: Optional[Tuple[int, int]] = Field(allow_null=True)

    # We need to have the data on the cursor that produced the above page info
    previous_cursor: Cursor

    def to_progressed_cursor(self) -> Cursor:
        if self.previous_cursor.xid_next == 0:
            # We just got the first batch for this cursor.
            # Since it's the first batch, that cursor will have filtered away anything
            # soft-deleted. For any subsequent requests, we _may_ need to see soft-deleted
            # objects. However, the xmin as of this snapshot will be safe to ignore anything
            # deleted up until to. Anything deleted so far, we will not have seen. Anything
            # deleted between this snapshot and this xmin will have a last_modified_txid
            # higher than this threshold, so we'll get to it then
            xid_skip_deletes_before: Optional[int] = self.snapshot.xmin
        else:
            # For all subsequent cursors, the threshold will be the same
            xid_skip_deletes_before = self.previous_cursor.xid_skip_deletes_before

        if self.count < self.batch_size:
            # We did not fill the batch, so we know we're done with anything not
            # still in progress (as of the snapshot we got, of course)
            xid_next = self.snapshot.xmax
            if self.last:
                last_txid, _ = self.last
                if last_txid == xid_next:
                    # The last row we got happens to be the xmax, so xmax can't be some
                    # in-progress transaction, so skip past it so we don't get these
                    # rows again on the next >= comparison
                    xid_next += 1

            return self.previous_cursor.progress_to(
                xid_next=xid_next, xip_list=self.snapshot.xip_list, xid_skip_deletes_before=xid_skip_deletes_before
            )

        # Since we're still here, we know that we filled the batch:
        assert self.count == self.batch_size
        # ... which must have provided us with records on the last row:
        assert self.last

        # Since we filled the batch, we can't know whether we're done with the txid,
        # so just assume we're scrolling through the txid still. If we happened to
        # get the very last ones to fit perfectly in this batch, then the
        # first union arm will just yield 0 rows in the next go. The overhead of that
        # will be very small: it'll be a single index lookup on (last_modified_txid, seq)
        # to conclude we're done with the txid
        xid_at, xid_at_id = self.last

        # When we're done scrolling through seqs for this txid, then + 1 is where we'll
        # go next. Unless we've been brought back in time (via a xip_list), which the max further down
        # guards against
        xid_next = xid_at + 1

        # xid_next should never go backwards. The last item in a batch might have been contributed by
        # an old xip (which could in turn have become an xid_at)
        # This makes sure we can't be tricked back. If another old transaction has committed
        # and has changes now visible, we'll get to them via the xip_list check, not via
        # last_modified_txid >= xid_next
        xid_next = max(self.previous_cursor.xid_next, xid_next)

        if xid_at == self.snapshot.xmax:
            # xmax happens to be something we just saw.
            # Move past it, so we don't keep getting the same rows back until something else
            # progresses xmax.
            xid_next = xid_at + 1
        else:
            # Unless we just saw xmax ourselves, don't move past it. It could be another transaction
            # in progress.
            xid_next = min(xid_next, self.snapshot.xmax)

        if self.previous_cursor.xid_at == xid_at:
            # If xid_at did not move, then nothing from the xip_list will have been processed
            xips_to_keep = self.previous_cursor.xip_list
        else:
            # If it did move, then at least some xip_list txids have been involved, and we don't need to
            # retain anything less than xid_at, unless they're still in the snapshot, which
            # get union-ed below. Now-committed txids from xip_list are processed in order, and the
            # last one will become the new xid_at if we don't process all the changes.
            xips_to_keep = [xip for xip in self.previous_cursor.xip_list if xip > xid_at]

        xip_list = list(
            # We obviously have to carry forward what's still in the snapshot:
            set(self.snapshot.xip_list) | set(xips_to_keep)
        )

        return self.previous_cursor.progress_to(
            xid_next=xid_next,
            xid_at=xid_at,
            xid_at_id=xid_at_id,
            xip_list=xip_list,
            xid_skip_deletes_before=xid_skip_deletes_before,
        )

    def to_progressed_cursor_without_comments_and_optimizations(self) -> Cursor:
        if self.count < self.batch_size:
            xid_next = self.snapshot.xmax
            if self.last:
                last_txid, _ = self.last
                if last_txid == xid_next:
                    xid_next += 1

            return self.previous_cursor.progress_to(xid_next=xid_next, xip_list=self.snapshot.xip_list)

        assert self.last  # the batch is full, so there must be a last row
        xid_at, xid_at_id = self.last

        xid_next = max(self.previous_cursor.xid_next, xid_at + 1)

        if xid_at == self.snapshot.xmax:
            xid_next = xid_at + 1
        else:
            xid_next = min(xid_next, self.snapshot.xmax)

        return self.previous_cursor.progress_to(
            xid_next=xid_next, xid_at=xid_at, xid_at_id=xid_at_id, xip_list=self.snapshot.xip_list
        )


class OpaqueCursor(bytes):
    """An opaque cursor decodes into a `Cursor` via a list of encryption keys"""

    @staticmethod
    def _safe_decompress(lz4_compressed_frame: bytes, max_size_in_bytes: int) -> bytes:
        """Decompresses the input, unless it's larger than `max_size_in_bytes`.

        Useful if decompressing untrusted sources into memory, to avoid zip bombs.

        >>> from lz4.frame import compress
        >>> OpaqueCursor._safe_decompress(compress(b'1234'), max_size_in_bytes=4)
        b'1234'
        >>> OpaqueCursor._safe_decompress(compress(b'1234'), max_size_in_bytes=1)
        Traceback (most recent call last):
        ...
        ValueError: refusing to decompress [4] bytes
        """
        content_size_in_bytes = get_frame_info(lz4_compressed_frame)["content_size"]
        if content_size_in_bytes > max_size_in_bytes:
            raise ValueError(f"refusing to decompress [{content_size_in_bytes}] bytes")
        return decompress(lz4_compressed_frame)

    @classmethod
    def to_cursor(cls, opaque_cursor: bytes, possible_keys: Optional[List[bytes]] = None) -> Cursor:
        """Unpack the opaque cursor to a structured one.

        Args:
            possible_keys (List[bytes]): List of allowed keys. It's a list to allow key rotation without breaking existing cursors.

        Raises:
            ValueError: The provided cursor was garbage somehow

        Returns:
            Cursor
        """
        possible_keys = possible_keys or [default_cursor_key_not_for_security]
        for key in possible_keys:
            try:
                return Cursor.parse_obj(
                    loads(
                        cls._safe_decompress(
                            Fernet(key).decrypt(urlsafe_b64decode(opaque_cursor)), MAX_UNCOMPRESSED_CURSOR_SIZE
                        )
                    )
                )
            except (InvalidToken, InvalidSignature):
                # Unless the key is the last one, we just try more keys
                if key is possible_keys[-1]:
                    raise

        # This shouldn't really be possible, but just in case
        raise ValueError("invalid cursor")

    def __str__(self) -> str:
        return self.decode("utf-8")


routes = web.RouteTableDef()


async def fetch_from(conn: asyncpg.connection.Connection, cursor_for_next_batch: Cursor):
    # We need to make sure that the snapshot we read is the same as the one we fetch the
    # batch of changes with, so we do repeatable read to ensure a static snapshot
    t = conn.transaction(isolation="repeatable_read")
    await t.start()

    # Load the snapshot
    snapshot = json.loads((await conn.fetchval("select adjusted_txid_snapshot_as_json()")))

    # The arguments all come from the cursor
    args = (
        cursor_for_next_batch.xid_at,
        cursor_for_next_batch.xid_at_id,
        cursor_for_next_batch.xip_list,
        cursor_for_next_batch.xid_next,
    )

    # Fetch the batch
    rows = await conn.fetch(get_sql_for_query(cursor_for_next_batch.batch_size), *args)

    # Computing the next cursor might need the txid and id of the last row
    last_txid_and_id = None
    if rows:
        last_txid_and_id = (rows[-1]["last_modified_txid"], rows[-1]["id"])

    cursor_context = CursorContext(
        snapshot=Snapshot(**snapshot),
        count=len(rows),
        batch_size=cursor_for_next_batch.batch_size,
        last=last_txid_and_id,
        previous_cursor=cursor_for_next_batch,
    )
    next_cursor = cursor_context.to_progressed_cursor()

    await t.rollback()

    return next_cursor, [json.loads(row["data"]) for row in rows[: cursor_for_next_batch.batch_size]]


def get_sql_for_query(batch_size: int) -> str:
    # $1 = xid_at
    # $2 = xid_at_id
    # $3 = xip_list
    # $4 = xid_next
    return f"""
-- The "changes" table expression provide WHICH rows need syncing
with changes as (

    -- We need to wrap in a sub-select to be allowed to do a
    -- LIMIT and ORDER BY in a UNION branch
    select * from (
        select 1 as priority, last_modified_txid, id
        from version_info
        where last_modified_txid = $1 and id > $2
        order by last_modified_txid asc, id asc
        limit {batch_size}
    ) as _1

    union all

    -- The following UNION ALL branches are only executed
    -- if the preceeding branches do not fill the batch
    select * from (
        select 2 as priority, last_modified_txid, id
        from version_info
        where last_modified_txid = ANY($3)
        order by last_modified_txid asc, id asc
        limit {batch_size}
    ) as _2

    union all

    select * from (
        select 3, last_modified_txid, id
        from version_info
        where last_modified_txid >= $4
        order by last_modified_txid asc, id asc
        limit {batch_size}
    ) as _3

    -- This ORDER BY and LIMIT applies to the overall UNION ALL chain
    order by priority asc, last_modified_txid asc, id asc
    limit {batch_size}
)
-- With "changes" providing WHAT needs syncing, the following
-- assembles the data that we want to emit.
select changes.*, to_jsonb(version_info) || to_jsonb(some_table) || to_jsonb(changes) - ARRAY['priority'] as data
from changes
join some_table using(id)
join version_info using(id)
limit {batch_size}
"""


@routes.post("/api/events")
async def events(request):
    body = await request.json()

    if body.get("cursor") is None:
        # No cursor has been provided, so we're starting anew
        cursor = Cursor(xip_list=[], xid_next=0)
    else:
        try:
            cursor = OpaqueCursor.to_cursor(body["cursor"].encode("utf-8"))
        except Exception:
            raise web.HTTPBadRequest(reason="Invalid cursor")

    async with request.app["pool"].acquire() as conn:
        next_cursor, rows = await fetch_from(conn, cursor)
        print("Got {} rows from {}. Next: {}".format(len(rows), cursor, next_cursor))

    return web.json_response({"cursor": str(next_cursor.to_opaque_cursor()), "events": rows})


async def init():
    pool = await asyncpg.create_pool(
        os.environ.get("POSTGRES_CONFIG", "postgresql://@/sync-test?application_name=sync-api&client_encoding=utf-8")
    )
    app = web.Application()
    app["pool"] = pool
    app.add_routes(routes)
    return app


web.run_app(init())
