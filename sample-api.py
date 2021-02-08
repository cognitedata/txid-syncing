#!/usr/bin/env python3
import os
import aiohttp
import asyncpg
import json
from aiohttp import web

from txid_sync.cursor import Cursor


routes = web.RouteTableDef()


async def fetch_from(conn: asyncpg.connection.Connection, positional_cursor: Cursor):
    t = conn.transaction(isolation="repeatable_read")
    await t.start()
    snapshot = json.loads( (await conn.fetchval("select adjusted_txid_snapshot_as_json()")) )

    sql = positional_cursor.get_changes_cte() + """
    select changes.*, to_jsonb(some_table) || to_jsonb(changes) - ARRAY['priority'] as data 
    from changes
    join some_table using(id)
    """

    rows = await conn.fetch(sql)

    # End the transaction as early as we can. We have everything we need, and what follows
    # might involve external systems that might block forever for all we know.
    await t.rollback()

    return positional_cursor.advance(snapshot, rows), [
        json.loads(row["data"]) for row in rows[:positional_cursor.batch_size]
    ]


@routes.get('/')
async def root(request):
    return web.Response(text="🤷🏻‍♂️")


@routes.post('/api/scan')
async def scan(request):
    body = (await request.json())

    if body.get("cursor") is None:
        assert (body.get("number_of_partitions") is None) == (body.get("partition_id") is None)
        number_of_partitions = int(body.get("number_of_partitions", 1))
        partition_id = int(body.get("partition_id", 0))
        cursor = Cursor.empty(partition_id=partition_id, number_of_partitions=number_of_partitions)
    else:
        try:
            cursor = Cursor.from_token(body["cursor"].encode("utf-8"))
        except Exception as e:
            raise web.HTTPBadRequest(reason="your cursor, I hates it, grr")

    if body.get("token_only", False):
        # To make a token to use via sockets
        return web.json_response({
            "cursor": cursor.to_token()
        })        

    async with request.app["pool"].acquire() as conn:
        next_cursor, rows = await fetch_from(conn, cursor)
        print("Got {} rows from {}. Next: {}".format(len(rows), cursor, next_cursor))

    return web.json_response({
        "cursor": next_cursor.to_token(),
        "rows": rows
    })

async def init():
    pool = await asyncpg.create_pool(os.environ.get("POSTGRES_CONFIG", "postgresql://@/sync-test?application_name=sync-api&client_encoding=utf-8"))
    app = web.Application()
    app["pool"] = pool
    app.add_routes(routes)
    return app

web.run_app(init())

