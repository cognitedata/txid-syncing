#!/usr/bin/env python3
import os
import aiohttp
import asyncio
from aiohttp.http_websocket import WSMsgType
import asyncpg
import json
import collections
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


class SocketManager:

    def __init__(self, app):
        self.app = app
        self.pool = app["pool"]
        self.cohort_for_cursor = dict()
        self.is_running = True
        self.sockets = set()

        # TODO: enforce
        self.max_connections = 1000
        self.max_cohorts = 100
        self.max_backlog_per_cohort = 40*1000

    def new_socket(self, request):
        s = Socket(request)
        self.sockets.add(s)
        return s

    def socket_closed(self, socket):
        for cursor, cohort in list(self.cohort_for_cursor.items()):
            cohort.sockets.discard(socket)
            if not cohort.sockets:
                del self.cohort_for_cursor[cursor]

    def socket_progressed(self, socket, cursor):
        previous_cursor = socket.cursor
        if previous_cursor:
            # Merging of cohorts is handled by the cohort as it's getting
            # new rows and monitoring cursor progress
            print('Socket had previous', socket, previous_cursor)
            return

        # Socket just got its first cursor. Either join an existing cohort,
        # or create a new one
        cohort = self.cohort_for_cursor.get(cursor)
        if cohort:
            cohort.sockets.add(socket)
        else:
            self.cohort_for_cursor[cursor] = cohort = Cohort(self, cursor)
            cohort.sockets.add(socket)

            asyncio.get_event_loop().create_task(
                cohort.run_forever()
            )

    def remove_cohort(self, cohort):
        assert not cohort.sockets
        self.sockets.discard(cohort)

    def cohort_progressed(self, cohort, new_cursor):
        print('New cursor', new_cursor, id(cohort))

        print(self.cohort_for_cursor)

        other_cohort = self.cohort_for_cursor.get(new_cursor)

        if other_cohort and other_cohort is not cohort:
            print('Merging', cohort, 'with', other_cohort, new_cursor)
            other_cohort.sockets.update(cohort.sockets)
            cohort.sockets.clear()
            del self.cohort_for_cursor[cohort.cursor]
        else:
            print('nothing to merge with')
            self.cohort_for_cursor.pop(cohort.cursor)
            cohort.cursor = new_cursor
            self.cohort_for_cursor[new_cursor] = cohort



i = 0
# TODO: Wrap cohort too, so cohorts can share a connection if they're all caught up and in listen mode
class Cohort:

    def __init__(self, manager, cursor):
        self.manager = manager
        self.cursor = cursor
        self.sockets = set()
        global i
        self.i = i
        i += 1

    async def run_forever(self):
        manager = self.manager
        print('New cohort starting', self, self.cursor)
        while self.sockets:

            async with self.manager.pool.acquire() as conn:
                next_cursor, rows = await fetch_from(conn, self.cursor)

                if rows:
                    # Distribute the changes to all the sockets
                    await asyncio.gather(*[
                        socket.queue.put( (next_cursor, rows) )
                        for socket in self.sockets
                    ])

                # This could merge us with another cohort if we've caught up and want the same changes
                # as someone else
                manager.cohort_progressed(self, next_cursor)

            await asyncio.sleep(1)
        print('This cohort is done', self)

    def remove_socket(self, socket):
        self.sockets.discard(socket)
        if not self.sockets:
            self.manager.remove_cohort(self)

class Socket:

    def __init__(self, request):
        self.request = request
        self.manager = request.app["socket_manager"]
        self.websocket = web.WebSocketResponse()
        self.is_listening = False
        self.cursor = None
        self.queue = asyncio.Queue()
        self.is_running = False

    async def prepare(self):
        await self.websocket.prepare(self.request)

    def progress_to_cursor(self, cursor):
        self.manager.socket_progressed(self, cursor)
        self.cursor = cursor

    async def run_until_closed(self):
        self.is_running = True
        wait_for_socket = None
        wait_for_row_batch = None
        while self.manager.is_running and self.is_running:
            wait_for_socket = wait_for_socket or asyncio.create_task(self.websocket.receive())
            wait_for_row_batch = wait_for_row_batch or asyncio.create_task(self.queue.get())

            try:
                done, pending = await asyncio.wait([wait_for_socket, wait_for_row_batch], return_when=asyncio.FIRST_COMPLETED)
            except RuntimeError:
                # If the websocket abruptly closes, this might be raised, which isn't a great exception, but oh well
                self.manager.socket_closed(self)
                return self.websocket

            for task in done:
                if task is wait_for_socket:
                    wait_for_socket = None
                    msg = await task

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data == 'close':
                            await self.websocket.close()
                            self.is_running = False
                        elif msg.data.startswith("cursor:"):
                            # This should probably only ever happen once per socket,
                            # but it's not costing us anything to allow re-cursoring
                            token = msg.data[len("cursor:"):]

                            self.progress_to_cursor(Cursor.from_token(token.encode("utf8")))

                    else:
                        print('ws connection closed with exception %s' % self.websocket.exception())
                        self.is_running = False
                elif task is wait_for_row_batch:
                    wait_for_row_batch = None
                    next_cursor, rows = await task
                    if rows:
                        await self.websocket.send_str(json.dumps({
                            "cursor": next_cursor.to_token(),
                            "rows": rows
                        }))
                        print('Shipped rows', len(rows))
                    self.progress_to_cursor(next_cursor)

        print('websocket connection closed')            
        self.manager.socket_closed(self)

        return self.websocket


@routes.get('/api/listen')
async def websocket_handler(request):
    socket = request.app["socket_manager"].new_socket(request)
    await socket.prepare()

    return (await socket.run_until_closed())


async def init():
    pool = await asyncpg.create_pool(os.environ.get("POSTGRES_CONFIG", "postgresql://@/sync-test?application_name=sync-api&client_encoding=utf-8"))
    app = web.Application()
    app["pool"] = pool
    socket_manager = app["socket_manager"] = SocketManager(app)
    app.add_routes(routes)

    loop = asyncio.get_event_loop()

    return app

web.run_app(init())

