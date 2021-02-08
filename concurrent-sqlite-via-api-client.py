#!/usr/bin/env python3
import aiohttp, json, aiosqlite, asyncio, sys


class Coordinator:

    def __init__(self, loop, number_of_workers=4):
        self.loop = loop
        self.workers = []
        self.number_of_workers = number_of_workers
        self._queue = []
        self.is_running = False

    def queue_batch(self, rows, worker):
        d = self.loop.create_future()
        self._queue.append((rows, worker, d))
        return d

    async def run_forever(self):
        self.is_running = True
        async with aiosqlite.connect('synced-test.db') as db:
            await db.execute("create table if not exists synced(id bigint primary key, name text, version int, last_modified_txid bigint, data text)")
            await db.execute("create table if not exists cursors(id int primary key, cursor text)")
            await db.commit()

            cursor_for_worker = {}
            async with db.execute("select id, cursor from cursors") as cur:
                async for row in cur:
                    cursor_for_worker[int(row[0])] = row[1]

            for i in range(self.number_of_workers):
                cursor = cursor_for_worker.get(i)

                worker = Worker(i, cursor, coordinator=self)
                self.workers.append(worker)
                self.loop.create_task(worker.run_forever())

            while self.is_running:
                if self._queue:
                    # Process anything queued up so far by any of the workers.
                    # They'll be blocked waiting on their respective future
                    queue, self._queue = self._queue, []
                else:
                    # Hacky, something not a toy would presumably use a better
                    # asyncio queuing primitive
                    await asyncio.sleep(.1)
                    continue

                async with db.cursor() as cur:
                    flushed = 0
                    for rows, worker, _ in queue:
                        flushed += len(rows)
                        await cur.executemany("replace into synced(id, name, version, last_modified_txid, data) values (?, ?, ?, ?, ?)", [
                            (row['id'], row['name'], row['version'], row['last_modified_txid'], json.dumps(row['data']))
                            for row in rows
                        ])
                        await cur.execute("replace into cursors(id, cursor) values (?, ?)", (worker.id, worker.cursor))

                    await db.commit()
                    print('Committed rows', flushed)

                    # Let workers continue
                    for _, _, d in queue:
                        d.set_result(True)


class Worker:

    def __init__(self, id, cursor, coordinator):
        self.id = id
        self.cursor = cursor
        self.coordinator = coordinator

    async def run_forever(self):
        async with aiohttp.ClientSession() as session:
            print('Worker running', self.id)

            while self.coordinator.is_running:
                if self.cursor:
                    request = {"cursor": self.cursor}
                else:
                    request = {"number_of_partitions": self.coordinator.number_of_workers, "partition_id": self.id}

                response = await (await session.post("http://localhost:8080/api/scan", data=json.dumps(request))).json()

                self.cursor = response["cursor"]
                await self.coordinator.queue_batch(response["rows"], self)
                await asyncio.sleep(.2)


async def main(loop):
    number_of_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    c = Coordinator(loop, number_of_workers)
    await c.run_forever()


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main(loop))
except KeyboardInterrupt:
    pass
