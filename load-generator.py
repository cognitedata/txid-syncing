#!/usr/bin/env python3
import asyncio
import aiopg
import random, os
import psycopg2
import time


if os.environ.get("RANDOM_SEED"):
    random.seed(int(os.environ["RANDOM_SEED"]))

import sys

dsn = os.environ.get("POSTGRES_CONFIG", "dbname=sync-test fallback_application_name=load-generator client_encoding=utf-8")

class LoadGenerator:

    def __init__(self, num, total):
        self.num = num
        self.total = total

    async def run_forever(self):
        conn = await aiopg.connect(dsn)

        # Smear out startup a bit
        await asyncio.sleep(self.total * .2 * random.random())

        async with conn.cursor() as cur:
            await cur.execute("select configure_txid_offset()")

            # This just gets an estimate, which is fine and doesn't require scanning the table as count(*) will
            await cur.execute("select n_live_tup from pg_stat_user_tables where relname='version_info'")
            number_of_nodes = (await cur.fetchone())[0]

            if number_of_nodes == 0:
                print("Postgres stats think there aren't any nodes. Maybe VACUUM? Pretending it's 30 000")
                number_of_nodes = 30*1000

        # things are called "node" as this was pulled out of a property graph example
        number_of_nodes_to_change = 10
        hot_nodes = 100
        hot_node_probability = 0.01
        last_status_report = time.time()

        while True:

            async with conn.cursor() as cur:
                try:
                    t = await cur.begin()

                    already_updated = set()
                    for _ in range(number_of_nodes_to_change):
                        id_to_change = random.randrange(1, number_of_nodes)

                        # Occasionally hit the same small pool of rows, to make
                        # it very likely that different concurrent writes hit the same
                        # rows. Out of order commits to the same rows is the most important
                        # case to test, since if everything is updating different stuff, then
                        # we can more likely pass the consistency check even though we shouldn't
                        if random.random() < hot_node_probability:
                            id_to_change = random.randrange(1, hot_nodes)

                        if id_to_change in already_updated:
                            continue
                        already_updated.add(id_to_change)

                        await cur.execute("update some_table set name='Generator was here - {}' where id={}".format(random.random(), id_to_change))

                        if random.random() < .1:
                            # Occasionally create new nodes too
                            await cur.execute("""
                            with new_node as (
                                insert into some_table (name, data)
                                values ('generated-' || random(), json_build_object('random', random()))
                                returning id
                            )
                            insert into version_info (id) select id from new_node
                            """)                        

                        # Brief sleep, cause a somewhat long-running transaction in total
                        await asyncio.sleep(random.random() * .1)

                    if random.random() < 0.005:
                        # Occasionally update a lot more than batch size number of things
                        up_to = id_to_change + 10000 * ( 1 + random.random())
                        print('Doing big batch')
                        await cur.execute("update some_table set name='Generator was here - {}' where id between {} and {}".format(random.random(), id_to_change, up_to))

                    if random.random() < .01:
                        # Occasionally do a long sleep, while keeping the transaction going, to get the occasional
                        # quite long running transaction with changes
                        await asyncio.sleep(random.random() * 10)

                    await t.commit()

                    if self.num == 0 and (time.time() - last_status_report) > 1:
                        await cur.execute("select state, count(*) from pg_stat_activity where application_name ='load-generator' group by 1 order by 1")
                        async for row in cur:
                            state, count = row
                            print("{}\t\t{}".format(state.rjust(32), count))

                # Generating these errors is a big part of the purpose of this load generator
                except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected):
                    await t.rollback()



async def main():
    number_of_connections = int(sys.argv[1]) if len(sys.argv) > 1 else 32
    print(number_of_connections)
    lgs = [LoadGenerator(i, number_of_connections).run_forever() for i in range(number_of_connections)]
    await asyncio.gather(*lgs)


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    pass
