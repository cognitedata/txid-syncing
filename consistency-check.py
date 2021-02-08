#!/usr/bin/env python3
"""
Dump all the docs/rows in Elasticsearch/Postgres and diff their respective
(external_id, txid, version) tuples. Ideally they're the same.

This is not expected to be necessary in a production setting, but is useful
to sanity check what the syncing process has done, especially after subjecting
it to the load generator for a while.
"""

import os, sys
import psycopg2
import sqlite3
import elasticsearch
from elasticsearch.helpers import scan

pg_view = {}

dsn = os.environ.get("POSTGRES_CONFIG", "dbname=sync-test fallback_application_name=consistency-checker client_encoding=utf-8")
conn = psycopg2.connect(dsn)
with conn.cursor() as cur:
    cur.execute("select id::text, last_modified_txid, version from version_info join some_table using(id)")
    for row in cur:
        external_id, txid, version = row
        pg_view[external_id] = txid, version
conn.close()



if 'es' in sys.argv or len(sys.argv) == 1:
    es = elasticsearch.Elasticsearch()
    es_view = {}

    i = 0
    for row in scan(es, {"query": {"exists": {"field": "doc_version"}}, "fields": ["doc_version", "txid"], "_source": False}, index="some-index", size=10*1000):
        es_view[row["_id"]] = row["fields"]["txid"][0], row["fields"]["doc_version"][0]
        i += 1
        if not (i % 100000):
            print(i)

    diff = sorted(set(pg_view.items()) ^ set(es_view.items()))

    if diff:
        print("Elasticsearch: 🔥 There were inconsistencies 😮")
        print(diff)
    else:
        print("Elasticsearch in sync 🎉")

if 'sqlite' in sys.argv or len(sys.argv) == 1:
    sqlite_conn = sqlite3.connect('synced-test.db')
    cur = sqlite_conn.cursor()
    cur.execute("select cast(id as text), last_modified_txid, version from synced")

    sqlite_view = {}
    for row in cur:
        external_id, txid, version = row
        sqlite_view[external_id] = txid, version

    diff = sorted(set(pg_view.items()) ^ set(sqlite_view.items()))

    if diff:
        print("SQLite: 🔥 There were inconsistencies 😮")
        print(diff)
    else:
        print("SQLite in sync 🎉")
