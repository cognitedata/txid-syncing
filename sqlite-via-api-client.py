#!/usr/bin/env python3
import requests, json, time, sqlite3

s = requests.Session()

sqlite_conn = sqlite3.connect('synced-test.db')

sqlite_conn.execute("create table if not exists synced(id bigint primary key, name text, version int, last_modified_txid bigint, data text)")
sqlite_conn.execute("create table if not exists cursors(id int primary key, cursor text)")

cursor = None
cur = sqlite_conn.cursor()
cur.execute("select cursor from cursors where id=1")
r = cur.fetchone()
if r:
    cursor = r[0]
sqlite_conn.rollback()

while True:
    r = s.post("http://localhost:8080/api/scan", json={"cursor": cursor}).json()
    cursor, rows = r["cursor"], r["rows"]
    cur = sqlite_conn.cursor()
    cur.executemany("replace into synced(id, name, version, last_modified_txid, data) values (?, ?, ?, ?, ?)", [
        (row['id'], row['name'], row['version'], row['last_modified_txid'], json.dumps(row['data']))
        for row in rows
    ])
    cur.execute("replace into cursors(id, cursor) values (?, ?)", (1, cursor))
    sqlite_conn.commit()
    if not rows:
        print(".")
        time.sleep(.2)



