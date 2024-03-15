#!/usr/bin/env python3
import requests, json, time, sqlite3

s = requests.Session()

sqlite_conn = sqlite3.connect("synced-test.db")

# Ensure the schema exists
sqlite_conn.execute(
    "create table if not exists synced(id bigint primary key, name text, version int, last_modified_txid bigint, data text)"
)
sqlite_conn.execute("create table if not exists cursors(id int primary key, cursor text)")

# Load any existing cursors when we restart
cursor = None
cur = sqlite_conn.cursor()
cur.execute("select cursor from cursors where id=1")
r = cur.fetchone()
if r:
    cursor = r[0]
sqlite_conn.rollback()

# Keep polling for changes, making sure to persist the cursor atomically to saving the changes
while True:
    r = s.post("http://localhost:8080/api/events", json={"cursor": cursor})
    r.raise_for_status()
    data = r.json()
    cursor, rows = data["cursor"], data["events"]
    cur = sqlite_conn.cursor()
    cur.executemany(
        "replace into synced(id, name, version, last_modified_txid, data) values (?, ?, ?, ?, ?)",
        [(row["id"], row["name"], row["version"], row["last_modified_txid"], json.dumps(row["data"])) for row in rows],
    )
    cur.execute("replace into cursors(id, cursor) values (?, ?)", (1, cursor))
    sqlite_conn.commit()
    if not rows:
        time.sleep(0.4)
    else:
        print(f"Synced {len(rows)} rows")
