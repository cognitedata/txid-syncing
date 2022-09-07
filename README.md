# What?

This repository has examples of to reliably sync changes out of
Postgres and into external systems.

It supports the talk "Flexible change data capture with Postgres". A
link to a recording of that talk will be provided here when the
recording is available.

# Background material

This code assumes understanding a bit of Postgres' multi-version
concurrency control (MVCC) internals, particularly how snapshots and
visibility works.

The following material is worth checking out to learn more about MVCC:

- [MVCC unmasked by Bruce Momjian](https://www.youtube.com/watch?v=KVEfxb5lid8) ([slides](https://momjian.us/main/writings/pgsql/mvcc.pdf))
- [The Internals of PostgreSQL: Concurrency Control by Hironobu SUZUKI](https://www.interdb.jp/pg/pgsql05.html)

# How?

Assuming you have Postgres >=12 running somewhere, do something like
`createdb sync-test; psql sync-test -f apply-schema.sql`. Set the env
var `POSTGRES_CONFIG` if you need to configure host, username, etc.

Configurability hasn't been a priority, so things assume the database
is called "sync-test", so grep for that and replace or just call it
that.

You'll need python3 and the stuff in requirements.txt, e.g. via `pip3
install -r requirements.txt`.

* `demo-app.py` will provide a tiny API that provides bulks of changes and a cursor.
* `load-generator.py` generates random transactions that commit out of order. This load would cause naïve syncing based on timestamps to fail pretty quickly.
* `sqlite-via-api-client.py` uses the small API to sync all the data into SQLite.
* `consistency-check.py` verifies that everything in Postgres has been copied into SQLite without any differences. It is expected to find differences while load-generator is running.

# Licence

This code sample is licenced under the MIT open source licence, and
Copyright 2022 Cognite AS