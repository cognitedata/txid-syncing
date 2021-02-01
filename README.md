# What?

This repository has examples of to reliably sync changes out of Postgres and into external systems, such as Elasticsearch (which is the demo use case), and Kafka.

# How?

Assuming you have Postgres 12 running somewhere, do something like `createdb sync-test; psql sync-test -f apply-schema.sql`. Set the env var `POSTGRES_CONFIG` if you need to configure host, username, etc. It's a psycopg2 string.

Configurability hasn't been a priority, so things assume the database is called "sync-test", so grep for that and replace or just call it that.

You'll need python3 and the stuff in requirements.txt, e.g. via `pip3 install -r requirements.txt`.

If you run sync.py, it assumes Elasticsearch is running on localhost, accepting anonymous connections. If you download Elasticsearch and just start it locally, that'll be enough.

`sync.py` will kill itself at random intervals - it'll run for a minute on average by default. For long-ish testing, run it like `while true; do ./sync.py; done` or something.

Then, while sync is running, run `./load-generator.py`. By default it'll open 32 connections. You can change that via `./load-generator.py 16`.

After killing load-generator, you should be able to run `./consistency-check.py`, which will dump everything in Postgres and Elasticsearch and see that they have exactly the same versions.

# How to break it?

If you can make a sequence of transactions in Postgres that when done will cause consistency-check.py to fail, then you've successfully broken it. Note that the checker will not be able to succeed _while_ changes are being made. It'll totally fail if you're running load-generator simultaneously.

When sync.py mention errors, these are likely due to the doc already having been indexed. (e.g. when it self-crashes
before it has persisted its progress) This is expected, and the consistency checker should not find any problems.

# How to read

- This [draft document on syncing out of Postgres](https://docs.google.com/document/d/142S_AqHig1I3mU12lAuWS3Cxff44pIiske6GU6IzBy0/edit) explains a fair bit of the Postgres internals used. (It's not updated to reflect this approach here, yet.)
- If you read load-generator first, you'll see the kind of things we're trying to make sure we can sync.
- consistency-check.py is pretty trivial too
- Then, sync.py might be worth checking, as it puts context around how the cursor is used.
- txid_sync.cursor has most of the interesting logic, though. (It's kept free of async code so it can easily be used in sync Python code too. It needs tests, for now we just have the concurrency fuzzing that is the load-generator + consistency-check)