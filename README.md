# cdc-pipeline

Change data capture from an operational PostgreSQL database into an analytics
database, in near real time, with **Debezium + Kafka** and a small Python consumer.
The point of the project is the hard part of CDC: what happens when things break.
A chaos script kills and restarts the pieces while traffic keeps flowing, then
checks the replica against the source row by row.

```
source PostgreSQL ──WAL──▶ Debezium (Kafka Connect) ──▶ Kafka ──▶ Python consumer ──▶ analytics PostgreSQL
  (fictional shop)          pgoutput, logical slot      topic per table   batch + LSN check   replica.<table>
                                                                                              replica.changes (history)
```

## What it does

- **Captures every insert, update and delete** of `customers`, `products` and `orders`,
  starting with a consistent snapshot of the existing rows, then streaming the WAL.
- **Keeps two things in the analytics database:**
  - `replica.<table>` is the current state of each source row. Deleted rows are kept and flagged `_deleted`.
    Each row also stores the `_lsn` that wrote it and its source commit time.
  - `replica.changes` is the full history, including `before`/`after` of every change.
    It answers questions the source cannot, such as "how many orders were cancelled while still pending?"
- **Creates and evolves replica tables from the event schema.** A column added in the
  source shows up in the replica without a deploy. If it has a default, rows that existed
  before the change get that default too, as they do in PostgreSQL.
- **Preserves exact types.** Money uses `decimal.handling.mode=precise`
  (base64 unscaled + scale) and stays `numeric(12,2)`, never a float.

## Correctness under failure

Kafka delivers **at least once**. The consumer turns that into an **exactly-once effect**:

1. Read a batch (up to 500 events).
2. Apply it to PostgreSQL in **one transaction**.
3. Only then commit the Kafka offsets.

If the process dies between 2 and 3, the batch comes again. Each change is recorded
in `replica.changes` under a unique `(table, pk, lsn, op)`. Each replica row only
accepts an event whose LSN is newer than its own. A replay is therefore a no-op,
and an old event can never overwrite a newer one.

## Chaos demo

`scripts/chaos.sh` runs four failures while the simulator keeps writing orders,
stock and price changes, new customers and cancellations:

| # | Failure | What should happen |
|---|---------|--------------------|
| 1 | Consumer crashes **after** the DB commit, **before** the offset commit (the worst moment) | The batch is redelivered and skipped as already applied |
| 2 | Consumer is down for 20 s | Kafka buffers the changes and the consumer catches up |
| 3 | Kafka Connect (Debezium) restarts mid-traffic | Debezium resumes from its stored LSN with nothing lost |
| 4 | `ALTER TABLE orders ADD COLUMN channel text NOT NULL DEFAULT 'web'` | Replica gets the column, existing rows get `'web'` |

It then compares a SHA-256 fingerprint of every table, source vs replica. Output of a
full run ([docs/chaos-output.txt](docs/chaos-output.txt)):

```
== 1. Consumer crashes after writing a batch but before committing its Kafka offsets
WARNING simulated crash after the DB commit, before committing offsets
INFO skipped 23 changes that were already applied (redelivered after a restart)
...
== Verify
table      source rows replica rows  source hash       replica hash      match
customers          405          405  f7cbbed85a8a487b  f7cbbed85a8a487b  yes
products            50           50  64f4530a3f55e635  64f4530a3f55e635  yes
orders             320          320  885267fbf2c595fa  885267fbf2c595fa  yes

history: 2576 changes recorded (2326 live, 250 from the initial snapshot); 186 deletes kept as history
lag source commit -> replica: p50 3.16s, p95 22.65s (includes any time the consumer was stopped)
```

The CI runs the same chaos script on every push (`end-to-end` job).

### A bug this caught

The first run of scenario 4 failed: `orders` had the same row count on both sides but a
different hash. `ADD COLUMN ... DEFAULT 'web'` fills the existing rows **without writing a
change event for them**, so 151 replica rows had `channel = NULL`. The fix reads the
default from the Debezium field schema (`"default": "web"`) and applies it in the replica's
`ADD COLUMN`. `tests/test_sink.py::test_new_column_with_default_backfills_existing_rows`
covers it now.

A default that is an expression, such as `DEFAULT now()`, is evaluated once in the source, and
Debezium does not send its value. Backfill those columns explicitly.

## Numbers

Measured on a laptop, with Docker limited to **4 GB RAM and 2 CPUs** for all five containers:

- Steady traffic of about 25 changes/s: lag from source commit to replica row was
  **p50 1.1 s, p95 1.6 s**. Most of it is Debezium's polling interval and the consumer's 1 s batch wait.
- During the chaos run the p95 rises to about 23 s, because scenario 2 stops the consumer for 20 s on purpose.
- Restarting Kafka Connect takes one to two minutes on this machine, which is why `register.sh` retries.

These are small numbers on purpose. They show the behaviour, not a benchmark.

## Run it

Requirements: Docker (about 3 GB free RAM) and Python 3.11+.

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
docker compose up -d --wait          # source DB, analytics DB, Kafka (KRaft), Debezium
./scripts/register.sh                # registers the connector, waits until RUNNING

.venv/bin/python -m cdc.consumer &   # stream into the analytics DB
.venv/bin/python scripts/simulate.py --seconds 60 --rate 20
.venv/bin/python scripts/verify.py --wait 30

./scripts/chaos.sh                   # the four failures + verification
.venv/bin/pytest                     # 11 tests (sink tests need the analytics DB up)
```

Optional UI for topics, messages and the connector: `docker compose --profile ui up -d`,
then http://localhost:8088.

| Service | Port |
|---|---|
| Source PostgreSQL (`shop`/`shop`) | 5554 |
| Analytics PostgreSQL (`analytics`/`analytics`) | 5555 |
| Kafka | 9092 |
| Kafka Connect REST | 8083 |
| Redpanda Console (optional) | 8088 |

## Layout

```
cdc/events.py        Debezium message -> ChangeEvent (pure, typed values, schema -> PG types)
cdc/sink.py          Idempotent apply, soft deletes, history, auto-created/evolved tables
cdc/consumer.py      Batch loop: DB transaction first, offset commit second
connect/             Debezium connector config
db/source.sql        The fictional shop database
scripts/simulate.py  Random shop traffic
scripts/verify.py    Source vs replica fingerprint + lag
scripts/chaos.sh     The failure scenarios
tests/               Parsing tests + sink tests against real PostgreSQL
```

## Not covered

- Primary key changes and `TRUNCATE` are not handled. Debezium can emit them, but this sink does not.
- One consumer and one partition per topic. Ordering per row relies on Debezium keying messages by primary key.
- Dropping or renaming a source column leaves the old column in the replica. Nothing is dropped automatically.
- Credentials are demo values in plain text.

All data is generated; the shop is fictional.
