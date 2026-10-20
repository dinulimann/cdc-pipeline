#!/usr/bin/env bash
# Breaks the pipeline in the ways production breaks it, while traffic keeps
# flowing, then checks that the replica still matches the source exactly.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
LOG=${LOG:-/tmp/cdc-consumer.log}

start_consumer() { $PY -m cdc.consumer >>"$LOG" 2>&1 & echo $! > /tmp/cdc-consumer.pid; }
stop_consumer()  { kill "$(cat /tmp/cdc-consumer.pid)" 2>/dev/null || true; sleep 2; }

echo "== 1. Consumer crashes after writing a batch but before committing its Kafka offsets"
stop_consumer
$PY scripts/simulate.py --seconds 10 --rate 25 &
SIM=$!
CDC_CRASH_AFTER_BATCHES=3 $PY -m cdc.consumer >>"$LOG" 2>&1 || true
grep -h "simulated crash" "$LOG" | tail -1
wait $SIM
start_consumer
for _ in $(seq 1 60); do   # the replacement gets the partitions after the session timeout
  grep -q "already applied (redelivered" "$LOG" && break
  sleep 1
done
grep -h "already applied (redelivered" "$LOG" | tail -1

echo "== 2. Consumer is down for 20 s while orders keep coming in"
stop_consumer
$PY scripts/simulate.py --seconds 20 --rate 25
start_consumer

echo "== 3. Debezium (Kafka Connect) restarts mid-traffic"
$PY scripts/simulate.py --seconds 25 --rate 25 &
SIM=$!
sleep 5
docker compose restart connect >/dev/null
./scripts/register.sh
wait $SIM

echo "== 4. Schema change on the source: a new column"
docker compose exec -T source-db psql -U shop -d shop -q \
  -c "ALTER TABLE orders ADD COLUMN IF NOT EXISTS channel text NOT NULL DEFAULT 'web'"
$PY scripts/simulate.py --seconds 10 --rate 25

echo "== Verify"
$PY scripts/verify.py --wait 90
