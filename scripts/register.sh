#!/usr/bin/env bash
# Registers (or updates) the Debezium connector and waits until it runs.
# Safe to call while Kafka Connect is still starting: it retries.
set -euo pipefail
cd "$(dirname "$0")/.."
URL=${CONNECT_URL:-http://localhost:8083}
config=$(python3 -c "import json; print(json.dumps(json.load(open('connect/shop-connector.json'))['config']))")

state=""
for _ in $(seq 1 150); do   # Connect can take a few minutes to restart on a small machine
  if curl -sf -X PUT "$URL/connectors/shop-source/config" -H 'Content-Type: application/json' \
       -d "$config" >/dev/null 2>&1; then
    state=$(curl -s "$URL/connectors/shop-source/status" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'], *[t['state'] for t in d.get('tasks', [])])" \
      2>/dev/null || true)
    [ "$state" = "RUNNING RUNNING" ] && { echo "connector running"; exit 0; }
  fi
  sleep 2
done
echo "connector did not start: ${state:-Connect API not reachable}" >&2
exit 1
