#!/usr/bin/env bash
# Register (or update) the source and sink connectors, then wait until both run.
# PUT /connectors/<name>/config is idempotent, so this is safe to re-run.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "$0")/../connectors" && pwd)"

echo "Waiting for Kafka Connect at ${CONNECT_URL}"
for i in $(seq 1 60); do
  if curl -fsS "${CONNECT_URL}/connector-plugins" > /dev/null 2>&1; then break; fi
  if [ "$i" -eq 60 ]; then echo "Kafka Connect did not become ready" >&2; exit 1; fi
  sleep 2
done

plugins="$(curl -fsS "${CONNECT_URL}/connector-plugins")"
for class in io.debezium.connector.mysql.MySqlConnector io.debezium.connector.jdbc.JdbcSinkConnector; do
  if ! grep -q "$class" <<< "$plugins"; then
    echo "Connector plugin ${class} is not installed in the Connect image" >&2
    exit 1
  fi
done

# Source first, so the snapshot starts and the topics exist before the sink subscribes.
for name in mysql-source postgres-sink; do
  echo "Registering ${name}"
  curl -fsS -X PUT -H "Content-Type: application/json" \
    --data @"${DIR}/${name}.json" \
    "${CONNECT_URL}/connectors/${name}/config" > /dev/null
done

for name in mysql-source postgres-sink; do
  for i in $(seq 1 45); do
    state="$(curl -fsS "${CONNECT_URL}/connectors/${name}/status" | python3 -c '
import json, sys
s = json.load(sys.stdin)
print(" ".join([s["connector"]["state"]] + [t["state"] for t in s["tasks"]]))
')"
    if [ "$state" = "RUNNING RUNNING" ]; then echo "${name}: RUNNING"; break; fi
    if [[ "$state" == *FAILED* ]] || [ "$i" -eq 45 ]; then
      echo "${name} is not running: ${state}" >&2
      curl -fsS "${CONNECT_URL}/connectors/${name}/status" >&2 || true
      exit 1
    fi
    sleep 2
  done
done
