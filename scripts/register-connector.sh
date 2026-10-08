#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
for attempt in $(seq 1 60); do
  if curl --silent --fail --max-time 2 http://localhost:8083/connectors >/dev/null; then
    exec curl --fail-with-body --max-time 15 -X PUT -H 'Content-Type: application/json' --data-binary @ops/connector.json http://localhost:8083/connectors/customers-cdc/config
  fi
  sleep 2
done
echo "Kafka Connect did not become ready within 120 seconds" >&2
exit 1
