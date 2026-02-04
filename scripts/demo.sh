#!/usr/bin/env bash
# Runs the demo against the stack started by `make up`: registers a healthy and
# a flaky endpoint, enqueues 200 jobs, and prints delivered/retrying/dead counts.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! docker compose ps --status running relay | grep -q relay; then
  echo "relay is not running; start it with: make up" >&2
  exit 1
fi

docker compose exec -T relay /app/democlient \
  --addr localhost:9090 \
  --healthy-url http://receiver-healthy:8081/hook \
  --flaky-url http://receiver-flaky:8081/hook \
  --jobs "${JOBS:-200}"

echo
echo "receiver stats:"
printf '  healthy: %s\n' "$(curl -fsS localhost:8081/stats)"
printf '  flaky:   %s\n' "$(curl -fsS localhost:8082/stats)"

# Give the queue-depth gauge one refresh interval to catch up.
sleep 2
echo
echo "relay metrics (http://localhost:8080/metrics):"
curl -fsS localhost:8080/metrics | grep -E '^relay_(jobs_enqueued_total|deliveries_total|queue_depth|breaker_open)'
