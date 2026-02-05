#!/usr/bin/env bash
# Measures statement coverage of internal/store, internal/delivery and
# internal/dispatch (as exercised by all of ./internal/...) and fails if it is
# below the threshold given as $1 (default 80). Postgres tests count only when
# Docker or RELAY_TEST_DATABASE_URL is available; otherwise they skip.
set -euo pipefail
cd "$(dirname "$0")/.."
threshold="${1:-80}"
go test -count=1 \
  -coverpkg=./internal/store/...,./internal/delivery/...,./internal/dispatch/... \
  -coverprofile=coverage.out ./internal/... >/dev/null
total=$(go tool cover -func=coverage.out | awk '/^total:/ {sub("%", "", $3); print $3}')
echo "coverage of internal/store, internal/delivery, internal/dispatch: ${total}% (threshold ${threshold}%)"
awk -v t="$total" -v min="$threshold" 'BEGIN { exit (t + 0 >= min + 0) ? 0 : 1 }' || {
  echo "coverage below threshold" >&2
  exit 1
}
