#!/usr/bin/env bash
# Runs cmd/loadgen against a local relay: first on the memory store, then on
# Postgres (in Docker) if Docker is available. Prints machine specs and the
# exact commands so results can be copied into the README verbatim.
set -euo pipefail
cd "$(dirname "$0")/.."

N="${N:-20000}"
RATE="${RATE:-2000}"
GRPC=127.0.0.1:19090
HTTP=127.0.0.1:18080
PG_PORT=55432

go build -o bin/ ./cmd/relay ./cmd/loadgen

echo "== machine"
if [[ "$(uname)" == "Darwin" ]]; then
  echo "$(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(( $(sysctl -n hw.memsize) / 1073741824 )) GB RAM, macOS $(sw_vers -productVersion)"
else
  echo "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(nproc) cores, $(free -g | awk '/Mem:/{print $2}') GB RAM, $(uname -sr)"
fi
go version

relay_pid=""
cleanup() {
  [[ -n "$relay_pid" ]] && kill "$relay_pid" 2>/dev/null && wait "$relay_pid" 2>/dev/null || true
  docker rm -f relay-loadtest-pg >/dev/null 2>&1 || true
}
trap cleanup EXIT

start_relay() {
  RELAY_GRPC_ADDR=$GRPC RELAY_HTTP_ADDR=$HTTP RELAY_LOG_LEVEL=warn RELAY_DATABASE_URL="$1" ./bin/relay >/tmp/relay-loadtest.log 2>&1 &
  relay_pid=$!
  for _ in $(seq 1 100); do
    curl -fsS "http://$HTTP/readyz" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "relay did not become ready; see /tmp/relay-loadtest.log" >&2
  exit 1
}

stop_relay() {
  kill "$relay_pid"; wait "$relay_pid" 2>/dev/null || true; relay_pid=""
}

run_loadgen() {
  local cmd="./bin/loadgen --addr $GRPC --n $N --rate $RATE"
  echo "\$ $cmd"
  $cmd
}

echo
echo "== memory store (RELAY_DATABASE_URL empty, default config)"
start_relay ""
run_loadgen
stop_relay

if docker info >/dev/null 2>&1; then
  echo
  echo "== postgres store (postgres:16-alpine in Docker, default config)"
  docker run -d --rm --name relay-loadtest-pg -e POSTGRES_PASSWORD=relay -e POSTGRES_USER=relay -e POSTGRES_DB=relay \
    -p $PG_PORT:5432 postgres:16-alpine >/dev/null
  for _ in $(seq 1 60); do
    docker exec relay-loadtest-pg pg_isready -U relay -d relay >/dev/null 2>&1 && break
    sleep 0.5
  done
  sleep 1
  start_relay "postgres://relay:relay@127.0.0.1:$PG_PORT/relay?sslmode=disable"
  run_loadgen
  stop_relay
else
  echo
  echo "== postgres store: SKIPPED (Docker not available)"
fi
