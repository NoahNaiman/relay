# Decisions

One line each: decision — reason — alternative rejected.

- Generated code lives in `gen/relay/v1` (package `relayv1`) — keeps generated files out of hand-written packages — generating next to the `.proto` under `proto/`.
- Config comes only from `RELAY_*` env vars, with an injectable lookup function — tests stay hermetic — flags or a config file.
- Empty `RELAY_DATABASE_URL` selects the in-memory store — `make test` and local runs need no Postgres — making Postgres mandatory.
- Config rejects `RELAY_REQUEST_TIMEOUT >= RELAY_LEASE` — a request that can outlive its lease guarantees duplicate deliveries — allowing it silently.
- Added `golangci-lint` v2 as a dev tool (not a module dependency) for `make lint` — the spec asks for `.golangci.yml` — `go vet` alone.
