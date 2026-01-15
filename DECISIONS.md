# Decisions

One line each: decision — reason — alternative rejected.

- Generated code lives in `gen/relay/v1` (package `relayv1`) — keeps generated files out of hand-written packages — generating next to the `.proto` under `proto/`.
- Config comes only from `RELAY_*` env vars, with an injectable lookup function — tests stay hermetic — flags or a config file.
- Empty `RELAY_DATABASE_URL` selects the in-memory store — `make test` and local runs need no Postgres — making Postgres mandatory.
- Config rejects `RELAY_REQUEST_TIMEOUT >= RELAY_LEASE` — a request that can outlive its lease guarantees duplicate deliveries — allowing it silently.
- Added `golangci-lint` v2 as a dev tool (not a module dependency) for `make lint` — the spec asks for `.golangci.yml` — `go vet` alone.
- Added `github.com/google/uuid` for IDs and lease tokens — pgx maps it natively and it accepts an injected reader — pgtype.UUID (awkward outside SQL) or string IDs.
- Added `internal/clock` (Clock interface + Fake) — the spec requires an injected clock everywhere — passing `now` into every method.
- Store timestamps are truncated to microseconds in UTC in both stores — matches Postgres `timestamptz` so pagination cursors compare identically — nanosecond precision in memory only.
- Lease tokens are generated in Go and applied with `unnest` in the claim transaction — keeps randomness injectable — `gen_random_uuid()` in SQL.
- `Claim` returns each job joined with its endpoint — the worker needs URL, secret and limits without an extra lookup — a separate endpoint cache in the dispatcher.
- The store, not the caller, decides retry-vs-dead from `max_attempts` inside the conditional UPDATE — atomic with the lease check — deciding in the dispatcher from a possibly stale attempt count.
- Added `OutcomeReschedule` (pending, no attempt counted) — used for open breakers and shutdown lease release — separate `Release` method.
- A lease is valid through `locked_until` inclusive; reclaim needs `locked_until < now` — follows the spec's SQL literally — `<=`.
- Migrations take `pg_advisory_xact_lock` — several instances may start at once — relying on `CREATE TABLE IF NOT EXISTS`.
- Postgres tests create a fresh database per test from `RELAY_TEST_DATABASE_URL` or one testcontainers container per package — isolation without TRUNCATE ordering issues — one shared schema.
- With Colima, testcontainers needs `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock` and `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` — Colima has no `/var/run/docker.sock` on the host — writing `~/.testcontainers.properties`.
