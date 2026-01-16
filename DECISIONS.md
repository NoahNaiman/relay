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
- Store chaos test asserts only on *provably* stale writes (the job's lease token changed before the write) and counts other rejections separately — any other notion of "stale" races between goroutines — asserting on every lease-expired write.
- Oversleeping completions run in their own goroutine — keeps 5% oversleeps from serializing a worker's batch and blowing the 30s budget — sleeping inline.
- Backoff `attempt` is the number of attempts made before the failing one, so the first retry draws from `[0, base]` — matches `base·2^attempt` with a 0-based attempt — 1-based (first retry up to 2·base).
- `Retry-After` accepts delay-seconds only; HTTP-dates are ignored and normal backoff applies — the spec says "in seconds" — parsing HTTP-dates.
- The breaker counts only retryable failures; permanent 4xx neither trip nor reset it (a half-open probe ending in 4xx is released) — a 404 for one job says nothing about endpoint health — counting 4xx as failures.
- While half-open with a probe in flight, other jobs are rescheduled to now+cooldown — the spec defines only the open-state reschedule time — busy-polling.
- The rate limiter uses `ReserveN` with the injected clock; if the wait exceeds a max (set by the dispatcher) the job is rescheduled without an attempt — keeps workers from sleeping on one slow endpoint and stays testable — `Limiter.Wait` with real time.
- Job headers cannot override `Relay-*`, `Content-Length` or `Host`; `Content-Type` defaults to `application/json` but can be overridden — a job must not be able to forge its signature — allowing any header.
- Non-2xx, non-4xx, non-5xx statuses (e.g. 3xx; redirects are followed by the client first) are permanent — the spec's table lists only 2xx/4xx/5xx — retrying them.
- The worker pool is a semaphore of size `RELAY_WORKERS` with one goroutine per claimed job, and the dispatcher claims only as many jobs as there are free slots — leases are never held for jobs waiting in a queue — fixed worker goroutines reading from a buffered channel.
- Deliveries cut short by the shutdown deadline (or a limiter wait) release the lease without counting an attempt — the receiver did not fail — counting it as a retry.
- `Dispatcher.Kill()` simulates a crash by suppressing all further store writes — the memory store ignores contexts, so a canceled context alone would not stop writes — killing a subprocess.
- Metrics are reported through a `dispatch.Observer` interface — keeps Prometheus out of the dispatch package and tests — calling Prometheus directly.
- `relay_deliveries_total{result}` reports the job's resulting state, so a final retry that exhausts `max_attempts` counts as `dead` — matches what operators see in the store — labelling by classifier verdict.
