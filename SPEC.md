# SPEC: `relay`, a reliable webhook delivery service in Go

Build this project from an empty directory, top to bottom, without stopping to ask questions.

## Ground rules
1. **Decisions:** when something is ambiguous, pick the simplest reasonable option and log it in `DECISIONS.md` as one line: decision, reason, alternative rejected.
2. **Progress:** keep `PROGRESS.md` as a checklist of the milestones below and update it after each one, so a new session can resume with "continue from PROGRESS.md".
3. **Commits:** `git init` first. One commit per milestone, using conventional-commit messages. Before each commit, `go vet ./...` and `go test -race ./...` must pass.
4. **No keys, no paid services.** Everything builds and tests with only Go installed. Docker is optional: Postgres tests skip cleanly without it, with a clear skip message.
5. **No invented numbers.** Every performance figure in the README comes from a command you ran, printed next to the command and the machine specs. If you couldn't run it, write `PENDING`, followed by the exact command to produce it.
6. **Scope discipline:** build only what this spec lists. Put any extra idea in the README's "Next steps" section instead of the code.
7. **Stuck rule:** if an environment problem (a tool won't install, no network) survives 3 attempts, log it in PROGRESS.md, skip that piece, and continue.
8. **Hygiene:** MIT `LICENSE`, `.gitignore`, no secrets in the repo, no TODO comments in the final code.

## What it does
Clients register webhook endpoints and enqueue jobs over gRPC. `relay` delivers each job as a signed HTTP POST, with retries, per-endpoint rate limiting, a circuit breaker and a dead-letter queue.

**Guarantees, stated in the README:**
- **At-least-once delivery.** Receivers dedupe using the `Relay-Job-Id` header. The README explains in a few sentences why exactly-once isn't possible over HTTP.
- **No ordering guarantee.**
- **A job is never lost** once `EnqueueJob` returns. The chaos tests prove this.

## Stack
- Go 1.22+, module `github.com/NoahNaiman/relay`, logging with `log/slog` (JSON).
- gRPC + protobuf. Install `buf` with `go install github.com/bufbuild/buf/cmd/buf@latest` along with `protoc-gen-go` and `protoc-gen-go-grpc`, and run `buf generate`. **Commit the generated code** so a clone builds without the tools. Never hand-write generated code.
- Postgres through `jackc/pgx/v5`, with migrations as embedded SQL that runs at startup.
- `golang.org/x/time/rate`, `prometheus/client_golang`, `testcontainers-go`.
- No other dependencies without a line in DECISIONS.md.

## Layout
```
cmd/relay         server: gRPC :9090, HTTP :8080 (/healthz, /readyz, /metrics)
cmd/receiver      demo receiver: verifies signatures, dedupes by job ID, flags --fail-rate and --max-latency
cmd/loadgen       enqueues N jobs at a target rate, reports throughput and enqueue→delivery latency percentiles
cmd/democlient    registers endpoints and enqueues jobs for the demo
internal/config   env config, defaults, validation
internal/store    Store interface + postgres/ + memory/
internal/delivery signer, sender, failure classifier, backoff, breaker, limiter registry
internal/dispatch dispatcher, worker pool, retry policy
internal/api      gRPC handlers
proto/relay/v1/relay.proto
test/chaos        store-level and end-to-end chaos tests (build tag: chaos)
```

## gRPC API: `relay.v1.RelayService`
| RPC | Behavior |
|---|---|
| `RegisterEndpoint(url, secret, max_rps, max_attempts)` | Only `http`/`https` URLs. `max_rps` ≥ 1, default 10. `max_attempts` 1–25, default 8. |
| `EnqueueJob(endpoint_id, payload, idempotency_key?, headers?)` | Payload limit 256 KB (`InvalidArgument` if over). If `idempotency_key` is set and already exists for that endpoint, return the **existing** job with no error. |
| `GetJob(job_id)` | Returns status, attempts, last_error, next_attempt_at. |
| `ListDeadLetters(endpoint_id, page_size, page_token)` | Keyset pagination on `(updated_at, id)`. |
| `ReplayDeadLetter(job_id)` | Only dead jobs (`FailedPrecondition` otherwise). Resets attempts and makes the job due now. |

Use correct status codes everywhere: `InvalidArgument`, `NotFound`, `FailedPrecondition`.

## Data model
```sql
endpoints(id uuid pk, url text, secret text, max_rps int, max_attempts int, created_at timestamptz)
jobs(id uuid pk, endpoint_id uuid fk, payload bytea, headers jsonb, idempotency_key text null,
     status text check (status in ('pending','in_flight','delivered','dead')),
     attempts int, next_attempt_at timestamptz, locked_until timestamptz null, lease_token uuid null,
     last_error text null, created_at timestamptz, updated_at timestamptz)
unique (endpoint_id, idempotency_key) where idempotency_key is not null
index (status, next_attempt_at)
```
Secrets are stored in plaintext. List that as a known limitation.

## Core mechanics
**Time and randomness:** inject a clock and a random source everywhere they're used, so tests are deterministic.

**Claiming:** a single transaction selects due rows with `status='pending' AND next_attempt_at<=now()` **or** `status='in_flight' AND locked_until<now()` (an expired lease), using `ORDER BY next_attempt_at LIMIT n FOR UPDATE SKIP LOCKED`. It then sets `status='in_flight'`, `locked_until=now()+lease` (default 30s) and a **fresh `lease_token`**. The expired-lease condition is what makes a crashed worker's jobs recoverable. The memory store implements the same semantics behind a mutex.

**Completing:** every result write is conditional on `status='in_flight' AND lease_token=<the token from this claim>`. That stops a slow worker whose lease expired from overwriting the result of the worker that reclaimed the job; the stale write affects 0 rows and is logged. Delivered → `delivered`. Retryable → `pending`, with `attempts+1` and `next_attempt_at=now()+backoff`, or `dead` once `max_attempts` is reached. Permanent failure → `dead`.

**Failure classification:**
| Response | Result |
|---|---|
| 2xx | delivered |
| 408, 429, 5xx, timeout, connection error | retry (on 429, honor `Retry-After` in seconds, capped at 10m) |
| any other 4xx, invalid URL | dead immediately |

**Backoff:** exponential with full jitter, `rand(0, min(cap, base·2^attempt))`. Base 1s, cap 10m, both configurable.

**Circuit breaker (per endpoint, in-process):** it opens after 5 consecutive failures and stays open for a configurable cooldown (default 30s), then goes half-open and lets one probe through. A successful probe closes it; a failed one reopens it. A job that hits an open breaker is rescheduled to the end of the cooldown **without counting an attempt**.

**Rate limit:** a token bucket per endpoint, set from `max_rps`, in-process only. The README notes that a multi-instance setup would need a shared limiter.

**Signing:** headers `Relay-Job-Id`, `Relay-Attempt`, `Relay-Timestamp` (unix seconds), and `Relay-Signature: sha256=<hex HMAC-SHA256(secret, timestamp + "." + body)>`. Export `delivery.Verify(secret, headers, body, now, tolerance)`. The receiver uses it with a 5-minute tolerance and compares in constant time.

**Workers:** the dispatcher polls every 200ms, or immediately after a full batch, and hands jobs to a bounded pool of goroutines (default 16). The per-request timeout defaults to 10s.

**Graceful shutdown (SIGINT/SIGTERM):** stop claiming, let in-flight deliveries finish within a 15s deadline, then release the leases on anything left (`pending`, due now).

**Metrics:**
- `relay_jobs_enqueued_total`
- `relay_deliveries_total{result="delivered|retry|dead"}`
- `relay_delivery_duration_seconds` (histogram)
- `relay_queue_depth{status}` (gauge, refreshed every 5s)
- `relay_breaker_open{endpoint_id}` (gauge)

## Milestones
1. **Scaffold:** module, Makefile (`build test lint proto up down demo chaos loadtest`), `.golangci.yml`, proto + generated code, config, README skeleton, LICENSE.
2. **Store:** the interface, a memory implementation, and a Postgres implementation. **One shared contract suite** runs against both and covers: idempotent enqueue, claiming only due jobs, concurrent claimers never getting the same job (run 8 claimers against 500 jobs and assert no overlap), reclaim after lease expiry, the stale-completion guard (an old token's write is rejected after another worker reclaims the job), retry scheduling, dead-lettering, pagination, and replay.
3. **Store-level chaos test** (`test/chaos/store_chaos_test.go`): this is the earliest proof that no jobs are lost, built before any HTTP code exists. Enqueue 5,000 jobs. Run 8 goroutine "workers" that claim batches with a 200ms lease, then for each job, at random: complete it (60%), schedule a retry (25%), or **abandon it without writing anything**, simulating a crash (15%). Abandoned jobs must come back after their lease expires. Workers also randomly sleep past their lease before completing, which exercises the stale-token guard. Run until every job is `delivered` or `dead`, then assert: **zero lost jobs**, no job delivered twice by the store's accounting, and every stale write rejected. Runs on the memory store by default and on Postgres when `RELAY_TEST_PG=1`. Must finish in under 30s.
4. **Delivery:** signer/Verify, classifier, backoff, breaker and limiter. Table-driven tests with a fake clock.
5. **Dispatch:** dispatcher, pool and shutdown. Tests run against `httptest` servers covering success, retry then success, permanent failure, `Retry-After`, the breaker opening, and shutdown releasing leases.
6. **End-to-end chaos test** (`test/chaos/e2e_chaos_test.go`): memory store by default, Postgres when `RELAY_TEST_PG=1`. Enqueue 2,000 jobs to an `httptest` receiver with a 30% failure rate. Run 3 dispatcher instances, each with a 2s lease, a 10ms backoff base, a 200ms backoff cap and a 200ms breaker cooldown (all set through config, so the test fits its time budget). Every 500ms, cancel a random instance **without graceful shutdown** and start a fresh one. Afterwards, assert that every job is `delivered` or `dead`, that every `delivered` job was received at least once, and that **zero jobs were lost**. Log the duplicate rate seen by the receiver; duplicates are expected under at-least-once. Must finish in under 60s.
7. **API + server:** handlers, health and readiness probes (ready = store reachable), metrics. Tests use `bufconn`.
8. **Demo:** `docker-compose.yml` with postgres, relay, a healthy receiver, and a flaky receiver (`--fail-rate 0.3 --max-latency 500ms`). `scripts/demo.sh` runs `cmd/democlient` to register both endpoints, enqueue 200 jobs, then print the delivered, retrying and dead counts.
9. **Load test:** run `loadgen` against the memory store, and against Postgres if Docker is available. Record real numbers.
10. **CI:** a GitHub Actions workflow with jobs for lint, `test -race`, both chaos tests, and Postgres integration (using a Postgres service container and `RELAY_TEST_PG=1`).
11. **README:**
    - pitch (3 sentences)
    - Mermaid diagram
    - quickstart (`make up && make demo`)
    - guarantees
    - failure classification table
    - how claiming, leases and lease tokens work (short, with the SQL)
    - load-test results
    - known limitations: plaintext secrets, in-process limiter and breaker, no retention of delivered jobs, no SSRF protection on endpoint URLs
    - next steps: outbox pattern, shared limiter, sharded dispatch, retention job

## Definition of done
- [ ] `make test` passes with no Docker and no network.
- [ ] `make chaos` runs both chaos tests, and both pass with 0 lost jobs.
- [ ] `make up && make demo` shows delivered, retried and dead-lettered jobs; `/metrics` reflects them.
- [ ] Coverage ≥ 80% across `internal/store`, `internal/delivery` and `internal/dispatch`.
- [ ] Every number in the README is either real (with its command) or marked `PENDING`.