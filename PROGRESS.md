# Progress

Resume with "continue from PROGRESS.md". Each milestone is one commit.

- [x] 1. Scaffold: module, Makefile, .golangci.yml, proto + generated code, config, README skeleton, LICENSE
- [x] 2. Store: interface, memory, Postgres, shared contract suite
- [x] 3. Store-level chaos test
- [x] 4. Delivery: signer/Verify, classifier, backoff, breaker, limiter
- [x] 5. Dispatch: dispatcher, pool, shutdown
- [x] 6. End-to-end chaos test
- [x] 7. API + server
- [x] 8. Demo (docker-compose, scripts/demo.sh)
- [x] 9. Load test
- [ ] 10. CI
- [ ] 11. README

## Environment notes
- Go 1.27.4 installed via Homebrew; Docker provided by Colima (Docker 29.5.2).
- Docker Compose comes from Homebrew; `~/.docker/config.json` was given `cliPluginsExtraDirs: ["/opt/homebrew/lib/docker/cli-plugins"]` so `docker compose` works.
- Load test (milestone 9) found two dispatcher bugs, both fixed with regression tests: the poll loop throttled to one claim per 200ms when `Workers < BatchSize`, and the default HTTP transport exhausted ephemeral ports, tripping breakers. Raw results are in the README.
