CREATE TABLE endpoints (
    id           uuid PRIMARY KEY,
    url          text        NOT NULL,
    secret       text        NOT NULL,
    max_rps      int         NOT NULL,
    max_attempts int         NOT NULL,
    created_at   timestamptz NOT NULL
);

CREATE TABLE jobs (
    id              uuid PRIMARY KEY,
    endpoint_id     uuid        NOT NULL REFERENCES endpoints (id),
    payload         bytea       NOT NULL,
    headers         jsonb       NOT NULL DEFAULT '{}',
    idempotency_key text        NULL,
    status          text        NOT NULL CHECK (status IN ('pending', 'in_flight', 'delivered', 'dead')),
    attempts        int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    locked_until    timestamptz NULL,
    lease_token     uuid        NULL,
    last_error      text        NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE UNIQUE INDEX jobs_endpoint_idempotency_key ON jobs (endpoint_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX jobs_status_next_attempt_at ON jobs (status, next_attempt_at);

CREATE INDEX jobs_dead_letters ON jobs (endpoint_id, updated_at, id) WHERE status = 'dead';
