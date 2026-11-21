-- Saga engine schema (PostgreSQL 12+).
-- Copy this file into one of your Flyway migrations (e.g. V5__saga.sql).

CREATE TABLE saga (
    id              uuid        PRIMARY KEY,
    type            text        NOT NULL,                 -- e.g. ITEM_SETUP
    business_key    text        NOT NULL,                 -- e.g. the item id
    state           text        NOT NULL CHECK (state IN ('RUNNING', 'COMPENSATING', 'COMPLETED', 'FAILED')),
    step_index      int         NOT NULL CHECK (step_index >= 0),
    step_status     text        NOT NULL CHECK (step_status IN ('READY', 'IN_PROGRESS', 'UNKNOWN', 'STUCK')),
    attempts        int         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    check_failures  int         NOT NULL DEFAULT 0 CHECK (check_failures >= 0),
    next_attempt_at timestamptz NOT NULL,
    locked_until    timestamptz,
    last_error      text,
    traceparent     text,                                 -- W3C traceparent of the originating request
    input           text,                                 -- the saga's input, encoded; written once
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    CONSTRAINT saga_lock CHECK ((step_status = 'IN_PROGRESS') = (locked_until IS NOT NULL))
);

-- At most one unfinished saga per business key.
CREATE UNIQUE INDEX saga_one_running_per_key ON saga (business_key)
    WHERE state IN ('RUNNING', 'COMPENSATING');

-- Recovery: everything to look at (ready step, due check, expired lock).
CREATE INDEX saga_due ON saga (next_attempt_at)
    WHERE state IN ('RUNNING', 'COMPENSATING') AND step_status <> 'STUCK';

-- Monitoring: stuck sagas.
CREATE INDEX saga_stuck ON saga (updated_at) WHERE step_status = 'STUCK';

-- Retention (SagaEngine.purgeFinished): finished sagas, oldest first.
CREATE INDEX saga_finished ON saga (updated_at) WHERE state IN ('COMPLETED', 'FAILED');

CREATE TABLE saga_step_result (
    saga_id        uuid        NOT NULL REFERENCES saga (id),
    step_index     int         NOT NULL,
    step_name      text        NOT NULL,
    output         text,                                  -- the step's output, encoded
    done_at        timestamptz NOT NULL,
    compensated_at timestamptz,
    PRIMARY KEY (saga_id, step_index)
);

CREATE TABLE saga_log (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    saga_id    uuid        NOT NULL REFERENCES saga (id),
    at         timestamptz NOT NULL,
    step_index int         NOT NULL,
    step_name  text,
    event      text        NOT NULL,                      -- e.g. EXECUTE:Done, CHECK:NotDone, MANUAL_RETRY
    detail     text,
    actor      text,                                      -- author of a manual action
    trace_id   text
);

CREATE INDEX saga_log_saga ON saga_log (saga_id, at);
CREATE INDEX saga_log_trace ON saga_log (trace_id) WHERE trace_id IS NOT NULL;
