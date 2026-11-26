-- Saga engine schema (PostgreSQL 12+).
-- Copy this file into one of your Flyway migrations (e.g. V5__saga.sql).

CREATE TABLE saga (
    id              uuid        PRIMARY KEY,
    type            text        NOT NULL,                 -- e.g. ITEM_SETUP
    business_key    text        NOT NULL,                 -- e.g. the item id
    state           text        NOT NULL CHECK (state IN ('RUNNING', 'COMPENSATING', 'FINISHING', 'COMPLETED', 'FAILED')),
    step_index      int         NOT NULL CHECK (step_index >= 0),
    step_status     text        NOT NULL CHECK (step_status IN ('READY', 'IN_PROGRESS', 'WAITING', 'CHECKING', 'REPLAYING', 'STUCK')),
    attempts        int         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    check_failures  int         NOT NULL DEFAULT 0 CHECK (check_failures >= 0),
    next_attempt_at timestamptz NOT NULL,
    locked_until    timestamptz,
    call_action     text        CHECK (call_action IN ('EXECUTE', 'REPLAY', 'CHECK', 'COMPENSATE')), -- the call in flight
    last_error      text,
    traceparent     text,                                 -- W3C traceparent of the originating request
    input           text,                                 -- the saga's input, encoded; written once
    cancel_requested_at timestamptz,                      -- set by cancel: compensate once no call is in flight
    cancel_reason   text,                                 -- given to cancel; who cancelled is in saga_log
    failure_reason  text,                                 -- why it compensates: set when compensation starts, never changed
    stuck_reason    text        CHECK (stuck_reason IN ('CANNOT_TELL', 'CHECK_FAILING', 'TOO_MANY_ATTEMPTS',
                                                        'OUTPUT_UNSTORABLE', 'RECORD_FAILED', 'NO_SIGNAL',
                                                        'KEY_EXPIRED', 'HOOK_FAILED', 'RESUME_FAILING')),
    stuck_at        timestamptz,                          -- when it became STUCK
    stuck_from      text        CHECK (stuck_from IN ('READY', 'IN_PROGRESS', 'WAITING', 'CHECKING', 'REPLAYING')), -- step_status before RESUME_FAILING
    key_since       timestamptz,                          -- first use of the current step's idempotency key
    key_generation  int         NOT NULL DEFAULT 0 CHECK (key_generation >= 0), -- bumped by retry(newKey = true); reset at each step
    resume_failures int         NOT NULL DEFAULT 0 CHECK (resume_failures >= 0), -- RESUME_FAILED in a row; reset when the saga runs
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    CONSTRAINT saga_lock CHECK ((step_status = 'IN_PROGRESS') = (locked_until IS NOT NULL)),
    CONSTRAINT saga_call CHECK ((step_status = 'IN_PROGRESS') = (call_action IS NOT NULL)),
    CONSTRAINT saga_cancel CHECK ((cancel_requested_at IS NULL) = (cancel_reason IS NULL)),
    CONSTRAINT saga_stuck_reason CHECK ((step_status = 'STUCK') = (stuck_reason IS NOT NULL AND stuck_at IS NOT NULL)),
    CONSTRAINT saga_stuck_at CHECK ((stuck_reason IS NULL) = (stuck_at IS NULL))
);

-- At most one unfinished saga per business key.
CREATE UNIQUE INDEX saga_one_running_per_key ON saga (business_key)
    WHERE state IN ('RUNNING', 'COMPENSATING', 'FINISHING');

-- Recovery: everything to look at (ready step, due check, expired lock).
CREATE INDEX saga_due ON saga (next_attempt_at)
    WHERE state IN ('RUNNING', 'COMPENSATING', 'FINISHING') AND step_status <> 'STUCK';

-- Monitoring: stuck sagas.
CREATE INDEX saga_stuck ON saga (stuck_at, id) WHERE step_status = 'STUCK';

-- Retention (SagaEngine.purgeFinished): finished sagas, oldest first.
CREATE INDEX saga_finished ON saga (updated_at) WHERE state IN ('COMPLETED', 'FAILED');

-- Lookup by business key (SagaEngine.findByKey), with saga_one_running_per_key for the unfinished ones.
-- Partial: a saga enters it once, when it finishes, and is never updated after.
CREATE INDEX saga_finished_key ON saga (business_key) WHERE state IN ('COMPLETED', 'FAILED');

CREATE TABLE saga_step_result (
    saga_id        uuid        NOT NULL REFERENCES saga (id),
    step_index     int         NOT NULL,
    step_name      text        NOT NULL,
    output         text,                                  -- the step's output, encoded
    done_at        timestamptz NOT NULL,
    compensated_at timestamptz,
    PRIMARY KEY (saga_id, step_index)
);

-- Signals received for signal steps: at most one per name and saga, so a redelivery changes nothing.
-- Kept until the saga is purged: a signal that arrives before its step waits is used when it does.
CREATE TABLE saga_signal (
    saga_id     uuid        NOT NULL REFERENCES saga (id),
    signal_name text        NOT NULL,                     -- e.g. payment: one step of the saga waits for it
    value       text        NOT NULL,                     -- encoded by the engine's serializer
    received_at timestamptz NOT NULL,
    PRIMARY KEY (saga_id, signal_name)
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

-- In the order history and historyByTrace read them (at, then id for entries of the same instant): no sort.
CREATE INDEX saga_log_saga ON saga_log (saga_id, at, id);
CREATE INDEX saga_log_trace ON saga_log (trace_id, at, id) WHERE trace_id IS NOT NULL;
