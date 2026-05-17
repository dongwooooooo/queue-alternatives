CREATE TABLE IF NOT EXISTS waiting_queue (
    seq BIGSERIAL PRIMARY KEY,
    token VARCHAR(64) NOT NULL UNIQUE,
    user_id VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'WAITING',
    enqueued_at TIMESTAMP NOT NULL DEFAULT now(),
    admitted_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_waiting_seq ON waiting_queue(status, seq) WHERE status = 'WAITING';
