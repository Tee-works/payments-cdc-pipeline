-- Warehouse side.
--
-- public.merchants / public.transactions / public.refunds are created and
-- kept current by the Debezium JDBC sink connector (upsert by primary key).
--
-- The cdc schema below is written by the Python consumer: an append-only
-- history of every status change, and a log of rule violations.

CREATE SCHEMA IF NOT EXISTS cdc;

CREATE TABLE cdc.transaction_status_history (
    transaction_id  BIGINT      NOT NULL,
    event_id        TEXT        NOT NULL,  -- binlog file:position:row, unique per change
    op              CHAR(1)     NOT NULL,  -- c insert, u update, r snapshot read
    from_status     TEXT,
    to_status       TEXT        NOT NULL,
    amount_minor    BIGINT      NOT NULL,
    source_ts       TIMESTAMPTZ NOT NULL,  -- when the change happened in MySQL
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, event_id)
);

CREATE INDEX idx_history_source_ts ON cdc.transaction_status_history (source_ts);

CREATE TABLE cdc.transition_violations (
    transaction_id  BIGINT      NOT NULL,
    event_id        TEXT        NOT NULL,
    rule            TEXT        NOT NULL,
    detail          TEXT        NOT NULL,
    from_status     TEXT,
    to_status       TEXT,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, event_id, rule)
);

-- How long each transaction took to leave 'pending', straight from the history.
CREATE VIEW cdc.time_to_final_status AS
SELECT
    h.transaction_id,
    h.to_status AS final_status,
    h.source_ts - c.source_ts AS time_to_final
FROM cdc.transaction_status_history h
JOIN cdc.transaction_status_history c
  ON c.transaction_id = h.transaction_id AND c.to_status = 'pending' AND c.from_status IS NULL
WHERE h.from_status = 'pending';
