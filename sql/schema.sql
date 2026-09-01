-- Schema for the real-time customer heart-rate monitoring system.
-- Applied automatically to the "heartbeat_monitoring" database on
-- docker-compose startup (see docker-compose.yml).

CREATE TABLE IF NOT EXISTS heartbeat_readings (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     TEXT        NOT NULL,
    reading_time    TIMESTAMPTZ NOT NULL,
    heart_rate      INTEGER     NOT NULL,
    is_anomalous    BOOLEAN     NOT NULL DEFAULT FALSE,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Time-series queries (e.g. "last hour of readings") are the dominant
-- access pattern, so index on reading_time. The composite index also
-- serves "readings for one customer over time" lookups.
CREATE INDEX IF NOT EXISTS idx_heartbeat_readings_reading_time
    ON heartbeat_readings (reading_time);

CREATE INDEX IF NOT EXISTS idx_heartbeat_readings_customer_time
    ON heartbeat_readings (customer_id, reading_time);
