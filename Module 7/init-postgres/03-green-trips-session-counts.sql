-- Session window results: one row per (session window × PULocationID).
-- Composite PK required for Flink JDBC upserts on streaming session aggregates.
CREATE TABLE IF NOT EXISTS green_trips_session_counts (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    pulocationid INTEGER NOT NULL,
    num_trips BIGINT NOT NULL,
    PRIMARY KEY (window_start, window_end, pulocationid)
);

CREATE INDEX IF NOT EXISTS idx_green_trips_session_counts_num_trips
    ON green_trips_session_counts (num_trips DESC);
