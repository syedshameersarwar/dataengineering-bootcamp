-- Matches Flink JDBC upsert: composite primary key required for windowed aggregates.
-- Use lowercase column names (PostgreSQL folds unquoted identifiers to lowercase).
CREATE TABLE IF NOT EXISTS green_trips_5min_counts (
    window_start TIMESTAMP NOT NULL,
    pulocationid INTEGER NOT NULL,
    num_trips BIGINT NOT NULL,
    PRIMARY KEY (window_start, pulocationid)
);
