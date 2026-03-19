-- Single-row result: PULocationID and trip count for the longest session.
-- Populated by longest_session_job.py after green_trips_session_counts exists.
CREATE TABLE IF NOT EXISTS green_trips_longest_session (
    id INTEGER NOT NULL PRIMARY KEY,
    pulocationid INTEGER NOT NULL,
    num_trips BIGINT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL
);
