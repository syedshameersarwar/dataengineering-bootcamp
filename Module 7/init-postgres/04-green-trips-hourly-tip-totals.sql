-- Global 1-hour tumbling window: total tip_amount per hour (all locations).
CREATE TABLE IF NOT EXISTS green_trips_hourly_tip_totals (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_tip_amount DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_green_trips_hourly_tip_totals_amount
    ON green_trips_hourly_tip_totals (total_tip_amount DESC);
