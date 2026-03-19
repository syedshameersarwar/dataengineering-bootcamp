CREATE TABLE IF NOT EXISTS processed_events (
    pulocationid INTEGER,
    dolocationid INTEGER,
    passenger_count INTEGER NULL,
    trip_distance REAL,
    tip_amount REAL,
    total_amount REAL,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP
);
