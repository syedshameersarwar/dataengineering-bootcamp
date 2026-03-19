import json
from dataclasses import dataclass
from math import isnan

@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    tip_amount: float
    total_amount: float
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    passenger_count: int | None = None


def _parse_optional_int(value):
    if value is None:
        return None

    try:
        if isnan(value):
            return None
    except TypeError:
        pass

    return int(value)


def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=_parse_optional_int(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
        lpep_pickup_datetime=row['lpep_pickup_datetime'].strftime("%Y-%m-%d %H:%M:%S"),
        lpep_dropoff_datetime=row['lpep_dropoff_datetime'].strftime("%Y-%m-%d %H:%M:%S"),
    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

'''
CREATE TABLE processed_events (
 PULOCATIONID INTEGER,
 DOLocationID INTEGER,
 passenger_count INTEGER NULL,
 trip_distance FLOAT,
 tip_amount FLOAT,
 total_amount FLOAT,
 lpep_pickup_datetime TIMESTAMP,
 lpep_dropoff_datetime TIMESTAMP
);
'''