import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-group',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name}...")

record_count = 0
distance_gt5_count = 0
for message in consumer:
    ride = message.value
    record_count += 1
    if ride.trip_distance > 5:
        distance_gt5_count += 1
    if record_count == 49416:
        break

consumer.close()

print(f"Count of rides with distance greater than 5 Kilometers: {distance_gt5_count}")