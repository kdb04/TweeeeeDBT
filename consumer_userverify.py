from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'VerifiedUserCheck',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id=None
)

for msg in consumer:
    print(msg.value)
