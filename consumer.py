from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'kk-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='my-group'
)

for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
