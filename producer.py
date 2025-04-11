from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

topic = 'kk-topic'
message = b'Hello Kafka from Python! from kk'

producer.send(topic, message)
producer.flush()
print("Message sent!")
