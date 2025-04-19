from kafka import KafkaConsumer
import json
import psycopg2
import os
from dotenv import load_dotenv
import psutil
import time

load_dotenv()

# PostgreSQL connection
DB_CONFIG = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT')
        }

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Kafka Consumer
consumer = KafkaConsumer(
    'VerifiedUserCheck',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id="verified_tweets_group"
)

print("Listening to Kafka topic: VerifiedUserCheck")

for msg in consumer:
    start_time = time.time()
    process = psutil.Process()

    data = msg.value
    user_name = data.get("user_name")
    user_verified = str(data.get("user_verified")).lower() == "true"
    text = data.get("text")

    try:
        cursor.execute(
            "INSERT INTO verified_tweets (user_name, user_verified, text) VALUES (%s, %s, %s)",
            (user_name, user_verified, text)
        )
        conn.commit()
        print(f"Inserted tweet from user: {user_name}")
    except Exception as e:
        print(f"Error inserting into DB: {e}")
        conn.rollback()

    end_time = time.time()
    cpu = process.cpu_percent(interval=1)
    mem = process.memory_info().rss / 1024**2

    print("\nConsumer for verified users")
    print(f"Execution Time: {end_time-start_time:.2f} sec")
    print(f"CPU Usage: {cpu:.2f}%")
    print(f"Memory Usage: {mem:.2f} MB")

    

