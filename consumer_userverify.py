from kafka import KafkaConsumer
import json
import time
import psycopg2
import os
import psutil
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection
DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

# Connect to PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Kafka consumer setup
consumer = KafkaConsumer(
    'VerifiedUserCheck',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id="verified_tweets_group"
)

process = psutil.Process()
print("Waiting for verified user tweets...")
print("-" * 50)

start_time = time.time()
cpu_start = process.cpu_times()

try:
    for msg in consumer:
        tweet = msg.value
        try:
            user_name = tweet.get("user_name")
            user_verified = str(tweet.get("user_verified")).lower() == "true"
            text = tweet.get("text")

            print(f"\nUser: {user_name}")
            print(f"Tweet: {text}")
            print(f"Verified: {user_verified}")
            print("-" * 50)

            insert_query = """
                INSERT INTO verified_tweets (user_name, user_verified, text)
                VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, (user_name, user_verified, text))
            conn.commit()
            print("Inserted tweet")

            time.sleep(5)

        except Exception as e:
            print("Error:", e)
            conn.rollback()
            continue

except KeyboardInterrupt:
    end_time = time.time()
    cpu_end= process.cpu_times()
    cpt_time_used = (cpu_end.user + cpu_end.system) - (cpu_start.user + cpu_start.system)
    wall_time = end_time - start_time
    cpu = (cpt_time_used/wall_time)*100 if wall_time>0 else 0
    mem = process.memory_info().rss / 1024**2

    print("\nConsumer for verified users")
    print(f"Execution Time: {end_time - start_time:.2f} sec")
    print(f"CPU Usage: {cpu:.2f}%")
    print(f"Memory Usage: {mem:.2f} MB")

    print("\nStopping consumer...")
    consumer.close()
    conn.close()
