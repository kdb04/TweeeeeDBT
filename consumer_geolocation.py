from kafka import KafkaConsumer
import json
import re
import time
import os
import psycopg2
import psutil
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

delhi_pattern = re.compile(r"\b(new\s*)?delhi\b", re.IGNORECASE)
mumbai_pattern = re.compile(r"\bmumbai\b", re.IGNORECASE)

consumer = KafkaConsumer(
    'GeoLocation',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

process = psutil.Process()

delhi_count = 0
mumbai_count = 0

print("Monitoring tweets from Delhi and Mumbai...")
print("-" * 50)

try:
    for msg in consumer:
        start_time = time.time()

        data = msg.value
        location = data.get("user_location", "").lower()

        if delhi_pattern.search(location):
            delhi_count += 1
            city = "Delhi"
        elif mumbai_pattern.search(location):
            mumbai_count += 1
            city = "Mumbai"
        else:
            continue

        print(f"\nLocation: {city}")
        print(f"User: {data['user_name']}")
        print(f"Tweet: {data['text']}")
        print(f"Date: {data['date']}")
        print(f"Current Counts - Delhi: {delhi_count} | Mumbai: {mumbai_count}")
        print("-" * 50)

        insert_query = """
            INSERT INTO geo_location (user_name, text, location)
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (data['user_name'], data['text'], city))
        conn.commit()
        print("Tweet inserted into database")

        end_time = time.time()
        cpu = process.cpu_percent(interval=0.5)
        mem = process.memory_info().rss / 1024**2

        print("\nConsumer for geo-location tweets")
        print(f"Execution Time: {end_time - start_time:.2f} sec")
        print(f"CPU Usage: {cpu:.2f}%")
        print(f"Memory Usage: {mem:.2f} MB")

        time.sleep(5)

except KeyboardInterrupt:
    print("\n Stopping consumer...")
    consumer.close()
    conn.close()
