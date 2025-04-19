from kafka import KafkaConsumer
import json
import time
import ast  # to handle string rep of lists
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
    'TeamSpecific',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id=None
)

process = psutil.Process()
print("Waiting for tweets with RCB/CSK hashtags...")
print("-" * 50)

for msg in consumer:
    start_time = time.time()

    tweet = msg.value
    try:
        hashtags = ast.literal_eval(tweet['hashtags'])
        team_hashtags = [tag for tag in hashtags if 'rcb' in tag.lower() or 'csk' in tag.lower()]

        if team_hashtags:
            print(f"\nUser: {tweet['user_name']}")
            print(f"Tweet: {tweet['text']}")
            formatted_hashtags = '[' + ', '.join(str(tag) for tag in team_hashtags).replace("'", "") + ']'
            print(f"Team Hashtags: {formatted_hashtags}")
            print(f"Date: {tweet['date']}")
            print("-" * 50)

            # Insert into PostgreSQL
            insert_query = """
                INSERT INTO team_mentions (user_name, text, hashtags)
                VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, (tweet['user_name'], tweet['text'], formatted_hashtags))
            conn.commit()
            print(f"Inserted tweet")

            end_time = time.time()
            cpu = process.cpu_percent(interval=1)
            mem = process.memory_info().rss / 1024**2

            print("\nConsumer for team mentions")
            print(f"Execution Time:{end_time - start_time:.2f} sec")
            print(f"CPU Usage: {cpu:.2f}%")
            print(f"Memory Usage: {mem:.2f} MB")

            time.sleep(5)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
        consumer.close()
        break
    except Exception as e:
        print("Error:", e)
        continue
