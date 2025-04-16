from kafka import KafkaConsumer
import json
import time
import ast  # to handle string rep of lists
import psycopg2
import os

# PostgreSQL connection
DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'tweedbt'),
    'user': os.getenv('POSTGRES_USER', 'kk'),
    'password': os.getenv('POSTGRES_PASSWORD', 'admin'),
    'host': os.getenv('POSTGRES_HOST', '127.0.0.1'),
    'port': os.getenv('POSTGRES_PORT', '5432')
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

print("Waiting for tweets with RCB/CSK hashtags...")
print("-" * 50)

for msg in consumer:
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

            time.sleep(5)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
        consumer.close()
        break
    except Exception as e:
        print("Error:", e)
        continue
