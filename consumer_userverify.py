from kafka import KafkaConsumer
import json
import psycopg2
import os

# PostgreSQL connection
# other users make a .env file and fill it 
DB_CONFIG = {
        'dbname': os.getenv('POSTGRES_DB', 'tweedbt'),
        'user': os.getenv('POSTGRES_USER', 'kk'),
        'password': os.getenv('POSTGRES_PASSWORD', 'admin'),
        'host': os.getenv('POSTGRES_HOST', '127.0.0.1'),
        'port': os.getenv('POSTGRES_PORT', '5432')
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
