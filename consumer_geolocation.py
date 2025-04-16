from kafka import KafkaConsumer
import json
import re
import time

# Precompiled regex patterns
delhi_pattern = re.compile(r"\b(new\s*)?delhi\b", re.IGNORECASE)
mumbai_pattern = re.compile(r"\bmumbai\b", re.IGNORECASE)

consumer = KafkaConsumer(
    'GeoLocation',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id=None,  # Removed group_id to receive all messages
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

delhi_count = 0
mumbai_count = 0

print("Monitoring tweets from Delhi and Mumbai...")
print("-" * 50)

try:
    for msg in consumer:
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
        time.sleep(5)  # Add delay between tweets

except KeyboardInterrupt:
    print("\nStopping consumer...")
    consumer.close()
