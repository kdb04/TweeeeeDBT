from kafka import KafkaConsumer
import json
import time
import ast  #to handle string rep of lists

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
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        consumer.close()
        break
    except:
        continue

