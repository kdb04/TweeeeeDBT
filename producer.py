from kafka import KafkaProducer
import json
import pandas as pd
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv("IPL_2022_tweets.csv")

for _, row in df.head(1500).iterrows():
    data = row.to_dict()
    producer.send("ipl_raw", value=data)
    time.sleep(0.5)  # simulate streaming
