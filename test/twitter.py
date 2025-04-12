import tweepy
import json
from os.path import exists
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Twitter API credentials from environment variables
bearer_token = os.getenv('BEARER_TOKEN')

# JSON file path
json_file_path = "tweets.json"

# If file doesn't exist, create it with an empty list
if not exists(json_file_path):
    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump([], f)

def fetch_tweets(client, query="python", max_results=10):
    """Fetch tweets using Twitter API v2"""
    try:
        tweets = client.search_recent_tweets(
            query=query,
            max_results=max_results,
            tweet_fields=['created_at', 'author_id']
        )

        if not tweets.data:
            return

        for tweet in tweets.data:
            tweet_data = {
                "username": tweet.author_id,
                "text": tweet.text,
                "created_at": str(tweet.created_at)
            }

            print(f"Tweet by @{tweet_data['username']}: {tweet_data['text']}")

            # Write to JSON
            with open(json_file_path, "r+", encoding="utf-8") as f:
                data = json.load(f)
                data.append(tweet_data)
                f.seek(0)
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.truncate()

    except tweepy.TweepyException as e:
        print(f"Error: {e}")

def main():
    client = tweepy.Client(bearer_token=bearer_token)

    while True:
        fetch_tweets(client, query="python")  # Change query as needed
        print("Waiting 60 seconds before next request...")
        time.sleep(60)  # Rate limit: 180 requests per 15 minutes for Basic access

if __name__ == "__main__":
    main()
