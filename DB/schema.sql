-- schema.sql

CREATE TABLE IF NOT EXISTS verified_tweets (
    id SERIAL PRIMARY KEY,
    user_name TEXT NOT NULL,
    user_verified BOOLEAN NOT NULL,
    text TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_mentions (
    id SERIAL PRIMARY KEY,
    user_name TEXT NOT NULL,
    text TEXT NOT NULL,
    hashtags TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS geo_location (
    id SERIAL PRIMARY KEY,
    user_name TEXT NOT NULL,
    text TEXT NOT NULL,
    location TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE verified_tweets_batch (
    id SERIAL PRIMARY KEY,
    user_name TEXT,
    text TEXT,
    user_verified BOOLEAN
);

CREATE TABLE geo_tweets_batch (
    id SERIAL PRIMARY KEY,
    user_name TEXT,
    text TEXT,
    user_location TEXT,
    user_verified BOOLEAN
);

CREATE TABLE team_tweets_batch (
    id SERIAL PRIMARY KEY,
    user_name TEXT,
    text TEXT,
    hashtags TEXT,
    user_verified BOOLEAN
);

CREATE TABLE IF NOT EXISTS batch_metrics (
    id SERIAL PRIMARY KEY,
    execution_time FLOAT,
    cpu_percent FLOAT,
    memory_usage_mb FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


TRUNCATE verified_tweets;
TRUNCATE team_mentions;
TRUNCATE geo_location;
TRUNCATE verified_tweets_batch;
TRUNCATE geo_tweets_batch;
TRUNCATE team_tweets_batch;