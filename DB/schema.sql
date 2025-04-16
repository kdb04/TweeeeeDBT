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