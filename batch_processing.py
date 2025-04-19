from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, to_timestamp
import psycopg2
import time
import psutil
import os
from dotenv import load_dotenv

load_dotenv()

# Spark session setup
spark = SparkSession.builder \
    .appName("Independent Batch Processing") \
    .getOrCreate()

# Load CSV and limit to 1500 rows
df = spark.read.csv("IPL_2022_tweets.csv", header=True, inferSchema=True)
df = df.limit(1500)

# Add timestamp column
df = df.withColumn("timestamp", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

# Filter datasets
verified_df = df.filter(lower(trim(col("user_verified"))) == "true")
geo_df = df.filter(lower(col("user_location")).rlike(r"\b(new\s*)?delhi\b|\bmumbai\b"))
team_df = df.filter(lower(col("hashtags")).rlike(r'.*?(rcb|csk).*?'))

# PostgreSQL DB config
DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

# Check for missing env vars
required_vars = ['POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_HOST', 'POSTGRES_PORT']
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(1)

# Insert helper
def insert_data(df, table, cols):
    for row in df.collect():
        try:
            values = tuple(row[col] for col in cols)
            placeholders = ','.join(['%s'] * len(cols))
            query = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
            cursor.execute(query, values)
            conn.commit()
        except Exception as e:
            print(f"[ERROR] Inserting into {table}: {e}")
            conn.rollback()

# Start timing and resource tracking
start_time = time.time()
process = psutil.Process()
cpu_start = process.cpu_times()

# Insert data into respective tables
insert_data(verified_df, "verified_tweets_batch", ["user_name", "text", "user_verified"])
insert_data(geo_df, "geo_tweets_batch", ["user_name", "text", "user_location", "user_verified"])
insert_data(team_df, "team_tweets_batch", ["user_name", "text", "hashtags", "user_verified"])

# End timing and calculate CPU usage manually
end_time = time.time()
cpu_end = process.cpu_times()
cpu_time_used = (cpu_end.user + cpu_end.system) - (cpu_start.user + cpu_start.system)
wall_time = end_time - start_time
cpu_percent = (cpu_time_used / wall_time) * 100 if wall_time > 0 else 0
mem = process.memory_info().rss / 1024**2

# Report
print("\n✅ Batch Job Complete")
print(f"Execution Time: {wall_time:.2f} sec")
print(f"CPU Usage: {cpu_percent:.2f}%")
print(f"Memory Usage: {mem:.2f} MB")

conn.close()
