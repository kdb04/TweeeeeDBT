from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, trim
from pyspark.sql.types import StructType, StringType, BooleanType, ArrayType

# Start Spark session
spark = SparkSession.builder \
    .appName("IPL Kafka Stream Processor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema of your IPL tweet
schema = StructType() \
    .add("user_name", StringType()) \
    .add("user_location", StringType()) \
    .add("user_description", StringType()) \
    .add("user_created", StringType()) \
    .add("user_followers", StringType()) \
    .add("user_friends", StringType()) \
    .add("user_favourites", StringType()) \
    .add("user_verified", StringType()) \
    .add("date", StringType()) \
    .add("text", StringType()) \
    .add("hashtags", StringType()) \
    .add("source", StringType()) \
    .add("is_retweet", StringType())

# Read from the raw Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ipl_raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert Kafka value to string and parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 1. Verified users filter
verified_df = parsed_df.filter(lower(trim(col("user_verified"))) == "true")

# 2. Geo-tagged tweets (assuming `user_location` is not null/empty)
geo_df = parsed_df.filter(col("user_location").isNotNull() & (col("user_location") != ""))

# 3. Team-specific mentions (checking hashtags for team mentions)
team_df = parsed_df.filter(
    lower(col("hashtags")).rlike(r'\[.*?(rcb|csk).*?\]')  # Look for RCB or CSK within the list of hashtags in csv
)

# Function to write to Kafka
def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value") \
      .writeStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("topic", topic) \
      .option("checkpointLocation", f"/tmp/checkpoint_{topic}") \
      .start()

write_to_kafka(verified_df, "VerifiedUserCheck")
write_to_kafka(geo_df, "GeoLocation")
write_to_kafka(team_df, "TeamSpecific")

# Keep it running
spark.streams.awaitAnyTermination()
