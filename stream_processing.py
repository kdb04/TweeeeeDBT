# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, lower, trim, current_timestamp, window, collect_list, date_format, count, to_timestamp, collect_list
# from pyspark.sql.types import StructType, StringType

# # Start Spark session
# spark = SparkSession.builder \
#     .appName("IPL Kafka Stream Processor") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Define schema of your IPL tweet
# schema = StructType() \
#     .add("user_name", StringType()) \
#     .add("user_location", StringType()) \
#     .add("user_description", StringType()) \
#     .add("user_created", StringType()) \
#     .add("user_followers", StringType()) \
#     .add("user_friends", StringType()) \
#     .add("user_favourites", StringType()) \
#     .add("user_verified", StringType()) \
#     .add("date", StringType()) \
#     .add("text", StringType()) \
#     .add("hashtags", StringType()) \
#     .add("source", StringType()) \
#     .add("is_retweet", StringType())

# # Read from the raw Kafka topic
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "ipl_raw") \
#     .option("startingOffsets", "earliest") \
#     .option("failOnDataLoss", "false") \
#     .load()

# # Convert Kafka value to string and parse JSON
# parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
#     .select(from_json(col("json"), schema).alias("data")) \
#     .select("data.*")

# # Convert date string to actual timestamp
# parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))


# # 1. Verified users filter
# verified_df = parsed_df.filter(lower(trim(col("user_verified"))) == "true")

# # Add event time for windowing
# verified_df_with_time = verified_df.withColumn("event_time", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))

# # Count number of verified tweets in 10-second windows
# verified_windowed_count = verified_df_with_time \
#     .groupBy(window(col("event_time"), "10 seconds")) \
#     .count() \
#     .orderBy("window")


# """ COUNTING TWEETS (verified users) WITHIN A WINDOW OF 10 SECONDS"""
# verified_windowed_msgs = verified_df_with_time \
#     .withWatermark("event_time", "1 minute") \
#     .groupBy(window(col("event_time"), "10 seconds")) \
#     .agg(
#         count("*").alias("count"),
#         collect_list("user_name").alias("usernames")
#     ) \
#     .orderBy("window.start")


# # Format nicely
# pretty_df = verified_df \
#     .withWatermark("timestamp", "1 minute") \
#     .groupBy(window(col("timestamp"), "10 seconds")) \
#     .agg(
#         collect_list("user_name").alias("usernames"),
#         count("*").alias("count")
#     ) \
#     .select(
#         date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("start_time"),
#         date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("end_time"),
#         col("count"),
#         col("usernames")
#     ) \
#     .orderBy("start_time")


# # Writing to console 
# pretty_df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()


# # 2. Geo-tagged tweets (assuming `user_location` is not null/empty)
# geo_df = parsed_df.filter(col("user_location").isNotNull() & (col("user_location") != ""))

# # 3. Team-specific mentions (checking hashtags for team mentions)
# team_df = parsed_df.filter(
#     lower(col("hashtags")).rlike(r'\[.*?(rcb|csk).*?\]')  # Look for RCB or CSK within the list of hashtags in csv
# )

# # Function to write to Kafka
# def write_to_kafka(df, topic):
#     df.selectExpr("to_json(struct(*)) AS value") \
#       .writeStream \
#       .format("kafka") \
#       .option("kafka.bootstrap.servers", "localhost:9092") \
#       .option("topic", topic) \
#       .option("checkpointLocation", f"/tmp/checkpoint_{topic}") \
#       .start()




# write_to_kafka(verified_df, "VerifiedUserCheck")
# write_to_kafka(geo_df, "GeoLocation")
# write_to_kafka(team_df, "TeamSpecific")

# # Keep it running
# spark.streams.awaitAnyTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, trim, to_timestamp, window, collect_list, date_format, count
from pyspark.sql.types import StructType, StringType

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

# Convert date string to timestamp
parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

# Verified users only
verified_df = parsed_df.filter(lower(trim(col("user_verified"))) == "true")

# Function to write any DF to Kafka
def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value") \
      .writeStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("topic", topic) \
      .option("checkpointLocation", f"/tmp/checkpoint_{topic}") \
      .start()

# Function: Verified User Windowed Count to Kafka
def verified_user_windowed_count_to_kafka(df, topic):
    df_with_event_time = df.withColumn("event_time", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

    aggregated_df = df_with_event_time \
        .withWatermark("event_time", "1 minute") \
        .groupBy(window(col("event_time"), "10 seconds")) \
        .agg(
            count("*").alias("count"),
            collect_list("user_name").alias("usernames")
        ) \
        .select(
            date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("start_time"),
            date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("end_time"),
            col("count"),
            col("usernames")
        ) \
        .orderBy("start_time")

    aggregated_df \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", topic) \
        .option("checkpointLocation", f"/tmp/checkpoint_{topic}") \
        .outputMode("complete") \
        .start()

# 1. Send verified user tweets to Kafka
write_to_kafka(verified_df, "VerifiedUserCheck")

# 2. Geo-tagged tweets
geo_df = parsed_df.filter(col("user_location").isNotNull() & (col("user_location") != ""))
write_to_kafka(geo_df, "GeoLocation")

# 3. Team-specific mentions (e.g., RCB, CSK)
team_df = parsed_df.filter(
    lower(col("hashtags")).rlike(r'\[.*?(rcb|csk).*?\]')
)
write_to_kafka(team_df, "TeamSpecific")

# 4. Windowed count of verified users to Kafka
verified_user_windowed_count_to_kafka(verified_df, "VerifiedUserWindowedCount")

# Keep streaming job alive
spark.streams.awaitAnyTermination()
