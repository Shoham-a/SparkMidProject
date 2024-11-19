from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, LongType

spark_session = SparkSession.builder \
    .appName("PrintData") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Define schema for the Kafka message data
kafka_message_schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("event_id", IntegerType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True),
    StructField("driver_id", LongType(), True),
    StructField("brand_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("color_name", StringType(), True),
    StructField("expected_gear", IntegerType(), True)
])

# read from alert-data topic
events_df = spark_session.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "alert-data") \
    .option('startingOffsets', 'earliest') \
    .option("failOnDataLoss", "false") \
    .load() \

# cast kafka bytes data (value) to string(appears as json)
events_df = events_df.selectExpr("CAST(value AS STRING) as value")

# extract json to columns based on our schema 
fixed_df = events_df \
    .withColumn("parsed_json", F.from_json(F.col("value"), kafka_message_schema)) \
    .select("parsed_json.*")

# perform calculations - removed time filter since it runs for too long
aggregated_df = fixed_df \
    .agg(
        F.count(F.col("event_id")).alias("num_of_rows"),
        F.sum(F.when(F.col("color_name") == "Black", 1).otherwise(0)).alias("num_of_black"),
        F.sum(F.when(F.col("color_name") == "White", 1).otherwise(0)).alias("num_of_white"),
        F.sum(F.when(F.col("color_name") == "Silver", 1).otherwise(0)).alias("num_of_silver"),
        F.max("speed").alias("maximum_speed"),
        F.max("gear").alias("maximum_gear"),
        F.max("rpm").alias("maximum_rpm")
    )

# Print the data
query = aggregated_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

spark_session.stop()