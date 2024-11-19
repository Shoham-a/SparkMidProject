from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, LongType

spark_session = SparkSession.builder \
    .appName("AlertDetection") \
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



events_df = spark_session.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "samples-enriched") \
    .option('startingOffsets', 'earliest') \
    .option("failOnDataLoss", "false") \
    .load() \
    
# cast kafka bytes data (value) to string(appears as json)
events_df = events_df.selectExpr("CAST(value AS STRING) as value")

# extract json to columns based on our schema 
fixed_df = events_df \
    .withColumn("parsed_json", F.from_json(F.col("value"), kafka_message_schema)) \
    .select("parsed_json.*")


# Apply filters based on the given conditions
result_df = fixed_df.filter(
    (F.col("speed") > 120) & 
    (F.col("expected_gear") != F.col("gear")) & 
    (F.col("rpm") > 6000)
)

# # Print out data
# query = fixed_df.selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .trigger(processingTime='5 seconds') \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# kafka iterates each df's row, applying 'struct(*)' to each row, then it converts it to json and send it to kafka
query = result_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .trigger(processingTime='1 seconds') \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "alert-data") \
    .option("checkpointLocation", "s3a://spark/checkpoints/alert-data") \
    .start()

query.awaitTermination()

spark_session.stop()