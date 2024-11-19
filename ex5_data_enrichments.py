from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

spark_session = SparkSession.builder \
    .appName("KafkaEnrichment") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Define schema for the Kafka message data
kafka_message_schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("event_id", IntegerType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])

# read sensors-sample topic from kafka
# TODO: fix data loss
events_df = spark_session.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "sensors-sample") \
    .option('startingOffsets', 'earliest') \
    .option("failOnDataLoss", "false") \
    .load() \
    
# cast kafka bytes data (value) to string(appears as json)
fixed_df = events_df.selectExpr("CAST(value AS STRING) as value")

# extract json to columns based on our schema 
fixed_df = fixed_df \
    .withColumn("parsed_json", F.from_json(F.col("value"), kafka_message_schema)) \
    .select("parsed_json.*")

# Load S3 data as dataframes
car_models_df = spark_session.read.parquet("s3a://spark/data/dims/car_models")
car_colors_df = spark_session.read.parquet("s3a://spark/data/dims/car_colors")
cars_df = spark_session.read.parquet("s3a://spark/data/dims/cars")

# Perform joins on our tables. using broadcast to save resources
enriched_df = fixed_df.join(F.broadcast(cars_df), 'car_id')\
                       .join(F.broadcast(car_models_df), 'model_id')\
                       .join(F.broadcast(car_colors_df), 'color_id')\
    .select(
        fixed_df["*"],
        cars_df.driver_id,
        car_models_df.car_brand.alias("brand_name"),
        car_models_df.car_model.alias("model_name"),
        car_colors_df.color_name
    )

# Calculate expected gear column
enriched_df = enriched_df.withColumn("expected_gear", F.round(F.col("speed") / 30).cast(IntegerType()))

enriched_df.printSchema()

# # print out df
# query = enriched_df.selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .trigger(processingTime='5 seconds') \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# kafka iterates each df's row, applying 'struct(*)' to each row, then it converts it to json and send it to kafka
query = enriched_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "samples-enriched") \
    .option("checkpointLocation", "s3a://spark/checkpoints/samples-enriched") \
    .start()

query.awaitTermination()

spark_session.stop()