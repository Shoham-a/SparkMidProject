from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from time import sleep

# Init Spark session
spark_session = SparkSession.builder \
    .appName('events_generator') \
    .master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()    

# schema for reading generated cars
car_schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("driver_id", LongType(), True),
    StructField("model_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True)
])

# Read the cars data from S3
generated_cars = spark_session.read \
    .schema(car_schema) \
    .parquet('s3a://spark/data/dims/cars') \
    .cache() 

# Set variables for streaming values
MAX_SPEED = 200
MAX_RPM = 8000
MAX_GEAR = 6

while True:
    # generate events values
    events = generated_cars.select(F.col('car_id').cast("int")) \
        .withColumn('event_id', F.round(F.rand() * 10000000).cast("int")) \
        .withColumn('event_time', F.current_timestamp()) \
        .withColumn('speed', F.round(F.rand() * MAX_SPEED).cast("int")) \
        .withColumn('rpm', F.round(F.rand() * MAX_RPM).cast("int")) \
        .withColumn('gear', (F.round(F.rand() * MAX_GEAR) + 1).cast("int"))

    # kafka iterates each df's row, applying 'struct(*)' to each row, then it converts it to json and send it to kafka
    events.selectExpr("to_json(struct(*)) AS value")  \
        .write \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("topic", "sensors-sample") \
        .option("checkpointLocation", "s3a://spark/checkpoints/sensors-sample") \
        .save()

    # Sleep for a second before generating the next batch
    sleep(1)
