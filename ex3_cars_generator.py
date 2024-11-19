from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import random

# init spark session
spark_session = SparkSession.builder \
    .appName('cars') \
    .master("local") \
    .getOrCreate()

# define our schema
car_schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("driver_id", LongType(), True),
    StructField("model_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True)
])

# Generate random data (20 rows)
data = [
    (
        random.randint(1000000, 9999999),      # car_id
        random.randint(100000000, 999999999),  # driver_id
        random.randint(1, 6),                  # model_id
        random.randint(1, 6)                   # color_id
    )
    for _ in range(20)
]

# Create DataFrame using the generated data
df = spark_session.createDataFrame(data, schema=car_schema)

# show dataframe+schema (truncate so it will display whole number)
df.show(truncate=False)
df.printSchema()

# write to s3 in overwrite mode
try:
    df.write.parquet(f's3a://spark/data/dims/{spark_session.sparkContext.appName}', mode='overwrite')
    print(f"Sucessfuly written data in .parquet format to s3a://spark/data/dims/{spark_session.sparkContext.appName}")
except Exception as e:
    print(f"Error writing to S3 bucket: {e}")
# stop spark run
spark_session.stop()