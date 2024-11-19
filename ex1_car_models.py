from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# init spark session
spark_session = SparkSession.builder \
    .appName('car_models') \
    .master("local")\
    .getOrCreate()

# Define dataframe's schema. nullable=True
car_models_schema = StructType([
    StructField("model_id", IntegerType(), True),
    StructField("car_brand", StringType(), True),
    StructField("car_model", StringType(), True)
])

# define our data for the DataFrame
car_models_data = [
    (1, "Mazda", "3"),
    (2, "Mazda", "6"),
    (3, "Toyota", "Corolla"),
    (4, "Hyundai", "i20"),
    (5, "Kia", "Sportage"),
    (6, "Kia", "Rio"),
    (7, "Kia", "Picanto")
]

# Create a DataFrame to display and write the data with to s3
df = spark_session.createDataFrame(car_models_data, schema=car_models_schema)

# show dataframe+schema
df.show(truncate=False)
df.printSchema()

# write to s3 bucket, using overwrite mode so whenever we run it it'll write the whole data 
try:
    df.write.parquet(f's3a://spark/data/dims/{spark_session.sparkContext.appName}', mode='overwrite')
    print(f"Sucessfuly written data in .parquet format to s3a://spark/data/dims/{spark_session.sparkContext.appName}")
except Exception as e:
    print(f"Error writing to S3 bucket: {e}")
# stop spark run
spark_session.stop()


