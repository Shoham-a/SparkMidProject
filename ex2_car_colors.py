from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# init spark session
spark_session = SparkSession.builder \
    .appName('car_colors') \
    .master("local")\
    .getOrCreate()

# Define dataframe's schema. nullable=True
car_colors_schema = StructType([
    StructField("color_id", IntegerType(), True),
    StructField("color_name", StringType(), True)
])

# define our data for the DataFrame
car_colors_data = [
    (1, "Black"),
    (2, "Red"),
    (3, "Gray"),
    (4, "White"),
    (5, "Green"),
    (6, "Blue"),
    (7, "Pink")
]

# Create a DataFrame to display and write the data with to s3
df = spark_session.createDataFrame(car_colors_data, schema=car_colors_schema)

# show dataframe+schema
df.show()
df.printSchema()

# write to s3 bucket, using overwrite mode so whenever we run it it'll write the whole data 
try:
    df.write.parquet(f's3a://spark/data/dims/{spark_session.sparkContext.appName}', mode='overwrite')
    print(f"Sucessfuly written data in .parquet format to s3a://spark/data/dims/{spark_session.sparkContext.appName}")
except Exception as e:
    print(f"Error writing to S3 bucket: {e}")
# stop spark run
spark_session.stop()


