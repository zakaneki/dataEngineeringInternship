import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import from_unixtime

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define S3 source and destination paths
source_bucket = "s3://first-bucket-az/sensor/**/*"
destination_bucket = "s3://first-bucket-az/partitioned-data-sensor/"

# Define the schema explicitly based on the input file structure
schema = StructType([
    StructField("time_nano", LongType(), True),  # Timestamp in nanoseconds
    StructField("location_latitude", DoubleType(), True),  # Latitude
    StructField("location_longitude", DoubleType(), True),  # Longitude
    StructField("location_name", StringType(), True),  # Location description
    StructField("name", StringType(), True),  # Device or source name
    StructField("pms7003Measurement_pm10Atmo", LongType(), True),  # PM10
    StructField("pms7003Measurement_pm25Atmo", LongType(), True),  # PM2.5
    StructField("pms7003Measurement_pm100Atmo", LongType(), True),  # PM100
    StructField("bmp280Measurement_temperature", DoubleType(), True),  # Temperature
    StructField("bmp280Measurement_pressure", DoubleType(), True),  # Pressure
    StructField("dht11Measurement_humidity", LongType(), True)  # Humidity
])

# Read the files from the S3 bucket using the specified schema
weather_df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .schema(schema) \
    .csv(source_bucket)

# Check schema and preview the data
weather_df.printSchema()
print("Count of rows in weather_df:", weather_df.count())
weather_df.show(5)

# Convert the time_date column to a proper timestamp type
weather_df = weather_df.withColumn("partition_date", from_unixtime(col("time_nano") / 1e9, "yyyy-MM-dd"))

# Partition the data by the time_date column
partitioned_df = weather_df.repartition(4, "time_nano")

# Save the partitioned DataFrame to S3 in a new location as Parquet
partitioned_df.write \
    .partitionBy("partition_date") \
    .mode("overwrite") \
    .format("parquet") \
    .save(destination_bucket)

# Print a sample of the data to ensure it's partitioned correctly
partitioned_df.show(10)
