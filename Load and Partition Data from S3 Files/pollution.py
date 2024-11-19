import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import col, to_timestamp

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define S3 source and destination paths
source_bucket = "s3://first-bucket-az/pollution/**/*"
destination_bucket = "s3://first-bucket-az/partitioned-data-pollution/"

# Define the schema explicitly based on the input file structure
schema = StructType([
    StructField("name", StringType(), True),  # Location name
    StructField("time_nano", LongType(), True),  # Timestamp in nanoseconds
    StructField("time_date", StringType(), True),  # Date and time as a string (convert later if needed)
    StructField("location_latitude", DoubleType(), True),  # Latitude
    StructField("location_longitude", DoubleType(), True),  # Longitude
    StructField("location_name", StringType(), True),  # Location description
    StructField("measurement_pm10Atmo", DoubleType(), True),  # PM10 (nullable)
    StructField("measurement_pm25Atmo", DoubleType(), True),  # PM2.5
    StructField("measurement_pm100Atmo", DoubleType(), True)  # PM100
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
weather_df = weather_df.withColumn("time_date", to_timestamp(col("time_date"), "yyyy-MM-dd HH:mm:ss"))

# Partition the data by the time_date column
partitioned_df = weather_df.repartition(4, "time_date")

# Save the partitioned DataFrame to S3 in a new location as Parquet
partitioned_df.write \
    .partitionBy("time_date") \
    .mode("overwrite") \
    .format("parquet") \
    .save(destination_bucket)

# Print a sample of the data to ensure it's partitioned correctly
partitioned_df.show(10)
