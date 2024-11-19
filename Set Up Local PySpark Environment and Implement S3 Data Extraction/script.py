from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws
import boto3
import os
os.environ['HADOOP_HOME'] = "C:\\Users\\AndrijaZakic\\Downloads\\hadoop-2.8.1"
os.environ['HADOOP_CONF_DIR'] = "C:\\Users\\AndrijaZakic\\Downloads\\hadoop-2.8.1\\bin"

# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .config("spark.security.manager.enabled", "false") \
    .getOrCreate()


# S3 bucket details
bucket_name = "first-bucket-az"
prefix = "weather/"  # Base folder
local_folder = "/weather_data/"  # Temporary folder for local storage

# Use boto3 to list files from S3
s3_client = boto3.client('s3')

def list_files(bucket, prefix):
    """List all files in an S3 bucket under a given prefix."""
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page.get('Contents', []):
            files.append(content['Key'])
    return files

# Fetch all files under the weather folder
files = list_files(bucket_name, prefix)

# Download files locally for processing
#for file in files:
 #   local_path = 'C:\\Users\\AndrijaZakic\\Downloads\\weather_data\\' + file.split('/')[-1]
  #  s3_client.download_file(bucket_name, file, local_path)

# Read all local CSV files into a DataFrame
weather_df = spark.read.csv(
    'C:\\Users\\AndrijaZakic\\Downloads\\weather_data\\*',
    header=True,
    inferSchema=True
)

# Process the data: Extract name, ISO datetime, temperature, and wind speed (convert to km/h)
processed_df = weather_df \
    .withColumn("datetime_iso", col("time_date")) \
    .withColumn("wind_speed_kmh", col("weather_windSpeed") * 3.6) \
    .select(
        col("location_name").alias("name"),
        col("datetime_iso"),
        col("weather_temperature").alias("temperature"),
        col("wind_speed_kmh")
    )

# Save the processed DataFrame locally
output_path = "./processed_weather_data.csv"
processed_df.coalesce(1).write.csv(
    path=output_path,
    mode="overwrite",
    header=True,
    sep=";"
)

print(f"Processed file saved at {output_path}")

# Stop Spark session
spark.stop()
