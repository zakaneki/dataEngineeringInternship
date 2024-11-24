import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from fetch import get_city_tourist_estimate  # Import the function you created
import concurrent.futures

# Initialize the S3 client
s3_client = boto3.client('s3')

def process_s3_file(bucket_name, key):
    """
    Processes a single S3 file, adds tourist estimate, and uploads the enriched file back to the S3 bucket.
    """
    try:
        print(f"Processing: {key}")

        # Skip files that already have '_enriched' in their name
        if key.endswith('_enriched'):
            print(f"Skipping already enriched file: {key}")
            return

        # Extract city and date from folder structure
        parts = key.split('/')
        if len(parts) < 3:  # Ensure folder structure is valid
            print(f"Skipping invalid key: {key}")
            return

        city = 'Kyiv'  # You can modify this for other cities as necessary

        # Extract the date from the folder name (dd-mm-yyyy)
        folder_date = parts[-2]
        try:
            # Convert date from dd-mm-yyyy to yyyy-mm-dd
            date = datetime.strptime(folder_date, '%d-%m-%Y').strftime('%Y-%m-%d')
        except ValueError:
            print(f"Skipping invalid date format: {folder_date}")
            return

        # Download the file
        obj_body = s3_client.get_object(Bucket=bucket_name, Key=key)['Body']
        file_content = obj_body.read().decode('utf-8')

        # Check if the content can be read as CSV (skip if not)
        try:
            df = pd.read_csv(StringIO(file_content))  # Attempt to read CSV
        except Exception as e:
            print(f"Skipping non-CSV file {key}: {e}")
            return

        # Fetch tourist estimate
        estimate = get_city_tourist_estimate(city, date)

        if estimate is not None:
            # Add tourist estimate column
            df['tourist_estimation'] = estimate

            # Create a new filename with 'enriched' appended to the original filename
            enriched_key = key + '_enriched'

            # Convert DataFrame back to CSV
            enriched_csv = StringIO()
            df.to_csv(enriched_csv, index=False)
            enriched_csv.seek(0)

            # Upload the enriched CSV back to the bucket with a new filename
            s3_client.put_object(Body=enriched_csv.getvalue(), Bucket=bucket_name, Key=enriched_key)
            print(f"Enriched file saved: {enriched_key}")
        else:
            print(f"No estimate available for {city} on {date}. File skipped.")
    except Exception as e:
        print(f"Error processing file {key}: {e}")

def process_s3_bucket(bucket_name):
    """
    Enrich files in the 'weather' and 'pollution' folders of an S3 bucket with tourist estimates.
    New enriched files are saved with 'enriched' appended to their filenames.

    Args:
        bucket_name (str): Name of the S3 bucket.
    """
    allowed_prefixes = ['weather/', 'pollution/']
    paginator = s3_client.get_paginator('list_objects_v2')

    # Using ThreadPoolExecutor to speed up processing of multiple files
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=""):
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                key = obj['Key']

                # Skip directories and files outside 'weather/' and 'pollution/'
                if key.endswith('/') or not any(key.startswith(prefix) for prefix in allowed_prefixes):
                    continue

                # Submit the file for processing in parallel
                futures.append(executor.submit(process_s3_file, bucket_name, key))

        # Wait for all threads to finish
        for future in concurrent.futures.as_completed(futures):
            future.result()  # We don't need the result, just wait for completion

if __name__ == "__main__":
    bucket_name = "first-bucket-az"
    process_s3_bucket(bucket_name)
