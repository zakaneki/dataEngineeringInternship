import boto3
import csv
from io import StringIO

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Input bucket and output bucket names
    input_bucket = "first-bucket-az"
    output_bucket = "first-bucket-az"
    output_key = "processed/result.csv"

    try:
        # Extract file information from the event
        input_key = 'test.csv'

        # Read the CSV content from the source bucket
        response = s3_client.get_object(Bucket=input_bucket, Key=input_key)
        csv_content = response['Body'].read().decode('utf-8')

        # Process the CSV data (if needed, here it's directly passed as is)
        output_content = StringIO()
        writer = csv.writer(output_content)
        reader = csv.reader(StringIO(csv_content))
        for row in reader:
            writer.writerow(row)  # Modify this line to transform data if needed

        # Upload the processed CSV to the destination bucket
        s3_client.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=output_content.getvalue()
        )

        return {
            'statusCode': 200,
            'body': f"File processed and stored at {output_bucket}/{output_key}"
        }

    except Exception as e:
        print(f"Error processing file: {e}")
        raise
