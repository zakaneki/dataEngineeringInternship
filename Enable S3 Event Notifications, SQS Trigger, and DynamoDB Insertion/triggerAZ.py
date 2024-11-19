import json
import boto3
import os
from datetime import datetime

# Initialize DynamoDB client (if required for inserting data)
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    # Define the allowed folder prefixes
    allowed_prefixes = ["pollution/", "sensor/", "weather/"]
    
    # Loop through all messages in the SQS event
    for record in event['Records']:
        # Decode the SQS message body
        sqs_message_body = json.loads(record['body'])
        
        # Extract the S3 event details
        s3_event = sqs_message_body['Records'][0]['s3']
        
        # Get the object key (file name with prefix)
        object_key = s3_event['object']['key']
        
        # Check if the object key starts with an allowed prefix
        if not any(object_key.startswith(prefix) for prefix in allowed_prefixes):
            error_message = f"Error: The file {object_key} is not in an allowed folder. Skipping processing."
            print(error_message)
            raise Exception(error_message)  # Raise exception to trigger Lambda failure and send to DLQ
        
        # Remove the prefix from the file name (if necessary)
        for prefix in allowed_prefixes:
            if object_key.startswith(prefix):
                object_key = object_key[len(prefix):]
                break
        
        # Extract the file name after the last "/"
        file_name = object_key.rsplit('/', 1)[-1]
        
        # Get the event time (timestamp)
        event_time = sqs_message_body['Records'][0]['eventTime']
        
        # Convert the timestamp to a human-readable format
        event_time_human = datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        
        # Print file details for debugging
        print(f"Processing file: {file_name}")
        print(f"Event Time (UTC): {event_time_human}")
        
        # Optionally insert details into DynamoDB
        dynamodb.put_item(
            TableName=os.getenv('TABLE_NAME'),
            Item={
                'FileName': {'S': file_name},
                'Timestamp': {'S': event_time_human.isoformat()},
                'Status': {'N': '0'}  # Initial status set to 0
            }
        )
        print(f"Inserted record for {file_name} into DynamoDB")
    
    return {
        'statusCode': 200,
        'body': 'Successfully processed S3 events'
    }
