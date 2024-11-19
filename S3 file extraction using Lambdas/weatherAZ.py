import boto3
import os
from concurrent.futures import ThreadPoolExecutor
s3_client = boto3.client('s3')

def copy_object(source_bucket, destination_bucket, file_key):
    copy_source = {'Bucket': source_bucket, 'Key': file_key}
    s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=file_key)



def lambda_handler(event, context):
    source_bucket = os.getenv('SOURCE_BUCKET')
    destination_bucket = os.getenv('DESTINATION_BUCKET')
    prefix = 'weather/'
    continuation_token = None
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix, Delimiter='/')
    #print(f"Response {response}")
    if 'CommonPrefixes' in response:
        with ThreadPoolExecutor(max_workers = 150) as executor:
            for prefix_info in response['CommonPrefixes']:
                prefix = prefix_info['Prefix']
                #print(prefix)
                if 'Kyiv' in prefix:    
                    while True:
                        if not continuation_token:
                            objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
                        else:
                            objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix, ContinuationToken=continuation_token)
                        if 'Contents' in objects:
                            for obj in objects['Contents']:
                                file_key = obj['Key']
                                #file_key = ""
                                #print(f"Key {file_key}")
                                #print(obj['Prefix'])
                                executor.submit(copy_object, source_bucket, destination_bucket, file_key)
                        continuation_token = objects.get('NextContinuationToken')
                        if not continuation_token:
                            break
    return {'statusCode': 200, 'body': f"Transferred files from {source_bucket}/{prefix} to {destination_bucket}"}
