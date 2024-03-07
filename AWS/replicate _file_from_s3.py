import boto3
import json
import os
s3 = boto3.client('s3')
def lambda_handler(event, context):
    # Get the source bucket and key from the S3 event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    # Copy the object to the destination bucket
    destination_bucket = 'taskk-bucket2'
    destination_key = source_key  # Optionally, you can change the key if needed
    s3.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key},
                   Bucket=destination_bucket, Key=destination_key)
    # Transform the content to uppercase and upload to Bucket C
    response = s3.get_object(Bucket=destination_bucket, Key=destination_key)
    content = response['Body'].read().decode('utf-8').upper()
    destination_bucket_c = 'taskk-bucket3'
    transformed_key = destination_key  # Optionally, you can change the key if needed
    s3.put_object(Bucket=destination_bucket_c, Key=transformed_key, Body=content)
    # Print the transformed content (optional)
    if transformed_key.endswith('.txt'):
        filename = os.path.basename(source_key)  # Get the filename from the source key
        local_file_path = os.path.join(os.path.expanduser('~'), 'Downloads', filename)
        s3.download_file(destination_bucket_c, transformed_key, local_file_path)
        print(f"Downloaded {transformed_key} to {local_file_path}")
    else:
        print(f"Ignored non-txt file: {transformed_key}")