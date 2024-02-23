import boto3
import json

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket names from the event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Download the file from the source bucket
    response = s3.get_object(Bucket=source_bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    
    # Upload the file to the second bucket
    destination_bucket = "sonu-bucket-2"
    s3.put_object(Body=content, Bucket=destination_bucket, Key=key)
    
    # Convert content to uppercase
    uppercase_content = content.upper()
    
    # Upload the uppercase content to the third bucket
    uppercase_bucket = "sonu-bucket-3"
    s3.put_object(Body=uppercase_content, Bucket=uppercase_bucket, Key=key)
    
    print(uppercase_content)
    
    return {
        'statusCode': 200,
        'body': json.dumps('File replicated and content converted to uppercase successfully!')
    }
