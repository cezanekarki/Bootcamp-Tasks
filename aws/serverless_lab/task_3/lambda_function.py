import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket names from the event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Download the file from the source bucket
    response = s3.get_object(Bucket=source_bucket, Key=key)
    content = response['Body'].read().decode('utf-8')

    # Convert content to uppercase
    uppercase_content = content.upper()
    
    # Upload the uppercase content to the next bucket
    uppercase_bucket = "utshalab3bucket2"
    s3.put_object(Body=uppercase_content, Bucket=uppercase_bucket, Key=key)
    
    # Return a response
    return {
        'statusCode': 200,
        'body': json.dumps('Data of the file transformed and uploaded successfully')
    }
