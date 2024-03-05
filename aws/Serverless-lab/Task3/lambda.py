import boto3
from io import StringIO

def lambda_handler(event, context):
    # Define the S3 bucket and file path
    bucket_name = 'datpipebaltin'
    input_key = event['Records'][0]['s3']['object']['key']

    # Check if the file is in the 'input' folder
    if not input_key.startswith('input/'):
        print(f"The file '{input_key}' is not in the 'input' folder. Skipping.")
        return

    # Create an S3 client
    s3 = boto3.client('s3')

    # Read the content of the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=input_key)
    content = response['Body'].read().decode('utf-8')

    # Transform the content to uppercase
    transformed_content = content.upper()

    # Upload the transformed content to the 'output' folder in the same bucket
    output_key = input_key.replace('input/', 'output/')
    s3.put_object(Body=transformed_content, Bucket=bucket_name, Key=output_key)

    print(f"File '{input_key}' successfully transformed and uploaded to '{output_key}' in S3.")
