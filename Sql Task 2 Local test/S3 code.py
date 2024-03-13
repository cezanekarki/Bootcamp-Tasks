import boto3
from io import BytesIO
import zipfile
import pandas as pd
from datetime import datetime

def lambda_handler(event, context):
    s3_client = boto3.client('s3')

   
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
    compressed_data = response['Body'].read()

    compressed_file = BytesIO(compressed_data)

    with zipfile.ZipFile(compressed_file, 'r') as zip_ref:
        zip_ref.extractall('/tmp')

    target_bucket = 'forzipfile'

    before_conversion_dict = {}
    after_conversion_dict = {}
    transformed_data = {}  

    for file_name in zip_ref.namelist():
        target_key = f'unzipped/{file_name}'  # Adjust the target key as needed

        df = read_csv_from_zip(zip_ref, file_name)
        before_conversion_dict[file_name] = df.shape[0]


        transformed_data[file_name] = df  # Store the DataFrame for later use
        save_txt_file(df, target_bucket, target_key)

        after_conversion_dict[file_name] = df.shape[0]

    print("Before Conversion:", before_conversion_dict)
    print("After Conversion:", after_conversion_dict)

    replicate_file_to_s3(target_bucket, target_key, 'taskk-bucket2')

    print("Transformation Completed")

def save_txt_file(df, bucket, key):
    txt_file_content = df.to_csv(index=False, sep=',').encode('utf-8')
    s3_client.put_object(Body=txt_file_content, Bucket=bucket, Key=key)

def replicate_file_to_s3(source_bucket, source_key, destination_bucket):
    try:
        destination_key = source_key  # Optionally, you can change the key if needed
        s3_client.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Bucket=destination_bucket,
            Key=destination_key
        )
        print(f"Replicated {source_key} to {destination_bucket}")
    except Exception as e:
        print(f"An error occurred during replication: {e}")

def read_csv_from_zip(zip_ref, file):
    # Function to read a CSV file from a Zip archive
    with zip_ref.open(file) as file_data:
        return pd.read_csv(file_data, sep='|', header=None, encoding='utf-8', low_memory=False)
