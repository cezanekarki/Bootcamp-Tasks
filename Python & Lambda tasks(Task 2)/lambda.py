import json
import boto3
from zipfile import ZipFile
import pandas as pd
from io import BytesIO
from datetime import datetime
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Extract bucket and key information from the S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        if key.endswith('.xlsx'):
            handle_excel_upload(bucket, key)
        elif key.endswith('.zip'):
            handle_zip_upload(bucket, key)

        # Read the zip file directly from S3
        zip_file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        zip_file_content = BytesIO(zip_file_obj['Body'].read())

        # Perform RxNorm transformation
        rxnorm_transformation(zip_file_content, bucket, key)

    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error occurred during transformation')
        }

def rxnorm_transformation(zip_file_content, bucket, zip_key):
    try:
        print("Transformation Started")
        before_conversion_dict = {}
        after_conversion_dict = {}
        transformed_data = {}  # Placeholder for your transformed data

        # Read the zip file directly from memory
        with ZipFile(zip_file_content, 'r') as zip_ref:
            rrf_files = [file[len('rrf/'):] for file in zip_ref.namelist() if file.startswith('rrf/')]
            sorted_rrf_files = sorted(rrf_files)

            if sorted_rrf_files:
                # Find the Excel file dynamically
                excel_key = find_file_in_bucket(bucket, 'RxNorm_Header.xlsx')

                if not excel_key:
                    print("Excel file not found in the bucket.")
                    return

                excel_file_obj = s3_client.get_object(Bucket=bucket, Key=excel_key)
                excel_file_content = BytesIO(excel_file_obj['Body'].read())
                
                for file in sorted_rrf_files:
                    # Read CSV files and count rows before conversion
                    df = read_csv_from_zip(zip_ref, file)
                    before_conversion_dict[file] = df.shape[0]

                # Read the sheet names from the Excel file
                excelfile = pd.ExcelFile(excel_file_content)
                sheet_names = excelfile.sheet_names
                sheet_names.sort()
                
                # print(sheet_names,"names")

                # Get the date from the zip file key
                zip_file_date = extract_date_from_zip_path(zip_key)
                # print(zip_file_date,"date")

                for i, file in enumerate(sorted_rrf_files):
                    # Read Excel sheets, perform transformations, and count rows after conversion
                    df_dict = read_excel_sheet(excel_file_content, sheet_name=sheet_names[i])
                    filet = zip_ref.open(f'rrf/{sorted_rrf_files[i]}')
                    df = pd.read_table(filet, sep='|', header=None, low_memory=False)
                    df['VERSION_MONTH'] = [zip_file_date] * df.shape[0]
                    df.columns = df_dict
                    df['CODE_SET'] = ['RXNORM'] * df.shape[0]
                    format_dates(df)
                    # print(df_dict,"df_dict")
                    after_conversion_dict[sheet_names[i]] = df.shape[0]

                    # Save DataFrame to a text file
                    transformed_data[sheet_names[i]] = df
                    # print(df.head(2))
                    
        # print("Before Conversion:", before_conversion_dict)
        # print("After Conversion:", after_conversion_dict)

        # Your existing code to print the final table
        before_conversion_dict = {key.replace('.RRF', ''): value for key, value in before_conversion_dict.items()}
        # print(before_conversion_dict,"before")
        before_df = pd.DataFrame(list(before_conversion_dict.items()), columns=['File Name', 'Before Conversion'])
        after_df = pd.DataFrame(list(after_conversion_dict.items()), columns=['File Name', 'After Conversion'])
        merged_df = pd.merge(before_df, after_df, on='File Name')
        print(merged_df)
# 
        # # Save the transformed text files back to S3
        for sheet_name, df in transformed_data.items():
            save_txt_file(df, bucket, sheet_name)
            print(f"{sheet_name}.txt is saved")

        print("Transformation Completed")
    except Exception as e:
        print(f"An error occurred during transformation: {e}")
        raise e
def handle_excel_upload(bucket, key):
    # Logic to handle .xlsx file upload
    # You can update your state, database, or any mechanism to track the uploaded .xlsx file
    print(f".xlsx file uploaded: {key}")
    
def handle_zip_upload(bucket, key):
    # Logic to handle .zip file upload
    # You can update your state, database, or any mechanism to track the uploaded .zip file
    print(f".zip file uploaded: {key}")

def find_file_in_bucket(bucket, file_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=file_name)
        for obj in response.get('Contents', []):
            if obj['Key'] == file_name:
                # print(obj['Key'])
                return obj['Key']
        return None
    except Exception as e:
        print(f"An error occurred while finding the file: {e}")
        raise e

def read_excel_sheet(excel_file_content, sheet_name):
    df_dict = pd.read_excel(excel_file_content, sheet_name=sheet_name, header=None)
    return df_dict[1].to_list()

def extract_date_from_zip_path(zip_path):
    file_name = os.path.basename(zip_path)
    digits = ''.join(filter(str.isdigit, file_name))

    if len(digits) >= 8:
        month = digits[:2]
        day = digits[2:4]
        year = digits[4:8]

        date_str = f"{month}{day}{year}"
        date_obj = datetime.strptime(date_str, "%m%d%Y")
        formatted_date = date_obj.strftime("%Y-%m-%d")

        return formatted_date
    else:
        return None

def format_dates(df):
    date_cols = [col for col in df.columns if 'DATE' in col or 'RXNORM_TERM_DT' in col]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors='coerce').dt.strftime("%Y-%m-%d")

def read_csv_from_zip(zip_ref, file):
    # Function to read a CSV file from a Zip archive
    return pd.read_csv(zip_ref.open(f'rrf/{file}'), sep='|', header=None, encoding="utf-8", low_memory=False)

def save_txt_file(df, bucket, sheet_name):
    # Function to save a DataFrame to a text file and upload to S3
    txt_file_content = df.to_csv(index=False, sep=',').encode('utf-8')
    s3_client.put_object(Body=txt_file_content, Bucket=bucket, Key=f'allfiles/{sheet_name}.txt')