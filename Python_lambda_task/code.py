import boto3
import zipfile
import pandas as pd
import io
import os
import tempfile
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = 'python-lambda-task'
    source_key = 'RxNorm_full_02052024_comp.zip'

    # Extract the date part from the source_key
    date_str = source_key.split('_')[2].split('_')[0]
    date_obj = datetime.strptime(date_str, '%m%d%Y')
    date_formatted = date_obj.strftime('%Y-%m-%d')

    # Define the directory where the extracted rrf folder will be stored
    extracted_path = tempfile.mkdtemp()
    
    # Retrieve the zip file object directly from S3
    zip_obj = s3.get_object(Bucket=source_bucket, Key=source_key)['Body'].read()

    print(f"Unzipping the zip file {source_key}......")

    # Read the zip file object using ZipFile from the io module
    with zipfile.ZipFile(io.BytesIO(zip_obj)) as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.startswith('rrf/'):
                zip_ref.extract(file_info, extracted_path)

    print(f"Unzipping zip file {source_key} successfully.")
    
    print(f"Excel file loaded successfully. ")
    # Read column headers from Excel file
    excel_key = 'RxNorm_Header.xlsx'
    excel_path = os.path.join(extracted_path, excel_key)

    # Download Excel file from S3 to local temporary storage
    temp_excel_path = os.path.join('/tmp', excel_key)
    s3.download_file(source_bucket, excel_key, temp_excel_path)

    # Read Excel file locally into a Pandas DataFrame
    df_excel = pd.read_excel(temp_excel_path, sheet_name=None, header=None)

    rrf_col_list = {sheet_name: df.iloc[:, 0].tolist() for sheet_name, df in df_excel.items()}

    # Process .RRF files (Add two column and assign the column title from excel file)
    rrf_directory = os.path.join(extracted_path, 'rrf')
    rrf_files = [file[:-4] for file in os.listdir(rrf_directory) if file.endswith('.RRF')]
    df_rrf = {}

    for file in rrf_files:
        file_path = os.path.join(rrf_directory, file + '.RRF')
        df = pd.read_csv(file_path, sep='|', header=None, low_memory=False)
        df.drop(df.columns[-1], axis=1, inplace=True)
        df['second_last_row'] = "RxNorm"
        df['last_row'] = date_formatted  # Set the date from source_key

        # Rename columns based on the content of rrf_col_list
        if file in rrf_col_list:
            df.columns = rrf_col_list[file]

        lines_before = len(df)
        print(f"Number of rows in {file} initially: {lines_before}")

        df_rrf[file] = df

    # Write DataFrames to CSV files
    result_directory = tempfile.mkdtemp()

    for filename, df in df_rrf.items():
        csv_path = os.path.join(result_directory, filename + '.txt')
        df.to_csv(csv_path, index=False)
        lines_after = len(df)
        print(f"Number of rows in {filename} after processing: {lines_after}")

    # Upload result files to the "result" directory in S3 bucket
    result_bucket = 'python-lambda-task'
    result_folder_key = 'result/'
    for filename in os.listdir(result_directory):
        result_file_path = os.path.join(result_directory, filename)
        s3.upload_file(result_file_path, result_bucket, result_folder_key + filename)
    print(f"Processed .RRF files uploaded back to s3 successfully.")

    return {
        'statusCode': 200,
        'body': 'Processing completed successfully.'
    }
