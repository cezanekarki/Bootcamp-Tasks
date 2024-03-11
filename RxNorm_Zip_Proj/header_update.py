import boto3
import zipfile
import pandas as pd
import io
import os
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = 'buckettzipp'
    source_key = 'RxNorm_full_02052024.zip'
    excel_key = 'RxNorm_Header.xlsx'

    try:
        # Extract the date part from the source_key
        date_str = source_key.split('_')[-1].split('.')[0]  # Extract the date part without the extension
        date_obj = datetime.strptime(date_str, '%m%d%Y')
        date_formatted = date_obj.strftime('%Y-%m-%d')
    except ValueError as e:
        return {
            'statusCode': 400,
            'body': f"Error: {str(e)}; Source Key: {source_key}"
        }

    # Define the directory where the extracted rrf folder will be stored in S3
    extracted_folder_key = 'extracted_rrf/'

    # Retrieve the zip file object directly from S3
    zip_obj = s3.get_object(Bucket=source_bucket, Key=source_key)['Body'].read()

    print(f"Unzipping the zip file {source_key}......")

    # Read the zip file object using ZipFile from the io module
    with zipfile.ZipFile(io.BytesIO(zip_obj)) as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.startswith('rrf/'):
                extracted_file_key = extracted_folder_key + file_info.filename.split('/')[-1]
                s3.put_object(Bucket=source_bucket, Key=extracted_file_key, Body=zip_ref.read(file_info.filename))

    print(f"Unzipping zip file {source_key} successfully.")

    # Read Excel file into a Pandas DataFrame
    excel_obj = s3.get_object(Bucket=source_bucket, Key=excel_key)['Body'].read()
    df_excel = pd.read_excel(io.BytesIO(excel_obj), sheet_name=None, header=None)

    rrf_col_list = {sheet_name: df.iloc[:, 0].tolist() for sheet_name, df in df_excel.items()}

    # Process .RRF files (Add two columns and assign the column title from the Excel file)
    df_rrf = {}

    for file_info in s3.list_objects_v2(Bucket=source_bucket, Prefix=extracted_folder_key)['Contents']:
        file_key = file_info['Key']
        file_name = os.path.basename(file_key).split('.')[0]

        if file_key.endswith('.RRF'):
            file_obj = s3.get_object(Bucket=source_bucket, Key=file_key)['Body'].read()
            df = pd.read_csv(io.BytesIO(file_obj), sep='|', header=None, low_memory=False)
            df.drop(df.columns[-1], axis=1, inplace=True)
            df['second_last_row'] = "RxNorm"
            df['last_row'] = date_formatted  # Set the date from source_key

            # Rename columns based on the content of rrf_col_list
            if file_name in rrf_col_list:
                df.columns = rrf_col_list[file_name]

            lines_before = len(df)
            print(f"Number of rows in {file_name} initially: {lines_before}")

            df_rrf[file_name] = df

    # Upload result files to the "result" directory in S3 bucket
    result_folder_key = 'result/'
    for filename, df in df_rrf.items():
        txt_buffer = io.StringIO()  # Use StringIO to create a text buffer
        df.to_csv(txt_buffer, index=False)  # Write DataFrame to the text buffer in CSV format
        s3.put_object(Bucket=source_bucket, Key=result_folder_key + filename + '.txt', Body=txt_buffer.getvalue())  # Upload the text buffer content as .txt
        lines_after = len(df)
        print(f"Number of rows in {filename} after processing: {lines_after}")

    print(f"Processed .RRF files uploaded back to S3 successfully.")

    return {
        'statusCode': 200,
        'body': 'Processing completed successfully.'
    }
