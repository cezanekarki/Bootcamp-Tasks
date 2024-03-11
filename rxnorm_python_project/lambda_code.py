import boto3
import zipfile
import io
from io import StringIO
import os
import pandas as pd
import json
from datetime import datetime


s3 = boto3.client('s3')

# fgVersion Month date for the 
version_month = []

def read_excel_from_s3(bucket):
    try:
        folder_path = 'excel_file/'
        excel_file_name = 'RxNorm_Header.xlsx'
        key = folder_path + excel_file_name
        
        # Download the Excel file to the /tmp directory
        local_excel_file = '/tmp/RxNorm_Header.xlsx'
        
        s3.download_file(bucket, key, local_excel_file)
     
        # Check if the Excel file exists
        if os.path.exists(local_excel_file):
            print(f"Excel file downloaded to: {local_excel_file}")
        
        else:
            print(f"Excel file is not found yet")
            
    
        # Read the Excel file into an ExcelFile object
    
        excel_file = pd.ExcelFile(local_excel_file)
        print(f"The excel file RxNorm_Header.xlsx is read from S3")
            
        # Get the sheet names
        sheet_names = excel_file.sheet_names
        headers_data = {}
        for sheets in sheet_names:
            print(sheets)
            sheets_data = excel_file.parse(sheets,header=None)
            excel_headers = sheets_data.iloc[:, 0].tolist()
            print(f"The Sheet, {sheets} is read from RxNorm_Header.xlsx,for it's header {excel_headers}")
            headers_data[sheets] = excel_headers
            
        return headers_data
        
    except Exception as e:
        print(f"Error occurred: {e}")
   

  
#Read the zip file and relocate the files in the rrf folder
def read_and_relocate_rrf_files(s3, bucket, key):
    try:
        # My columns for the Version Month field
        text_data = key
        text_file = text_data.split('/')[1].split('_')[2].split('.')[0]
        date_string = datetime.strptime(text_file, '%m%d%Y')
        date_object = date_string.strftime('%B %d, %Y')
        version_month.append(date_object)
        print(f"The data for Version Month is: {version_month[0]}")
        
        # Reading the rrf files
        zip_response = s3.get_object(Bucket=bucket, Key=key)
        zip_data = zip_response['Body'].read()
        file_path = 'rrf'
        destination_path = 'extracted_files'
        
        
      # Read the zip file without unzipping it
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.startswith(file_path) and not file_info.filename.endswith('/'):
                    filename = os.path.basename(file_info.filename)
                    if filename.endswith('.RRF'):
                        print(f"The {filename} is read from zip file.")
                        with zip_ref.open(file_info) as source_file:
                            s3.put_object(
                                Bucket=bucket,
                                Key=f"{destination_path}/{filename}",
                                Body=source_file.read()
                            )
                        print(f"The {filename} is relocated to {destination_path}")

        return {
            'statusCode': 200,
            'body': json.dumps('Zip file processed successfully')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


# Updating the dataframe for some files
def update_dataframe(df, file_name):
    if file_name == 'RXNSAB.RRF':
        print(f"Updating the columns VSTART and VEND for {file_name}....")
        df['VSTART'] = df['VSTART'].apply(lambda x: datetime.strptime(x, '%Y_%m_%d').strftime('%Y-%m-%d') if pd.notnull(x) else x)
        df['VEND'] = df['VEND'].apply(lambda x: datetime.strptime(x, '%Y_%m_%d').strftime('%Y-%m-%d') if pd.notnull(x) else x)
    elif file_name == 'RXNATOMARCHIVE.RRF':
        print(f"Updating the columns CREATED_TIMESTAMP,UPDATED_TIMESTAMP and LAST_RELEASED for {file_name}....")
        df['CREATED_TIMESTAMP'] = df['CREATED_TIMESTAMP'].apply(lambda x: datetime.strptime(x, "%m/%d/%Y %I:%M:%S %p").strftime("%Y-%m-%d %I:%M:%S %p") if pd.notnull(x) else x)
        df['UPDATED_TIMESTAMP'] = df['UPDATED_TIMESTAMP'].apply(lambda x: datetime.strptime(x, "%m/%d/%Y %I:%M:%S %p").strftime("%Y-%m-%d %I:%M:%S %p") if pd.notnull(x) else x)
        df['LAST_RELEASED'] = df['LAST_RELEASED'].fillna('').astype(str).apply(lambda x: datetime.strptime(x, '%d-%b-%y').strftime('%Y-%m-%d') if x else '')
    return df

# Function for reading the unzipped rrf files and converting it to csv after adding headers
def process_files(bucket,headers_data):
    unzipped_folder = 'extracted_files/'
    destination_folder = 'transformed_files'
    headers = read_excel_from_s3(bucket)
    
    unzziped_response = s3.list_objects_v2(Bucket=bucket, Prefix=unzipped_folder)
    
    if 'Contents' in unzziped_response:
        for obj in unzziped_response['Contents']:
            object_key = obj['Key']
            file_name = os.path.basename(object_key)
            print(f"Processing file: {file_name}")
            new_file = file_name.split('.')[0]
            
            if file_name.endswith('.RRF'):
                try:
                    base_filename = file_name.split('.')[0]
                    file_response = s3.get_object(Bucket=bucket, Key=object_key)
                    df = pd.read_csv(file_response['Body'], sep='|', header=None,low_memory=False)
                    df = df.iloc[:, :-1]
                    df['Code set'] = 'Rxnorm'
                    df['Version Month'] = version_month[0]

                    
                    if base_filename in headers:
                        df.columns = headers[base_filename]
                        df = update_dataframe(df,file_name)
                        
                        before_transformation = df.shape[0]
                        
                        print(f"The count of {file_name} before transformation is: {before_transformation}")
                    
                        csv_buffer = df.to_csv(sep=',',index=False)
                      
                 
                        
                        s3.put_object(
                                Bucket=bucket,
                                Key=f"{destination_folder}/{new_file}.CSV",
                                  Body=csv_buffer
                            )
                            
                        
                        print(f"The {file_name} is transformed into CSV and stored into {destination_folder} in S3")
                        
                        csv_data = StringIO(csv_buffer)
                        csv_data_length = len(csv_data.readlines()) - 1
                        
                        print(f"The count of {file_name} after transformation is: {csv_data_length}")

                except Exception as e:
                  print(f"Error reading file {file_name}: {e}")
 
    else:
        print("Zipped file is not found")
        
     
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    read_and_relocate_rrf_files(s3,bucket,key)
    
    headers_data = read_excel_from_s3(bucket)
    
    if headers_data:
        process_files(bucket,headers_data)
  
    
    
