import boto3
import zipfile
import io
import os
import pandas as pd
import json
import openpyxl
from io import BytesIO


s3 = boto3.client('s3')
excel_headers ={}

   
def read_excel_from_s3(bucket):
    try:
        folder_path = 'excelfiles/'
        excel_file_name = 'RxNorm_Header.xlsx'
        key = folder_path + excel_file_name
        
        # Download the Excel file to the /tmp directory
        local_excel_file = '/tmp/RxNorm_Header.xlsx'
        s3.download_file(bucket, key, local_excel_file)
     
        # Check if the Excel file exists
        if os.path.exists(local_excel_file):
            print(f"Excel file downloaded to: {local_excel_file}")
            
        
        
        
        # Read the Excel file into an ExcelFile object
        excel_file = pd.ExcelFile(local_excel_file)
            
           # Get the sheet names
        sheet_names = excel_file.sheet_names
        print("Sheet names:", sheet_names)
        
        for sheets in sheet_names:
            # Read the data from the sheet into a DataFrame
            sheets_data = excel_file.parse(sheets,header=None)
            headers_data = sheets_data.iloc[:, 0].tolist()
            excel_headers[sheets] = headers_data
        print(f'excel_headers dictionary for sheet {sheet_names[0]}: {excel_headers[sheet_names[0]]}')
    
     
    except Exception as e:
        print(f"Error occurred: {e}")
   
def code_set_and_version_month(zip_data, rrf_df):
    try:
        # Convert the zip data to a string to extract information
        zip_filename = zip_data.decode('utf-8')
        # Extract version month from the filename
        version_month = os.path.splitext(zip_filename)[0].split('_')[-1]

        # Convert version month to a more readable format
        version_month = pd.to_datetime(version_month, format='%m%d%Y').strftime('%Y-%m-%d')
        print(f"Version month: {version_month}")

        # Add 'Code Set' and 'Version Month' columns to the DataFrame
        rrf_df['Code Set'] = 'RxNorm'
        rrf_df['Version Month'] = version_month
    except Exception as e:
        print(f"Error occurred while extracting version month: {e}")

    return rrf_df

def apply_header_to_rrf(file_name, rrf_df):
    # Check if the corresponding Excel sheet exists
    if file_name in excel_headers:
        # Get the headers from the Excel sheet
        excel_headers_list = excel_headers[file_name]
        excel_headers_list = [header for header in excel_headers_list if header != 'SVER']
        # Take names from the excel header list up to the length of the split DataFrame
        excel_headers_list = excel_headers_list[:len(rrf_df.columns)]
        # Set the correct header for the DataFrame
        rrf_df.columns = excel_headers_list
    return rrf_df   
    
def convert_date_format(value):
    
    try:
        # Try to parse the value into datetime format
        parsed_date = pd.to_datetime(value, format='%Y_%m_%d').date()
        # Extract only the date part
        return parsed_date.strftime('%Y-%m-%d')
    except ValueError:
        if value == '2020':
            return '2020-01-01'
        elif value == '5.0_2024_01_04':
            # Remove the float value and parse the remaining string
            
            return convert_date_format('2024_01_04')
        elif value == '2020AA':
            return '2024-01-02'  
        elif value == '20AA_240205F':
            return '2024-02-05'   
        else:
            return value
   
def update_nato_date(value):
    try:
        # Attempt to parse the value using the first date format
        parsed_date = pd.to_datetime(value, format='%m/%d/%Y %I:%M:%S %p').date()
    except ValueError:
        try:
            # If the first format fails, attempt to parse using the second date format
            parsed_date = pd.to_datetime(value, format='%d-%b-%y').date()
        except ValueError:
            # If both formats fail, return None or handle the error appropriately
            return None  # Or handle the error appropriately
          # Check if the parsed_date is NaT
    if pd.isnull(parsed_date):
        return '0000-00-00'  # Replace NaT with '0000-00-00'
    else:
        # Extract only the date part and return it in the desired format
        return parsed_date.strftime('%Y-%m-%d')   
        
def process_date_columns(file_name,rrf_df):
    date_columns = ['VSTART', 'VEND','CREATED_TIMESTAMP', 'UPDATED_TIMESTAMP', 'LAST_RELEASED']
    for column in date_columns:
        if column in rrf_df.columns:
            if file_name == 'RXNSAB':
            # Apply the conversion function to each value in the column
                

                rrf_df[column] = rrf_df[column].apply(convert_date_format)
                # Extract year from the VSTART column after date conversion and save it directly as a string
                rrf_df['SVER'] = pd.to_datetime(rrf_df['VSTART'], format='%Y-%m-%d').dt.year.astype(str)
                # Reorder the columns to place 'SVER' before 'VSTART'
                # Reorder columns
                # Reorder columns
                sver_index = rrf_df.columns.get_loc('SVER')
                vstart_index = rrf_df.columns.get_loc('VSTART')
                sf_index = rrf_df.columns.get_loc('SF')

                # Remove 'SVER' from its original position

                column_sver = rrf_df.pop('SVER')

                # Insert 'SVER' after 'SF', before 'VSTART'
                if sver_index < vstart_index:
                    rrf_df.insert(vstart_index - 1, 'SVER', column_sver)
                elif sver_index > vstart_index:
                    rrf_df.insert(vstart_index, 'SVER', column_sver)
            if file_name == 'RXNATOMARCHIVE': 
                rrf_df[column] = rrf_df[column].apply(update_nato_date)
    return rrf_df
        
def save_as_txt_file(rrf_df, file_name, bucket_name):
    # Construct the filename for the output text file
    transformation_folder = 'transformation/'

    # Convert DataFrame to CSV format in memory
    csv_buffer = io.StringIO()
    rrf_df.to_csv(csv_buffer, sep='|', index=False)

    # Upload the CSV buffer to S3
    s3_key = transformation_folder + file_name + '.txt'
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

    print(f"Transformed data saved to: s3://{bucket_name}/{s3_key}")

    

def read_and_relocate_rrf_files(s3, bucket, key):
    try:
        zip_response = s3.get_object(Bucket=bucket, Key=key)
        zip_data = zip_response['Body'].read()

        # Wrap the zip data in a BytesIO object
        zip_file = BytesIO(zip_data)

        file_path = 'rrf'

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.startswith(file_path) and not file_info.filename.endswith('/'):
                    filename = os.path.basename(file_info.filename)
                    print(f"The {filename} is read from zip file.")

                    with zip_ref.open(file_info) as source_file:
                        file_content = source_file.read().decode('utf-8')
                        if file_content.endswith('|'):
                            file_content = file_content[:-1]
                        file_content_io = io.StringIO(file_content)
                        rrf_df = pd.read_csv(file_content_io, delimiter='|', header=None)
                        rrf_df = rrf_df.iloc[:, :-1]
                        print(f"Row count before transformation: {rrf_df.shape[0]}")

                        file_name = os.path.splitext(filename)[0]
                        apply_header_to_rrf(file_name, rrf_df)
                        rrf_df = process_date_columns(file_name, rrf_df)
                        code_set_and_version_month(zip_data, rrf_df)

                        print(f"Row count of {file_name} after transformation: {rrf_df.shape[0]}")

                        pd.set_option('display.max_columns', None)
                        print(rrf_df.head(5))

                        # Save the transformed DataFrame to a text file
                        save_as_txt_file(rrf_df, file_name, bucket)
    except Exception as e:
        print(f"Error occurred: {e}")
    
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    
    # This is the function that relocate the rrf files from zip file
    read_excel_from_s3(bucket)
    read_and_relocate_rrf_files(s3,bucket,key)