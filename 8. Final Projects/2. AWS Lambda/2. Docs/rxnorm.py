# Imports Used For The Task
import json
import boto3
from zipfile import ZipFile
import pandas as pd
from io import BytesIO
from datetime import datetime
import os


# S3 Client Object For Interacting With Bucket
s3_client = boto3.client('s3')


#Working After Tiggered
def lambda_handler(event, context):
    try:
        # Extracting Bucket And Key Information From S3 Event
        bucket = 'pratikzippedbucket'
        
        zip_file_name = 'RxNorm_full_02052024.zip'
        zip_key = f'zipped_file/{zip_file_name}'
        
        # bucket = event['Records'][0]['s3']['bucket']['name']
        # key = event['Records'][0]['s3']['object']['key']

        # Reading zip file directly from S3
        zip_file_obj = s3_client.get_object(Bucket=bucket, Key=zip_key)
        zip_file_content = BytesIO(zip_file_obj['Body'].read())
        
        print("Zipper")
        
        # Perform RxNorm transformation
        rxnorm_transformation(zip_file_content, bucket, zip_key, zip_file_name)

    except Exception as e:
        print(f"An error occurred in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error while transforming')
        }

# Function for Required Transofrmations
def rxnorm_transformation(zip_file_content, bucket, zip_key, zip_file_name):
    try:
        print("Transformation Start")
        dict_b_conversion = {} # Before Conversion
        dict_a_conversion = {} # After Conversion
        transformed_data = {}  # Placeholder for Transformed Data

        # Reading Zip File Directly
        with ZipFile(zip_file_content, 'r') as zip_ref:
            rrf_files = [file[len('rrf/'):] for file in zip_ref.namelist() if file.startswith('rrf/')]
            sorted_rrf_files = sorted(rrf_files)
            
            print("Sorted Files: ",sorted_rrf_files)
            
            if sorted_rrf_files:
                # Finding Excel File
                excel_key = find_xlsx_in_bucket(bucket, 'head/RxNorm_Header.xlsx')

                if not excel_key:
                    print("Excel file not in the bucket")
                    return

                # Reading Content From Body Of get_object
                excel_file_obj = s3_client.get_object(Bucket=bucket, Key=excel_key)
                excel_file_content = BytesIO(excel_file_obj['Body'].read())
                
                for file in sorted_rrf_files:
                    
                    # Reading CSV Files And Counting Rows Before Conversion
                    df = read_rrf_from_zip(zip_ref, file)
                    dict_b_conversion[file] = df.shape[0]

                # Reading Sheet Names From Excel File
                excelfile = pd.ExcelFile(excel_file_content)
                sheet_names = excelfile.sheet_names
                sheet_names = sorted(sheet_names)
                
                # print("Sheet Names: ", sheet_names)

                # Getting Date From Zip File Key
                zip_file_date = get_version_month(zip_file_name)
                print("Zip File Date: ",zip_file_date)

                # Enumerating Through The Sorted RRF Files
                for i, file in enumerate(sorted_rrf_files):
                    # Reading Excel Sheets
                    df_dict = read_excel_sheet(excel_file_content, sheet_name=sheet_names[i])
                    
                    print(df_dict)
                    print("Reading Excel File")
                    print(sheet_names[i])
                    
                    filet = zip_ref.open(f'rrf/{sorted_rrf_files[i]}')
                    df = pd.read_table(filet, sep='|', header=None, low_memory=False)

                    # Adding Columns AND Required Data
                    df['VERSION_MONTH'] = [zip_file_date] * rrf_row_count(df) # Using date function here caused extra column to be created. So fixed like this.
                    print("Version Month Checkpoint")
                    
                    df.columns = df_dict
                    print("Column Equals Dict Check")
                    
                    df['CODE_SET'] = ['RXNORM'] * rrf_row_count(df)
                    print("Code Set Checkpoint")
                    
                    print(df_dict,"df_dict")
                    print(df.head())

                    #Performing Date Transformations And Counting Rows After Conversion
                    if sheet_names[i] == 'RXNATOMARCHIVE':
                        date_formatting(df)
                        
                    #print("df_dict: ", df_dict)
                    dict_a_conversion[sheet_names[i]] = rrf_row_count(df)

                    # Save DataFrame to a text file
                    transformed_data[sheet_names[i]] = df
                    # print("Headers From Excel: "df.head(2))
                    
        print("Before Conversion:", dict_b_conversion)
        print("After Conversion:", dict_a_conversion)

        # To Print The Final Table
        # dict_b_conversion = {key.replace('.RRF', ''): value for key, value in dict_b_conversion.items()}
        # print("Before: ", dict_b_conversion)
        
        # Conversion Checkpoints
        # before_df = pd.DataFrame(list(dict_b_conversion.items()), columns=['File Name', 'Before Conversion'])
        # after_df = pd.DataFrame(list(dict_a_conversion.items()), columns=['File Name', 'After Conversion'])
        # merged_df = pd.merge(before_df, after_df, on='File Name')
        # print("Merged DF: ", merged_df)

        # Saving Transformed Files Back To S3 In Specified, Different Directory To The Zip Files
        for sheet_name, df in transformed_data.items():
            
            # print("Loop Checkpoint")
            txt_file_content = df.to_csv(index=False, sep=',').encode('utf-8')
            # print(txt_file_content)

            # Dynamically create the path based on the zip file key
            # txt_key = f'{os.path.dirname(zip_key)}/allfiles/{sheet_name}.txt'
            # s3_client.put_object(Body=txt_file_content, Bucket=bucket, Key=txt_key)
            print("sheet_name:  ", sheet_name)
           
            response = s3_client.put_object( 
                Body=txt_file_content,
                Bucket=bucket, 
                Key=f'transformed_files/{sheet_name}.txt' 
            )

        print("Transformation Completed")
        
        #Row Count Validation
        row_count_validation(bucket)
        
    except Exception as e:
        print(f"An error occurred during transformation: {e}")
        raise e


# Function For Finding Excel File In The Bucket
def find_xlsx_in_bucket(bucket, file_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=file_name)
        for obj in response.get('Contents', []):
            if obj['Key'] == file_name:
                print(obj['Key'])    #Printing Object Key
                return obj['Key']
                
        return None
    except Exception as e:
        print(f"An error occurred while finding the rrf file: {e}")
        raise e


# Function For Reading Excel File
def read_excel_sheet(excel_file_content, sheet_name):
    df_dict = pd.read_excel(excel_file_content, sheet_name=sheet_name, header=None)
    
    xl_read = df_dict[1].to_list()
    print(xl_read, "Checking to list Function")     
    return xl_read
    

# Function For Data In Version Month
def get_version_month(file_name):
    # Splitting Filename Using _ As Delimiter
    partition = file_name.split('_')
    
    # Extracting Month From Splitting Date
    month_strng = partition[2][:2]
    return month_strng


# Function For Date Formatting 
def date_formatting(df):
    try:
        df['ARCHIVE_DATE'] = pd.to_datetime(df['ARCHIVE_DATE']).dt.strftime('%Y-%m-%d')
        date_formats = ['%m/%d/%Y %I:%M:%S %p', '%Y-%m-%d']
        
        for rqd_date_format in date_formats:
            df['CREATE_DATE'] = pd.to_datetime(df['CREATE_DATE'], format=rqd_date_format, errors='coerce')
            if df['CREATE_DATE'].isnull().any():
                continue
            else:
                break
            
        df['CREATE_DATE'] = df['CREATE_DATE'].dt.strftime('%Y-%m-%d %I:%M:%S %p')
        df['UPDATE_DATE'] = pd.to_datetime(df['UPDATE_DATE'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d %I:%M:%S %p')
        df['RXNORM_TERM_DT'] = pd.to_datetime(df['RXNORM_TERM_DT'], format='%d-%b-%y', errors='coerce').dt.strftime('%Y-%m-%d %I:%M:%S %p')
        
    except Exception as e:
        print (f"An error occurred while date conversion: {e}")
    

# Function For Reading RRF Files AS CSV
def read_rrf_from_zip(zip_ref, file):
    print(f"Reading RRF file: {file}")
    rrf_data = pd.read_csv(zip_ref.open(f'rrf/{file}'), sep='|', header=None, encoding="utf-8", low_memory=False)
    print("Read RRF As CSV Successfully")
    return rrf_data
    
    
# Function For Counting Rows    
def rrf_row_count(df):
    rows = df.shape[0]
    print("Row Count: ", rows)
    return rows


#for counting rows of text files obtained
def row_count_validation(bucket):
    #Empty Holders
    txt_files = {}
    transformed_files = []
    
    #Get Transformed Files
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix='transformed_files/')
        
        #Grabbing Content 
        for obj in response.get('Contents', []):
            transformed_files.append(obj['Key'])
            
        #File List    
        transformed_files = transformed_files[1:]
        print(f'Transformed File List: {transformed_files}')
        
        for i in transformed_files:
            print(f"{i} file found")
            file_obj = s3_client.get_object(Bucket = bucket, Key = i)
            file_content = BytesIO(file_obj['Body'].read())
            df = pd.read_csv(file_content, sep = ',', header = None, encoding="utf-8", low_memory = False)
            
            print(f'Successfully Read txt File: {i}')
            
            row_count = rrf_row_count(df)
            txt_files[i] = row_count
            print(f'{i} Row Count: {row_count}')
            
        else:
            
            print(f"{i} N/A")
            
        print(f"txt File Row Count: {txt_files}")
        
        return txt_files
        
    except Exception as e:
        print (f"An error occurred while validating rows: {e}")