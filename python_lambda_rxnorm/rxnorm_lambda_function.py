import json
import boto3
from zipfile import ZipFile
import pandas as pd
from io import BytesIO
from datetime import datetime
import os

dev_client = boto3.client('s3') 

def lambda_handler(event, context):
    
    get_content_key(event)


def get_content_key(event):
    try:
        # bucket = event['Records'][0]['s3']['bucket']['name']
        # key = event['Records'][0]['s3']['object']['key']
        bucket ='rxnorm-file-bucket' 
        key = 'RxNorm_full_02052024.zip'
        zip_obj = dev_client.get_object(Bucket=bucket, Key=key)
        zip_content = BytesIO(zip_obj['Body'].read())
        transform_file(zip_content,bucket,key)

    except Exception as exc:
        print (f"Something went wrong: {exc}")

def transform_file(content, bucket, key):
    try:
        initial_files={}
        process_files={}
        transformed_file_content={}
        with ZipFile(content, 'r') as zip:
            rrf_files = get_rrf_files(zip)

            if rrf_files:
                # getting excel header file contents
                header_file_key=search_header_file(bucket, 'RxNorm_Header.xlsx')
                header_file_obj = dev_client.get_object(Bucket=bucket, Key=header_file_key)
                header_file_content = BytesIO(header_file_obj['Body'].read())

                for file in rrf_files:
                    rrf_df = read_rrf_file(zip, file)
                    initial_files[file]=count_rows(rrf_df)
                
                sheet_list=get_excel_sheet_names(header_file_content)
                print('sheet_list', sheet_list)
                #version month from zip file name
                version_month=get_zip_file_date(key)
                print('version_month',version_month)
                for index, file in enumerate(rrf_files):
                    excel_df=get_header(header_file_content, sheet_list[index])
                    filet = zip.open(rrf_files[index])
                    df = pd.read_table(filet, sep='|', header=None, low_memory=False)
                    df['VERSION_MONTH'] =[version_month]* count_rows(df)
                    df.columns = excel_df
                    df['CODE_SET']= ['RxNorm'] * count_rows(df)

                    #format dates
                    if sheet_list[index] == 'RXNATOMARCHIVE':
                        print("Converting date columns of RXNATOMARCHIVE file")
                        convert_dates(df)

                    #for row count
                    process_files[sheet_list[index]] = count_rows(df)
                    transformed_file_content[sheet_list[index]]=df
    
        print("Row count before transformation:", initial_files)
        print("Row count after transformation:", process_files)
        save_transformed_file(transformed_file_content, bucket)
        validate_rows(bucket)#getting row count of obtained text file
    except Exception as exc:
        print (f"Something went wrong transforming data: {exc}")
 
def save_transformed_file(transformed_file_content, bucket):
    try:
        for sheet_name, df in transformed_file_content.items():
            content = df.to_csv(index=False, sep=',').encode('utf-8')
            response = dev_client.put_object( 
                Body=content,
                Bucket=bucket, 
                Key=f'unzipped/{sheet_name}.txt' 
            )
            print(f"Succesfully saved data in unzipped/{sheet_name}.txt")
    
    except Exception as exc:
        print (f"Something went wrong saving data in unzipped/{sheet_name}.txt: {exc}")

#for counting rows of text files obtained
def validate_rows(bucket):
    text_files={}
    unzipped_file_list=[]
    try:
        response = dev_client.list_objects_v2(Bucket=bucket, Prefix='unzipped/')
        for obj in response.get('Contents', []):
            unzipped_file_list.append(obj['Key'])
        unzipped_file_list = unzipped_file_list[1:]
        print(f'The list of obtained text files: {unzipped_file_list}')
        
        for index in unzipped_file_list:
            print(f"{index} file found")
            file_obj = dev_client.get_object(Bucket=bucket, Key=index)
            file_content = BytesIO(file_obj['Body'].read())
            df = pd.read_csv(file_content, sep=',', header=None, encoding="utf-8", low_memory=False)
            print(f'Successfully read txt file: {index}')
            row_count=count_rows(df)
            text_files[index] = row_count
            print(f'{index} row count is:{row_count}')
        else:
            print(f"{index} not found")
        print(f"Text file row counts: {text_files}")
        return text_files
    except Exception as exc:
        print (f"Something went wrong in validating rows: {exc}")

def count_rows(df):
    return df.shape[0]

'''xlxs header file'''
#search header files in s3
def search_header_file(bucket, file):
    try:
        response = dev_client.list_objects_v2(Bucket=bucket, Prefix=file)
        for obj in response.get('Contents', []):
            if obj['Key'] == file:
                return obj['Key']
        return None
    except Exception as exc:
        print (f"Something went wrong searching file: {exc}")

#get header from excel file 1st index columns
def get_header(excel_content, sheet_name):
    excel_df = pd.read_excel(excel_content, sheet_name=sheet_name,header=None)  
    header_list =excel_df[1].to_list()
    print('Header file is read successfully')
    return header_list

#get excel file sheet list
def get_excel_sheet_names(content):
    excel_file = pd.ExcelFile(content)
    sheets = excel_file.sheet_names
    print('get excel sheet name',sheets)
    return sorted(sheets)

'''rrf file'''
#get rrf files list
def get_rrf_files(zip_file):
    files = [name for name in zip_file.namelist() if name.startswith('rrf/')]
    print(sorted(files))
    return sorted(files)

def get_zip_file_date(file_name):
    parts = file_name.split('_')
    month_str = parts[2][:2]
    return month_str

def read_rrf_file(zip, file):
    file_content=zip.open(file)
    rrf_df = pd.read_csv(file_content, sep='|', header=None, encoding="utf-8", low_memory=False)
    print(f'RRF file read sucessfully')
    return rrf_df

def convert_dates(df):
    try:
        df['ARCHIVE_DATE'] = pd.to_datetime(df['ARCHIVE_DATE']).dt.strftime('%Y-%m-%d')
        date_formats = ['%m/%d/%Y %I:%M:%S %p', '%Y-%m-%d']
        for format_str in date_formats:
            df['CREATE_DATE'] = pd.to_datetime(df['CREATE_DATE'], format=format_str, errors='coerce')
            if df['CREATE_DATE'].isnull().any():
                continue
            else:
                break
        df['CREATE_DATE'] = df['CREATE_DATE'].dt.strftime('%Y-%m-%d %I:%M:%S %p')
        df['UPDATE_DATE'] = pd.to_datetime(df['UPDATE_DATE'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d %I:%M:%S %p')
        df['RXNORM_TERM_DT'] = pd.to_datetime(df['RXNORM_TERM_DT'], format='%d-%b-%y', errors='coerce').dt.strftime('%Y-%m-%d %I:%M:%S %p')
    except Exception as exc:
        print (f"Something went wrong converting date: {exc}")
