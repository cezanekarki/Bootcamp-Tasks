import boto3
import zipfile
import io
import os
import pandas as pd
from io import BytesIO

class S3Operations:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def download_file(self, bucket, key, local_path):
        self.s3_client.download_file(bucket, key, local_path)

    def upload_file(self, bucket, key, content):
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=content)

class DataTransformer:
    def __init__(self, s3_operations):
        self.s3_operations = s3_operations
        self.excel_headers = {}

    def fetch_excel_data_from_s3(self, bucket, local_path):
        try:
            excel_folder = 'excelfiles/'
            excel_file_name = 'RxNorm_Header.xlsx'
            excel_key = excel_folder + excel_file_name

            # Download the Excel file to the specified local directory
            self.s3_operations.download_file(bucket, excel_key, local_path)

            # Check if the Excel file exists
            if os.path.exists(local_path):
                print(f"Excel file downloaded to: {local_path}")

            # Read the Excel file into an ExcelFile object
            excel_file = pd.ExcelFile(local_path)

            # Get the sheet names
            sheet_names = excel_file.sheet_names
            print("Sheet names:", sheet_names)

            for sheet_name in sheet_names:
                # Read the data from the sheet into a DataFrame
                sheet_data = excel_file.parse(sheet_name, header=None)
                headers_data = sheet_data.iloc[:, 0].tolist()
                self.excel_headers[sheet_name] = headers_data

            print(f'excel_headers dictionary for sheet {sheet_names[0]}: {self.excel_headers[sheet_names[0]]}')

        except Exception as e:
            print(f"Error occurred: {e}")

    def process_code_set_and_version(self, zip_filename, rrf_df):
        try:
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

    def apply_excel_header(self, file_name, rrf_df):
        if file_name in self.excel_headers:
            excel_headers_list = self.excel_headers[file_name]
            excel_headers_list = [header for header in excel_headers_list if header != 'SVER']
            excel_headers_list = excel_headers_list[:len(rrf_df.columns)]
            rrf_df.columns = excel_headers_list

        return rrf_df

    def transform_date_format(self, value):
        try:
            parsed_date = pd.to_datetime(value, format='%Y_%m_%d').date()
            return parsed_date.strftime('%Y-%m-%d')

        except ValueError:
            return self.handle_date_format_exceptions(value)

    def handle_date_format_exceptions(self, value):
        if value == '2020':
            return '2020-01-01'
        elif value == '5.0_2024_01_04':
            return self.transform_date_format('2024_01_04')
        elif value == '2020AA':
            return '2024-01-02'
        elif value == '20AA_240205F':
            return '2024-02-05'
        else:
            return value

    def update_nato_date_format(self, value):
        try:
            parsed_date = pd.to_datetime(value, format='%m/%d/%Y %I:%M:%S %p').date()
        except ValueError:
            parsed_date = self.handle_nato_date_format_exceptions(value)

        return '0000-00-00' if pd.isnull(parsed_date) else parsed_date.strftime('%Y-%m-%d')

    def handle_nato_date_format_exceptions(self, value):
        try:
            return pd.to_datetime(value, format='%d-%b-%y').date()
        except ValueError:
            return None

    def process_date_columns(self, file_name, rrf_df):
        date_columns = ['VSTART', 'VEND', 'CREATED_TIMESTAMP', 'UPDATED_TIMESTAMP', 'LAST_RELEASED']

        for column in date_columns:
            if column in rrf_df.columns:
                if file_name == 'RXNSAB':
                    rrf_df[column] = rrf_df[column].apply(self.transform_date_format)
                    rrf_df = self.add_sver_column(rrf_df)

                if file_name == 'RXNATOMARCHIVE':
                    rrf_df[column] = rrf_df[column].apply(self.update_nato_date_format)

        return rrf_df

    def add_sver_column(self, rrf_df):
        rrf_df['SVER'] = pd.to_datetime(rrf_df['VSTART'], format='%Y-%m-%d').dt.year.astype(str)
        sver_index = rrf_df.columns.get_loc('SVER')
        vstart_index = rrf_df.columns.get_loc('VSTART')

        column_sver = rrf_df.pop('SVER')

        if sver_index < vstart_index:
            rrf_df.insert(vstart_index - 1, 'SVER', column_sver)
        elif sver_index > vstart_index:
            rrf_df.insert(vstart_index, 'SVER', column_sver)

        return rrf_df

    def save_transformed_data_as_txt(self, rrf_df, file_name, bucket_name):
        transformation_folder = 'transformation/'
        csv_buffer = io.StringIO()
        rrf_df.to_csv(csv_buffer, sep=',', index=False)
        s3_key = transformation_folder + file_name + '.csv'
        self.s3_operations.upload_file(bucket_name, s3_key, csv_buffer.getvalue())
        print(f"Transformed data saved to: s3://{bucket_name}/{s3_key}")

    def process_and_relocate_rrf_files(self, bucket, key):
        try:
            zip_response = self.s3_operations.s3_client.get_object(Bucket=bucket, Key=key)
            zip_data = zip_response['Body'].read()
            zip_filename = os.path.basename(key)
            print(zip_filename)

            zip_file = BytesIO(zip_data)
            file_path = 'rrf'
            unzipped_folder = 'unzipped/'

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
                            rrf_dataframe = pd.read_csv(file_content_io, delimiter='|', header=None)
                            rrf_dataframe = rrf_dataframe.iloc[:, :-1]

                            print(f"Row count before transformation: {rrf_dataframe.shape[0]}")
                            file_name = os.path.splitext(filename)[0]
                            self.apply_excel_header(file_name, rrf_dataframe)
                            rrf_dataframe = self.process_date_columns(file_name, rrf_dataframe)
                            self.process_code_set_and_version(zip_filename, rrf_dataframe)
                            print(f"Row count of {file_name} after transformation: {rrf_dataframe.shape[0]}")

                            pd.set_option('display.max_columns', None)
                            print(rrf_dataframe.head(5))

                            self.save_transformed_data_as_txt(rrf_dataframe, file_name, bucket)

                        unzipped_key = unzipped_folder + filename
                        self.s3_operations.upload_file(bucket, unzipped_key, file_content)
                        print(f"Unzipped file saved to: s3://{bucket}/{unzipped_key}")

        except Exception as e:
            print(f"Error occurred: {e}")

# Lambda handler function
def process_s3_event(event, context):
    try:
        s3_operations = S3Operations()
        data_transformer = DataTransformer(s3_operations)

        bucket = 'pythonlambdabucket1'
        local_excel_path = '/tmp/RxNorm_Header.xlsx'
        key = 'zipfiles/RxNorm_full_02052024.zip'

        data_transformer.fetch_excel_data_from_s3(bucket, local_excel_path)
        data_transformer.process_and_relocate_rrf_files(bucket, key)

    except Exception as e:
        print(f"Error occurred: {e}")

# Lambda handler function
lambda_handler = process_s3_event
