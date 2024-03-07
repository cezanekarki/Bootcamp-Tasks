from zipfile import ZipFile
import pandas as pd
import os
from datetime import datetime

def read_csv_from_zip(zip_ref, file):
    # Function to read a CSV file from a Zip archive
    return pd.read_csv(zip_ref.open(f'rrf/{file}'), sep='|', header=None, encoding="utf-8", low_memory=False)

def read_excel_sheet(excel_file_path, sheet_name):
    # Function to read a sheet from an Excel file
    df_dict = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=None)
    return df_dict[1].to_list()

def format_dates(df):
    # Function to format date columns in a DataFrame
    date_cols = [col for col in df.columns if 'DATE' in col or 'RXNORM_TERM_DT' in col]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors='coerce').dt.strftime("%Y-%m-%d")

def save_txt_file(df, folder_path, sheet_name):
    # Function to save a DataFrame to a text file and print a message
    df.to_csv(f'{folder_path}\\{sheet_name}.txt', sep=',', index=False)
    print(f"File {sheet_name}.txt is saved")

def extract_date_from_zip_path(zip_path):
    # Get the file name from the full path
    file_name = os.path.basename(zip_path)

    # Extract digits from the file name
    digits = ''.join(filter(str.isdigit, file_name))

    # Check if we have at least 8 digits for a date (MMDDYYYY)
    if len(digits) >= 8:
        # Extract the date parts
        month = digits[:2]
        day = digits[2:4]
        year = digits[4:8]

        # Combine the date parts and convert to a datetime object
        date_str = f"{month}{day}{year}"
        date_obj = datetime.strptime(date_str, "%m%d%Y")

        # Format the date as YYYY-MM-DD
        formatted_date = date_obj.strftime("%Y-%m-%d")

        return formatted_date
    else:
        return None  

def rxnorm_transformation():
    print("Transformation Started")

    try:
        before_conversion_dict = {}
        after_conversion_dict = {}

        file_path = "C:\\Users\\sabind\\Downloads"
        zip_file_path = f'{file_path}\\RxNorm_full_02052024.zip'
        excel_file_path = f'{file_path}\\RxNorm_Header.xlsx'
        txt_file_path = f'{file_path}\\allfiles'

        with ZipFile(zip_file_path, 'r') as zip_ref:
            rrf_files = [file[len('rrf/'):] for file in zip_ref.namelist() if file.startswith('rrf/')]
            sorted_rrf_files = sorted(rrf_files)

            if sorted_rrf_files:
                excelfile = pd.ExcelFile(excel_file_path)
                sheet_names = excelfile.sheet_names
                sheet_names.sort()
                
                zip_file_date=extract_date_from_zip_path(zip_file_path)


                for file in sorted_rrf_files:
                    # Read CSV files and count rows before conversion
                    df = read_csv_from_zip(zip_ref, file)
                    before_conversion_dict[file] = df.shape[0]

                for i, file in enumerate(sorted_rrf_files):
                    # Read Excel sheets, perform transformations, and count rows after conversion
                    df_dict = read_excel_sheet(excel_file_path, sheet_name=sheet_names[i])
                    filet = zip_ref.open(f'rrf/{sorted_rrf_files[i]}')
                    df = pd.read_table(filet, sep='|', header=None, low_memory=False)
                    df['VERSION_MONTH'] = [zip_file_date] * df.shape[0]
                    df.columns = df_dict
                    df['CODE_SET'] = ['RXNORM'] * df.shape[0]
                    format_dates(df)
                    # print the first 2 rows from the evry file
                    print(df.head(2))
                    after_conversion_dict[sheet_names[i]] = df.shape[0]

                    # Save DataFrame to a text file
                    save_txt_file(df, txt_file_path, sheet_names[i])

        # Remove '.RRF' from keys in before_conversion_dict
        before_conversion_dict = {key.replace('.RRF', ''): value for key, value in before_conversion_dict.items()}

        # Convert dictionaries to DataFrames
        before_df = pd.DataFrame(list(before_conversion_dict.items()), columns=['File Name', 'Before Conversion'])
        after_df = pd.DataFrame(list(after_conversion_dict.items()), columns=['File Name', 'After Conversion'])

        # Merge DataFrames on 'File Name'
        merged_df = pd.merge(before_df, after_df, on='File Name')

        # Print the final table
        print(merged_df)

        print("Transformation Completed")
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

rxnorm_transformation()
