import pandas as pd
import os

file_location = r"C:\Users\upsal\Downloads\RxNorm_Header.xlsx"

try:
    # Dynamically fetch sheet names
    xls = pd.ExcelFile(file_location)
    sheet_names = xls.sheet_names
except Exception as e:
    print("Error reading sheet names from Excel file:", e)
    sheet_names = []

data_directory = r"C:\Users\upsal\Downloads\RXNORM\rrf"

for sheet_name in sheet_names:
    file_path = os.path.join(data_directory, f"{sheet_name}.RRF")
    
    try:
        # Fetch column names from Excel file
        headers = pd.read_excel(file_location, sheet_name=sheet_name, usecols=[1], header=None).iloc[:, 0].tolist()
        
        df = pd.read_csv(file_path, sep='|', header=None, names=headers, dtype=str)
        
        df = df.fillna('')
        
        if 'ARCHIVE_DATE' in df.columns:
            df['ARCHIVE_DATE'] = pd.to_datetime(df['ARCHIVE_DATE'], errors='coerce').dt.strftime('%Y-%m-%d')
        if 'CREATE_DATE' in df.columns:
            df['CREATE_DATE'] = pd.to_datetime(df['CREATE_DATE'], errors='coerce').dt.strftime('%Y-%m-%d')
        if 'UPDATE_DATE' in df.columns:
            df['UPDATE_DATE'] = pd.to_datetime(df['UPDATE_DATE'], errors='coerce').dt.strftime('%Y-%m-%d')
        if 'RXNORM_TERM_DT' in df.columns:
            df['RXNORM_TERM_DT'] = pd.to_datetime(df['RXNORM_TERM_DT'], format='%d-%b-%y', errors='coerce').dt.strftime('%Y-%m-%d')
        df['CODE_SET'] = 'Rxnorm'
        df['VERSION_MONTH'] = 'May'
        
        output_txt_path = os.path.join(data_directory, f"{sheet_name}_processed.txt")
        df.to_csv(output_txt_path, index=False, sep=',', date_format='%Y-%m-%d %I:%M:%S %p')
        
        rrf_row_count = len(df)
        txt_row_count = len(pd.read_csv(output_txt_path))
        
        if rrf_row_count == txt_row_count:
            print(f"Data processed successfully for '{sheet_name}'. Row count validated: {rrf_row_count} rows")
        else:
            print(f"Data processing failed for '{sheet_name}'. Row counts do not match: RRF has {rrf_row_count} rows, processed TXT has {txt_row_count} rows")
    except Exception as e:
        print(f"Error processing data for '{sheet_name}':", e)
