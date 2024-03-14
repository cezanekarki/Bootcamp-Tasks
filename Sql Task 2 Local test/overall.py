import pandas as pd
import os

# Function to validate row count between original and converted files
def get_row_count(file_path):
    row_count = sum(1 for _ in open(file_path))
    return row_count

# Function to add row count to the top of the file
def add_row_count_to_file(file_path, row_count):
    with open(file_path, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(f"Total Rows: {row_count}\n" + content)

# Excel file containing sheet names
file_location = r"C:\Users\upsal\Downloads\RxNorm_Header.xlsx"

# Read sheet names from the Excel file
try:
    sheet_names = pd.ExcelFile(file_location).sheet_names
except Exception as e:
    print("Error reading sheet names from Excel file:", e)
    sheet_names = []

# Directory containing files with data
data_directory = r"C:\Users\upsal\Downloads\RXNORM\rrf"

# Process each sheet
for sheet_name in sheet_names:
    # File path for the current sheet
    file_path = os.path.join(data_directory, f"{sheet_name}.RRF")
    processed_file_path = os.path.join(data_directory, f"{sheet_name}_processed.txt")
    
    try:
        # Read data from the original file
        df_original = pd.read_csv(file_path, sep='|', dtype=str, header=None)
        
        # Data cleaning operations if needed
        df_original = df_original.fillna('')
        
        # Convert specific date columns to datetime format and format them
        if 'ARCHIVE_DATE' in df_original.columns:
            df_original['ARCHIVE_DATE'] = pd.to_datetime(df_original['ARCHIVE_DATE']).dt.strftime('%Y-%m-%d')
        if 'CREATE_DATE' in df_original.columns:
            df_original['CREATE_DATE'] = pd.to_datetime(df_original['CREATE_DATE'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d')
        if 'UPDATE_DATE' in df_original.columns:
            df_original['UPDATE_DATE'] = pd.to_datetime(df_original['UPDATE_DATE'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d')
        if 'RXNORM_TERM_DT' in df_original.columns:
            df_original['RXNORM_TERM_DT'] = pd.to_datetime(df_original['RXNORM_TERM_DT'], format='%d/%m/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d %I:%M:%S %p')
        
        # Save the processed data to a text file
        df_original.to_csv(processed_file_path, index=False, sep=',')
        
        print(f"Processed data for '{sheet_name}' saved to: {processed_file_path}")
        
        # Get row counts
        original_row_count = get_row_count(file_path)
        processed_row_count = get_row_count(processed_file_path)
        
        # Add row counts to the top of the processed file
        add_row_count_to_file(processed_file_path, original_row_count)
        add_row_count_to_file(processed_file_path, processed_row_count)
        
    except Exception as e:
        print(f"Error processing data for '{sheet_name}':", e)
