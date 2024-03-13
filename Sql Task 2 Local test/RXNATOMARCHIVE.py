import pandas as pd
import os
from openpyxl import Workbook

file_location = r"C:\Users\upsal\Downloads\RxNorm_Header.xlsx"
rxnconso_sheet_name = 'RXNATOMARCHIVE'

# Read headers from Excel file
headers = pd.read_excel(file_location, sheet_name=rxnconso_sheet_name, usecols=[1], header=None).iloc[:, 0].tolist()

rxnconso_file_location = os.path.abspath(r"C:\Users\upsal\Downloads\RXNORM\rrf\RXNATOMARCHIVE.RRF")

# Read data from RRF file
df = pd.read_csv(rxnconso_file_location, sep='|', header=None, names=headers, dtype=str, na_values=[''])

# Add new columns
df['CODE_SET'] = 'Rxnorm'
df['VERSION_MONTH'] = 'May'

# Convert date columns to YYYY-MM-DD format
df['ARCHIVE_DATE'] = pd.to_datetime(df['ARCHIVE_DATE']).dt.strftime('%Y-%m-%d')
df['CREATE_DATE'] = pd.to_datetime(df['CREATE_DATE'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d')
df['UPDATE_DATE'] = pd.to_datetime(df['UPDATE_DATE'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d')

# Convert mixed-format date columns to YYYY-MM-DD HH:MM:SS format
df['RXNORM_TERM_DT'] = pd.to_datetime(df['RXNORM_TERM_DT'], format='%d/%m/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d %I:%M:%S %p')

# Save to CSV with explicit date formatting
output_csv_path = r"C:\Users\upsal\Downloads\RXNORM\rrf\rxnconso_processed.csv"
df.to_csv(output_csv_path, index=False, date_format='%Y-%m-%d %I:%M:%S %p')  # Add this parameter to specify the date format
