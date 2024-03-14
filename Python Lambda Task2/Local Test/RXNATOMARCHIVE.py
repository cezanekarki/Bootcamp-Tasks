import pandas as pd
import os
from openpyxl import Workbook

file_location = r"C:\Users\upsal\Downloads\RxNorm_Header.xlsx"
rxnconso_sheet_name = 'RXNATOMARCHIVE'

headers = pd.read_excel(file_location, sheet_name=rxnconso_sheet_name, usecols=[1], header=None).iloc[:, 0].tolist()

rxnconso_file_location = os.path.abspath(r"C:\Users\upsal\Downloads\RXNORM\rrf\RXNATOMARCHIVE.RRF")

df = pd.read_csv(rxnconso_file_location, sep='|', header=None, names=headers, dtype=str, na_values=[''])

df['CODE_SET'] = 'Rxnorm'
df['VERSION_MONTH'] = 'May'

df['ARCHIVE_DATE'] = pd.to_datetime(df['ARCHIVE_DATE']).dt.strftime('%Y-%m-%d')
df['CREATE_DATE'] = pd.to_datetime(df['CREATE_DATE'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d')
df['UPDATE_DATE'] = pd.to_datetime(df['UPDATE_DATE'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce').dt.strftime('%Y-%m-%d')

df['RXNORM_TERM_DT'] = pd.to_datetime(df['RXNORM_TERM_DT'], format='%d-%b-%y', errors='coerce').dt.strftime('%Y-%m-%d')
output_txt_path = r"C:\Users\upsal\Downloads\RXNORM\rrf\rxn_processed.txt"
df.to_csv(output_txt_path, index=False, sep=',', date_format='%Y-%m-%d %I:%M:%S %p')