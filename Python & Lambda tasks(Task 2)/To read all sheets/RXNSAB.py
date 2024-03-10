import pandas as pd
import os

file_location = r"C:\Users\upsal\Downloads\RxNorm_Header.xlsx"
rxnconso_sheet_name = 'RXNSAB'

headers = pd.read_excel(file_location, sheet_name=rxnconso_sheet_name, usecols=[1], header=None).iloc[:, 0].tolist()

rxnconso_file_location = os.path.abspath(r"C:\Users\upsal\Downloads\RXNORM\rrf\RXNSAB.RRF")

# Read data into DataFrame
df = pd.read_csv(rxnconso_file_location, sep='|', header=None, names=headers, dtype=str, na_values=[''])

# Convert date columns to YYYY-MM-DD format
date_columns = ['SOURCE_VERSION']  # Replace with actual date column names
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

# Add new columns
df['CODE_SET'] = 'Rxnorm'
df['VERSION_MONTH'] = 'May'

# Save processed data to CSV
output_csv_path = r"C:\Users\upsal\Downloads\RXNORM\rrf\rxnsab_processed.csv"
df.to_csv(output_csv_path, sep=',', index=False)

print(f"Processed data saved to CSV: {output_csv_path}")

# Save processed data to TXT with columns separated by commas
output_txt_path = r"C:\Users\upsal\Downloads\RXNORM\rrf\rxnsab_processed.txt"
df.to_csv(output_txt_path, sep=',', index=False)

print(f"Processed data saved to TXT: {output_txt_path}")
