import pandas as pd
import os

file_location = r"C:\Users\upsal\Downloads\RxNorm_Header.xlsx"
rxnconso_sheet_name = 'RXNCUI'

headers = pd.read_excel(file_location, sheet_name=rxnconso_sheet_name, usecols=[1], header=None).iloc[:, 0].tolist()
print (headers)

rxnconso_file_location = os.path.abspath(r"C:\Users\upsal\Downloads\RXNORM\rrf\RXNCUI.RRF")

df = pd.read_csv(rxnconso_file_location, sep='|', header=None, names=headers, dtype=str, na_values=[''])

df['CODE_SET'] = 'Rxnorm'
df['VERSION_MONTH'] = 'May'

print("DataFrame before saving to CSV:")
print(df)

# output_csv_path = r"C:\Users\upsal\Downloads\RXNORM\rrf\rxnconso_processed.csv"
# df.to_csv(output_csv_path, index=False)

#print(f"\nProcessed data saved to: {output_csv_path}")
