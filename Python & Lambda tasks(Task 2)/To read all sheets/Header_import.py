import pandas as pd

sheet_names = ['RXNCONSO', 'RXNSAT', 'RXNDOC', 'RXNREL', 'RXNSAB', 'RXNSTY', 'RXNATOMARCHIVE', 'RXNCUI', 'RXNCUICHANGES']
file_location = r"C:\Users\upsal\Downloads/RxNorm_Header.xlsx"

dfs = {}

for sheet_name in sheet_names:
    df = pd.read_excel(file_location, sheet_name=sheet_name)
    dfs[sheet_name] = df

for sheet_name, df in dfs.items():
    print(f"Sheet Name: {sheet_name}")
    print(df)
