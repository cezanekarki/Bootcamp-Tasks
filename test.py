# import zipfile
# import io
# import pandas as pd

# def process_zip(zip_file_path):
#     with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#         rrf_files = [file for file in zip_ref.namelist() if file.startswith('rrf/')]

#         if rrf_files:
#             print("Contents of 'rrf' folder:")
#             for file_name in rrf_files:
#                 print(file_name)
#             print("*****************")

#             # Reading and printing the content of the first file in the 'rrf' folder
#             if rrf_files:
#                 first_file_content = zip_ref.read(rrf_files[0])
#                 print(f"Content of the first file '{rrf_files[0]}':\n{first_file_content.decode('utf-8')}")
#         else:
#             print("The 'rrf' folder does not exist in the zip file.")

# def read_excel_file(excel_file_path):
#     try:
#         # Read the Excel file using pandas
#         xls = pd.ExcelFile(excel_file_path)

#         # Select the second column (column 'B') from the first sheet without treating the first row as the header
#         first_sheet_df = pd.read_excel(excel_file_path, sheet_name=xls.sheet_names[0], header=None, engine='openpyxl')
#         print(f"Sheet names: {xls.sheet_names}")
#         print(f"First sheet:\n{first_sheet_df}")
#         second_column = first_sheet_df.iloc[:, 1]  # Selecting the second column (column 'B' in zero-based index)

#         # Display the selected column
#         print(f"\nSelected Column (Column 'B') from the first sheet:\n{second_column}")
#     except Exception as e:
#         print(f"An error occurred while reading the Excel file: {e}")

# # Example usage:
# zip_file_path = "C:\\Users\\sabind\\Downloads\\RxNorm_full_11072022.zip"
# excel_file_path = "C:\\Users\\sabind\\Downloads\\RXNORM.xlsx"

# # process_zip(zip_file_path)
# read_excel_file(excel_file_path)

import pandas as pd

# Your columns and data
columns = ['ATOM_ID', 'META_ATOM_ID', 'STRING', 'ARCHIVE_DATE', 'CREATE_DATE', 'UPDATE_DATE', 'CODE', 'BRAND_FLAG', 'LANGUAGE', 'RXNORM_TERM_DT', 'SOURCE_ATOM_ID', 'VERSION_SOURCE_ABB', 'CONCEPT_ID', 'CONCEPT_SOURCE_ABB', 'TERM_TYPE', 'NEW_RXCUI', 'CODE_SET', 'VERSION_MONTH']
data = [['947', 'A10335796', 'Mesna', '2020-04-27', '03/10/2005 02:03:47 PM', '04/27/2020 09:04:10 PM', '44', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '44', 'RXNORM', 'IN', '44', ''], ['1424', 'A10334758', 'beta-Alanine', '2020-04-27', '03/10/2005 02:03:47 PM', '04/27/2020 09:04:16 PM', '61', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '61', 'RXNORM', 'IN', '61', ''], ['1684', 'A10334529', '4-Aminobenzoic Acid', '2020-04-27', '03/10/2005 02:03:47 PM', '04/27/2020 09:04:14 PM', '74', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '74', 'RXNORM', 'IN', '74', ''], ['2192', 'A16791816', 'Eicosapentaenoic Acid', '2020-04-27', '03/10/2005 02:03:47 PM', '04/27/2020 09:04:18 PM', '90', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '90', 'RXNORM', 'PIN', '90', ''], ['2265', 'A10334531', '5-Hydroxytryptophan', '2020-04-27', '03/10/2005 02:03:47 PM', '11/06/2020 06:11:15 AM', '94', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '94', 'RXNORM', 'IN', '94', ''], ['2311', 'A16793037', 'Ticlopidine Hydrochloride', '2020-04-27', '03/10/2005 02:03:47 PM', '04/27/2020 09:04:19 PM', '97', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '97', 'RXNORM', 'PIN', '97', ''], ['2332', 'A10334533', '6-Aminocaproic Acid', '2020-04-27', '03/10/2005 02:03:47 PM', '04/27/2020 09:04:09 PM', '99', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '99', 'RXNORM', 'IN', '99', ''], ['2453', 'A10334534', '6-Mercaptopurine', '2010-10-21', '03/10/2005 02:03:47 PM', '10/21/2010 02:10:12 AM', '103', '', 'ENG', '04-oct-2010 00:00:00', '', 'RXNORM_10AA_101004F', '103', 'RXNORM', 'IN', '103', ''], ['2663', 'A10336065', 'Oxyquinoline', '2020-04-27', '03/10/2005 02:03:47 PM', '04/27/2020 09:04:09 PM', '110', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '110', 'RXNORM', 'IN', '110', ''], ['4330', 'A10334539', 'Acebutolol', '2020-04-27', '03/10/2005 02:03:47 PM', '04/27/2020 09:04:14 PM', '149', '', 'ENG', '06-apr-2020 00:00:00', '', 'RXNORM_19AB_200406F', '149', 'RXNORM', 'IN', '149', '']]

# Count the number of columns
num_columns = len(columns)
print(f"Number of columns: {num_columns}")

# # Pad each row with empty strings if the number of columns doesn't match
data = [row + [''] * (num_columns - len(row)) for row in data]
# print columns of the data
print(data.columns)


# # Creating a DataFrame
# df = pd.DataFrame(data, columns=columns)

# Displaying the DataFrame
# print(df)
