import pandas as pd
import zipfile
import json

# Example usage:
zip_file_path = "C:\\Users\\sabind\\Downloads\\RxNorm_full_02052024.zip"
excel_file_path = "C:\\Users\\sabind\\Downloads\\RXNORM.xlsx"

def process_zip(zip_file_path):
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            rrf_files = [file[len('rrf/'):] for file in zip_ref.namelist() if file.startswith('rrf/')]
            sorted_rrf_files = sorted(rrf_files) 

            if sorted_rrf_files:
                # Read the CSV file
                df = pd.read_csv(zip_ref.open(f'rrf/{sorted_rrf_files[4]}'), sep='|', header=None, encoding='utf-8')

                # Count the number of columns
                # num_columns = len(df.columns)

                # Remove the last column
                df = df.iloc[:, :-1]

                # Adding two columns at the end of the DataFrame
                # For the second last column, 'RxNorm' is added as the default value
                df[len(df.columns)] = 'RxNorm'

                # For the last column, '2024/03/05' is added as the default value
                df[len(df.columns)] = '2024/03/05'
                
                # print(df.head())

                return df
            else:
                print("The 'rrf' folder does not exist in the zip file.")
                return None

    except Exception as e:
        print(f"An error occurred while processing the zip file: {e}")
        return None



def read_excel_column(excel_file_path):
    try:
        xls = pd.ExcelFile(excel_file_path)
        sorted_sheet_names = sorted(xls.sheet_names)
        
        first_sheet_df = pd.read_excel(excel_file_path, sheet_name=sorted_sheet_names[4], header=None)
        second_column = first_sheet_df.iloc[:, 1]

        return second_column

    except Exception as e:
        print(f"An error occurred while processing the Excel column: {e}")
        return None

def combine_heading_and_data(zip_data, excel_columns):
    try:
        print(f"Length of zip_data: {len(zip_data.columns)}")
        print(f"Length of excel_columns: {len(excel_columns)}")
        # print(zip_data.head())

        # Create a DataFrame using zip_data and add column names from excel_columns
        combined_df = pd.DataFrame(zip_data, columns=[excel_columns])
        print(combined_df.head())
        return combined_df

    except Exception as e:
        print(f"An error occurred while combining heading and data: {e}")
        return None


# Use this function to combine the data
zip_lines = process_zip(zip_file_path)
excel_column = read_excel_column(excel_file_path)
combined_df = combine_heading_and_data(zip_lines, excel_column)


