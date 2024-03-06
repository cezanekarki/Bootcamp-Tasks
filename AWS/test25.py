from zipfile import ZipFile
import pandas as pd

file_path="C:\\Users\\sabind\\Downloads"
zip_file_path = file_path+"\\RxNorm_full_02052024.zip"
excel_file_path=file_path+'\\RxNorm_Header.xlsx'
txt_file_path=file_path+'\\allfiles'
 
def getAllSheets():
    print("Transformtion Started")
    try:
        before_conversion_dict={}
        after_conversion_dict={}
        with ZipFile(zip_file_path, 'r') as zip_ref:
            rrf_files = [file[len('rrf/'):] for file in zip_ref.namelist() if file.startswith('rrf/')]
            sorted_rrf_files = sorted(rrf_files) 

            if sorted_rrf_files:
                # print(sorted_rrf_files)
                #sheet list
                excelfile = pd.ExcelFile(excel_file_path)
                sheet=excelfile.sheet_names
                sheet.sort()
                
                # # count row in each file
                for file in sorted_rrf_files:
                    df=pd.read_csv(zip_ref.open(f'rrf/{file}'),sep='|',header=None, encoding="utf-8",low_memory=False)
                    # # file with its total row count
                    # print(file,df.__len__())
                    # count the number of rows of each file
                    # print(f"Number of rows  before conversion for {file}: {df.shape[0]}")
                    before_conversion_dict[file]=df.shape[0]
                    before_conversion_dict = {key.replace('.RRF', ''): value for key, value in before_conversion_dict.items()}


                # loop over all sheets
                for i in range(len(sorted_rrf_files)):
                    df_dict = pd.read_excel(excel_file_path, sheet_name=sheet[i], header=None)
                    df_dict = df_dict[1].to_list()
                    filet = zip_ref.open(f'rrf/{sorted_rrf_files[i]}')
                    df = pd.read_table(filet, sep='|', header=None, low_memory=False)
                    df['VERSION_MONTH'] = ['2024-6'] * df.__len__()
                    df.columns = df_dict
                    df['CODE_SET'] = ['RXNORM'] * df.__len__()
                    
                    
                    #for date formatting 
                    date_cols = [col for col in df.columns if 'DATE' in col or 'RXNORM_TERM_DT' in col]     #filtering through columns where columns with 'DATA' and 'RXNORM_TERM_DT' are filtered out 
                    for a in date_cols:     #iterating the column in the variable date_cols
                        # df[a] = pd.to_datetime(df['CREATE_DATE']).dt.strftime("%Y-%m-%d") 
                        df[a] = pd.to_datetime(df[a], format="%Y-%m-%d", errors='coerce').dt.strftime("%Y-%m-%d")
                        
                    after_conversion_dict[sheet[i]]=df.shape[0]
                    
                    df.to_csv(txt_file_path + str(sheet[i]) + '.txt', sep=',', index=False)

                    # print the every file name after after it is saved into the folder as filename.txt
                    print(f"File {sheet[i]}.txt is saved")
    

        # Convert dictionaries to DataFrames
        before_df = pd.DataFrame(list(before_conversion_dict.items()), columns=['File Name', 'Before Conversion'])
        after_df = pd.DataFrame(list(after_conversion_dict.items()), columns=['File Name', 'After Conversion'])

        # Merge DataFrames on 'File Name'
        merged_df = pd.merge(before_df, after_df, on='File Name')

        # Print the final table
        print(merged_df)
        
        print("Transformation Completed")
    except Exception as e:
        print(f"An error occurred : {e}")
        return None


getAllSheets()
 
