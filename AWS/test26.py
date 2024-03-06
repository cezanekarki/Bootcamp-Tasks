from zipfile import ZipFile
import pandas as pd
import os
import re 

path = "F:\\rx"

# #opening the zip file in READ mode
with ZipFile(path + "\\RxNorm_full_11072022.zip",'r') as zip:   
    if (os.path.exists(path+"\\rrf")):  #if the file is already extracted then extraction is note executed 
        pass
    else:   #if there is no file extracted then the extraction process is being processed 
        #extracting all the files
        for file in zip.namelist():     #filtering through all files inside the zip file 
            if file.startswith('rrf/'):     #only extracting the file that starts with 'rrf'
                print('Extracting...')
                zip.extract(file, 'F:\\rx')     #EXTRACTION PROCESS 
        print('Extraction Completed')

e_file = pd.ExcelFile(path+'\\RXNORM.xlsx')   #opening the excel file using pandas 
sheet_list = e_file.sheet_names   #seperating the sheets inside the excel file and saving it into a list 
sheet_list.sort()   #sorting the list in order 
#print(sheet_list)

dir_list = os.listdir(path+"\\rrf")     #listing all the rrf file in a list 
dir_list.sort()     #sorting the list in order 
#print(dir_list)

for i in range(10):
    df_dict = pd.read_excel(e_file, sheet_name=sheet_list[i], header=None)        #reding the excel sheet from the excel file using index sheet are accessed
    print(sheet_list[i] + ' is being displayed.')   #message
    print('******************')   #message
    df_dict = df_dict[1].to_list()      #select the 1 index of the sheet (which is the heading content column) and converting it into list  
    # print(df_dict)
    t_file = open(path + "\\rrf\\"+ dir_list[i], 'r',  encoding="utf-8")    #opening the rrf file using the index the files are opened order wise 
    df = pd.read_table(t_file,sep="|", header=None)     #reading the rrf file 
    df['VERSION_MONTH'] = ['2022-11'] * df.__len__()    #adding version month cloumn 
    df.columns = df_dict        #adding the list of colums from excel file in the dataframe 
    df['CODE_SET'] = ['RXNORM'] * df.__len__()      #setting the value of codeset column into RXNORM 

    #for date formatting 
    date_cols = [col for col in df.columns if 'DATE' in col or 'RXNORM_TERM_DT' in col]     #filtering through columns where columns with 'DATA' and 'RXNORM_TERM_DT' are filtered out 
    for a in date_cols:     #iterating the column in the variable date_cols
        df[a] = pd.to_datetime(df['CREATE_DATE']) .dt.strftime("%Y-%m-%d")      #changing the time format of all the filtered columns 

    pd.set_option('display.max_columns', None)  #displays all the columns 
    print(df)
    print('*******************')



# df_dict = pd.read_excel(e_file, sheet_name=sheet_list[1], header=None)        #reding the excel sheet from the excel file using index sheet are accessed
# print(sheet_list[1] + ' is being displayed.')   #message
# print('************************************')   #message
# df_dict = df_dict[1].to_list()      #select the 1 index of the sheet (which is the heading content column) and converting it into list 
# # print(df_dict)
# t_file = open(path + "\\rrf\\"+ dir_list[1], 'r',  encoding="utf-8") 
# df = pd.read_table(t_file,sep="|", header=None)
# df['VERSION_MONTH'] = ['2022-11'] * df.__len__() 
# df.columns = df_dict
# df['CODE_SET'] = ['RXNORM'] * df.__len__()

# date_cols = [col for col in df.columns if 'DATE' in col or 'RXNORM_TERM_DT' in col] 
# for a in date_cols:
#     df[a] = pd.to_datetime(df['CREATE_DATE']) .dt.strftime("%Y-%m-%d")

# pd.set_option('display.max_columns', None)  #displays all the columns 
# print(df)