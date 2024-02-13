"""
Loading the data from different format such as csv,json,excel,text in databrick and 
creating tables based on the provided data.
"""

#Reading and creating table from Detail.csv file
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/satishsubedi18@gmail.com/Detail.csv")
temp_table_name = 'Detail_csv'
df1.createOrReplaceTempView(temp_table_name)

df1.write.mode("overwrite").saveAsTable("DetailsInformation")


# Reading and creating table from text file
text_file = spark.read.format("csv") .option("header","true").option("sep","\t").load("dbfs:/FileStore/shared_uploads/satishsubedi18@gmail.com/contactinfo.txt")
text_file.createOrReplaceTempView('TextTable')

spark.sql("""
    CREATE or REPLACE TABLE ContactInformation
    AS
    SELECT *
    FROM TextTable
""")


# Reading and creating table from header file
json_path = "dbfs:/FileStore/shared_uploads/satishsubedi18@gmail.com/header-1.json"
json_header = spark.read.option("multiline","true").json(json_path)
json_header.createOrReplaceTempView("TempJsonHeader")

spark.sql("""
          Create or Replace table HeaderTable 
          as 
          SELECT * from TempJsonHeader
""")


#Reading and creting table form excel file
# Excel External library 
# com.crealytics:spark-excel_2.12:0.13.5
excel_file_path = "dbfs:/FileStore/shared_uploads/satishsubedi18@gmail.com/Address-6.xlsx"
excel_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(excel_file_path)


excel_df.createTempView("ExcelData")
spark.sql("""
          CREATE or replace table Address
          as 
          SELECT * from ExcelData
""")

