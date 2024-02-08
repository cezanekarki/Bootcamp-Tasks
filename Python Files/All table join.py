# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql import SparkSession
 
# Create a SparkSession
spark = SparkSession.builder \
.appName("ReadExcelWithHeader") \
.config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
.getOrCreate()
 
# Define the path to your Excel file
excel_file_path =  "/FileStore/tables/Address.xlsx"
addressdf = spark.read \
.format("com.crealytics.spark.excel") \
.option("header", "true") \
.option("inferSchema", "true") \
.load(excel_file_path)
 
# Show the DataFrame
addressdf.show() 

json_file_path = "/FileStore/tables/header.json"
 
# # Read the JSON file into a DataFrame and cache the result
headerdf = spark.read.json(json_file_path).cache()
headerdf=spark.read.json(json_file_path, multiLine=True)
 
 
headerdf.createOrReplaceTempView('your_temporary_view')
display(headerdf)
# File location and type
file_location = "/FileStore/tables/contactinfo.txt"
file_type = "csv"
 
# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = "\t"
 
# The applied options are for CSV files. For other file types, these will be ignored.
contactinfodf = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
 
display(contactinfodf)
# File location and type
file_location = "/FileStore/tables/Detail.csv"
file_type = "csv"
 
# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
 
# The applied options are for CSV files. For other file types, these will be ignored.
detaildf = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
 
display(detaildf)

# COMMAND ----------



# COMMAND ----------

# Create a view or table

header = "headerjson"
headerdf.createOrReplaceTempView(header)

detail = "detailcsv"
detaildf.createOrReplaceTempView(detail)

contactinfo = "contactinfotxt"
contactinfodf.createOrReplaceTempView(contactinfo)

address = "addressxlsx"
addressdf.createOrReplaceTempView(address)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from headerjson
# MAGIC inner join detailcsv 
# MAGIC on 
# MAGIC headerjson.id= detailcsv.id
# MAGIC inner join contactinfotxt
# MAGIC on
# MAGIC contactinfotxt.id=headerjson.id
# MAGIC inner join addressxlsx
# MAGIC on
# MAGIC headerjson.id= addressxlsx.id

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "header-2_json"

# df.write.format("parquet").saveAsTable(permanent_table_name)
