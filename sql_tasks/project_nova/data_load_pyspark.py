# Loading the dbfs path for the each files in the databrick environment
address_path = "dbfs:/FileStore/Address.xlsx" 
contactinfo_path = "dbfs:/FileStore/contactinfo.txt" 
detail_path = "dbfs:/FileStore/Detail.csv" 
header_path = "dbfs:/FileStore/header.json"

# Table creation for the Address
df_address = spark.read.format("com.crealytics.spark.excel").option("header","True").option("inferSchema","True").load(address_path)
df_address.write.mode("overwrite").saveAsTable("Address") ## Overwrites and Saves in a Table as Address

# Table creation for the header
df_header = spark.read.option("multiline", "true").json(header_path)
df_header.write.mode("overwrite").option("path", "Header").option("dataType", "LongType").saveAsTable("Header")

# Table creation for the contacts information
df_contactinfo = spark.read.format("csv").option("header", "true").option("sep", "\t").load(contactinfo_path)
pandas_df = df_contactinfo.toPandas()
spark_df = spark.createDataFrame(pandas_df)
spark_df.createOrReplaceTempView("Temp")
# Create or replace a table using Spark SQL
spark.sql("CREATE OR REPLACE TABLE contactinfo AS SELECT * FROM Temp")


# Table creation for the Details
# Read CSV file into a DataFrame
df_csv = spark.read.csv(detail_path, header=True, inferSchema=True)
df_csv.write.mode("overwrite").option("path", "Detail").option("dataType", "IntegerType").saveAsTable("Detail")

