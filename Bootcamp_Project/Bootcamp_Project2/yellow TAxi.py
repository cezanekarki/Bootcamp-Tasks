# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/taxi_data-1.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking null values before transformation

# COMMAND ----------

null_counts = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}

for column, count in null_counts.items():
    print(f" '{column}': {count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting dates whichever in string fromat into proper date format

# COMMAND ----------

df = df.withColumn("lpep_pickup_datetime", F.to_timestamp(F.col("lpep_pickup_datetime"), "dd-MM-yy h:mm:ss a"))\
    .withColumn("lpep_dropoff_datetime", F.to_timestamp(F.col("lpep_dropoff_datetime"), "dd-MM-yy h:mm:ss a"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking null values after transformation

# COMMAND ----------

null_counts = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}

for column, count in null_counts.items():
    print(f" '{column}': {count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ####With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# MAGIC ####Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# MAGIC ####To do so, choose your table name and uncomment the bottom line.

# COMMAND ----------

permanent_table_name = "taxi_data"

df.write.format("parquet").saveAsTable(permanent_table_name)
