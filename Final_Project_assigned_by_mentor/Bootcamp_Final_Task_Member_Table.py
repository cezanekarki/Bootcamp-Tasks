# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql import functions as F

# COMMAND ----------

# Task#3: Create target table by appliying below transformation logic:
# 1. Remove duplicate records and keep only valid records.
# 2. Blank fields should have 'NULL' value.
# 3. Zip should have length of 5 characters, if its less then 5 then put 'NULL' value and if its grater than 5 then accept only first 5.
# 4. Phones should have length of 10 characters, if its less then 10 then put NULL value. and if its grater than 10 then accept only first 10.
# 5. Invalid mail and date_of_birth should be 'NULL' value.
# 6. Add new field with : address, City, State, zip.
# address_details nvarchar

# COMMAND ----------

email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

df_member = spark.read.format("csv").option("inferSchema", True).option("header", True).load("dbfs:/FileStore/tables/Member2.csv") \
    .withColumn("ZIP", F.when(F.length(F.col("ZIP")) >= 5, F.substring(F.col("ZIP"), 1, 5))) \
    .withColumn("phones", F.when(F.length(F.col("phones")) >= 10, F.col("phones").substr(1, 10))) \
    .withColumn("email", F.when(F.col("email").rlike(email_pattern), F.col("email"))) \
    .withColumn("date_of_birth", F.when(F.col("date_of_birth") < F.current_date(), F.col("date_of_birth"))) \
    .withColumn("address_details", F.concat_ws(", ", F.col("address"), F.col("City"), F.col("State"), F.col("ZIP")))

df_member = df_member.dropDuplicates(["Member_ID"])
df_member.display()

# COMMAND ----------

df_member.printSchema()

# COMMAND ----------

df_member.createOrReplaceTempView("temp_df")


spark.sql("""
    CREATE TABLE Member10
    AS
    SELECT
        CAST(Member_ID AS STRING) AS Member_ID,
        first_name,
        middle_name,
        last_name,
        address,
        city,
        state,
        ZIP,
        phones,
        email,
        gender,
        date_of_birth,
        CAST(year_of_birth AS STRING) AS year_of_birth,
        address_details
    FROM
        temp_df
""")


spark.catalog.dropTempView("temp_df")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE Member10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from member10

# COMMAND ----------

df_member = spark.read.format("csv").option("inferSchema", True).option("header", True).load("dbfs:/FileStore/tables/Member2.csv")
df_member.write.mode("overwrite").saveAsTable("source_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE table source_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_table

# COMMAND ----------

source_schema_query = "DESCRIBE source_table"
source_schema = spark.sql(source_schema_query)
source_schema.show()

target_schema_query = "DESCRIBE Member10"
target_schema = spark.sql(target_schema_query)
target_schema.show()

# COMMAND ----------

source_schema.collect()

# COMMAND ----------

# 5.1 : Validate below things on soruce and target tables(DDL validation):
# Validate column count
# Validate unique columns
# Validate mistmatch columns between source and target
# Validate column data types(matched vs mistached)
# Validate row count
# Final query to retrieve validation results

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

def validate_tables(spark, source_table_name, target_table_name):
    source_schema = spark.sql(f"DESCRIBE {source_table_name}")
    target_schema = spark.sql(f"DESCRIBE {target_table_name}")
    
    source_columns = [row['col_name'] for row in source_schema.collect()]
    target_columns = [row['col_name'] for row in target_schema.collect()]
    source_datatypes = [row['data_type'] for row in source_schema.collect()]
    target_datatypes = [row['data_type'] for row in target_schema.collect()]
    
    source_row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {source_table_name}").collect()[0]['row_count']
    target_row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {target_table_name}").collect()[0]['row_count']
    
    source_columns_count = len(source_columns)
    target_columns_count = len(target_columns)
    common_columns = set(source_columns).intersection(target_columns)
    source_unique_columns = set(source_columns).difference(target_columns)
    target_unique_columns = set(target_columns).difference(source_columns)

    count_check = [
        ("Source Table Columns:", source_columns_count, ", ".join(source_columns)),
        ("Target Table Columns:", target_columns_count, ", ".join(target_columns)),
        ("Common Columns:", None, ", ".join(common_columns) if common_columns else "None"),
        ("Columns Unique to Source Table:", None, ", ".join(source_unique_columns) if source_unique_columns else "None"),
        ("Columns Unique to Target Table:", None, ", ".join(target_unique_columns) if target_unique_columns else "None")
    ]
    df_column_count_check =  spark.createDataFrame(count_check, ["Description", "Count", "Columns"])

    # ------------------------Validate column count----------------------------------------
    column_comparison = []
    for column in source_columns:
        if column in target_columns:
            column_comparison.append((column, column, "Matched"))
        else:
            column_comparison.append((column, None, "Mismatched"))
    for column in target_columns:
        if column not in source_columns:
            column_comparison.append((None, column, "Mismatched"))
    result_df_for_column_comparison = spark.createDataFrame(column_comparison, ["Source_Column", "Target_Column", "Result"])
    
    # ------------------------Validate column data types-----------------------------------
    result = []
    for i, (source_type, target_type, column_name) in enumerate(zip(source_datatypes, target_datatypes, target_columns)):
        result.append(Row(Column_Name=column_name, Source_Data_Type=source_type, Target_Data_Type=target_type, Result="Matched" if source_type == target_type else "Mismatched"))
    max_length = max(len(source_datatypes), len(target_datatypes))
    for i in range(len(source_datatypes), max_length):
        result.append(Row(Column_Name=target_columns[i], Source_Data_Type=source_datatypes[i] if i < len(source_datatypes) else None, Target_Data_Type=target_datatypes[i] if i < len(target_datatypes) else None, Result="Mismatched"))
    result_df_for_datatypes = spark.createDataFrame(result)
    
    # ------------------------Validate row count-------------------------------------------
    validate_row_count = []
    if source_row_count == target_row_count:
        validate_row_count.append((source_row_count, target_row_count, 'Matched'))
    else:
        validate_row_count.append((source_row_count, target_row_count, 'Mismatched'))

    result_for_row_validation = spark.createDataFrame(validate_row_count, ["Source_table_row_count", "Target_table_row_count", "Result"])

    return df_column_count_check, result_df_for_column_comparison, result_df_for_datatypes, result_for_row_validation


source_table_name = "source_table"
target_table_name = "member10"
column_count_check, column_comparison, datatype_comparison, row_count_validation = validate_tables(spark, source_table_name, target_table_name)
column_count_check.show(truncate = False )
print("-----------Validate mistmatch columns between source and target-----------")
column_comparison.show()
print()
print("------------Validate column data types(matched vs mistached)between source and target-------------")
datatype_comparison.show()
print()
print("------------Validate row count------------")
row_count_validation.show()


import pandas as pd
column_count_check_pd = column_count_check.toPandas()
column_comparison_pd = column_comparison.toPandas()
datatype_comparison_pd = datatype_comparison.toPandas()
row_count_validation_pd = row_count_validation.toPandas()

excel_file_path = "/tmp/validation_results5_1.xlsx"

with pd.ExcelWriter(excel_file_path) as writer:
    column_count_check_pd.to_excel(writer, sheet_name='Column_Count_Check', index=False)
    column_comparison_pd.to_excel(writer, sheet_name='Column Comparison', index=False)
    datatype_comparison_pd.to_excel(writer, sheet_name='Data Type Comparison', index=False)
    row_count_validation_pd.to_excel(writer, sheet_name='Row Count Validation', index=False)

dbutils.fs.mv('file:/tmp/validation_results5_1.xlsx','dbfs:/FileStore/validation_results5_1.xlsx')

file_path = "/files/validation_results5_1.xlsx"
temp_token = "3784171750263355"
download_link = f"https://community.cloud.databricks.com{file_path}?o={temp_token}"

download_link


# COMMAND ----------

# 5.2 : Validate below things on each fields on source and target table(Data profiling)
# Validate record count
# validate distinct record counts for each field.
# Validate null count for each field.
# validate distinct record percenatge for each field.
# Validate null percentage for each field.
# Final query to retrieve validation results

# COMMAND ----------

from pyspark.sql.functions import countDistinct

def distinct_record_counts(df):
    distinct_counts = {}
    for col in df.columns:
        distinct_counts[col] = df.select(countDistinct(col)).collect()[0][0]
    return distinct_counts

def null_counts(df):
    rows = []
    for col in df.columns:
        null_count = df.where(df[col].isNull()).count()
        not_null_count = df.count() - null_count 
        distinct_count = distinct_record_counts(df)[col]
        percent_distinct = (distinct_count / not_null_count) * 100 if not_null_count != 0 else 0
        percent_null = (null_count / not_null_count) * 100 if not_null_count != 0 else 0
        rows.append(Row(Column_Name=col, Distinct_Record_Count=distinct_count, Null_Count=null_count, Not_Null_Count=not_null_count, Percentage_Distinct_Count=round(percent_distinct, 2), Percentage_Null_Count=round(percent_null, 2)))
    return spark.createDataFrame(rows)

def validate_counts(source_df, target_df):
    source_null_counts = null_counts(source_df)
    target_null_counts = null_counts(target_df)

    print("Source table validation:")
    source_null_counts.show()

    print("\nTarget table validation:")
    target_null_counts.show()
    
    source_pd_df = source_null_counts.toPandas()
    target_pd_df = target_null_counts.toPandas()
    
    excel_file_path2 = "/tmp/validation_results5_2.xlsx"

    with pd.ExcelWriter(excel_file_path2) as writer:
        source_pd_df.to_excel(writer, sheet_name='Source_Table_Validation', index=False)
        target_pd_df.to_excel(writer, sheet_name='Target_Table_Validation', index=False)
    dbutils.fs.mv('file:/tmp/validation_results5_2.xlsx','dbfs:/FileStore/validation_results5_2.xlsx')

source_df = spark.table(source_table_name)
target_df = spark.table(target_table_name)

validate_counts(source_df, target_df)

file_path = "/files/validation_results5_2.xlsx"
temp_token = "3784171750263355"
download_link = f"https://community.cloud.databricks.com{file_path}?o={temp_token}"

download_link

