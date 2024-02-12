# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

sch = StructType([
    StructField("id",IntegerType()),
    StructField("name",StringType()),
    StructField("dob",DateType()),
    StructField("age",IntegerType()),
    StructField("salary",IntegerType()),
    StructField("department",StringType()),
])

# COMMAND ----------

df_csv = spark.read.format("csv").schema(sch).option("header",True).load("dbfs:/FileStore/tables/csv/batch.csv")

# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

df_json = spark.read.format("json").load("dbfs:/FileStore/tables/json")

# COMMAND ----------

df_json = df_json.select(df_csv.columns)

# COMMAND ----------

df = df_csv.union(df_json)

# COMMAND ----------

df.sort("id").show()

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.window import Window

# COMMAND ----------

window_spec = Window.partitionBy("id").orderBy("id")

# COMMAND ----------

df = df.withColumn("row_number",f.row_number().over(window_spec))

# COMMAND ----------

df = df.withColumn("is_duplicate",f.when(df["row_number"]==1,False).otherwise(True))

# COMMAND ----------

df.show()

# COMMAND ----------

#Calculate mean salary and check if it is greater or equal to the salary of employees in each department.

# COMMAND ----------

window_spec = Window.partitionBy("department")

# COMMAND ----------

df = df.withColumn("mean salary",f.mean("salary").over(window_spec))

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.withColumn("sal_is_greater_or_equal",f.when(df["salary"]>=df["mean salary"],"True").otherwise("False"))

# COMMAND ----------

df.show()

# COMMAND ----------

#Calculate mean salary and check if it is greater or equal to the salary of all employees.

# COMMAND ----------

window_spec = Window.partitionBy()

# COMMAND ----------

df = df.withColumn("emp_mean_salary",f.mean("salary").over(window_spec))

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.withColumn("emp_mean_sal_is_greater_or_equal",f.when(df["salary"]>=df["emp_mean_salary"],"False").otherwise("True"))

# COMMAND ----------

df.show()

# COMMAND ----------


