# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark

# COMMAND ----------

from pyspark.sql.types import *


# COMMAND ----------

schema = StructType([StructField("id", IntegerType()),
                     StructField("name", StringType()),
                     StructField("dob", DateType()),
                     StructField("age", IntegerType()),
                     StructField("salary", IntegerType()),
                     StructField("department", StringType())])
type(schema)

# COMMAND ----------

df_csv = spark.read.format('csv').schema(schema).load('dbfs:///FileStore/tables/csv/batch.csv',header='True')

# COMMAND ----------

df_csv.show()

# COMMAND ----------

df_json = spark.read.format('json').load('dbfs:///FileStore/tables/json')

# COMMAND ----------

df_json.show()

# COMMAND ----------

df_json = df_json.select(df_csv.columns)

# COMMAND ----------

df = df_csv.union(df_json)
df.sort('id').show(50)

# COMMAND ----------



# COMMAND ----------

# from pyspark.sql import functions as F

# df_new = df.withColumn('is_Duplicate', F.expr("Count(*) over ( partition by id, name order by id)"))
# df_is_duplicate = df_new.withColumn('is_Duplicate', F.when(F.col("is_Duplicate") > 1, True).otherwise(False))

# # df_new.show()
# df_is_duplicate.show()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

window = Window.partitionBy(df.columns).orderBy("id")
# df_new= df.withColumn("row_number", F.row_number().over(window))
df_is_duplicate = df_new.withColumn("is_Duplicate", F.row_number().over(window) != 1)
df_is_duplicate.show()

# COMMAND ----------

df1=df_is_duplicate).drop("row_number")

df1.sort("id").show()

# COMMAND ----------



# COMMAND ----------

# Calculate mean salary and check if it is greater or equal to the salary of employees in each department.
 
window_1 = Window.partitionBy("department")

df_salary_by_dept= df.withColumn(
    "mean_salary_by_department",
    F.mean("Salary").over(window_1)
)
df_salary_by_dept.display()


# COMMAND ----------

# help(F.mean("salary"))

# COMMAND ----------


df_compared = df_salary_by_dept.withColumn(
    "is_higher_salary_than_dept_avg",
    F.when(F.col("salary") >= F.col("mean_salary_by_department"), True).otherwise(False)
)

df_compared.show()

# COMMAND ----------

# Calculate mean salary and check if it is greater or equal to the salary of all employees.


window_2 = Window.orderBy()
df_with_comparison = df.withColumns(
    {
    "mean_sal_of_all_emp": F.mean("salary").over(window_2), 
    "is_higher_salary_than_avg_of_all": F.when(F.col("mean_sal_of_all_emp") >= F.col("salary"), True).otherwise(False)
    }
)

df_with_comparison.show()


# COMMAND ----------

ref_df = spark.read.format('parquet').load('dbfs:///FileStore/tables/parquet/reference.parquet')

# COMMAND ----------

ref_df.show()

# COMMAND ----------

df.join(
    ref_df,
    "department",
    "left",
).show()

# COMMAND ----------

df.write.mode("overwrite").format('csv').save("dbfs:/FileStore/tables/final")

# COMMAND ----------

# MAGIC %fs ls dbfs:///FileStore/tables/final

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

spark.sql("show tables").show()

# COMMAND ----------


