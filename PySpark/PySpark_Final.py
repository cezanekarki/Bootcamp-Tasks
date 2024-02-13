# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
spark

# COMMAND ----------

df_csv = spark.read.format("csv").load("dbfs:/FileStore/tables/batch.csv")

# COMMAND ----------

df_csv.show()

# COMMAND ----------

df_csv=spark.read.format("csv").option("header",True).load("dbfs:/FileStore/tables/batch.csv")

# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DateType

# COMMAND ----------

sch = StructType([
    StructField("id",IntegerType()),
    StructField("name",StringType()),
    StructField("dob",DateType()),
    StructField("age",IntegerType()),
    StructField("salary",IntegerType()),
    StructField("Department",StringType()),    
])

# COMMAND ----------

df_csv=spark.read.format("csv").schema(sch).option("header",True).load("dbfs:/FileStore/tables/batch.csv")

# COMMAND ----------

df_json = spark.read.format("Json").load("dbfs:/FileStore/tables/Json")

# COMMAND ----------

df_json.show()

# COMMAND ----------

df_json = df_json.select(
    df_csv.columns
)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as F

# COMMAND ----------

df_json.select(
    "salary"
).show()

# COMMAND ----------

df_json.select(
    df_json.salary + 0.5 *df_json.salary,
    F.year(F.current_timestamp()) - F.year("dob"),
    F.year(F.current_timestamp()) - F.year(F.col("dob"))
).show()

# COMMAND ----------



df_union = df_csv.union(df_json)





# COMMAND ----------


df_union.show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

cols = Window.partitionBy([col(x) for x in df_union.columns])


df_union = df_union.withColumn("count", count("*").over(cols))

df_union = df_union.withColumn("is_duplicate", when(col("count") > 1, True).otherwise(False))

df_union = df_union.drop("count")




# COMMAND ----------


 
window = Window.partitionBy(df_union.columns).orderBy("id")
#df_new= df_union.withColumn("row_number", F.row_number().over(window))
#df_is_duplicate = df_new.withColumn("is_Duplicate", F.when(F.col("row_number") == 1, False).otherwise(True))
df_is_duplicate = df_new.withColumn("is_Duplicate", F.row_number().over(window) != 1)

#df1=df_is_duplicate.drop("row_number")
 
df1.show()


# COMMAND ----------



window_spec = Window.partitionBy("department").orderBy("salary")

df_with_row_number = df_union.withColumn("row_number", F.row_number().over(window_spec))

df_with_comparison = df_with_row_number.withColumn(
    "is_unique",
    F.when(F.col("row_number") == 1, True.otherwise(False)
)


df_with_comparison.show()



# COMMAND ----------


df_union.show()


# COMMAND ----------

df_json.show()


# COMMAND ----------

window_spec = Window.partitionBy("department")

a = df_union.withColumn(
    "mean_salary_department",
    F.mean("Salary").over(window_spec)
)
a.show()


# COMMAND ----------



window_spec = Window.partitionBy("department")

df_with_comparison = a.withColumn(
    "is_higher_salary",
    F.when(F.col("salary") >= F.col("mean_salary_department"), True).otherwise(False)
)

df_with_comparison.show()



# COMMAND ----------


mean_salary = df_union.agg(F.mean("salary")).collect()[0][0]

df_with_comparison = df_union.withColumn(
    "is_higher_salary",
    F.when(F.col("salary") >= mean_salary, True).otherwise(False)
)

df_with_comparison.show()




# COMMAND ----------


