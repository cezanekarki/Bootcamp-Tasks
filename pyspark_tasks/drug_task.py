# Databricks notebook source
# MAGIC %md ## Imports

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load CSV Data") \
    .getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql.functions import split, explode



# COMMAND ----------

# MAGIC %md ## Load CSV Product and Package

# COMMAND ----------



csv_file_path = "dbfs:/FileStore/tables/Drugs_package.csv"

csv_file_path1 = "dbfs:/FileStore/tables/Drugs_product.csv"
df1 = spark.read.csv(csv_file_path, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)




# COMMAND ----------

df1.show()

# COMMAND ----------

df2.show()

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

display(df2)

# COMMAND ----------

print(type(df2['PRODUCTID']))

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

# MAGIC %md ## Task 1: Renaming header of whole df to snake_case
# MAGIC

# COMMAND ----------

# new_columns = [col_name.lower().replace(' ', '_') for col_name in df2.columns]


# COMMAND ----------

# print(new_columns)

# COMMAND ----------

df_dp = df2 \
    .withColumnRenamed("PRODUCTID", 'product_id') \
    .withColumnRenamed("PRODUCTNDC", 'product_ndc') \
    .withColumnRenamed("PRODUCTTYPENAME", 'product_type_name') \
    .withColumnRenamed("PROPRIETARYNAME", 'proprietary_name') \
    .withColumnRenamed("PROPRIETARYNAMESUFFIX", 'proprietary_name_suffix') \
    .withColumnRenamed("NONPROPRIETARYNAME", 'non_proprietary_name') \
    .withColumnRenamed("DOSAGEFORMNAME", 'dosage_form_name') \
    .withColumnRenamed("ROUTENAME", 'route_name') \
    .withColumnRenamed("STARTMARKETINGDATE", 'start_marketing_date') \
    .withColumnRenamed("ENDMARKETINGDATE", 'end_marketing_date') \
    .withColumnRenamed("MARKETINGCATEGORYNAME", 'marketing_category_name') \
    .withColumnRenamed("APPLICATIONNUMBER", 'application_number') \
    .withColumnRenamed("LABELERNAME", 'labeler_name') \
    .withColumnRenamed("SUBSTANCENAME", 'substance_name') \
    .withColumnRenamed("ACTIVE_NUMERATOR_STRENGTH", 'active_numerator_strength') \
    .withColumnRenamed("ACTIVE_INGRED_UNIT", 'active_ingred_unit') \
    .withColumnRenamed("PHARM_CLASSES", 'pharm_classes') \
    .withColumnRenamed("DEASCHEDULE", 'deaschedule')


# COMMAND ----------

df_pk = df1 \
    .withColumnRenamed("PRODUCTID", "product_id") \
    .withColumnRenamed("PRODUCTNDC", "product_ndc") \
    .withColumnRenamed("NDCPACKAGECODE", "ndc_package_code") \
    .withColumnRenamed("PACKAGEDESCRIPTION", "package_description")

# COMMAND ----------

display(df_pk)

# COMMAND ----------

df_dp.printSchema()

# COMMAND ----------

# MAGIC %md ## Task 2: Change date to proper date

# COMMAND ----------


df_dp = df_dp.withColumn('start_marketing_date', to_date(df_dp['start_marketing_date'], 'yyyyMMdd'))


# COMMAND ----------

df_dp = df_dp.withColumn('end_marketing_date', to_date(df_dp['end_marketing_date'], 'yyyyMMdd'))


# COMMAND ----------

df1.count()
df2.count()

# COMMAND ----------

display(df_dp)
display(df_pk)

# COMMAND ----------

df_dp.count()

# COMMAND ----------

df_pk.count()

# COMMAND ----------

df_dp.select("substance_name")

# COMMAND ----------

# MAGIC %md ## Task 3: Splitting the ";"  separated rows

# COMMAND ----------

df = df_dp.select(
    "*",
    split(df_dp["substance_name"], ";").alias("sub_array"),
    split(df_dp["active_numerator_strength"], ";").alias("numero_array"),
    split(df_dp["active_ingred_unit"], ";").alias("ingred_array")
)


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ## Task 4: Exploding

# COMMAND ----------

# display(df.select(
#     # "*",  
#     explode(df.sub_array).alias("sub_explode",df.product_id)
#     # explode(df["numero_array"]).alias("numero_explode"),
#     # explode(df["ingred_array"]).alias("ingred_explode")
# ))


# COMMAND ----------

df_exploded = df.withColumn("sub_explode", explode(df.sub_array)) \
                .withColumn("numero_explode", explode(df.numero_array)) \
                .withColumn("ingred_explode", explode(df.ingred_array)) \
                .select("*")
                 #"sub_explode", "numero_explode", "ingred_explode", "product_id")

# COMMAND ----------

display(df_exploded)

# COMMAND ----------

df_exploded.count()

# COMMAND ----------
