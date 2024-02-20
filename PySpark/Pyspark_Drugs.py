# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Drugs_package.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Drugs_product.csv

# COMMAND ----------

df_package = spark.read.format("csv").option("header",True).load("dbfs:/FileStore/tables/Drugs_package.csv")

# COMMAND ----------

df_product = spark.read.format("csv").option("header",True).load("dbfs:/FileStore/tables/Drugs_product.csv")

# COMMAND ----------

df_package = df_package.withColumnRenamed("PRODUCTID","product_id" )\
                       .withColumnRenamed("PRODUCTNDC","product_ndc")\
                       .withColumnRenamed("NDCPACKAGECODE","ndc_package_code")\
                       .withColumnRenamed("PACKAGEDESCRIPTION","package_description")  

# COMMAND ----------

df_product = df_product.withColumnRenamed("PRODUCTID","product_id" )\
                       .withColumnRenamed("PRODUCTNDC","product_ndc")\
                       .withColumnRenamed("PRODUCTTYPENAME","product_type_name")\
                       .withColumnRenamed("PROPRIETARYNAME","proprietary_name")\
                       .withColumnRenamed("PROPRIETARYNAMESUFFIX","proprietary_name_suffix")\
                       .withColumnRenamed("NONPROPRIETARYNAME","non_proprietary_name")\
                       .withColumnRenamed("DOSAGEFORMNAME","dosage_form_name")\
                       .withColumnRenamed("ROUTENAME","route_name")\
                       .withColumnRenamed("STARTMARKETINGDATE","start_marketing_date")\
                       .withColumnRenamed("ENDMARKETINGDATE","end_marketing_date")\
                       .withColumnRenamed("MARKETINGCATEGORYNAME","marketing_category_name")\
                       .withColumnRenamed("APPLICATIONNUMBER","application_number")\
                       .withColumnRenamed("LABELERNAME","labeler_name")\
                       .withColumnRenamed("SUBSTANCENAME","substance_name")\
                       .withColumnRenamed("ACTIVE_NUMERATOR_STRENGTH","active_numerator_strength")\
                       .withColumnRenamed("ACTIVE_INGRED_UNIT","active_ingred_unit")\
                       .withColumnRenamed("PHARM_CLASSES","pharm_classes")\
                       .withColumnRenamed("DEASCHEDULE","dea_schedule")

# COMMAND ----------

from pyspark.sql.functions import col, date_format, to_date


# COMMAND ----------

df_product = df_product.withColumn("start_marketing_date",to_date(col("start_marketing_date"),"yyyyMMdd"))

# COMMAND ----------

df_product = df_product.withColumn("end_marketing_date",to_date(col("end_marketing_date"),"yyyyMMdd"))

# COMMAND ----------

df_drugs = df_package.join(
    df_product,
    ["product_id","product_ndc"],
    "full"
)

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

df_drugs = df_drugs.withColumn("substance_name", split(col("substance_name"), ";"))\
    .withColumn("active_numerator_strength",split(col("active_numerator_strength"),";"))\
    .withColumn("active_ingred_unit",split(col("active_ingred_unit"),";"))

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

df_drugs = df_drugs.withColumn("substance_name", f.explode(df_drugs["substance_name"]))\
                    .withColumn("active_numerator_strength",f.explode(df_drugs["active_numerator_strength"]))\
                    .withColumn("active_ingred_unit",f.explode(df_drugs["active_ingred_unit"]))

# COMMAND ----------

display(df_drugs)

# COMMAND ----------


