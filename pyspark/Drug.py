# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark

# COMMAND ----------

# MAGIC %fs ls dbfs:////FileStore/tables

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Drugs_package.csv

# COMMAND ----------

df_csv1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/Drugs_package.csv")

# COMMAND ----------

df_csv1.show()

# COMMAND ----------

df_csv2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/Drugs_product.csv")

# COMMAND ----------

df_csv2.show()

# COMMAND ----------

df_csv2.columns

# COMMAND ----------

df_csv2.printSchema()

# COMMAND ----------

df_csv1.printSchema()

# COMMAND ----------

df_csv1.select(
    df_csv1.PRODUCTID.alias("product_id"),
).show()

# COMMAND ----------

df_csv1 = df_csv1.withColumnRenamed("PRODUCTID","product_id").withColumnRenamed("PRODUCTNDC", "product_ndc").withColumnRenamed("NDCPACKAGECODE", "ndc_package_code").withColumnRenamed("PACKAGEDESCRIPTION", "package_description")

# COMMAND ----------

display(df_csv1)

# COMMAND ----------

display(df_csv2)

# COMMAND ----------



# COMMAND ----------

# Renaming the columns
from pyspark.sql import functions as F
df_csv2 = df_csv2.select(
    F.col("PRODUCTID").alias("product_id"),
    F.col("PRODUCTNDC").alias("product_ndc"),
    F.col("PRODUCTTYPENAME").alias("product_type_name"),
    F.col("PROPRIETARYNAME").alias("proprietary_name"),
    F.col("PROPRIETARYNAMESUFFIX").alias("proprietary_name_suffix"),
    F.col("NONPROPRIETARYNAME").alias("non_proprietary_name"),
    F.col("DOSAGEFORMNAME").alias("dosage_form_name"),
    F.col("ROUTENAME").alias("route_name"),
    F.col("STARTMARKETINGDATE").alias("start_marketing_date"),
    F.col("ENDMARKETINGDATE").alias("end_marketing_date"),
    F.col("MARKETINGCATEGORYNAME").alias("marketing_category_name"),
    F.col("APPLICATIONNUMBER").alias("application_number"),
    F.col("LABELERNAME").alias("labeler_name"),
    F.col("SUBSTANCENAME").alias("substance_name"),
    F.col("ACTIVE_NUMERATOR_STRENGTH").alias("active_numerator_strength"),
    F.col("ACTIVE_INGRED_UNIT").alias("active_ingred_unit"),
    F.col("PHARM_CLASSES").alias("pharm_classes"),
    F.col("DEASCHEDULE").alias("dea_schedule"),
)

# COMMAND ----------

df_csv2 = df_csv2.withColumns(
    {
        "substance_name": F.split("substance_name", ";"),
        "active_numerator_strength": F.split("active_numerator_strength", ";"),
        "active_ingred_unit": F.split("active_ingred_unit", ";"),
    }
)

# COMMAND ----------

display(df_csv2)

# COMMAND ----------

# Creating a column that creates an array of structs.
# You can't use explode directly on the three columns.
df_csv2 = df_csv2.withColumn(
    "substance_details",
    F.arrays_zip("substance_name", "active_numerator_strength", "active_ingred_unit")
)

# COMMAND ----------

display(df_csv2.select(
    "substance_name",
    "active_numerator_strength",
    "active_ingred_unit",
    "substance_details"
))

# COMMAND ----------

# Finally you can explode the array of structs
df_csv2 = df_csv2.withColumn("substance_info", F.explode("substance_details"))

# COMMAND ----------

display(df_csv2.select(
    "substance_name",
    "active_numerator_strength",
    "active_ingred_unit",
    "substance_details",
    "substance_info"
))

# COMMAND ----------

# substance_info gives out all the keys as column name and the values as the rows.

df_csv2 = df_csv2.select(
    "*",
    "substance_info.*"
)

# Alternative but hectic
# df = df.select(
#     "*",
#     "substance_info.substance_name",
#     "substance_info.active_numerator_strength",
#     "substance_info.active_ingred_unit"
# )
display(df_csv2)

# COMMAND ----------

from pyspark.sql.functions import split, explode

# COMMAND ----------

df_exploded = df_split.select("PRODUCTID", "PRODUCTNDC", explode("substances_name").alias("substance_name"))

# COMMAND ----------

display(df_exploded)

# COMMAND ----------



# df_join1 = df_split1.join(df_split2, ["PRODUCTID", "PRODUCTNDC"])
# df_join_final = df_join1.join(df_split3, ["PRODUCTID", "PRODUCTNDC"])
# display(df_join_final)


# COMMAND ----------

df_split = df_csv2.withColumn("substances_name", split(df_csv2["SUBSTANCENAME"], ";"))
df_split = df_split.withColumn("numerator_strength", split(df_split["ACTIVE_NUMERATOR_STRENGTH"], ";"))
df_split= df_split.withColumn("active_ingred", split(df_split["ACTIVE_INGRED_UNIT"], ";"))


df_substance_explode = df_split.select("PRODUCTID", "PRODUCTNDC", explode("substances_name").alias("substance_name"))
df_strength_explode = df_split.select("PRODUCTID", "PRODUCTNDC", explode("numerator_strength").alias("active_numerator_strength"))
df_ingred_explode = df_split.select("PRODUCTID", "PRODUCTNDC", explode("active_ingred").alias("active_ingred_unit"))




df_join1 = df_substance_explode.join(df_strength_explode, ["PRODUCTID", "PRODUCTNDC"])


df_final = df_join1.join(df_ingred_explode, ["PRODUCTID", "PRODUCTNDC"])

display(df_final)


# df_exploded = df_substance_exploded.join(df_strength_exploded, ["PRODUCTID", "PRODUCTNDC"])
# df_exploded_final = df_exploded.join(df_ingred_exploded, ["PRODUCTID", "PRODUCTNDC"])

# display(df_exploded_final)

# COMMAND ----------

df_final = df_final.distinct()
display(df_final)

# COMMAND ----------


