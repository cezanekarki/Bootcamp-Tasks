# Databricks notebook source
# MAGIC %fs ls dbfs:///FileStore/tables

# COMMAND ----------

# Loading the csv file
df_package = spark.read.format("csv").option("header", True).load("dbfs:/FileStore/tables/Drugs_package.csv")


# COMMAND ----------

display(df_package)

# COMMAND ----------

df_product = spark.read.format("csv").option("header", True).load("dbfs:/FileStore/tables/Drugs_product.csv")

# COMMAND ----------

display(df_product)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df_package.printSchema()

# COMMAND ----------

df_package=df_package.withColumnRenamed("PRODUCTID","product_id").withColumnRenamed("PRODUCTNDC","product_ndc").withColumnRenamed("NDCPACKAGECODE","ndcpackage_code").withColumnRenamed("PACKAGEDESCRIPTION","package_description")

# COMMAND ----------

df_package.printSchema()

# COMMAND ----------

df_product.printSchema()

# COMMAND ----------

# Renaming the columns
df_product = df_product.withColumnRenamed("PRODUCTID", "product_id")\
.withColumnRenamed("PRODUCTNDC", "product_ndc")\
.withColumnRenamed("PRODUCTTYPENAME", "product_type_name")\
.withColumnRenamed("PROPRIETARYNAME", "proprietary_name")\
.withColumnRenamed("PROPRIETARYNAMESUFFIX", "proprietary_name_suffix")\
.withColumnRenamed("NONPROPRIETARYNAME", "non_proprietary_name")\
.withColumnRenamed("DOSAGEFORMNAME", "dosage_form_name")\
.withColumnRenamed("ROUTENAME", "route_name")\
.withColumnRenamed("STARTMARKETINGDATE", "start_marketing_date")\
.withColumnRenamed("ENDMARKETINGDATE", "end_marketing_date")\
.withColumnRenamed("MARKETINGCATEGORYNAME", "marketing_category_name")\
.withColumnRenamed("APPLICATIONNUMBER", "application_number")\
.withColumnRenamed("LABELERNAME", "labeler_name")\
.withColumnRenamed("SUBSTANCENAME", "substance_name")\
.withColumnRenamed("ACTIVE_NUMERATOR_STRENGTH", "active_numerator_strength")\
.withColumnRenamed("ACTIVE_INGRED_UNIT", "active_ingred_unit")\
.withColumnRenamed("PHARM_CLASSES", "pharm_classes")\
.withColumnRenamed("DEASCHEDULE", "dea_schedule")

# COMMAND ----------

# DBTITLE 1,Next Method
# # Renaming the columns

# df_product = df_product.select(
#     F.col("PRODUCTID").alias("product_id"),
#     F.col("PRODUCTNDC").alias("product_ndc"),
#     F.col("PRODUCTTYPENAME").alias("product_type_name"),
#     F.col("PROPRIETARYNAME").alias("proprietary_name"),
#     F.col("PROPRIETARYNAMESUFFIX").alias("proprietary_name_suffix"),
#     F.col("NONPROPRIETARYNAME").alias("non_proprietary_name"),
#     F.col("DOSAGEFORMNAME").alias("dosage_form_name"),
#     F.col("ROUTENAME").alias("route_name"),
#     F.col("STARTMARKETINGDATE").alias("start_marketing_date"),
#     F.col("ENDMARKETINGDATE").alias("end_marketing_date"),
#     F.col("MARKETINGCATEGORYNAME").alias("marketing_category_name"),
#     F.col("APPLICATIONNUMBER").alias("application_number"),
#     F.col("LABELERNAME").alias("labeler_name"),
#     F.col("SUBSTANCENAME").alias("substance_name"),
#     F.col("ACTIVE_NUMERATOR_STRENGTH").alias("active_numerator_strength"),
#     F.col("ACTIVE_INGRED_UNIT").alias("active_ingred_unit"),
#     F.col("PHARM_CLASSES").alias("pharm_classes"),
#     F.col("DEASCHEDULE").alias("dea_schedule"),
# )

# COMMAND ----------

df_product.printSchema()

# COMMAND ----------

# data transformation: (changing start_marketing_date, end_marketing_date= > to DateType(), ("substance_name", "active_numerator_strength", "active_ingred_unit"=> add data in array)

# COMMAND ----------

display(df_product)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, ArrayType

# COMMAND ----------

product_schema = StructType(
    [
        StructField("product_id", StringType()),
        StructField("product_ndc", StringType()),
        StructField("product_type_name", StringType()),
        StructField("proprietary_name", StringType()),
        StructField("proprietary_name_suffix", StringType()),
        StructField("non_proprietary_name", StringType()),
        StructField("dosage_form_name", StringType()),
        StructField("route_name", StringType()),
        StructField("start_marketing_date", DateType()),
        StructField("end_marketing_date", DateType()),
        StructField("marketing_category_name", StringType()),
        StructField("application_number", StringType()),
        StructField("labeler_name", StringType()),
        StructField("substance_name", ArrayType(StringType())),
        StructField("active_numerator_strength", ArrayType(StringType())),
        StructField("active_ingred_unit", ArrayType(StringType())),
        StructField("pharm_classes", StringType()),
        StructField("dea_schedule", StringType())
    ]
)

# COMMAND ----------

df_with_schema = spark.createDataFrame(df_product.rdd, product_schema)

# COMMAND ----------

df_with_schema.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

# COMMAND ----------

# Splitting the columns by ; seperator and converting to arrays.

# df = df.withColumns(
#     {
#         "substance_name": F.split("substance_name", ";"),
#         "active_numerator_strength": F.split("active_numerator_strength", ";"),
#         "active_ingred_unit": F.split("active_ingred_unit", ";"),
#     }
# )

# COMMAND ----------

# Splitting the columns by ; seperator and converting to arrays.

df_with_schema = df_product.withColumns(
    {
        "start_marketing_date":to_date(F.col("start_marketing_date"), "yyyyMMdd"),
        "end_marketing_date":to_date(F.col("end_marketing_date"), "yyyyMMdd"),
        "substance_name": F.split("substance_name", ";"),
        "active_numerator_strength": F.split("active_numerator_strength", ";"),
        "active_ingred_unit": F.split("active_ingred_unit", ";"),
    }
)

# COMMAND ----------

df_with_schema = df_with_schema.withColumn(
    "substance_details",
    F.arrays_zip("substance_name", "active_numerator_strength", "active_ingred_unit")
)

# Creating a column that creates an array of structs.
# You can't use explode directly on the three columns.

# COMMAND ----------

display(df_with_schema.select(
    "substance_name",
    "active_numerator_strength",
    "active_ingred_unit",
    "substance_details"
))

# COMMAND ----------

df_with_schema = df_with_schema.withColumn("substance_info", F.explode("substance_details"))
# Finally you can explode the array of structs

# COMMAND ----------

display(df_with_schema.select(
    "substance_name",
    "active_numerator_strength",
    "active_ingred_unit",
    "substance_details",
    "substance_info"
))

# COMMAND ----------

df_with_schema = df_with_schema.select(
    "*",
    "substance_info.*"
)
# substance_info gives out all the keys as column name and the values as the rows.

# COMMAND ----------

display(df_with_schema)
