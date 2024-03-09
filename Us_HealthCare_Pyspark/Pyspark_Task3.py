# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

enroll_data = spark.read.format('csv').load("dbfs:/FileStore/tables/Pyspark_project2_csv/Enroll_data.csv" , header =True, inferschema= True)

plan_crosswalk = spark.read.format('csv').load("dbfs:/FileStore/tables/Pyspark_project2_csv/Plan_crosswalk-1.csv" , header =True, inferschema= True)

coverage_crosswalk = spark.read.format('csv').load("dbfs:/FileStore/tables/Pyspark_project2_csv/Coverage_crosswalk.csv" , header =True, inferschema= True)

group_crosswalk = spark.read.format('csv').load("dbfs:/FileStore/tables/Pyspark_project2_csv/Group_crosswalk.csv" , header =True, inferschema= True)

demographic_data = spark.read.format('csv').load("dbfs:/FileStore/tables/Pyspark_project2_csv/Demographic_data.csv" , header =True, inferschema= True)

gender_crosswalk = spark.read.format('csv').load("dbfs:/FileStore/tables/Pyspark_project2_csv/Gender_crosswalk.csv" , header =True, inferschema= True)

relationship_crosswalk = spark.read.format('csv').load("dbfs:/FileStore/tables/Pyspark_project2_csv/Relationship_crosswalk.csv" , header =True, inferschema= True)


# COMMAND ----------

# MAGIC %md
# MAGIC Renaming coulmn name of enroll_data, plan_crosswalk, coverage_crosswalk

# COMMAND ----------

enroll_data = enroll_data.withColumnRenamed("GROUP_ID", "ENROLLMENT_GROUP_ID") \
                        .withColumnRenamed("COVERAGE_TYPE", "ENROLLMENT_COVERAGE_TYPE") \
                        .withColumnRenamed("PLAN_ID", "ENROLLMENT_PLAN_ID") \
                        .withColumnRenamed("VENDOR", "ENROLLMENT_VENDOR_NAME") \
                        .withColumnRenamed("EFFECTIVE_DATE", "MEMBER_EFFECTIVE_DATE") \
                        .withColumnRenamed("TERMINATION_DATE", "MEMBER_TERMINATION_DATE")

plan_crosswalk = plan_crosswalk.withColumnRenamed("PLAN_ID", "ENROLLMENT_PLAN_ID")\
                        .withColumnRenamed("Plan Name", "ENROLLMENT_PLAN_NAME")\
                        .withColumnRenamed("BENEFIT_TYPE", "ENROLLMENT_PLAN_BENEFIT_TYPE")\
                        .withColumnRenamed("EFFECTIVE_DATE", "ENROLLMENT_PLAN_EFFECTIVE_DATE")\
                        .withColumnRenamed("TERMINATION_DATE", "ENROLLMENT_PLAN_TERMINATION_DATE")

coverage_crosswalk = coverage_crosswalk.withColumnRenamed("Coverage_ID", "ENROLLMENT_COVERAGE_TYPE")\
                        .withColumnRenamed("Coverage_Description", "ENROLLMENT_COVERAGE_DESCRIPTION")

group_crosswalk = group_crosswalk.withColumnRenamed("GROUP_ID", "ENROLLMENT_GROUP_ID") \
                        .withColumnRenamed("GROUP_NAME", "ENROLLMENT_GROUP_NAME")


# COMMAND ----------

# MAGIC %md
# MAGIC Renaming coulmn name of demographic_data, gender_crosswalk, relationship_crosswalk

# COMMAND ----------

demographic_data = demographic_data.withColumnRenamed("FIRST_NAME", "MEMBER_FIRST_NAME") \
                        .withColumnRenamed("LAST_NAME", "MEMBER_LAST_NAME") \
                        .withColumnRenamed("MIDDLE_NAME", "MEMBER_MIDDLE_NAME") \
                        .withColumnRenamed("GENDER", "MEMBER_GENDER_CODE") \
                        .withColumnRenamed("DOB", "MEMBER_DOB") \
                        .withColumnRenamed("RELATIONSHIP", "MEMBER_RELATIONSHIP_CODE") \
                        .withColumnRenamed("PERSON_CODE", "MEMBER_PERSON_CODE") \
                        .withColumnRenamed("ADDRESS_1", "MEMBER_ADDRESS_1") \
                        .withColumnRenamed("ADDRESS_2", "MEMBER_ADDRESS_2") \
                        .withColumnRenamed("CITY", "MEMBER_CITY") \
                        .withColumnRenamed("STATE", "MEMBER_STATE") \
                        .withColumnRenamed("COUNTY", "MEMBER_COUNTY") \
                        .withColumnRenamed("ZIP", "MEMBER_ZIP")

gender_crosswalk = gender_crosswalk.withColumnRenamed("Code", "MEMBER_GENDER_CODE") \
                        .withColumnRenamed("Rollup_Code", "GENDER") \
                        .withColumnRenamed("Rollup_Description", "GENDER_DESCRIPTION")

relationship_crosswalk = relationship_crosswalk.withColumnRenamed("Rollup_Code", "MEMBER_RELATIONSHIP_CODE") \
                        .withColumnRenamed("Rollup_Description", "RELATIONSHIP_DESCRIPTION")

# COMMAND ----------


enroll_data = enroll_data.join(plan_crosswalk, ['ENROLLMENT_PLAN_ID'], "left")

enroll_data = enroll_data.join(coverage_crosswalk, ['ENROLLMENT_COVERAGE_TYPE'], "left")

enroll_data = enroll_data.join(group_crosswalk,['ENROLLMENT_GROUP_ID'], "left")


# COMMAND ----------

display(enroll_data)

# COMMAND ----------

demographic_data = demographic_data.join(gender_crosswalk, ['MEMBER_GENDER_CODE'], "leftouter")

demographic_data = demographic_data.join(relationship_crosswalk, ['MEMBER_RELATIONSHIP_CODE'], "leftouter")

# COMMAND ----------

display(demographic_data)

# COMMAND ----------

data = enroll_data.join(demographic_data ,['MEMBER_ID'], "leftouter")

# COMMAND ----------

display(data)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, row_number, when, lit, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, DateType

# COMMAND ----------

windowSpec = Window.partitionBy("MEMBER_ID").orderBy(col("MEMBER_ID"))

targetTable = data \
    .withColumn("ABACUS_RECORD_ID", row_number().over(Window.orderBy("MEMBER_ID"))) \
    .withColumn("ABACUS_MEMBER_ID", concat(col("MEMBER_ID").substr(0, 3), row_number().over(windowSpec), col("MEMBER_GENDER_CODE"), col("MEMBER_DOB").substr(9, 2))) \
    .withColumn("MEMBER_PREFIX_NAME", 
                when((col("GENDER_DESCRIPTION") == "Male"), "Mr.")
                .when((col("GENDER_DESCRIPTION") == "Female") & (col("RELATIONSHIP_DESCRIPTION").isin('Fiance','Spouse','Grand Mother','Mother')), "Mrs.")
                .when((col("GENDER_DESCRIPTION") == "Female") & (col("RELATIONSHIP_DESCRIPTION").isin('Adapted Daughter','Niece','Daughter','Divorced Wife','Grand Daughter','Self')), "Ms.")
                .when((col("GENDER_DESCRIPTION") == "Female") & (col("RELATIONSHIP_DESCRIPTION").like('Unknown')), "Ms.")
                .when((col("GENDER_DESCRIPTION") == "Male") & (col("RELATIONSHIP_DESCRIPTION").like('Unknown')), "Mr.")
                .otherwise(None)) \
    .withColumn("MEMBER_SUFFIX_NAME", 
                when((col("MEMBER_FIRST_NAME") == col("MEMBER_MIDDLE_NAME")) & (col("RELATIONSHIP_DESCRIPTION") == 'Son'), "I")
                .when((col("MEMBER_FIRST_NAME") == col("MEMBER_MIDDLE_NAME")) & (col("RELATIONSHIP_DESCRIPTION") == 'Grand Son'), "II")
                .otherwise(None)) \
    .withColumn("ENROLLMENT_MEDICAL_EFFECTIVE_DATE", 
                when(col("ENROLLMENT_PLAN_BENEFIT_TYPE").isin('Medical','Medical and Vision','Medical, Dental and Vision','Medical and Dental'), col("MEMBER_EFFECTIVE_DATE")).otherwise(None)) \
    .withColumn("ENROLLMENT_MEDICAL_TERMINATION_DATE", 
                when(col("ENROLLMENT_PLAN_BENEFIT_TYPE").isin('Medical','Medical and Vision','Medical, Dental and Vision','Medical and Dental'), col("MEMBER_TERMINATION_DATE")).otherwise(None)) \
    .withColumn("ENROLLMENT_DENTAL_EFFECTIVE_DATE", 
                when(col("ENROLLMENT_PLAN_BENEFIT_TYPE").isin('Medical and Dental','Medical, Dental and Vision'), col("MEMBER_EFFECTIVE_DATE")).otherwise(None)) \
    .withColumn("ENROLLMENT_DENTAL_TERMINATION_DATE", 
                when(col("ENROLLMENT_PLAN_BENEFIT_TYPE").isin('Medical and Dental','Medical, Dental and Vision'), col("MEMBER_TERMINATION_DATE")).otherwise(None)) \
    .withColumn("ENROLLMENT_VISION_EFFECTIVE_DATE", 
                when(col("ENROLLMENT_PLAN_BENEFIT_TYPE").isin('Medical and Vision','Medical, Dental and Vision'), col("MEMBER_EFFECTIVE_DATE")).otherwise(None)) \
    .withColumn("ENROLLMENT_VISION_TERMINATION_DATE", 
                when(col("ENROLLMENT_PLAN_BENEFIT_TYPE").isin('Medical and Vision','Medical, Dental and Vision'), col("MEMBER_TERMINATION_DATE")).otherwise(None)) \
    .withColumn("ENROLLMENT_VENDOR_NAME", lit('DCT_INC')) \
    .withColumn("SOURCE_FILE_NAME", lit('Enrollment_data & Demographic_data')) \
    .withColumn("FILE_INGESTION_DATE", current_timestamp())\
    .withColumn("MEMBER_COUNTRY_CODE", lit(None).cast(IntegerType()))\
    .withColumn("MEMBER_HOME_PHONE", lit(None).cast(IntegerType()))\
    .withColumn("MEMBER_WORK_PHONE", lit(None).cast(IntegerType()))\
    .withColumn("MEMBER_MOBILE_PHONE", lit(None).cast(IntegerType()))\
    .withColumn("MEMBER_EMAIL", lit("").cast("string"))\
    .withColumn("MEMBER_IS_DECEASED",lit("").cast("string") )\
    .withColumn("MEMBER_DATE_OF_DEATH",lit(None).cast(DateType()) )\
    .withColumn("MEMBER_DECEASED_REASON", lit("").cast("string"))\
    .withColumn("ENROLLMENT_SUBGROUP_ID",lit(None).cast(IntegerType()) )\
    .withColumn("ENROLLMENT_SUBGROUP_NAME",lit("").cast("string") )



# COMMAND ----------

display(targetTable)

# COMMAND ----------

targetTable = targetTable.select(
    "ABACUS_RECORD_ID",
    "ABACUS_MEMBER_ID",
    "MEMBER_ID",
    "EMPLOYEE_ID",
    "MEMBER_FIRST_NAME",
    "MEMBER_LAST_NAME",
    "MEMBER_MIDDLE_NAME",
    "MEMBER_PREFIX_NAME",
    "MEMBER_SUFFIX_NAME",
    "MEMBER_MIDDLE_NAME",
    "MEMBER_GENDER_CODE",
    "GENDER",
    "GENDER_DESCRIPTION",
    "MEMBER_DOB",
    "MEMBER_RELATIONSHIP_CODE",
    "RELATIONSHIP_DESCRIPTION",
    "MEMBER_PERSON_CODE",
    "MEMBER_ADDRESS_1",
    "MEMBER_ADDRESS_2",
    "MEMBER_CITY",
    "MEMBER_STATE",
    "MEMBER_COUNTY",
    "MEMBER_ZIP",
    "MEMBER_COUNTRY_CODE",
    "MEMBER_HOME_PHONE",
    "MEMBER_WORK_PHONE",
    "MEMBER_MOBILE_PHONE",
    "MEMBER_EMAIL",
    "MEMBER_IS_DECEASED",
    "MEMBER_DATE_OF_DEATH",
    "MEMBER_DECEASED_REASON",
    "ENROLLMENT_GROUP_ID",
    "ENROLLMENT_GROUP_NAME",
    "ENROLLMENT_SUBGROUP_ID",
    "ENROLLMENT_SUBGROUP_NAME",
    "ENROLLMENT_COVERAGE_TYPE",
    "ENROLLMENT_COVERAGE_DESCRIPTION",
    "ENROLLMENT_PLAN_ID",
    "ENROLLMENT_PLAN_NAME",
    "ENROLLMENT_PLAN_BENEFIT_TYPE",
    "ENROLLMENT_MEDICAL_EFFECTIVE_DATE",
    "ENROLLMENT_MEDICAL_TERMINATION_DATE",
    "ENROLLMENT_DENTAL_EFFECTIVE_DATE",
    "ENROLLMENT_DENTAL_TERMINATION_DATE",
    "ENROLLMENT_VISION_EFFECTIVE_DATE",
    "ENROLLMENT_VISION_TERMINATION_DATE",
    "ENROLLMENT_VENDOR_NAME",
    "SOURCE_FILE_NAME",
    "FILE_INGESTION_DATE"
)


# COMMAND ----------

display(targetTable)

# COMMAND ----------


