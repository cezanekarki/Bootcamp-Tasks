# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

df_crosswalk = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/cw_crosswalk.csv")
df_crosswalk.show()
df_crosswalk.printSchema()

# COMMAND ----------

df_crosswalk = df_crosswalk.withColumnRenamed("Coverage_ID","COVERAGE_TYPE")


# COMMAND ----------

df_gender = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/cw_gender.csv")
df_gender.show()
df_gender.printSchema()

# COMMAND ----------

df_gender = df_gender.withColumnRenamed("Code","Gender_code")
df_gender = df_gender.withColumnRenamed("Rollup_Description","Gender_Description")

# COMMAND ----------

df_group = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/cw_group.csv")
df_group.show()
df_group.printSchema()

# COMMAND ----------

df_plan = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/cw_plan.csv")
df_plan.show()
df_plan.printSchema()

# COMMAND ----------

df_plan = df_plan.withColumnRenamed("Plan Name","Plan_Name")

# COMMAND ----------

df = df_plan.select("BENEFIT_TYPE").distinct()
display(df)

# COMMAND ----------

df = df_plan.withColumn("test",F.when(df_plan.TERMINATION_DATE <= df_plan.EFFECTIVE_DATE,'yes').otherwise('no'))
display(df)

# COMMAND ----------

df_relationship = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/cw_relationship.csv")
df_relationship.show()
df_relationship.printSchema()

# COMMAND ----------

df_relationship = df_relationship.withColumnRenamed("Rollup_code","Relationship_Code")
df_relationship = df_relationship.withColumnRenamed("Rollup_Description","Relationship_with_Suscriber")

# COMMAND ----------

df_demographics_mock_data = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/demographics_mock_data.csv")
df_demographics_mock_data.show()
df_demographics_mock_data.printSchema()

# COMMAND ----------

df_Enrollment_mock_data = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/Enrollment_mock_data.csv")
df_Enrollment_mock_data.show()
df_Enrollment_mock_data.printSchema()

# COMMAND ----------

df_Enrollment_mock_data = df_Enrollment_mock_data.withColumnRenamed("EFFECTIVE_DATE","effective_date_member")
df_Enrollment_mock_data = df_Enrollment_mock_data.withColumnRenamed("TERMINATION_DATE","termination_date_member")

# COMMAND ----------

df = df_Enrollment_mock_data.withColumn("test",F.when(df_Enrollment_mock_data.termination_date_member <= df_Enrollment_mock_data.effective_date_member,'yes').otherwise('no'))
display(df)

# COMMAND ----------

joined_demographics_mock_data = df_demographics_mock_data.join(df_gender, df_demographics_mock_data["GENDER"] == df_gender["Gender_code"], "left")
joined_demographics_mock_data.display()
joined_demographics_mock_data.printSchema()

# COMMAND ----------

joined_demographics_mock_data = joined_demographics_mock_data.join(df_relationship,joined_demographics_mock_data["RELATIONSHIP"]== df_relationship['Relationship_Code'], 'left')
display(joined_demographics_mock_data)

# COMMAND ----------

df_Enrollment_mock_data = df_Enrollment_mock_data.join(df_group,["GROUP_ID"],"left")

df_Enrollment_mock_data = df_Enrollment_mock_data.join(df_crosswalk,["COVERAGE_TYPE"],'left')

df_Enrollment_mock_data = df_Enrollment_mock_data.join(df_plan,["PLAN_ID"],'left')

df_Enrollment_mock_data.display()

# COMMAND ----------

Target_table = joined_demographics_mock_data.join(df_Enrollment_mock_data,["MEMBER_ID"],"left")

Target_table.printSchema()
Target_table.display()


# COMMAND ----------

Target_table.createOrReplaceTempView("Target_Table")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE final_target_table AS(
# MAGIC SELECT
# MAGIC  row_number() OVER (ORDER BY (SELECT NULL)) AS Abacus_Record_ID,
# MAGIC  CONCAT(substring(MEMBER_ID,0,3),ROW_NUMBER() OVER (PARTITION BY MEMBER_ID ORDER BY (SELECT NULL)),Gender_code,substring(DOB,9,2)) AS Abacus_Member_Id,
# MAGIC  MEMBER_ID AS Member_id,
# MAGIC  EMPLOYEE_ID AS Subscriber_id,
# MAGIC  FIRST_NAME AS Member_First_Name,
# MAGIC  LAST_NAME AS Member_Last_Name,
# MAGIC  MIDDLE_NAME AS Member_Middle_Name,
# MAGIC  CASE
# MAGIC     WHEN Gender_Description = 'Male' THEN 'Mr.'
# MAGIC     WHEN Gender_Description = 'Female' AND Relationship_with_Suscriber in ('Fiance','Spouse','Grand Mother','Mother') THEN 'Mrs.'
# MAGIC     WHEN Gender_Description = 'Female' AND Relationship_with_Suscriber in ('Adapted Daughter','Niece','Daughter','Divorced Wife','Grand Daughter','Self') THEN 'Ms.'
# MAGIC     WHEN Gender_Description = 'Female' AND Relationship_with_Suscriber LIKE 'Unknown' THEN 'Ms.'
# MAGIC     WHEN Gender_Description = 'Male' AND Relationship_with_Suscriber LIKE 'Unknown' THEN 'Mr.'
# MAGIC     ELSE null
# MAGIC   END AS Member_Prefix_Name,
# MAGIC   -- self,adapted daughter,niece,Daughter,unknown
# MAGIC  CASE
# MAGIC     WHEN FIRST_NAME = MIDDLE_NAME AND Relationship_with_Suscriber = 'Son' THEN 'I'
# MAGIC     WHEN FIRST_NAME = MIDDLE_NAME AND Relationship_with_Suscriber = 'Grand Son' THEN 'II'
# MAGIC     ELSE NULL
# MAGIC   END AS Member_Suffix_Name,
# MAGIC  Gender_Description AS Member_Gender,
# MAGIC  date_format(DOB,'MM-dd-yyyy') AS Member_Date_of_Birth,
# MAGIC  Relationship_Code AS Member_Relationship_Code,
# MAGIC  Relationship_with_Suscriber AS Member_Relationship_Description,
# MAGIC  PERSON_CODE AS Member_Person_Code,
# MAGIC  ADDRESS_1 AS Member_Address_Line_1,
# MAGIC  ADDRESS_2 AS Member_Address_Line_2,
# MAGIC  CITY AS Member_City,
# MAGIC  STATE AS Member_State,
# MAGIC  COUNTY AS Member_County,
# MAGIC  ZIP AS Member_Postal_Code,
# MAGIC  'USA' AS Member_Country,
# MAGIC  'null' AS Member_Home_Phone,
# MAGIC  'null' AS Member_Work_Phone,
# MAGIC  'null' AS Member_Mobile_Phone,
# MAGIC  'null' AS Member_Email,
# MAGIC  'null' AS Member_Is_Deceased,
# MAGIC  'null' AS Member_Date_of_Death,
# MAGIC  'null' AS Member_Deceased_Reason,
# MAGIC  GROUP_ID AS Enrollment_Group_ID,
# MAGIC  GROUP_NAME AS Enrollment_Group_Name,
# MAGIC  'null' AS Enrollment_SubGroup_ID,
# MAGIC  'null' AS Enrollment_SubGroup_Name,
# MAGIC  COVERAGE_TYPE AS Enrollment_Coverage_Code,
# MAGIC  Coverage_Description AS Enrollment_Coverage_Description,
# MAGIC  VENDOR AS Enrollment_Plan_ID,
# MAGIC  Plan_Name AS Enrollment_Plan_Name,
# MAGIC  BENEFIT_TYPE AS Enrollment_Plan_Coverage,
# MAGIC  CASE
# MAGIC     WHEN BENEFIT_TYPE IN ('Medical','Medical and Vision','Medical, Dental and Vision','Medical and Dental') THEN date_format(effective_date_member,'MM-dd-yyyy')
# MAGIC     ELSE NULL
# MAGIC  END AS Enrollment_Medical_Effective_Date,
# MAGIC  CASE
# MAGIC    WHEN BENEFIT_TYPE IN ('Medical','Medical and Vision','Medical, Dental and Vision','Medical and Dental') THEN date_format(termination_date_member,'MM-dd-yyyy')
# MAGIC    ELSE NULL
# MAGIC  END AS Enrollment_Medical_Termination_Date,
# MAGIC  CASE
# MAGIC    WHEN BENEFIT_TYPE IN ('Medical and Dental','Medical, Dental and Vision') THEN date_format(effective_date_member,'MM-dd-yyyy')
# MAGIC    ELSE NULL
# MAGIC  END AS Enrollment_Dental_Effective_Date,
# MAGIC  CASE
# MAGIC    WHEN BENEFIT_TYPE IN ('Medical and Dental','Medical, Dental and Vision') THEN date_format(termination_date_member,'MM-dd-yyyy')
# MAGIC    ELSE NULL
# MAGIC  END AS Enrollment_Dental_Termination_Date,
# MAGIC  CASE
# MAGIC    WHEN BENEFIT_TYPE IN ('Medical and Vision','Medical, Dental and Vision') THEN date_format(effective_date_member,'MM-dd-yyyy')
# MAGIC    ELSE NULL
# MAGIC  END AS Enrollment_Vision_Effective_Date,
# MAGIC  CASE
# MAGIC    WHEN BENEFIT_TYPE IN ('Medical and Vision','Medical, Dental and Vision') THEN date_format(termination_date_member,'MM-dd-yyyy')
# MAGIC    ELSE NULL
# MAGIC  END AS Enrollment_Vision_Termination_Date,
# MAGIC  'DCT' AS Enrollment_Vendor_Name,
# MAGIC  'Enrollment_data & Demographic_data' AS Source_File_Name,
# MAGIC   current_timestamp() AS File_Ingestion_Date
# MAGIC FROM Target_table
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC describe final_target_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM final_target_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE final_target_table

# COMMAND ----------


