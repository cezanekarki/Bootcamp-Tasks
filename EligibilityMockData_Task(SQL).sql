-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_plan = spark.read.format("csv").option("header",True).option("delimiter",",").option("inferschema",True).load("dbfs:/FileStore/tables/Plan_crosswalk-2.csv")
-- MAGIC
-- MAGIC df_group = spark.read.format("csv").option("header",True).option("delimiter",",").option("inferschema",True).load("dbfs:/FileStore/tables/Group_crosswalk.csv")
-- MAGIC
-- MAGIC df_gender = spark.read.format("csv").option("header",True).option("delimiter",",").option("inferschema",True).load("dbfs:/FileStore/tables/Gender_crosswalk.csv")
-- MAGIC
-- MAGIC df_enrollment = spark.read.format("csv").option("header",True).option("delimiter",",").option("inferschema",True).load("dbfs:/FileStore/tables/Enroll_data.csv")
-- MAGIC
-- MAGIC df_demographics = spark.read.format("csv").option("header",True).option("delimiter",",").option("inferschema",True).load("dbfs:/FileStore/tables/Demographic_data.csv")
-- MAGIC
-- MAGIC df_coverage = spark.read.format("csv").option("header",True).option("delimiter",",").option("inferschema",True).load("dbfs:/FileStore/tables/Coverage_crosswalk.csv")
-- MAGIC
-- MAGIC df_relationship = spark.read.format("csv").option("header",True).option("delimiter",",").option("inferschema",True).load("dbfs:/FileStore/tables/Relationship_crosswalk.csv")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_gender = df_gender.withColumnRenamed("Rollup_Code","Rollup_Code1")
-- MAGIC df_relationship = df_relationship.withColumnRenamed("Rollup_Description","Rollup_Description1")
-- MAGIC df_group = df_group.withColumnRenamed("GROUP_ID","GROUP_ID1")
-- MAGIC df_plan = df_plan.withColumnRenamed("PLAN_ID","PLAN_ID1").withColumnRenamed("EFFECTIVE_DATE","EFFECTIVE_DATE1").withColumnRenamed("TERMINATION_DATE","TERMINATION_DATE1").withColumnRenamed("Plan Name","Plan_Name")
-- MAGIC df_enrollment = df_enrollment.withColumnRenamed("MEMBER_ID","MEMBER_ID1")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC a1 = df_demographics.join(df_gender, df_demographics["gender"] == df_gender["code"],"outer")
-- MAGIC a2 = a1.join(df_relationship, a1["relationship"] == df_relationship["rollup_code"])
-- MAGIC
-- MAGIC b1 = df_enrollment.join(df_group, df_enrollment["group_id"] == df_group["group_id1"],"outer")
-- MAGIC b2 = b1.join(df_coverage, b1["coverage_type"] == df_coverage["coverage_id"],"outer")
-- MAGIC b3 = b2.join(df_plan, b2["plan_id"] == df_plan["plan_id1"],"outer")
-- MAGIC
-- MAGIC
-- MAGIC df_final = a2.join(b3, a2["member_id"] == b3["member_id1"],"outer")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_final)
-- MAGIC df_final.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_final.createOrReplaceTempView('final_table')

-- COMMAND ----------

create or replace table target_table as(
  select 
  ROW_NUMBER() OVER(ORDER BY MEMBER_ID) AS Abacus_Record_ID,
  CONCAT(substring(EMPLOYEE_ID,1,2), substring(MEMBER_ID,1,2) ,GENDER, substring(REPLACE(DOB, '-', ''),1,2)) AS Abacus_Member_ID,
  MEMBER_ID AS Member_ID,
  EMPLOYEE_ID AS Subscriber_ID,
  FIRST_NAME AS Member_First_Name,
  LAST_NAME AS Member_Last_Name,
  MIDDLE_NAME AS Member_Middle_Name,
  CASE 
  WHEN GENDER = 1 THEN "Mr."
  WHEN GENDER = 0 AND RELATIONSHIP in ('E','C2','G4','A2','N','F','D2','U') then "Ms."
  WHEN GENDER = 0 AND RELATIONSHIP in ('S','M','G2') then "Mrs."
  WHEN GENDER = 2 THEN "Unknown"
  END as Member_Prefix_Name,
  
  'null' AS Member_Suffix_Name,
  GENDER AS Member_Gender,
  DATE_FORMAT(DOB,'MM-dd-yyyy') AS Member_Date_of_Birth,
  Rollup_Code AS Member_Relationship_Code,
  Rollup_Description AS Member_Relationship_Description,
  PERSON_CODE AS Member_Person_Code,
  ADDRESS_1 AS Member_Address_Line_1,
  ADDRESS_2 AS Member_Address_Line_2,
  CITY AS Member_City,
  STATE AS Member_State,
  COUNTY AS Member_County,
  ZIP AS Member_Postal_Code,
  'null' AS Member_Country,
  'null' AS Member_Home_Phone,
  'null' AS Member_Work_Phone,
  'null' AS Member_Mobile_Phone,
  'null' AS Member_Email,
  'null' AS Member_Is_Deceased,
  'null' AS Member_Date_Of_Death,
  'null' AS Member_Deceased_Reason,
  GROUP_ID AS Enrollment_Group_ID,
  GROUP_NAME AS Enrollment_Group_Name,
  'null' AS Enrollment_SubGroup_ID,
  'null' AS Enrollment_Subgroup_Name,
  Coverage_ID AS Enrollment_Coverage_Code,
  Coverage_Description AS Enrollment_Coverage_Description,
  PLAN_ID AS Enrollment_Plan_ID,
  Plan_Name AS Enrollment_Plan_Name,
  BENEFIT_TYPE AS Enrollment_Plan_Coverage,
  EFFECTIVE_DATE AS Enrollment_Medical_Effective_Date,
  TERMINATION_DATE AS Enrollment_Medical_Termination_Date,
CASE
    WHEN BENEFIT_TYPE IN ('Medical and Dental', 'Medical, Dental and Vision') THEN EFFECTIVE_DATE
    ELSE NULL  -- or some default value
    END AS Enrollment_Dental_Effective_Date,
        
CASE
    WHEN BENEFIT_TYPE IN ('Medical and Dental', 'Medical, Dental and Vision') THEN TERMINATION_DATE
    ELSE NULL  -- or some default value
    END AS Enrollment_Dental_Termination_Date,
        
CASE
    WHEN BENEFIT_TYPE IN ('Medical and Vision', 'Medical, Dental and Vision') THEN EFFECTIVE_DATE
    ELSE NULL  -- or some default value
    END AS Enrollment_Vision_Effective_Date,
        
CASE
    WHEN BENEFIT_TYPE IN ('Medical and Vision', 'Medical, Dental and Vision') THEN TERMINATION_DATE
    ELSE NULL  -- or some default value
    END AS Enrollment_Vision_Termination_Date,
  'null' AS Enrollment_Vendor_Name,
  'null' AS Souce_File_Name,
  now() AS File_Ingestion_Date
FROM final_table
)














-- COMMAND ----------

select * from target_table

-- COMMAND ----------


