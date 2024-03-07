# Databricks notebook source

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read File") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()

# COMMAND ----------




# Replace 'file_path' with the actual path to your file
file_path1 = "dbfs:/FileStore/tables/Coverage_crosswalk.csv"
file_path2 = "dbfs:/FileStore/tables/Enroll_data.csv"
file_path3 = "dbfs:/FileStore/tables/Group_crosswalk.csv"
file_path4 = "dbfs:/FileStore/tables/Relationship_crosswalk.csv"
file_path5 = "dbfs:/FileStore/tables/Demographic_data.csv"
file_path6 = "dbfs:/FileStore/tables/Gender_crosswalk.csv"
file_path7 = "dbfs:/FileStore/tables/Plan_crosswalk.csv"

# Read the file into a DataFrame
df_Coverage = spark.read.format("csv").option("header","true").option("inferSchema","true")\
    .load(file_path1)
df_Enroll = spark.read.format("csv").option("header","true").option("inferSchema","true") \
    .load(file_path2)
df_Group= spark.read.format("csv").option("header","true").option("inferSchema","true") \
    .load(file_path3)
df_Relationship = spark.read.format("csv").option("header","true").option("inferSchema","true") \
    .load(file_path4)
df_Demographic = spark.read.format("csv").option("header","true").option("inferSchema","true") \
    .load(file_path5)
df_Gender = spark.read.format("csv").option("header","true").option("inferSchema","true") \
    .load(file_path6)
df_Plan = spark.read.format("csv").option("header","true").option("inferSchema","true") \
    .load(file_path7)




# COMMAND ----------


# Show the contents of the DataFrame
df_Coverage.show()
df_Enroll.show()
df_Group.show()
df_Relationship.show()
df_Demographic.show()
df_Gender.show()
df_Plan.show()


# COMMAND ----------


df_Gender = df_Gender.withColumnRenamed("Rollup_Code","Rollup_Code1")
df_Relationship = df_Relationship.withColumnRenamed("Rollup_Description","Rollup_Description1")
df_Group = df_Group.withColumnRenamed("GROUP_ID","GROUP_ID1")
df_Plan = df_Plan.withColumnRenamed("PLAN_ID","PLAN_ID1").withColumnRenamed("EFFECTIVE_DATE","EFFECTIVE_DATE1").withColumnRenamed("TERMINATION_DATE","TERMINATION_DATE1").withColumnRenamed("Plan Name","Plan_Name")
df_Enroll = df_Enroll.withColumnRenamed(
"MEMBER_ID"
,
"MEMBER_ID1"
)

# COMMAND ----------

# Perform the full outer join
new = df_Relationship.join(df_Demographic, df_Relationship.Rollup_Code== df_Demographic.RELATIONSHIP, 'outer').join(df_Gender,df_Relationship.Rollup_Code == df_Gender.Rollup_Code1,'outer')
new.display()

# COMMAND ----------

new1 = df_Enroll.join(df_Coverage,df_Enroll.COVERAGE_TYPE == df_Coverage.Coverage_ID,'outer').join(df_Plan,df_Enroll.PLAN_ID == df_Plan.PLAN_ID1,'outer').join(df_Group,df_Enroll.GROUP_ID==df_Group.GROUP_ID1,'outer')

# COMMAND ----------

new1.display()

# COMMAND ----------

ALL_join = new.join(new1,new.MEMBER_ID == new1.MEMBER_ID1,'outer')

# COMMAND ----------

ALL_join

# COMMAND ----------

ALL_join.display()

# COMMAND ----------




# COMMAND ----------

ALL_join.createOrReplaceTempView('main_table2');

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC ----Target Table Creation IN SQL
# MAGIC create or replace table Target_table As(
# MAGIC   Select MEMBER_ID,
# MAGIC   EMPLOYEE_ID as Subscriber_Id,
# MAGIC   FIRST_NAME as Member_First_Name,
# MAGIC   LAST_NAME as Member_Last_Name,
# MAGIC   MIDDLE_NAME as Member_Middle_Name,
# MAGIC   GENDER as Member_Gender,
# MAGIC   ROLLUP_CODE as Suffix_Name, 
# MAGIC   DOB as Member_Date_of_Birth,
# MAGIC   Rollup_CODE as Member_Relationship_Code,
# MAGIC   ROLLUP_DESCRIPTION as Member_Relationship_Description,
# MAGIC   PERSON_CODE as Member_Person_Code,
# MAGIC   ADDRESS_1 as Member_Address_Line_1,
# MAGIC   ADDRESS_2 as Member_Address_Line_2,
# MAGIC   CITY as Member_City,
# MAGIC   STATE as Member_State,
# MAGIC   COUNTY as Member_County,
# MAGIC   ZIP  as Member_Postal_Code,
# MAGIC   CODE as Member_Country_Code,
# MAGIC   GROUP_ID as Enrollment_Group_ID,
# MAGIC   GROUP_NAME as Enrollment_Group_Name,
# MAGIC   Coverage_ID as Enrollment_Coverage_Code,
# MAGIC   Coverage_Description as Enrollment_Coverage_Description,
# MAGIC   PLAN_ID as Enrollment_Plan_Id,
# MAGIC   "Plan Name" as Enrollment_Plan_Name,
# MAGIC   
# MAGIC   CONCAT(Subscriber_ID, '_', MEMBER_ID, '_', Member_Gender, '_', DATE_FORMAT(DOB, '%Y%m%d')) as Abacus_Subscriber_Id,
# MAGIC   CASE 
# MAGIC     WHEN GENDER = 1 THEN 'Mr.'
# MAGIC     WHEN GENDER = 0 AND Member_Relationship_Description = 'Divorced Wife' THEN 'Ms.'
# MAGIC     WHEN GENDER = 0 AND Member_Relationship_Description = 'Mother' THEN 'Mrs.'
# MAGIC     ELSE 'Miss.'
# MAGIC END AS Prefix_Name
# MAGIC
# MAGIC   
# MAGIC   from main_table2
# MAGIC       
# MAGIC
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Target_table

# COMMAND ----------

 from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, expr, when

# Create SparkSession
spark = SparkSession.builder \
    .appName("TargetTableCreation") \
    .getOrCreate()

# Read the main table into a DataFrame
main_table2 = spark.table("main_table2")

# Perform the transformation
target_table = main_table2.select(
    main_table2["MEMBER_ID"],
    main_table2["EMPLOYEE_ID"].alias("Subscriber_Id"),
    main_table2["FIRST_NAME"].alias("Member_First_Name"),
    main_table2["LAST_NAME"].alias("Member_Last_Name"),
    main_table2["MIDDLE_NAME"].alias("Member_Middle_Name"),
    main_table2["GENDER"].alias("Member_Gender"),
    main_table2["ROLLUP_CODE"].alias("Suffix_Name"),
    main_table2["DOB"].alias("Member_Date_of_Birth"),
    main_table2["ROLLUP_CODE"].alias("Member_Relationship_Code"),
    main_table2["ROLLUP_DESCRIPTION"].alias("Member_Relationship_Description"),
    main_table2["PERSON_CODE"].alias("Member_Person_Code"),
    main_table2["ADDRESS_1"].alias("Member_Address_Line_1"),
    main_table2["ADDRESS_2"].alias("Member_Address_Line_2"),
    main_table2["CITY"].alias("Member_City"),
    main_table2["STATE"].alias("Member_State"),
    main_table2["COUNTY"].alias("Member_County"),
    main_table2["ZIP"].alias("Member_Postal_Code"),
    main_table2["CODE"].alias("Member_Country_Code"),
    main_table2["GROUP_ID"].alias("Enrollment_Group_ID"),
    main_table2["GROUP_NAME"].alias("Enrollment_Group_Name"),
    main_table2["Coverage_ID"].alias("Enrollment_Coverage_Code"),
    main_table2["Coverage_Description"].alias("Enrollment_Coverage_Description"),
    main_table2["PLAN_ID"].alias("Enrollment_Plan_Id"),
    main_table2["Plan_Name"].alias("Enrollment_Plan_Name"),
    main_table2["EFFECTIVE_DATE"],
    main_table2["TERMINATION_DATE"],
    concat(
        main_table2["EMPLOYEE_ID"],
        expr("'_ '"),
        main_table2["MEMBER_ID"],
        expr("'_ '"),
        main_table2["GENDER"],
        expr("'_ '"),
        main_table2["DOB"].cast("string").substr(1, 10).alias("formatted_DOB")
    ).alias("Abacus_Subscriber_Id"),
    when(main_table2["GENDER"] == 1, "Mr.")
    .when((main_table2["GENDER"] == 0) & (main_table2["Rollup_Description"] == "Divorced Wife"), "Ms.")
    .when((main_table2["GENDER"] == 0) & (main_table2["Rollup_Description"] == "Mother"), "Mrs.")
    .otherwise("Miss.").alias("Prefix_Name")
    
)

# Show the resulting DataFrame
target_table.display()


# COMMAND ----------

from pyspark.sql.functions import lit
# Define the list of new column names
new_columns = ["Member_Home_Phone", "Member_Work_Phone", "Member_Mobile_Phone","Subgroup_Id","Subgroup_Name"]

# Add new columns with null values
for col_name in new_columns:
    target_table = target_table.withColumn(col_name, lit(None))

# Show the DataFrame with new columns
target_table.display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col
target_table = target_table.withColumn("Member_Email", concat_ws(".", col("Member_First_Name"), col("Member_Last_Name"), lit("@abacus.com")))
target_table.display()

# COMMAND ----------


