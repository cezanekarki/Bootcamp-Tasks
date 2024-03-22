# Databricks notebook source
# MAGIC %md
# MAGIC LAODING THE TEXT FILE INTO THE DATAFRAME

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Claims Data Loading Example") \
    .getOrCreate()

file_path = "/FileStore/tables/Claims2023NHXYZ.txt"

df = spark.read.option("delimiter", "\t").csv(file_path, header=True, inferSchema=True)

df.printSchema()
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC Using function to directly make changes in format in dataframe.

# COMMAND ----------

from pyspark.sql import functions as F

processed_df = df.withColumn("Patient Gender",
                             F.when(F.col("Patient Gender") == 'M', 'Male')
                             .when(F.col("Patient Gender") == 'F', 'Female')
                             .when(F.col("Patient Gender") == 'U', 'UNKNOWN')
                             .otherwise(F.col("Patient Gender")))

processed_df = processed_df.withColumn("Relationship Code",
                                       F.when(F.col("Relationship Code") == 1, 'Self')
                                       .when(F.col("Relationship Code") == 2, 'Spouse')
                                       .when(F.col("Relationship Code") == 3, 'Child')
                                       .when(F.col("Relationship Code") == 4, 'Other dependent')
                                       .otherwise(F.col("Relationship Code")))

processed_df = processed_df.withColumn("Claim Type",
                                       F.when(F.col("Claim Type") == 1, 'Medical')
                                       .when(F.col("Claim Type") == 2, 'Dental')
                                       .when(F.col("Claim Type") == 3, 'Vision')
                                       .when(F.col("Claim Type") == 4, 'PBM')
                                       .when(F.col("Claim Type") == 5, 'MISC')
                                       .when(F.col("Claim Type") == 6, 'Mental Health')
                                       .when(F.col("Claim Type") == 7, 'Long Term Disability')
                                       .when(F.col("Claim Type") == 8, 'Short Term Disability')
                                       .when(F.col("Claim Type") == 9, 'Other expenses')
                                       .otherwise(F.col("Claim Type")))

processed_df = processed_df.withColumn("Process Date",
                                       F.to_date(F.col("Process Date").cast("string"), 'yyyyMMdd'))

processed_df = processed_df.withColumn("DOS End",
                                       F.to_date(F.col("DOS End").cast("string"), 'yyyyMMdd'))

processed_df = processed_df.withColumn("DOS From",
                                       F.to_date(F.col("DOS From").cast("string"), 'yyyyMMdd'))

processed_df = processed_df.withColumn("Patient DOB",
                                       F.to_date(F.col("Patient DOB").cast("string"), 'yyyyMMdd'))

processed_df = processed_df.withColumn("Paid Date",
                                       F.to_date(F.col("Paid Date").cast("string"), 'yyyyMMdd'))

processed_df.printSchema()
display(processed_df)
processed_df_count = processed_df.count()
print("Count of duplicate data:", processed_df_count)


# COMMAND ----------

# MAGIC %md
# MAGIC Using window function to remove duplicate data for patients having same charges on same date

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("Charges", "Patient First Name", "Patient Last Name", "Paid Date").orderBy(F.lit(1))

processed_df = processed_df.withColumn("row_number", F.row_number().over(window_spec))

testdeduplicated_df = processed_df.filter(F.col("row_number") == 1).drop("row_number")

deduplicated_count = testdeduplicated_df.count()


duplicate_count = processed_df.count() - deduplicated_count

display(testdeduplicated_df)
print("Count of deduplicated data:", deduplicated_count)
print("Count of duplicate data:", duplicate_count)



# COMMAND ----------

# MAGIC %md
# MAGIC Simple count of data in different column which should be exactly same

# COMMAND ----------

from pyspark.sql import functions as F

same_data_count = processed_df.filter(F.col("Provider Name") == F.col("Payee Name")).count()

different_data_count = processed_df.filter(F.col("Provider Name") != F.col("Payee Name")).count()

print("Count of records with same data in 'Provider Name' and 'Payee Name':", same_data_count)
print("Count of records with different data in 'Provider Name' and 'Payee Name':", different_data_count)


# COMMAND ----------

# MAGIC %md
# MAGIC Double checking the data duplication to verify count using dropDuplicates

# COMMAND ----------

from pyspark.sql import functions as F
deduplicated_df = processed_df.dropDuplicates(["Patient First Name", "Patient Last Name", "Charges", "Paid Date"])
deduplicated_df.printSchema()
display(deduplicated_df)
deduplicated_count = deduplicated_df.count()
print("Count of deduplicated data:", deduplicated_count)
duplicate_count = processed_df.count() - deduplicated_count
print("Count of duplicate data:", duplicate_count)


# COMMAND ----------

# MAGIC %md
# MAGIC Creating a unique patient id for each patients on the basis of their name and DOB.

# COMMAND ----------

import uuid

window_spec = Window.partitionBy("Patient First Name", "Patient Last Name", "Patient DOB")

deduplicated_df_with_id = deduplicated_df.withColumn("Patient ID",
                                                     F.when(F.count("Patient First Name").over(window_spec) > 1,
                                                            F.concat(F.col("Patient First Name"), 
                                                                     F.col("Patient Last Name"), 
                                                                     F.col("Patient DOB")))
                                                            .otherwise(F.lit(str(uuid.uuid4()))))

display(deduplicated_df_with_id)


# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deduplicated_temp_table

# COMMAND ----------


deduplicated_df_with_id.createOrReplaceTempView("deduplicated_temp_table")
deduplicated_df_with_id.write.saveAsTable("Claims2023NHXYZ", format="parquet", mode="overwrite")


spark.sql("SELECT * FROM Claims2023NHXYZ").show()

spark.sql("INSERT INTO Claims2023NHXYZ SELECT * FROM deduplicated_temp_table")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Claims2023NHXYZ

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     `Provider Name` AS Provider_Name,
# MAGIC     `Person Code` AS Person_Code,
# MAGIC     `Patient Last Name` AS Patient_Last_Name,
# MAGIC     `Patient First Name` AS Patient_First_Name,
# MAGIC     `Patient DOB` AS Patient_DOB,
# MAGIC     `Patient Gender` AS Patient_Gender,
# MAGIC     `Relationship Code` AS Relationship_Code,
# MAGIC     `Group Name` AS Group_Name,
# MAGIC     `Paid Date` AS Paid_Date,
# MAGIC     `Payee Name` AS Payee_Name,
# MAGIC     `Payee State` AS Payee_State,
# MAGIC     `Plan Number` AS Plan_Number,
# MAGIC     `Plan Description` AS Plan_Description,
# MAGIC     `DOS From` AS DOS_From,
# MAGIC     `DOS End` AS DOS_End,
# MAGIC     `Process Date` AS Process_Date,
# MAGIC     `Diagnosis Code1` AS Diagnosis_Code1,
# MAGIC     `Diagnosis Code2` AS Diagnosis_Code2,
# MAGIC     `Diagnosis Code3` AS Diagnosis_Code3,
# MAGIC     `Diagnosis Code4` AS Diagnosis_Code4,
# MAGIC     `Diagnosis Code5` AS Diagnosis_Code5,
# MAGIC     `Diagnosis Code6` AS Diagnosis_Code6,
# MAGIC     `Diagnosis Code7` AS Diagnosis_Code7,
# MAGIC     `Diagnosis Code8` AS Diagnosis_Code8,
# MAGIC     `Diagnosis Code9` AS Diagnosis_Code9,
# MAGIC     `Diagnosis Code10` AS Diagnosis_Code10,
# MAGIC     `Diagnosis Code11` AS Diagnosis_Code11,
# MAGIC     `Diagnosis Code12` AS Diagnosis_Code12,
# MAGIC     `Claim Type` AS Claim_Type,
# MAGIC     Coverage,
# MAGIC     Charges,
# MAGIC     `Plan Responsibility` AS Plan_Responsibility,
# MAGIC     `Insured Responsibility` AS Insured_Responsibility,
# MAGIC     `Provider Responsibility` AS Provider_Responsibility,
# MAGIC     `Insurance Co Responsibility` AS Insurance_Co_Responsibility,
# MAGIC     `Base Benefit` AS Base_Benefit,
# MAGIC     `Surcharge Amount` AS Surcharge_Amount,
# MAGIC     `Covered Amount` AS Covered_Amount,
# MAGIC     `Line Discount` AS Line_Discount,
# MAGIC     `Standard Discount` AS Standard_Discount,
# MAGIC     `Scheduled Discount` AS Scheduled_Discount,
# MAGIC     `Payment Discount` AS Payment_Discount,
# MAGIC     `Total Discount` AS Total_Discount,
# MAGIC     Copayment,
# MAGIC     Coinsurance,
# MAGIC     Deductible,
# MAGIC     `COB Amount` AS COB_Amount,
# MAGIC     `Provider Paid Amount` AS Provider_Paid_Amount,
# MAGIC     `Employee Paid Amount` AS Employee_Paid_Amount,
# MAGIC     row_number,
# MAGIC     `Patient ID` AS Patient_ID
# MAGIC FROM Claims2023NHXYZ
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Total coverage amount for patients order by desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     `Patient ID` AS Patient_ID,
# MAGIC     SUM(`Covered Amount`) AS Total_Covered_Amount
# MAGIC FROM Claims2023NHXYZ
# MAGIC GROUP BY `Patient ID`
# MAGIC ORDER BY Total_Covered_Amount DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Finding covered amount accroding to name and paid date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     `Patient First Name` AS Patient_First_Name,
# MAGIC     `Patient Last Name` AS Patient_Last_Name,
# MAGIC     `Paid Date` AS Paid_Date,
# MAGIC     SUM(`Covered Amount`) AS Total_Covered_Amount
# MAGIC FROM Claims2023NHXYZ
# MAGIC GROUP BY `Paid Date`, `Patient First Name`, `Patient Last Name`
# MAGIC ORDER BY `Paid Date`, Total_Covered_Amount DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Finding Popular plans from the data

# COMMAND ----------

from pyspark.sql import functions as F

popular_plans = deduplicated_df.groupBy("Plan Description").count()

most_popular_plans = popular_plans.orderBy(F.desc("count"))

print("Most Popular Plans:")
display(most_popular_plans)
total_count = popular_plans.select(F.sum("count")).collect()[0][0]
print("Total Count of All Plans:", total_count)

# COMMAND ----------

# MAGIC %md
# MAGIC Representation of that data in a piechart

# COMMAND ----------

import matplotlib.pyplot as plt

most_popular_plans_pd = most_popular_plans.toPandas()

plan_descriptions = most_popular_plans_pd["Plan Description"].tolist()
counts = most_popular_plans_pd["count"].tolist()

total_count = sum(counts)

aggregated_descriptions = []
aggregated_counts = []

for description, count in zip(plan_descriptions, counts):
    percentage = (count / total_count) * 100
    if percentage < 5:
        if "Others" not in aggregated_descriptions:
            aggregated_descriptions.append("Others")
            aggregated_counts.append(count)
        else:
            idx = aggregated_descriptions.index("Others")
            aggregated_counts[idx] += count
    else:
        aggregated_descriptions.append(description)
        aggregated_counts.append(count)

plt.figure(figsize=(8, 8))
plt.pie(aggregated_counts, labels=aggregated_descriptions, autopct='%1.1f%%', startangle=140)
plt.title('Most Popular Plans')
plt.axis('equal')  
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Representation of data from different states

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

payee_state_counts = deduplicated_df.groupBy("Payee State").agg(F.countDistinct("Payee Name").alias("Payee Count"))

payee_state_counts_pd = payee_state_counts.toPandas()

payee_state_counts_pd = payee_state_counts_pd.sort_values(by="Payee Count", ascending=False)

plt.figure(figsize=(14, 8))  
plt.bar(payee_state_counts_pd["Payee State"], payee_state_counts_pd["Payee Count"], color='skyblue')
plt.xlabel('Payee State')
plt.ylabel('Number of Payees')
plt.title('Number of Payees by State')
plt.xticks(rotation=90)  
plt.tight_layout()  
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
