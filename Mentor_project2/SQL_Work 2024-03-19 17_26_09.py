# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TaskOfSQL") \
    .getOrCreate()    

# COMMAND ----------

table = spark.read.format("csv").load("dbfs:/FileStore/tables/taxi_data2.csv",header = True,inferSchema = True)

# COMMAND ----------

table.display()

# COMMAND ----------

# Write the DataFrame as a Delta table
spark.sql("DROP TABLE IF EXISTS taxi_data2")
table.write.format("delta").saveAsTable("taxi_data2")


  
#above there is different query to create delta table if we dont want to create dataframe using sparksession

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_data2;

# COMMAND ----------

# MAGIC %md
# MAGIC ##1.	Determine the maximum fare amount for trips where the pickup location is the same as the drop-off location.

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(fare_amount) as maximum_fare_amount from taxi_data2 where PULocationID = DOLocationID;

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.	Find the average fare amount per trip type (e.g., street-hail, dispatch) for trips longer than 10 miles.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     trip_type,
# MAGIC     ROUND(AVG(fare_amount), 2) AS average_fare_amount
# MAGIC FROM 
# MAGIC     taxi_data2
# MAGIC WHERE 
# MAGIC     trip_distance > 10 & trip_type is Not Null
# MAGIC GROUP BY 
# MAGIC     trip_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.	Identify the top 5 busiest hours of the day in terms of total number of trips.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT to_timestamp(lpep_pickup_datetime, 'MM-dd-yyyy hh:mm:ss a') AS datetime from taxi_data
# MAGIC --SELECT CAST(SUBSTRING(lpep_pickup_datetime, 12, 2) AS INT) AS hour_value
# MAGIC --FROM taxi_data;
# MAGIC SELECT HOUR(TO_TIMESTAMP(lpep_pickup_datetime, 'M/d/yyyy H:mm')) AS hour_value,COUNT(*) as num_trips
# MAGIC FROM taxi_data2
# MAGIC GROUP BY hour_value
# MAGIC ORDER BY num_trips DESC
# MAGIC LIMIT 5;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.	Calculate the percentage of trips where the tip amount exceeds 20% of the total fare amount.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH TripTotalAmount AS (
# MAGIC     SELECT fare_amount,tip_amount, total_amount
# MAGIC     FROM taxi_data2
# MAGIC )
# MAGIC SELECT 
# MAGIC     COUNT(CASE WHEN tip_amount > 0.2 * total_amount THEN 1 END) AS trips_with_tip_above_20_percent,
# MAGIC     COUNT(*) AS total_trips,
# MAGIC     round((trips_with_tip_above_20_percent * 100.0 )/ COUNT(*),2)AS percentage
# MAGIC FROM TripTotalAmount;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.	Calculate the average trip distance for each day of the week.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT round(avg(trip_distance),2),(DATE_FORMAT(TO_TIMESTAMP(lpep_pickup_datetime, 'M/d/yyyy H:mm'),'EEEE')) AS day_of_week
# MAGIC FROM taxi_data2 group by day_of_week
# MAGIC

# COMMAND ----------


