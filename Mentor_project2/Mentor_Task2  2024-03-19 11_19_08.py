# Databricks notebook source
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Assignment2") \
    .getOrCreate()    

# COMMAND ----------

taxi_data= spark.read.format("csv").load("dbfs:/FileStore/tables/taxi_data2.csv",header = True,inferSchema=True)
taxi_data.display()

# COMMAND ----------

taxi_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using "regexp_replace()" function to replace "/" with "-" in datecolumn

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
df1 = taxi_data.withColumn("lpep_pickup_datetime", regexp_replace(taxi_data["lpep_pickup_datetime"], "/", "-")).withColumn("lpep_dropoff_datetime", regexp_replace(taxi_data["lpep_dropoff_datetime"], "/", "-"))
df1.display()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##1.	Determine the maximum fare amount for trips where the pickup location is the same as the drop-off location.
# MAGIC In Pyspark, "FILTER" is same as "WHERE" in sql.
# MAGIC ### 
# MAGIC     "col()" ---function is use to access columns of dataframe.
# MAGIC     "max()" ---function is use to get maximum "fare amount".
# MAGIC     "first()"--- function returns the first row of the DataFrame. Since we're only interested in a single value (the maximum fare amount), we can use "first()" to retrieve the first row.
# MAGIC
# MAGIC ####    
# MAGIC     "[0]": This index [0] accesses the value of the maximum fare amount from the first row of the DataFrame. In PySpark, accessing column values typically returns a Row object, so we use [0] to extract the actual value.
# MAGIC
# MAGIC ####    
# MAGIC     in case we want two or more rows values then use "take()" function  and use row and cloumn format [0][0]  
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, max

same_location_trips = taxi_data.filter(col("PULocationID") == col("DOLocationID"))


# Determine the maximum fare amount
max_fare_same_location = same_location_trips.select(max("fare_amount")).first()[0]


print("\033[1mMaximum fare amount for trips with same pickup and dropoff location:\033[0m", max_fare_same_location)



print("\033[1m┌─────────────────────┬──────────────────┬────────────────────┐\033[0m")
print("\033[1m│ Maximum fare amount │\033[0m", str(max_fare_same_location).center(21), "\033[1m│\033[0m")
print("\033[1m└─────────────────────┴──────────────────┴────────────────────┘\033[0m")

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.	Find the average fare amount per trip type (e.g., street-hail, dispatch) for trips longer than 10 miles.

# COMMAND ----------

 from pyspark.sql.functions import format_number
# Filter trips longer than 10 miles
long_trips = taxi_data.filter(col("trip_distance") > 10)
long_trips.display()


# Calculate average fare amount per trip type
avg_fare_per_trip_type = long_trips.filter(col("trip_type").isNotNull())\
    .groupBy("trip_type")\
    .avg("fare_amount")

                                  
# Show the result
print("\033[1mAverage fare amount per trip type for trips longer than 10 miles:\033[0m")
avg_fare_per_trip_type.display()


#formating
formatted_avg_fare_per_trip_type = avg_fare_per_trip_type.withColumn("avg_fare_per_trip_type",format_number("avg(fare_amount)",2))
formatted_avg_fare_per_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.	Identify the top 5 busiest hours of the day in terms of total number of trips.

# COMMAND ----------

# MAGIC %md
# MAGIC #### "pickup_datetime" is new name for column "lpep_pickup_datetime".
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, hour,count  


#convert the datetime string to a timestamp type
df2 = df1.withColumn("pickup_datetime", to_timestamp("lpep_pickup_datetime", "M-d-yyyy H:mm"))
df2.display()


# Extract hour from the timestamp
df3 = df2.withColumn("hour", hour("pickup_datetime"))
df3.display()

# Identify the top 5 busiest hours
top_5_busiest_hours = df3.groupBy("hour").agg(count("*").alias("total_trips")) \
                         .orderBy("total_trips", ascending=False) \
                         .head(5)
display(top_5_busiest_hours)                        




# COMMAND ----------

# MAGIC %md
# MAGIC ##4.	Calculate the percentage of trips where the tip amount exceeds 20% of the total fare amount.
# MAGIC

# COMMAND ----------

# Calculate the total fare amount and tip amount for each trip
df4 = df1.withColumn("total_amount", col("fare_amount") + col("tip_amount"))
df4.display()

# Filter trips where the tip amount exceeds 20% of the total fare amount
filtered_df = df4.filter(col("tip_amount") > 0.2 * col("total_amount"))
filtered_df.display()

# Count the number of trips meeting the condition
num_trips_with_tip_over_20_percent = filtered_df.count()

print("1.Number of trips with tip amount exceeding 20% of total fare amount:", num_trips_with_tip_over_20_percent)

# Count the total number of trips
total_num_trips = df4.count()
print("2.Total number of trips:", total_num_trips)


# Calculate the percentage
percentage = (num_trips_with_tip_over_20_percent / total_num_trips) * 100


# Print the result
print("3.Percentage of trips where the tip amount exceeds 20% of the total fare amount:", percentage)




# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.	Calculate the average trip distance for each day of the week

# COMMAND ----------

# MAGIC %md
# MAGIC ####"EEEE" is for complete "DAY_NAME"
# MAGIC ####"date_format" is for extracting the days like sunday,monday from column.

# COMMAND ----------

 from pyspark.sql.functions import col, date_format ,format_number
 
 #Extract the day of the week from the datetime column
df5 = df2.withColumn("day_of_week", date_format(col("pickup_datetime"), "EEEE"))
df5.display()

# Calculate the total trip distance for each day of the week
total_distance_per_day = df5.groupBy("day_of_week").avg("trip_distance").alias("total_trip_distance")
total_distance_per_day.display()

# Format the average trip distance to display fewer decimal points
total_distance_per_day_formatted = total_distance_per_day.withColumn("avg_trip_distance", format_number("avg(trip_distance)", 2))
total_distance_per_day_formatted.display()


# COMMAND ----------


