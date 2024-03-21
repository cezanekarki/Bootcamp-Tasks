# Databricks notebook source
from pyspark.sql import SparkSession

saprk = SparkSession.builder.getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading CSV file

# COMMAND ----------

df = spark.read.format("csv").load("/FileStore/tables/taxi_data-1.csv",header=True, inferschema = True)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking null values before transformaation

# COMMAND ----------

null_counts = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}

for column, count in null_counts.items():
    print(f" '{column}': {count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting dates whichever in string fromat into proper date format

# COMMAND ----------

df = df.withColumn("lpep_pickup_datetime", F.to_timestamp(F.col("lpep_pickup_datetime"), "dd-MM-yy h:mm:ss a"))\
    .withColumn("lpep_dropoff_datetime", F.to_timestamp(F.col("lpep_dropoff_datetime"), "dd-MM-yy h:mm:ss a"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking null values after transformaation

# COMMAND ----------

null_counts = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}

for column, count in null_counts.items():
    print(f" '{column}': {count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Adding columns named day, day_no in dataframe for visulization

# COMMAND ----------

df= df.withColumn("day",F.date_format("lpep_pickup_datetime",'EEEE'))\
      .withColumn("day_no",F.dayofweek("lpep_pickup_datetime"))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Questions

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.	Calculate the average trip distance for each day of the week, considering only trips longer than 5 miles.

# COMMAND ----------

trips_longer_than_5 = df.filter(F.col('trip_distance')> 5)

result = trips_longer_than_5.groupBy('day_no','day').agg(F.round(F.avg('trip_distance'), 3).alias('average_trip_distance')).orderBy('day_no')

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.	Find the average fare amount per trip type (e.g., street-hail, dispatch) for trips longer than 10 miles, excluding trips with zero fare.

# COMMAND ----------

trips_longer_than_10 = df.filter((F.col('trip_distance')> 5) & (F.col('fare_amount') > 0) & (F.col('trip_type')>0))

result = trips_longer_than_10.groupBy('trip_type').agg(F.round(F.avg('fare_amount'), 3).alias('average_fare_amount'))

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Visulizaion

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization Task : Visualize the distribution of trip distances for each day of the week.
# MAGIC Expected steps:
# MAGIC
# MAGIC •	Aggregate data to calculate the total trip distance for each day of the week.
# MAGIC
# MAGIC •	Normalize the trip distances for each day of the week.
# MAGIC
# MAGIC •	Plot the normalized trip distances for each day of the week as a line chart.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Normalizing Trip Distance

# COMMAND ----------


total_distance = df.groupBy('day_no', 'day').agg(F.round(F.sum('trip_distance'), 3).alias('total_trip_distance'))

maximum_distance = total_distance.agg(F.max('total_trip_distance').alias('max_distance')).collect()[0]['max_distance']

normalize_data = total_distance.withColumn("normalized_distance", F.round(F.col('total_trip_distance') / maximum_distance , 3))

final_normalize_data = normalize_data.orderBy('day_no')

display(final_normalize_data)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Importing libraries for conversion and visulization 

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting to pandas dataframe

# COMMAND ----------

dayVsdistance = final_normalize_data.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Plotting Line chart :- Trip Distance of Each Week of the Day

# COMMAND ----------

plt.figure(figsize=(10, 6))
sns.lineplot(x='day', y='normalized_distance', data=dayVsdistance, marker='o')
plt.title(' Each Day Of The Week Trip Distances')
plt.xlabel('Days')
plt.ylabel('Trip Distance')

for index, row in dayVsdistance.iterrows():
    plt.text(row['day'], row['normalized_distance'], str(row['normalized_distance']), ha='right', va='bottom')

plt.grid(True)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Analysis of Each Day Of The Week Trip Distances Line Chart
# MAGIC
# MAGIC In week on thursday trip_distance is more (1) followed by Saturday (0.955)  and with low (0.607) on sunday.
# MAGIC
# MAGIC By seeing this we can say that on thursday, the total trip commutes are more, hence the total distance travelled is more.
# MAGIC Sunday being weekely off has the least distance travelled.
# MAGIC
# MAGIC

# COMMAND ----------


