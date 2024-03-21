# Databricks notebook source
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Assignment2") \
    .getOrCreate()    

# COMMAND ----------

taxi_data2= spark.read.format("csv").load("dbfs:/FileStore/tables/taxi_data2.csv",header = True,inferSchema=True)
taxi_data2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##1.	Determine the maximum fare amount for trips where the pickup location is the same as the drop-off location, excluding trips with zero fare.

# COMMAND ----------

from pyspark.sql.functions import col, max
filtered_df = taxi_data2.filter((col("PULocationID") == col("DOLocationID")) & (col("fare_amount") != 0))
filtered_df.display()

# Determine the maximum fare amount
max_fare = filtered_df.select(max("fare_amount")).first()[0]
display("maximum fare amount is ",max_fare)



# COMMAND ----------

# MAGIC %md
# MAGIC ##2.	Identify the top 5 vendors based on the total fare amount collected, considering only trips with a positive fare amount.

# COMMAND ----------

from pyspark.sql.functions import col, max ,sum ,format_number

# Filter trips with a positive fare amount
positive_fare_df = taxi_data2.filter((col("fare_amount") > 0) & (col("VendorID").isNotNull()))

positive_fare_df.display()

# Add filter
filtered_df = taxi_data2.filter(col("VendorID").isNotNull())

# Group by vendor and calculate the total fare amount collected for each vendor
total_fare_per_vendor = positive_fare_df.groupBy("VendorID").agg(sum("fare_amount").alias("total_fare_amount"))
total_fare_per_vendor.display()

# Sort the vendors based on total fare amount collected in descending order
sorted_vendors = total_fare_per_vendor.orderBy(col("total_fare_amount").desc())
sorted_vendors.display()

vendors = sorted_vendors.withColumn("total_fare_amount",format_number("total_fare_amount",2))
vendors.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Visualization Task : Visualize the relationship between trip distance and trip amount, colored by payment type.
# MAGIC Expected steps:
# MAGIC
# MAGIC •	Filter out trips with positive trip amount.
# MAGIC •	Extract trip distance and trip amount.
# MAGIC •	Plot a scatter plot of trip distance vs. trip amount, coloring points by payment type.
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Assuming you have a DataFrame named df with columns "trip_distance", "tip_amount", and "payment_type"

# Filter out trips with positive tip amount
positive_tip_df = taxi_data2.filter(col("tip_amount") > 0)

# Extract trip distance, tip amount, and payment type
trip_data = positive_tip_df.select("trip_distance", "tip_amount", "payment_type").toPandas()

# Plot a scatter plot of trip distance vs. tip amount, coloring points by payment type
plt.figure(figsize=(10, 6))
sns.scatterplot(data=trip_data, x="trip_distance", y="tip_amount", hue="payment_type", palette="Set2")
plt.title("Relationship between Trip Distance and Tip Amount (Colored by Payment Type)")
plt.xlabel("Trip Distance")
plt.ylabel("Tip Amount")
plt.legend(title="Payment Type")
plt.grid(True)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Correlation between Trip Distance and Tip Amount:
# MAGIC    We have a Weak Negative Correlation between trip distance and tip amount.As trip distance decreases, tip amount may slightly increase or stay relatively constant. This could indicate that passengers may be more likely to give a higher tip for shorter trips.
# MAGIC
# MAGIC
# MAGIC ###Effect of Payment Type:
# MAGIC
# MAGIC    Different payment types may exhibit different tipping behaviors. The plot uses green color to represent different payment types, allowing us to compare tipping patterns between payment methods.but we have a single payment type method .
# MAGIC
# MAGIC ###Density of Points:
# MAGIC    Denser regions indicate common trip distances or tipping amounts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### As there are two payment_types but showing data for only one payment type because "payment_type 2" has zero "tip_amount"

# COMMAND ----------

# Apply filter for payment type 2
trip_data = positive_tip_df.select("trip_distance", "tip_amount", "payment_type").toPandas()
filtered_df = taxi_data2.filter((col("payment_type") == 2))

# Select tip_amount
tip_amount_for_payment_type_2 = filtered_df.select("tip_amount")

# Display the result
tip_amount_for_payment_type_2.show()


# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
 
# Assuming you have a DataFrame named taxi_data with columns "trip_distance", "tip_amount", and "payment_type"
 
# Filter out trips with positive tip amount
positive_tip_df = taxi_data2[taxi_data2['tip_amount'] > 0]
trip_data = positive_tip_df.select("trip_distance", "tip_amount", "payment_type").toPandas()
 

 
# Create a DataFrame with all payment types
all_payment_types_df = pd.DataFrame({'payment_type': [1, 2]})
# Merge it with your original DataFrame to ensure all payment types are present
merged_df = all_payment_types_df.merge(trip_data, on='payment_type', how='left')
 
# Create the scatter plot
plt.figure(figsize=(10, 6))
sns.scatterplot(data=merged_df, x='trip_distance', y='tip_amount', hue='payment_type', palette='Set2')
 
# Set plot labels and title
plt.title("Relationship between Trip Distance and Tip Amount (Colored by Payment Type)")
plt.xlabel("Trip Distance")
plt.ylabel("Tip Amount")
plt.legend(title="Payment Type")
plt.grid(True)
plt.show()

# COMMAND ----------


