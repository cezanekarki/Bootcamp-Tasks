# Databricks notebook source
# MAGIC %md
# MAGIC # Visualization Script
# MAGIC ## Creating SparkSession

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Assignment1") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Dataframes from the sheets in the attached excel.
# MAGIC

# COMMAND ----------

sales_data= spark.read.format("csv").load("dbfs:/FileStore/tables/Sales-2.csv",header = True, inferschema= True)
from pyspark.sql.functions import col
sales_data = sales_data.drop(col("_c5"), col("_c6"), col("_c7"), col("_c8"),col("_c9"),col("_c10"))
customers_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Customer-2.csv",header = True, inferschema= True)
products_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Products-2.csv",header = True, inferschema= True)
employee_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Employee-2.csv",header = True, inferschema= True)
employeesales_data = spark.read.format("csv").load("dbfs:/FileStore/tables/EmployeeSale-2.csv",header = True, inferschema= True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Selling Product By Revenue

# COMMAND ----------

# Calculate total sales amount per product
sales_per_product = sales_data.groupBy('product_id') \
    .agg(sum('amount_paid').alias('total_sales')) \
    .orderBy('total_sales', ascending=False)

sales_per_product.display()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Extract top 10 selling products by revenue
top_products_revenue = sales_per_product.limit(10).toPandas()

# Set the style
sns.set_style("whitegrid")

# Set the color palette
colors = sns.color_palette("icefire", len(top_products_revenue))

# Create bar plot for top selling products by revenue
plt.figure(figsize=(12, 6))
bars = plt.bar(top_products_revenue['product_id'], top_products_revenue['total_sales'], color=colors)
plt.xlabel('Product ID')
plt.ylabel('Total Sales (Revenue)')
plt.title('Top Selling Products by Revenue')
plt.xticks(range(1, 11))

# Add annotations at the peak of each bar
for bar in bars:
    height = bar.get_height()
    plt.annotate(f"{height}",
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3),  # 3 points vertical offset
                 textcoords="offset points",
                 ha='center', va='bottom')

plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC -> Above bar chart shows the top  10 selling products of the company.
# MAGIC According to the chart product 2 (5522) is the highest selling product and product 9 (2648) is the lowest selling product.
# MAGIC We need to try different ways to sale product 9 and have to increase the quantity of product 2,5,7,8 and 10 to increase the profit.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Highest Performing Employee

# COMMAND ----------

from pyspark.sql import functions as F
top_performing_employee= employeesales_data.groupBy('employee_id').agg(F.count('transaction_id').alias('sales_quantity'),F.sum('amount').alias('top_performing_employee_amount')).orderBy('top_performing_employee_amount',ascending=False)
top_performing_employee.display()

# COMMAND ----------

top_performing_employee_pd = top_performing_employee.toPandas()


# Pie chart showing sales contribution by employee

plt.figure(figsize=(8, 8))
plt.pie(top_performing_employee_pd['top_performing_employee_amount'], labels=top_performing_employee_pd['employee_id'], autopct='%1.1f%%', startangle=140)
plt.title('Sales Contribution by Employee')
plt.axis('equal')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Above pie chart shows that employee no 9 contributes maximum in selling products.Hence employee 9 is the highest performing employee.
# MAGIC Employee 3 is the lowest performer and need to look into it to increase sales.
# MAGIC Employee no 2 and 4 are also below average performers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Trends to Find Peak Period of the Year

# COMMAND ----------


# Time series plot to see Monthly Sales Trends
# particularly we are using Line Chart
# Need to extract the month from "TRANSACTION DATE" column as only 2023 year data is given
from pyspark.sql.functions import year,month,sum

df_filtered = sales_data.withColumn('month', month('transaction_date')).withColumn('year', year('transaction_date'))

df_filtered.display()

# COMMAND ----------

monthly_sales = df_filtered.withColumn('year', year('transaction_date')) \
                           .withColumn('month', month('transaction_date')) \
                           .groupBy('year', 'month') \
                           .agg(sum('amount_paid').alias('total_sales')) \
                           .orderBy('year', 'month')
monthly_sales.display()         

# COMMAND ----------

monthly_sales_pd = monthly_sales.toPandas()
#process to create a line plot for monthly_sales
# it will show the peak period of the year for sales
# from below june is the peak month with highest sale.

import matplotlib.pyplot as plt
plt.figure(figsize=(8, 6))
plt.plot(monthly_sales_pd['month'], monthly_sales_pd['total_sales'])
plt.xlabel('Month')
plt.ylabel('Total Sales')
plt.title('Monthly Sales Trend')
plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.grid(True)

"""for index, row in monthly_sales_pd.iterrows():
    max_height = row['total_sales']  # Get the height of the current bar
    plt.text(index, max_height, str(max_height), color='black', ha='left', va='center_baseline')"""
for index, row in monthly_sales_pd.iterrows():
    max_height = row['total_sales']  # Get the height of the current bar
    plt.text(index, max_height, str(max_height), color='black', ha='left', va='center')    

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Month Of June is the Peak Period of Sales.
# MAGIC December also the second highest month to have maximum sales.
# MAGIC Month of August is the weakest period for sales
# MAGIC Month of Jan is too waeker month for sales.
# MAGIC In order to increase Sales we need to focus more on the month of JUNE and DECEMBER.
# MAGIC We need to Buy more stock in these two months.
# MAGIC Also need to make some unique ideas to cover the sales increase in the month of August and Jan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Most Valuable Customer

# COMMAND ----------

from pyspark.sql import functions as F
sales_per_customer = sales_data.groupBy('customer_id').agg(F.sum('amount_paid').alias('total_sales_per_customer')).orderBy('total_sales_per_customer',ascending=False)
sales_per_customer.display()

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Assuming 'sales_per_customer' DataFrame is already available
# Convert to Pandas DataFrame
top_customers_pd = sales_per_customer.limit(10).toPandas()

# Set up the seaborn style
sns.set_style("whitegrid")

# Create the bar plot using Seaborn
plt.figure(figsize=(10, 6))
ax = sns.barplot(x='customer_id', y='total_sales_per_customer', data=top_customers_pd)
plt.xlabel('Customer ID')
plt.ylabel('Total Sales (Revenue)')
plt.title('Most Valuable Customers')
plt.xticks(rotation=45)
plt.grid(axis='y')  # Add grid lines only on the y-axis

 # Add annotations at the peak of each bar
for index, row in top_customers_pd.iterrows():
    max_height = row['total_sales_per_customer']  # Get the height of the current bar
    ax.text(index, max_height, str(max_height), color='black', ha='center', va='bottom')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Purpose of knowing Customer who buys more products is that we should make sure our customers are happy with the buy and getting good service.
# MAGIC This will help us to make customer consistently buy our products.
# MAGIC Also,we can give them best service from our side.

# COMMAND ----------

# MAGIC %md
# MAGIC # DashBoard Of the Project
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Load data into Pandas DataFrames
# Assuming data is loaded into appropriate DataFrames

# Set figure size for the entire dashboard
plt.figure(figsize=(18, 14))

# Plot 1: Line plot
plt.subplot(3, 3, 1)
sns.lineplot(x='month', y='total_sales', data=monthly_sales_pd, color='skyblue', marker='o', markersize=8, linewidth=2)
plt.title('Monthly Sales Trend', fontsize=16, color='darkblue')  # Make title darker
plt.xlabel('Month', fontsize=12)
plt.ylabel('Total Sales', fontsize=12)
plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.grid(True)
plt.tight_layout()

for index, row in monthly_sales_pd.iterrows():
    max_height = row['total_sales']  # Get the height of the current bar
    plt.text(index, max_height, str(max_height), color='black', ha='left', va='center')    

# Plot 2: Bar plot
plt.subplot(3, 3, 2)
barplot = sns.barplot(x='employee_id', y='sales_quantity', data=top_performing_employee_pd, palette='viridis')
plt.title('Top Performing Employee', fontsize=16, color='darkblue')  # Make title darker
plt.xlabel('Employee ID', fontsize=12)
plt.ylabel('Sales Quantity', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='y')

# Add annotations at the peak of each bar
for bar in barplot.patches:
    height = bar.get_height()
    plt.annotate(f"{height}",
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3),  # 3 points vertical offset
                 textcoords="offset points",
                 ha='center', va='bottom')

plt.tight_layout()

# Plot 3: Pie chart
plt.subplot(3, 3, 3)
plt.pie(top_products_revenue['total_sales'], labels=top_products_revenue['product_id'], autopct='%1.1f%%', startangle=90, colors=sns.color_palette('Pastel1'), wedgeprops=dict(width=1.0))
plt.title('Top Selling Products', fontsize=16, color='darkblue')  # Make title darker
plt.tight_layout()

# Plot 4: Pie chart
plt.subplot(3, 3, 4)
plt.pie(top_performing_employee_pd['top_performing_employee_amount'], labels=top_performing_employee_pd['employee_id'], autopct='%1.1f%%', startangle=140)
plt.title('Sales Contribution by Employee', fontsize=16, color='darkblue')  # Make title darker
plt.axis('equal')

plt.tight_layout()

# Plot 5: Bar chart
plt.subplot(3, 3, 5)
ax = sns.barplot(x='customer_id', y='total_sales_per_customer', data=top_customers_pd)
plt.xlabel('Customer ID', fontsize=12)
plt.ylabel('Total Sales (Revenue)', fontsize=12)
plt.title('Most Valuable Customers', fontsize=16, color='darkblue')  # Make title darker
plt.xticks(rotation=45)
plt.grid(axis='y')

# Add annotations at the peak of each bar
for bar in ax.patches:
    height = bar.get_height()
    plt.annotate(f"{height}",
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3),  # 3 points vertical offset
                 textcoords="offset points",
                 ha='center', va='bottom')

plt.tight_layout()

# Plot 6: Bar chart
plt.subplot(3, 3, 6)
bars = plt.bar(top_products_revenue['product_id'], top_products_revenue['total_sales'], color=sns.color_palette("icefire", len(top_products_revenue)))
plt.xlabel('Product ID', fontsize=12)
plt.ylabel('Total Sales (Revenue)', fontsize=12)
plt.title('Top Selling Products by Revenue', fontsize=16, color='darkblue')  # Make title darker
plt.xticks(range(1, 11))

# Add annotations at the peak of each bar
for bar in bars:
    height = bar.get_height()
    plt.annotate(f"{height}",
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3),  # 3 points vertical offset
                 textcoords="offset points",
                 ha='center', va='bottom')

plt.tight_layout()

# Adjust spacing between second and third rows
plt.subplots_adjust(hspace=0.5)

plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Load data into Pandas DataFrames
# Assuming data is loaded into appropriate DataFrames

# Set figure size for the entire dashboard
plt.figure(figsize=(18, 14))

# Plot 1: Line plot
plt.subplot(3, 2, 1)
sns.lineplot(x='month', y='total_sales', data=monthly_sales_pd, color='skyblue', marker='o', markersize=8, linewidth=2)
plt.title('Monthly Sales Trend', fontsize=16, color='darkblue')  # Make title darker
plt.xlabel('Month', fontsize=12)
plt.ylabel('Total Sales', fontsize=12)
plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.grid(True)
for index, row in monthly_sales_pd.iterrows():
    max_height = row['total_sales']  # Get the height of the current bar
    plt.text(index, max_height, str(max_height), color='black', ha='left', va='center')    

# Plot 2: Bar plot
plt.subplot(3, 2, 2)
barplot = sns.barplot(x='employee_id', y='sales_quantity', data=top_performing_employee_pd, palette='viridis')
plt.title('Top Performing Employee', fontsize=16, color='darkblue')  # Make title darker
plt.xlabel('Employee ID', fontsize=12)
plt.ylabel('Sales Quantity', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='y')

# Add annotations at the peak of each bar
for bar in barplot.patches:
    height = bar.get_height()
    plt.annotate(f"{height}",
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3),  # 3 points vertical offset
                 textcoords="offset points",
                 ha='center', va='bottom')

# Plot 3: Pie chart
plt.subplot(3, 2, 3)
plt.pie(top_products_revenue['total_sales'], labels=top_products_revenue['product_id'], autopct='%1.1f%%', startangle=90, colors=sns.color_palette('Pastel1'), wedgeprops=dict(width=1.0))
plt.title('Top Selling Products', fontsize=16, color='darkblue')  # Make title darker

# Plot 4: Pie chart
plt.subplot(3, 2, 4)
plt.pie(top_performing_employee_pd['top_performing_employee_amount'], labels=top_performing_employee_pd['employee_id'], autopct='%1.1f%%', startangle=140)
plt.title('Sales Contribution by Employee', fontsize=16, color='darkblue')  # Make title darker
plt.axis('equal')

# Plot 5: Bar chart
plt.subplot(3, 2, 5)
ax = sns.barplot(x='customer_id', y='total_sales_per_customer', data=top_customers_pd)
plt.xlabel('Customer ID', fontsize=12)
plt.ylabel('Total Sales (Revenue)', fontsize=12)
plt.title('Most Valuable Customers', fontsize=16, color='darkblue')  # Make title darker
plt.xticks(rotation=45)
plt.grid(axis='y')

# Add annotations at the peak of each bar
for bar in ax.patches:
    height = bar.get_height()
    plt.annotate(f"{height}",
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3),  # 3 points vertical offset
                 textcoords="offset points",
                 ha='center', va='bottom')

# Plot 6: Bar chart
plt.subplot(3, 2, 6)
bars = plt.bar(top_products_revenue['product_id'], top_products_revenue['total_sales'], color=sns.color_palette("icefire", len(top_products_revenue)))
plt.xlabel('Product ID', fontsize=12)
plt.ylabel('Total Sales (Revenue)', fontsize=12)
plt.title('Top Selling Products by Revenue', fontsize=16, color='darkblue')  # Make title darker
plt.xticks(range(1, 11))

# Add annotations at the peak of each bar
for bar in bars:
    height = bar.get_height()
    plt.annotate(f"{height}",
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3),  # 3 points vertical offset
                 textcoords="offset points",
                 ha='center', va='bottom')

plt.tight_layout()

plt.show()



# COMMAND ----------


