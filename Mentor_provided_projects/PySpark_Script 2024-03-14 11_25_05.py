# Databricks notebook source
# MAGIC %md
# MAGIC ## A SparkSession in a Python script using PySpark.

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Assignment1") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Create Dataframes from the sheets in the attached excel.

# COMMAND ----------

sales_data= spark.read.format("csv").load("dbfs:/FileStore/tables/Sales-2.csv",header = True, inferschema= True)
from pyspark.sql.functions import col

# Drop multiple columns from the DataFrame
sales_data = sales_data.drop(col("_c5"), col("_c6"), col("_c7"), col("_c8"),col("_c9"),col("_c10"))
customers_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Customer-2.csv",header = True, inferschema= True)
products_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Products-2.csv",header = True, inferschema= True)
employee_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Employee-2.csv",header = True, inferschema= True)
employeesales_data = spark.read.format("csv").load("dbfs:/FileStore/tables/EmployeeSale-2.csv",header = True, inferschema= True)

# COMMAND ----------

sales_data.display()

# COMMAND ----------

sales_data.display()

# COMMAND ----------

customers_data.display()

# COMMAND ----------

products_data.display()

# COMMAND ----------

employee_data.display()

# COMMAND ----------

employeesales_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.	Merge customer information with sales transactions by identifying the required joining keys
# MAGIC

# COMMAND ----------

Merge_customer_sales = customers_data.join(sales_data,on = 'customer_id',how = 'inner')
Merge_customer_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.	Join product details with sales transactions by identifying the required joining keys.

# COMMAND ----------

Join_product_sales = products_data.join(sales_data,on='product_id',how = 'inner')
Join_product_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.	Calculate total sales amount per product and per customer.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, sum


total_sales_per_product_per_customer = Join_product_sales.groupBy( 'customer_id','product_id') \
    .agg(sum('amount_paid').alias('total_sales')) \
    .orderBy('customer_id')
total_sales_per_product_per_customer.display()    

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.	Determine top-selling products and highest-performing employees.

# COMMAND ----------

#Top Selling Product

top_selling_product = Join_product_sales.groupBy('product_id','product_name').agg(F.count('transaction_id').alias('sales_quantity_of_product'),F.sum('amount_paid').alias('top_selling_product_amount'),).orderBy('top_selling_product_amount',ascending=False)

top_selling_product.display()


# COMMAND ----------

#Highest Performing Employee

highest_performing_employee= employeesales_data.groupBy('employee_id').agg(F.count('transaction_id').alias('sales_quantity'),F.sum('amount').alias('highest_performing_employee_amount')).orderBy('highest_performing_employee_amount',ascending=False)

highest_performing_employee.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Extracting Month And Year From the Sales_Data

# COMMAND ----------


from pyspark.sql.functions import year, month
# Extract month and year from 'transaction_date' column
df_filtered = sales_data.withColumn('month', month('transaction_date'))\
                   .withColumn('year', year('transaction_date'))


df_filtered.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##6.	Aggregate sales data to analyze monthly and yearly trends.

# COMMAND ----------

 from pyspark.sql.functions import  sum
 #Group by year and month, and aggregate sales
Each_date_sale = df_filtered.groupBy('transaction_date','year', 'month',).agg(sum('amount_paid').alias('total_sales')) \
                         .orderBy('year', 'month')



monthly_sales = df_filtered.withColumn('year', year('transaction_date')) \
                           .withColumn('month', month('transaction_date')) \
                           .groupBy('year', 'month') \
                           .agg(sum('amount_paid').alias('total_sales')) \
                           .orderBy('year', 'month')


# Group by year and aggregate sales
yearly_sales = df_filtered.groupBy('year').agg(sum('amount_paid').alias('total_sales')) \
                        .orderBy('year')

Each_date_sale.display()
monthly_sales.display()     
yearly_sales.display()
