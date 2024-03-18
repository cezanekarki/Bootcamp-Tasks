# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading csv files to create a dataframes

# COMMAND ----------

sales_data  = spark.read.format("csv").load("dbfs:/FileStore/tables/Bootcamp_Assignment/Sales.csv", header = True, inferschema = True )

customers_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Bootcamp_Assignment/Customers.csv", header = True, inferschema = True )

products_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Bootcamp_Assignment/Products.csv", header = True, inferschema = True )

empolyee_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Bootcamp_Assignment/Employee.csv", header = True, inferschema = True )

employee_sales_data = spark.read.format("csv").load("dbfs:/FileStore/tables/Bootcamp_Assignment/EmployeeSale.csv", header = True, inferschema = True )


# COMMAND ----------

display(sales_data)

# COMMAND ----------

display(customers_data)

# COMMAND ----------

display(products_data)

# COMMAND ----------

display(empolyee_data)

# COMMAND ----------

display(employee_sales_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###chnaging sales_data (transaction_date) and employee_sales_data (sale_date) to date format

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------


sales_data =sales_data.withColumn("transaction_date", F.to_date(F.col("transaction_date"), "dd-MM-yy H:mm"))

employee_sales_data =employee_sales_data.withColumn("sale_date", F.to_date(F.col("sale_date"), "dd-MM-yy H:mm"))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Joining sales_data with customers_data using customer_id as key

# COMMAND ----------

customers_sales_data =  sales_data.join(customers_data, ["customer_id"] , "inner")

# COMMAND ----------

display(customers_sales_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joining sales_data with prducts_data using product_id as key

# COMMAND ----------

products_sales_data = products_data.join(sales_data, ["product_id"], "inner")

# COMMAND ----------

display(products_sales_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joining employee data with employee_sales_data using employee_id as key

# COMMAND ----------

employee_employeesales_data = empolyee_data.join(employee_sales_data, ["employee_id"], "inner")

# COMMAND ----------

display(employee_employeesales_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calculate total sales amount per product and per customer?
# MAGIC
# MAGIC sum of Amount paid by each customer for each product we calculate total sales

# COMMAND ----------

total_sales_per_product_per_customer = products_sales_data.groupBy('product_id','customer_id').agg(F.sum('amount_paid').alias('total_sales')).orderBy(F.desc('total_sales'))

print("\nTotal sales amount per product & customer:")
display(total_sales_per_product_per_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Determine top-selling products and highest-performing employees?
# MAGIC
# MAGIC On quantity basis we decide top selling product
# MAGIC
# MAGIC On amount basis that employee earns we decide highest performing employees 

# COMMAND ----------

top_selling_products = products_sales_data.groupBy('product_name').agg(F.count('transaction_id').alias('sales_qty')).orderBy(F.desc('sales_qty')).limit(5)

highest_performing_empolyee = employee_employeesales_data.groupBy('employee_name').agg(F.sum('amount').alias('total_sales_amount')).orderBy(F.desc('total_sales_amount')).limit(5)

print("top_selling_products : ")
top_selling_products.show()

print("\higest_performing_employee : ")
highest_performing_empolyee.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Aggregate sales data to analyze monthly and yearly trends?
# MAGIC
# MAGIC For this we firstly extract months and years from transaction_date column and then monthly & yearly sales we calculate by aggregating amount paid by customers.

# COMMAND ----------


customers_sales_data = customers_sales_data.withColumn("year", F.year("transaction_date"))
customers_sales_data = customers_sales_data.withColumn("month_no", F.month("transaction_date"))
customers_sales_data = customers_sales_data.withColumn("month", F.date_format("transaction_date", "MMM"))

yearly_sales = customers_sales_data.groupBy("year").agg(F.sum("amount_paid").alias("total_yearly_sales")).orderBy("year")


monthly_sales = customers_sales_data.groupBy("year", "month_no","month").agg(F.sum("amount_paid").alias("total_monthly_sales")).orderBy("month_no")

print("Yearly Sales Trends:")
yearly_sales.show()

print("\nMonthly Sales Trends:")
monthly_sales.show()

# COMMAND ----------


