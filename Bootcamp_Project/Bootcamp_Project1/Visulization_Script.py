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
# MAGIC ###joining customers_sales_data with products_data using product_id as key

# COMMAND ----------

products_customer_sales_data =  products_data.join(customers_sales_data, ["product_id"] , "inner")

# COMMAND ----------

display(products_customer_sales_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joining employee data with employee_sales_data using employee_id as key

# COMMAND ----------

employee_employeesales_data = empolyee_data.join(employee_sales_data, ["employee_id"], "inner")

# COMMAND ----------

display(employee_employeesales_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calculating total sales amount per product & per customer separately and  combinely
# MAGIC we calculate total sales from sum of Amount paid by each customer for each product

# COMMAND ----------

total_sales_per_product = products_customer_sales_data.groupBy('product_id', 'product_name').agg(F.sum('amount_paid').alias('total_sales')).orderBy('product_id')

total_sales_per_customer = products_customer_sales_data.groupBy('customer_id', 'name').agg(F.sum('amount_paid').alias('total_sales')).orderBy('customer_id')

total_sales_per_prodct_per_customer = products_customer_sales_data.groupBy('customer_id','product_id','product_name', 'name',).agg(F.sum('amount_paid').alias('total_sales')).orderBy('customer_id','product_id')

category_wise_sales = products_customer_sales_data.groupBy('cate')

print("Total sales amount per product:")
display(total_sales_per_product)

print("\nTotal sales amount per customer:")
display(total_sales_per_customer)

print("\nTotal sales amount per prodcut per customer:")
display(total_sales_per_prodct_per_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Determining top 5 selling products and highest-performing employees
# MAGIC
# MAGIC On quantity basis we decide top selling product
# MAGIC
# MAGIC On amount basis that employee earns we decide highest performing employees 

# COMMAND ----------

top_selling_products = products_customer_sales_data.groupBy('product_name').agg(F.count('transaction_id').alias('sales_qty')).orderBy(F.desc('sales_qty')).limit(5)

highest_performing_empolyee = employee_employeesales_data.groupBy('employee_name').agg(F.sum('amount').alias('total_sales_amount')).orderBy(F.desc('total_sales_amount')).limit(5)

print("top_selling_products : ")
top_selling_products.show()

print("\higest_performing_employee : ")
highest_performing_empolyee.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Aggregating sales data to analyze monthly and yearly trends
# MAGIC
# MAGIC For this we firstly extract months and years from transaction_date column and then monthly & yearly sales we calculate by aggregating amount paid by customers.

# COMMAND ----------

products_customer_sales_data = products_customer_sales_data.withColumn("year", F.year("transaction_date"))
products_customer_sales_data = products_customer_sales_data.withColumn("month_no", F.month("transaction_date"))
products_customer_sales_data = products_customer_sales_data.withColumn("month", F.date_format("transaction_date", "MMM"))

yearly_sales = products_customer_sales_data.groupBy("year").agg(F.sum("amount_paid").alias("yearly_sales")).orderBy("year")


monthly_sales = products_customer_sales_data.groupBy("year", "month_no","month").agg(F.sum("amount_paid").alias("monthly_sales")).orderBy("month_no")

print("Yearly Sales Trends:")
yearly_sales.show()

print("\nMonthly Sales Trends:")
monthly_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Importing libraries
# MAGIC
# MAGIC pandas library for converting sparkdataframe to pandas dataframe
# MAGIC
# MAGIC seaborn and matplotlib.pyplot for visulization purpose

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting spark-dataframe to pandas-dataframe using toPandas function

# COMMAND ----------

product_Vs_totalsales = total_sales_per_product.toPandas()
customer_Vs_totalsales = total_sales_per_customer.toPandas()
customer_products_Vs_totalsales = total_sales_per_prodct_per_customer.toPandas()
distributionofprodcuts_by_category = products_customer_sales_data.toPandas()
monthly_sales_trends = monthly_sales.toPandas()
distributionofemployees_by_role = employee_employeesales_data.toPandas()
top5_selling_products = top_selling_products.toPandas()
top5_performing_employees = highest_performing_empolyee.toPandas()

# COMMAND ----------

fig, ax = plt.subplots(1,2, figsize=(18, 10))

sns.barplot( x="product_name",y="total_sales",data=product_Vs_totalsales ,ax=ax[0])
ax[0].set_title('Total Sales Per Product')
ax[0].set_xlabel('Product Name')
ax[0].set_ylabel('Total Sales')

for index, row in product_Vs_totalsales.iterrows():
    ax[0].text(index, row['total_sales'], str(row['total_sales']), color='black', ha="center")

ax[0].tick_params(axis='x', rotation=45)

sns.barplot( x="name",y="total_sales",data=customer_Vs_totalsales ,ax=ax[1])
ax[1].set_title('Total Sales Per Customer')
ax[1].set_xlabel('Customer Name')
ax[1].set_ylabel('Total Sales')

for index, row in customer_Vs_totalsales.iterrows():
    ax[1].text(index, row['total_sales'], str(row['total_sales']), color='black', ha="center")

ax[1].tick_params(axis='x', rotation=45)

plt.tight_layout()

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis of (product vs total sales), (customer vs total sales) Bar Chart
# MAGIC
# MAGIC In (product vs total sales) bar chart above shows the prodcutwise total sales. Product2 has the highest value (5522) and product9 has the lowest value (2648).
# MAGIC
# MAGIC In (customer vs total sales) bar chart above shows the customerwise total sales. Customer4 has the highest value(4844)  and Customer14 has the lowest value (641).
# MAGIC
# MAGIC from (product vs total sales) this we konw that product2 sales are higher by ammountwise and product9 sales are lower.
# MAGIC
# MAGIC from (customer vs total sales) this we konw that customer4 has higher purchasing power than customer14 .
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


fig,ax = plt.subplots(figsize=(22, 10))

sns.barplot( x="name",y="total_sales",hue= "product_name",data=customer_products_Vs_totalsales)
ax.set_title('Total Sales Per Product Per Customer')
ax.set_xlabel('Customer Name')
ax.set_ylabel('Total Sales')

for p in ax.patches:
    ax.annotate('{:.2f}'.format(p.get_height()), (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='center', fontsize=10, color='black', xytext=(0, 5),
                textcoords='offset points')

plt.xticks(rotation=45)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis of group Bar Chart
# MAGIC
# MAGIC In above bar chart we know sales of individual product per customerwise

# COMMAND ----------


category_counts = distributionofprodcuts_by_category['category'].value_counts()
plt.figure(figsize=(8, 8))
plt.pie(category_counts, labels=category_counts.index, autopct='%1.1f%%', startangle=140)
plt.title('Distribution of Products by Category')
plt.axis('equal') 
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis of Distribution of Products Pie Chart
# MAGIC
# MAGIC The pie chart above shows the distribution of prodct across different product categories.
# MAGIC
# MAGIC Category1 has  the largest portion of products with 54% of the total products.
# MAGIC Category3 with 36% of the total products.
# MAGIC Categories2 with 10% of the total products.
# MAGIC
# MAGIC This distribution suggests that Category1 products are the most popular among customers .
# MAGIC Category3 products like by fewer customers and Category2 products are less popular.
# MAGIC

# COMMAND ----------


plt.figure(figsize=(10, 6))
sns.lineplot(x='month', y='monthly_sales', hue='year', data=monthly_sales_trends, marker='o')
plt.title('Monthly Sales Trends')
plt.xlabel('Month')
plt.ylabel('Sales')

for index, row in monthly_sales_trends.iterrows():
    plt.text(row['month'], row['monthly_sales'], str(row['monthly_sales']), ha='right', va='bottom')

plt.xticks(rotation=45) 
plt.grid(True)
# Show the plot
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Analysis of Monthly Sals Trends Line Chart
# MAGIC
# MAGIC In month of June selling was high (6602) followed by December (6368) of products and with low (2009) in month of August.
# MAGIC
# MAGIC Due to festival sales in month of june and december the customer purchasing capability  increases, so total sales are increasing on that months.

# COMMAND ----------

plt.figure(figsize=(10, 6))
sns.scatterplot(data=distributionofprodcuts_by_category, x='price', y='stock_quantity')
for i in range(len(distributionofprodcuts_by_category)):
    plt.text(distributionofprodcuts_by_category['price'][i], distributionofprodcuts_by_category['stock_quantity'][i], f"({distributionofprodcuts_by_category['price'][i]}, {distributionofprodcuts_by_category['stock_quantity'][i]})")

x_points = [166, 447]
y_points = [23, 95]

# Draw a line passing through the specified points
plt.plot(x_points, y_points, color='red', linestyle='-', linewidth=2)

plt.title('Price vs. Stock Quantity')
plt.xlabel('Price')
plt.ylabel('Stock Quantity')
plt.grid(True)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis of Price vs. Stock Quantity Scatter Plot
# MAGIC
# MAGIC The scatter plot above describe the relationship between product price and stock quantity.
# MAGIC
# MAGIC The scatter plot shows a wide range of prices across different stock quantities.
# MAGIC From the graph we saw trends like higher stock quantities for products with lower prices.
# MAGIC However higher stock qunatities for products with higher prices and same for lower qunatities.
# MAGIC
# MAGIC Products with lower prices tend to have higher stock quantities indicates higher demand or more affordable pricing.
# MAGIC Products with higher prices may have lower stock quantities, suggesting either limited availability or higher value per unit.

# COMMAND ----------

plt.figure(figsize=(10, 6))
sns.scatterplot(data=customer_Vs_totalsales, x='total_sales', y='customer_id')

for i in range(len(customer_Vs_totalsales)):
    plt.text(customer_Vs_totalsales['total_sales'][i], customer_Vs_totalsales['customer_id'][i], f"({customer_Vs_totalsales['total_sales'][i]}, {customer_Vs_totalsales['customer_id'][i]})")

x_points = [1087, 2950]
y_points = [7, 13]


plt.plot(x_points, y_points, color='red', linestyle='-', linewidth=2)

plt.title('Amount Paid vs. Customer ID')
plt.xlabel('Amount Paid')
plt.ylabel('Customer ID')
plt.grid(True)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis of Customer ID vs Amount Paid Scatter Plot
# MAGIC
# MAGIC The scatter plot above describe the relationship between customer ID and the amount paid for purchases.
# MAGIC
# MAGIC The scatter plot shows a scattered distribution of points across the graph indicates no clear relationship.
# MAGIC There appear to be clusters of points at various intervals along the x-axis, suggesting that certain groups of customers may have made purchases around similar amounts.
# MAGIC There are also outliers present in the data, representing customers who have made exceptionally high or low payments.
# MAGIC

# COMMAND ----------


plt.figure(figsize=(12, 6))
ax = sns.barplot(data=top5_selling_products.head(5), x='sales_qty', y='product_name', palette='viridis')

for index, value in enumerate(top5_selling_products['sales_qty']):
    ax.text(value, index, str(value), va='center')

plt.title('Top Selling Products by Sales Quantity')
plt.xlabel('Sales Quantity')
plt.ylabel('Product Name')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis of Top 5 Selling Products Bar Chart
# MAGIC Above bar chart shows top selling product on sales quantity basis. In this prodcut8 has highest sales qty=13 followed by product2&6 with 11 and prodcut5&4 with 10 sales qty.
# MAGIC
# MAGIC From this we know that customers want to purchase  product8 in more quantity as sales qty is more.

# COMMAND ----------



plt.figure(figsize=(12, 6))
ax = sns.barplot(data=top5_performing_employees.head(5), x='total_sales_amount', y='employee_name', palette='viridis')

for index, value in enumerate(top5_performing_employees['total_sales_amount']):
    ax.text(value, index, str(value), va='center')

plt.title('Highest Performing Employees by Sales Amount')
plt.xlabel('Total Selling')
plt.ylabel('Employee Name')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Analysis of Top 5 performing employees Bar Chart
# MAGIC Above bar chart shows top5 employees who earns more by selling products. In this employee9 has highest performing amount=10859 followed by employee6 with 5853 and on third rank employee1 with 5729 amount.
# MAGIC
# MAGIC From this we know that employee9 earns more than amongst all the employee that earn around same amount.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard1

# COMMAND ----------



fig, axs = plt.subplots(1, 2, figsize=(25, 10))

sns.barplot(x="product_name", y="total_sales", data=product_Vs_totalsales, ax=axs[0])
axs[0].set_title('Total Sales Per Product')
axs[0].set_xlabel('Product Name')
axs[0].set_ylabel('Total Sales')

for index, row in product_Vs_totalsales.iterrows():
    axs[0].text(index, row['total_sales'], str(row['total_sales']), color='black', ha="center")

axs[0].tick_params(axis='x', rotation=45)


sns.barplot(x="name", y="total_sales", data=customer_Vs_totalsales, ax=axs[1])
axs[1].set_title('Total Sales Per Customer')
axs[1].set_xlabel('Customer Name')
axs[1].set_ylabel('Total Sales')

for index, row in customer_Vs_totalsales.iterrows():
    axs[1].text(index, row['total_sales'], str(row['total_sales']), color='black', ha="center")

axs[1].tick_params(axis='x', rotation=45)


plt.subplots_adjust(hspace=0.5)


fig2, ax2 = plt.subplots(figsize=(22, 10))


sns.barplot(x="name", y="total_sales", hue="product_name", data=customer_products_Vs_totalsales, ax=ax2)
ax2.set_title('Total Sales Per Product Per Customer')
ax2.set_xlabel('Customer Name')
ax2.set_ylabel('Total Sales')

for p in ax2.patches:
    ax2.annotate('{:.2f}'.format(p.get_height()), (p.get_x() + p.get_width() / 2., p.get_height()),
                 ha='center', va='center', fontsize=10, color='black', xytext=(0, 5),
                 textcoords='offset points')

plt.xticks(rotation=45)

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Dashboard 2

# COMMAND ----------

from matplotlib.gridspec import GridSpec


fig = plt.figure(figsize=(20, 12))
grid = GridSpec(2, 3, figure=fig)


ax1 = fig.add_subplot(grid[0, 0:2])
sns.scatterplot(data=distributionofprodcuts_by_category, x='price', y='stock_quantity', ax=ax1)
for i in range(len(distributionofprodcuts_by_category)):
    ax1.text(distributionofprodcuts_by_category['price'][i], distributionofprodcuts_by_category['stock_quantity'][i], f"({distributionofprodcuts_by_category['price'][i]}, {distributionofprodcuts_by_category['stock_quantity'][i]})")

x_points = [166, 447]
y_points = [23, 95]


plt.plot(x_points, y_points, color='red', linestyle='-', linewidth=2)

ax1.set_title('Price vs. Stock Quantity')
ax1.set_xlabel('Price')
ax1.set_ylabel('Stock Quantity')
ax1.grid(True)


ax2 = fig.add_subplot(grid[1, 0:2])
sns.scatterplot(data=customer_Vs_totalsales, x='total_sales', y='customer_id', ax=ax2)
for i in range(len(customer_Vs_totalsales)):
    ax2.text(customer_Vs_totalsales['total_sales'][i], customer_Vs_totalsales['customer_id'][i], f"({customer_Vs_totalsales['total_sales'][i]}, {customer_Vs_totalsales['customer_id'][i]})")

x_points = [1087, 2950]
y_points = [7, 13]


plt.plot(x_points, y_points, color='red', linestyle='-', linewidth=2)

ax2.set_title('Amount Paid vs. Customer ID')
ax2.set_xlabel('Amount Paid')
ax2.set_ylabel('Customer ID')
ax2.grid(True)


ax3 = fig.add_subplot(grid[:, 2:])
category_counts = distributionofprodcuts_by_category['category'].value_counts()
ax3.pie(category_counts, labels=category_counts.index, autopct='%1.1f%%', startangle=140)
ax3.set_title('Distribution of Products by Category')
ax3.axis('equal')


plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Dashboard 3

# COMMAND ----------

from matplotlib.gridspec import GridSpec


fig = plt.figure(figsize=(18, 12))
grid = GridSpec(2, 2, figure=fig)


ax1 = fig.add_subplot(grid[1, 0])
top5_selling_products = highest_performing_empolyee.toPandas()
sns.barplot(data=top5_performing_employees.head(5), x='total_sales_amount', y='employee_name', palette='viridis', ax=ax1)

for index, value in enumerate(top5_performing_employees['total_sales_amount']):
    ax1.text(value, index, str(value), va='center')

ax1.set_title('Highest Performing Employees by Sales Amount')
ax1.set_xlabel('Total Selling')
ax1.set_ylabel('Employee Name')


ax2 = fig.add_subplot(grid[1, 1])
top5_selling_products = top_selling_products.toPandas()
sns.barplot(data=top5_selling_products.head(5), x='sales_qty', y='product_name', palette='viridis', ax=ax2)

for index, value in enumerate(top5_selling_products['sales_qty']):
    ax2.text(value, index, str(value), va='center')

ax2.set_title('Top Selling Products by Sales Quantity')
ax2.set_xlabel('Sales Quantity')
ax2.set_ylabel('Product Name')


ax3 = fig.add_subplot(grid[0, :])  
monthly_sales_trends = monthly_sales.toPandas()
sns.lineplot(x='month', y='monthly_sales', hue='year', data=monthly_sales_trends, marker='o', ax=ax3)

for index, row in monthly_sales_trends.iterrows():
    ax3.text(row['month'], row['monthly_sales'], str(row['monthly_sales']), ha='right', va='bottom')

ax3.set_title('Monthly Sales')
ax3.set_xlabel('Month')
ax3.set_ylabel('Sales')
ax3.grid(True)
ax3.tick_params(axis='x', rotation=45)  


plt.tight_layout()
plt.show()


# COMMAND ----------


