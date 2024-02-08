# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE employee_data (
# MAGIC     employee_id INT ,
# MAGIC     department_id INT,
# MAGIC     position VARCHAR(50),
# MAGIC     salary INT,
# MAGIC     tenure_months INT,
# MAGIC     job_experience_years INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO employee_data VALUES
# MAGIC (1, 101, 'Software Engineer', 90000, 24, 2),
# MAGIC (2, 102, 'Data Analyst', 60000, 18, 1),
# MAGIC (3, 101, 'Project Manager', 90000, 36, 5),
# MAGIC (4, 103, 'UX Designer', 75000, 27, 3),
# MAGIC (5, 102, 'Business Analyst', 70000, 22, 2),
# MAGIC (6, 101, 'Software Engineer', 80000, 30, 4),
# MAGIC (7, 103, 'Product Manager', 95000, 42, 7),
# MAGIC (8, 102, 'Data Scientist', 100000, 48, 8);

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC       position,
# MAGIC       department_id,
# MAGIC       salary,
# MAGIC       tenure_months,
# MAGIC       job_experience_years,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS row_num
# MAGIC FROM
# MAGIC     employee_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     position,
# MAGIC     department_id,
# MAGIC     salary,
# MAGIC     tenure_months,
# MAGIC     job_experience_years,
# MAGIC     RANK() OVER (PARTITION BY department_id ORDER BY tenure_months DESC) AS ranking
# MAGIC FROM
# MAGIC     employee_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     position,
# MAGIC     department_id,
# MAGIC     salary,
# MAGIC     tenure_months,
# MAGIC     job_experience_years,
# MAGIC     dense_rank() OVER (PARTITION BY employee_id ORDER BY salary DESC) AS dense_ranking
# MAGIC FROM
# MAGIC     employee_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     department_id,
# MAGIC     position,
# MAGIC     salary,
# MAGIC     tenure_months,
# MAGIC     AVG(salary) OVER (PARTITION BY position ORDER BY tenure_months) AS avg_salary_per_group
# MAGIC FROM
# MAGIC     employee_data;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     department_id,
# MAGIC     position,
# MAGIC     salary,
# MAGIC     tenure_months,
# MAGIC     SUM(salary) OVER (PARTITION BY position ORDER BY tenure_months) AS Position_running_total
# MAGIC FROM
# MAGIC     employee_data;                 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     department_id,
# MAGIC     position,
# MAGIC     salary,
# MAGIC     tenure_months,
# MAGIC     LAG(salary) OVER (PARTITION BY position ORDER BY tenure_months) AS Position_running_total
# MAGIC FROM
# MAGIC     employee_data;  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     department_id,
# MAGIC     position,
# MAGIC     salary,
# MAGIC     tenure_months,
# MAGIC     first_value(salary) OVER (PARTITION BY position ORDER BY tenure_months) AS Position_running_total
# MAGIC FROM
# MAGIC     employee_data;  

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     department_id,
# MAGIC     position,
# MAGIC     salary,
# MAGIC     tenure_months,
# MAGIC     last_value(salary) OVER (PARTITION BY position ORDER BY tenure_months) AS Position_running_total
# MAGIC FROM
# MAGIC     employee_data;  

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Create a view or table

temp_table_name = "dummy_data_2_txt"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `dummy_data_2_txt`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "dummy_data_2_txt"

# df.write.format("parquet").saveAsTable(permanent_table_name)
