# Databricks notebook source
# MAGIC %sql
# MAGIC -- WINDOW FUNCTION
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

# MAGIC %sql
# MAGIC select * from employee_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row Number
# MAGIC select employee_id,department_id,position,salary,tenure_months,ROW_NUMBER() over (partition by department_id order by salary desc) as row_num from employee_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Rank
# MAGIC select employee_id,department_id,position,salary,tenure_months,rank() over (partition by department_id order by salary desc) as rank_num from employee_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id,department_id,position,salary,tenure_months,dense_rank() over (partition by department_id order by salary desc) as dense_rank_num from employee_data;

# COMMAND ----------


