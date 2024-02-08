-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Overview
-- MAGIC
-- MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
-- MAGIC
-- MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

-- COMMAND ----------

CREATE TABLE employees (
  employee_id INT,
  employee_name STRING,
  department STRING,
  salary DOUBLE
);
 
INSERT INTO employees VALUES
  (6, 'Eva Green', 'HR', 52000),
  (7, 'Sam Johnson', 'Marketing', 59000),
  (8, 'Alex Turner', 'IT', 63000),
  (9, 'Sophie Walker', 'Finance', 72000),
  (10, 'David Clark', 'IT', 60000),
  (11, 'Olivia King', 'Marketing', 56000),
  (12, 'Michael Baker', 'HR', 48000),
  (13, 'Emma White', 'Finance', 68000),
  (14, 'Daniel Smith', 'IT', 65000),
  (15, 'Grace Taylor', 'HR', 50000),
  (16, 'Liam Wilson', 'Marketing', 58000),
  (17, 'Ava Hall', 'IT', 61000),
  (18, 'Mia Adams', 'Finance', 70000),
  (19, 'Noah Moore', 'IT', 64000),
  (20, 'Isabella Davis', 'Marketing', 57000);

-- COMMAND ----------

WITH HighSalaryEmployees AS (
  SELECT *
  FROM employees
  WHERE salary > 60000
)

SELECT *
FROM HighSalaryEmployees;

-- COMMAND ----------

WITH it_dept AS (
  SELECT *
  FROM employees
  WHERE department ='IT'
)

SELECT *
FROM it_dept;

-- COMMAND ----------

WITH ranked_employees AS (
  SELECT
    employee_id,
    employee_name,
    department,
    salary,
    RANK() OVER (ORDER BY salary DESC) AS salary_rank
  FROM employees
)

SELECT *
FROM ranked_employees
WHERE salary_rank <= 3;


-- COMMAND ----------

WITH avg_salary AS (
  SELECT department, AVG(salary) AS avg_salary
  FROM employees
  GROUP BY department
)

SELECT *
FROM avg_salary;


-- COMMAND ----------

WITH cte_performance AS (
    SELECT
        employee_id,
        employee_name,
        department,
        salary,
        CASE
            WHEN salary > 60000 THEN 'High Performer'
            WHEN salary > 50000 THEN 'Intermediate Performer'
            ELSE 'Low Performer'
        END AS performance_category
    FROM employees
)
 
SELECT
    employee_id,
    employee_name,
    department,
    salary,
    performance_category,
    CASE
        WHEN performance_category = 'High Performer' THEN salary * 0.1
        WHEN performance_category = 'Intermediate Performer' THEN salary * 0.05
        ELSE 0
    END AS performance_bonus
FROM cte_performance;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a view or table
-- MAGIC
-- MAGIC temp_table_name = "dummy_data_2-1_txt"
-- MAGIC
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------


/* Query the created temp table in a SQL cell */

select * from `dummy_data_2-1_txt`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
-- MAGIC # Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
-- MAGIC # To do so, choose your table name and uncomment the bottom line.
-- MAGIC
-- MAGIC permanent_table_name = "dummy_data_2-1_txt"
-- MAGIC
-- MAGIC # df.write.format("parquet").saveAsTable(permanent_table_name)
