-- Databricks notebook source


-- COMMAND ----------

CREATE TABLE employees (
  employee_id INT,
  employee_name STRING,
  department STRING,
  salary DOUBLE
);

-- COMMAND ----------

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

WITH sixtythousandabove AS (
  SELECT *
  FROM employees
  WHERE salary > 60000
)

SELECT * FROM sixtythousandabove;

-- COMMAND ----------

WITH it_dept AS (
  SELECT *
  FROM employees
  WHERE department = "IT"
)

SELECT * FROM it_dept;

-- COMMAND ----------

WITH top_earners AS (
  SELECT *,
  RANK() OVER (ORDER BY salary DESC) as salary_rank
  FROM employees
)

SELECT * FROM top_earners
WHERE salary_rank <=3;

-- COMMAND ----------

WITH dept_avg AS (
  SELECT department,
  AVG(salary) OVER (PARTITION BY department) as avg_salary
  FROM employees
)

SELECT DISTINCT *  FROM dept_avg;

-- COMMAND ----------

WITH bonus_calculator AS (
  SELECT *,
         CASE
           WHEN salary > 60000 THEN 'High Performer'
           WHEN salary > 50000 THEN 'Intermediate Performer'
           ELSE 'Standard Performer'
         END AS performance_category,
         CASE
           WHEN salary > 60000 THEN salary * 0.1
           WHEN salary > 50000 THEN salary * 0.05
           ELSE 0
         END AS performance_bonus
  FROM employees
)

SELECT * FROM bonus_calculator;


-- COMMAND ----------


