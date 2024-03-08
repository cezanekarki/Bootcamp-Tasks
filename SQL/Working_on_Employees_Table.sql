-- Databricks notebook source
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

with top_3_salary as (
  select * from (select employee_id,
        employee_name,
        department,salary,
        ROW_NUMBER() OVER (ORDER BY salary DESC) AS ranking
    FROM employees)
    where ranking < 4
) 
select * from top_3_salary;

-- COMMAND ----------

SELECT employee_id,
      employee_name,
      department,
      AVG(salary) OVER (PARTITION BY department) AS avg_salary
FROM employees;


-- COMMAND ----------

with bonus_eleigible_employees as (
    select *,
          case
            when salary > 60000 then 'High Performer'
            when salary > 50000 then 'Intermidiate Performer'
            else 'Low Performer'
          end as performance_category
    from employees
)

select 
      employee_id,
      employee_name,
      salary,
      performance_category,
      case 
          when performance_category = 'High Performer' then salary * 0.1
          when performance_category = 'Intermidiate Performer' then salary * 0.05
          else 0
      end as bonus
from bonus_eleigible_employees

-- COMMAND ----------


