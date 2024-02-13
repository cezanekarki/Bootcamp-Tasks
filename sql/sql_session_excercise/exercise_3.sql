
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


--Calculate salary of a employee greater than 60000 with CTE
WITH HighSalaryEmployees AS (
  SELECT *
  FROM employees
  WHERE salary > 60000
)

SELECT *
FROM HighSalaryEmployees;


--List all the IT department using CTE
WITH it_dept AS (
  SELECT *
  FROM employees
  WHERE department ='IT'
)

SELECT *
FROM it_dept;


--Extract top 3 earners using CTE with window function
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


--Calculate average salary in each department using CTE
WITH avg_salary AS (
  SELECT department, AVG(salary) AS avg_salary
  FROM employees
  GROUP BY department
)

SELECT *
FROM avg_salary;


--Use case with CTE
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
