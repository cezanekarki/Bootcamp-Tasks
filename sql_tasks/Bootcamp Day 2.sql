-- Databricks notebook source
CREATE TABLE training_data1 (
  participant_id INT ,
  participant_name VARCHAR(50),
  training_session_date DATE,
  training_session_duration_hours DECIMAL(5,2),
  trainer_name VARCHAR(50),
  training_location VARCHAR(100),
  training_subject VARCHAR(100)
);


-- COMMAND ----------

INSERT INTO training_data1 (participant_id, participant_name, training_session_date, training_session_duration_hours, trainer_name, training_location, training_subject)
VALUES
    (1, 'Alice Johnson', '2024-02-05', 2.5, 'Trainer C', 'Room 101', 'Introduction to Programming'),
    (2, 'Bob Williams', '2024-02-06', 3.0, 'Trainer A', 'Conference Room B', 'Web Development Basics'),
    (3, 'Charlie Davis', '2024-02-07', 2.0, 'Trainer B', 'Training Room A', 'Project Management Fundamentals'),
    (4, 'David Smith', '2024-02-08', 1.5, 'Trainer C', 'Room 102', 'Digital Marketing Essentials'),
    (5, 'Eva Brown', '2024-02-09', 2.0, 'Trainer A', 'Conference Room A', 'Data Visualization Techniques'),
    (6, 'Frank Johnson', '2024-02-10', 3.5, 'Trainer B', 'Training Room B', 'Effective Leadership Skills'),
    (7, 'Grace Miller', '2024-02-11', 2.0, 'Trainer C', 'Room 103', 'Introduction to Machine Learning'),
    (8, 'Henry Davis', '2024-02-12', 2.5, 'Trainer A', 'Conference Room B', 'Cybersecurity Basics'),
    (9, 'Ivy Wilson', '2024-02-13', 1.5, 'Trainer B', 'Training Room A', 'Public Speaking Techniques'),
    (10, 'Jack White', '2024-02-14', 2.0, 'Trainer C', 'Room 104', 'Agile Project Management'),
    (11, 'Kelly Johnson', '2024-02-15', 3.0, 'Trainer A', 'Conference Room A', 'Social Media Marketing Strategies'),
    (12, 'Leo Brown', '2024-02-16', 2.5, 'Trainer B', 'Training Room B', 'Python Programming Basics'),
    (13, 'Mia Davis', '2024-02-17', 1.0, 'Trainer C', 'Room 105', 'Effective Time Management'),
    (14, 'Nathan Wilson', '2024-02-18', 2.0, 'Trainer A', 'Conference Room B', 'Customer Service Excellence'),
    (15, 'Olivia Miller', '2024-02-19', 2.5, 'Trainer B', 'Training Room A', 'Financial Literacy')
    ;

-- COMMAND ----------

select participant_name
from training_data1 where training_session_duration_hours > (
  select avg(training_session_duration_hours)
  from training_data1
)

-- COMMAND ----------

select distinct training_location 
from training_data1 as main
where exists (
  select 1
  from training_data1 as sub
  where sub.training_location = main.training_location
  and sub.trainer_name <> main.trainer_name
  
);

-- COMMAND ----------

SELECT DISTINCT participant_id, participant_name
FROM training_data1
WHERE training_location IN (
    SELECT training_location
    FROM training_data1
    WHERE trainer_name = 'Trainer A'
);

-- COMMAND ----------



-- COMMAND ----------

CREATE or replace TABLE employee_data1 (
    employee_id INT ,
    department_id INT,
    position VARCHAR(50),
    salary INT,
    tenure_months INT,
    job_experience_years INT
);


-- COMMAND ----------

INSERT INTO employee_data1 VALUES
(1, 101, 'Software Engineer', 90000, 24, 2),
(2, 102, 'Data Analyst', 60000, 18, 1),
(3, 101, 'Project Manager', 90000, 36, 5),
(4, 103, 'UX Designer', 75000, 27, 3),
(5, 102, 'Business Analyst', 70000, 22, 2),
(6, 101, 'Software Engineer', 80000, 30, 4),
(7, 103, 'Product Manager', 95000, 42, 7),
(8, 102, 'Data Scientist', 100000, 48, 8);

-- COMMAND ----------

--Row number
select employee_id, department_id, position, salary, tenure_months,
      row_number() over(order by salary desc) as row_num
from employee_data1;

-- COMMAND ----------

select employee_id, department_id, position, salary, tenure_months,
        dense_rank() over(partition by department_id order by salary desc) as dense_rank_num
from employee_data1;


-- COMMAND ----------

select employee_id, department_id, position, salary, tenure_months,
      sum(salary) over (partition by department_id order by tenure_months) as running_salary_total
from employee_data1;

-- COMMAND ----------

select employee_id, department_id, position, salary, tenure_months,
      avg(salary) over (partition by department_id order by tenure_months) as avg_salary
from employee_data1;

-- COMMAND ----------

select employee_id, department_id, position, salary, tenure_months,
      lead(salary) over (partition by department_id order by tenure_months) as next_tenure_salary
from employee_data1;

-- COMMAND ----------

select employee_id, department_id, position, salary, tenure_months,
      lag(salary) over (partition by department_id order by tenure_months) as prev_tenure_salary
from employee_data1;

-- COMMAND ----------

select employee_id, department_id, position, salary, tenure_months,
      first_value(salary) over (partition by department_id order by tenure_months) as first_tenure_salary
from employee_data1;

-- COMMAND ----------

select employee_id, department_id, position, salary, tenure_months,
      last_value(salary) over (order by tenure_months) as last_tenure_salary
from employee_data1;

-- COMMAND ----------

--CTE
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

-- list high salaried employees having salary > 60000
with high_salary_employees as (
  select *
  from employees
  where salary > 60000
)
select *
from high_salary_employees



-- COMMAND ----------

with it_employees as (
  select *
  from employees
  where department = "IT"
)
select *
from it_employees

-- COMMAND ----------

WITH TopEarners AS (
    SELECT
        employee_id,
        employee_name,
        salary,
        ROW_NUMBER() OVER (ORDER BY salary DESC) AS RowNum
    FROM
        employees
)

SELECT
    employee_id,
    employee_name,
    salary
FROM
    TopEarners
WHERE
    RowNum <= 3;

-- COMMAND ----------

with avg_salary as (
  select department, avg(salary) as average, count(*) as num_employees
  from employees
  group by department

)
select *
from avg_salary

-- COMMAND ----------

with cte_bonus as (
  select *,
  case
  when salary > 60000 then "High Performer"
  when salary > 50000 then "Intermediate Performer"
  else "Low Performer"
  end as performance_category 
  from employees
)

select
  employee_id,
  employee_name,
  salary,
  performance_category,
  case
  when performance_category = "High Performer" then salary * 0.1
  when performance_category = "Intermediate Performer" then salary * 0.05
  else 0
  end as performance_bonus
  from cte_bonus

-- COMMAND ----------


