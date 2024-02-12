-- Databricks notebook source
-- MAGIC %fs 
-- MAGIC rm -r dbfs:/user/hive/warehouse/training_data

-- COMMAND ----------

---Window Functions

-- COMMAND ----------

CREATE TABLE training_data (
    participant_id INT ,
    participant_name VARCHAR(50),
    training_session_date DATE,
    training_session_duration_hours DECIMAL(5, 2),
    trainer_name VARCHAR(50),
    training_location VARCHAR(100),
    training_subject VARCHAR(100)
);

-- COMMAND ----------

INSERT INTO training_data (participant_id, participant_name, training_session_date, training_session_duration_hours, trainer_name, training_location, training_subject)
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

select * from training_data;

-- COMMAND ----------

#select distinct training_location from training_data t where exists(
  select 
)

-- COMMAND ----------

CREATE or replace TABLE employee_data (
    employee_id INT ,
    department_id INT,
    position VARCHAR(50),
    salary INT,
    tenure_months INT,
    job_experience_years INT
);

-- COMMAND ----------

INSERT INTO employee_data VALUES
(1, 101, 'Software Engineer', 90000, 24, 2),
(2, 102, 'Data Analyst', 60000, 18, 1),
(3, 101, 'Project Manager', 90000, 36, 5),
(4, 103, 'UX Designer', 75000, 27, 3),
(5, 102, 'Business Analyst', 70000, 22, 2),
(6, 101, 'Software Engineer', 80000, 30, 4),
(7, 103, 'Product Manager', 95000, 42, 7),
(8, 102, 'Data Scientist', 100000, 48, 8);

-- COMMAND ----------

select * from employee_data;

-- COMMAND ----------

--Row_Number
select employee_id,department_id,position,salary,tenure_months,
row_number() over (partition by department_id order by salary desc) as row_num 
from employee_data;

-- COMMAND ----------

--Rank
select employee_id,department_id,position,salary,tenure_months,
rank() over (partition by department_id order by salary desc) as rank_num 
from employee_data;

-- COMMAND ----------

--Dense_Rank
select employee_id,department_id,position,salary,tenure_months,
dense_rank() over (partition by department_id order by salary desc) as dense_rank_num 
from employee_data;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/user/hive/warehouse/employee_data
-- MAGIC

-- COMMAND ----------

select employee_id,department_id,position,salary,tenure_months,
sum(salary) over(partition by department_id order by tenure_months ) as running_total_salary
from employee_data;

-- COMMAND ----------

select employee_id,department_id,position,salary,tenure_months,
avg(salary) over(partition by department_id order by tenure_months ) as avg_salary
from employee_data;

-- COMMAND ----------

select employee_id,department_id,position,salary,tenure_months,
lead(salary) over(partition by department_id order by tenure_months ) as next_tenure_salary
from employee_data;

-- COMMAND ----------

select employee_id,department_id,position,salary,tenure_months,
lag(salary) over(partition by department_id order by tenure_months ) as prev_tenure_salary
from employee_data;

-- COMMAND ----------

select employee_id,department_id,position,salary,tenure_months,
first_value(salary) over(partition by department_id order by tenure_months ) as first_tenure_salary
from employee_data;

-- COMMAND ----------

select employee_id,department_id,position,salary,tenure_months,
last_value(salary) over(partition by department_id order by tenure_months ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as last_tenure_salary
from employee_data;

-- COMMAND ----------

--CTE
--list the high salaried employees having salary > 60000
with high_salaried_emp as (
  select * from employee_data
  where salary > 60000
)
select * from high_salaried_emp;

-- COMMAND ----------

--Identify top 3 earners in the company
with top3_earners as (
  select *,dense_rank() over (order by salary desc) as dense_rank_num from employee_data
)
select * from top3_earners 
where dense_rank_num<=3

-- COMMAND ----------

--Get a summary of the average salary for each department
with avg_sal as (
  select *,avg(salary) over(partition by department_id order by salary) as avg_sal_deptwise from employee_data
)
select * from avg_sal;

-- COMMAND ----------

with bonus_emp as (
  select *,
  case 
  when salary > 60000 then 'high performer'
  when salary > 50000 then 'intermediate performer'
  else 'low performer'
  end as performance_category
  from employee_data
)
select *,
case
when performance_category = 'high performer' then salary * 0.1
when performance_category = 'intermediate performer'then salary * 0.05
else 0
end as bonus
from bonus_emp;

-- COMMAND ----------


