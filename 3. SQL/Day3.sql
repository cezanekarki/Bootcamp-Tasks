-- Databricks notebook source
-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/training_data

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

INSERT INTO training_data (participant_id, participant_name, training_session_date, training_session_duration_hours, trainer_name, training_location, training_subject)
VALUES
    (16, 'Bob Dylan', '2024-02-20', 2.8, 'Trainer A', 'Room 101', 'Music Theory');

-- COMMAND ----------

select * from training_data;

-- COMMAND ----------

select trainer_name,count(training_session_duration_hours) from training_data
group by trainer_name
order by trainer_name asc;

-- COMMAND ----------

select participant_id from training_data
where training_session_duration_hours > (select avg(training_session_duration_hours) from training_data); 

-- COMMAND ----------

select distinct * from training_data as td1
where exists(
  select 1
  from training_data as td2
  where td1.training_location = td2.training_location
  and td1.trainer_name <> td2.trainer_name
 )



-- COMMAND ----------

select * from training_data
where trainer_name = (select trainer_name from training_data group by trainer_name order by trainer_name asc limit 1)

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/employee_data

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

select employee_id, department_id, position, salary, tenure_months,
row_number() over (partition by department_id order by salary DESC) AS row_num
FROM employee_data;

-- COMMAND ----------

SELECT employee_id, department_id, position, salary, tenure_months,
    RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS ranking
FROM
    employee_data;

-- COMMAND ----------

SELECT employee_id, department_id, position, salary, tenure_months,
    dense_rank() OVER (PARTITION BY department_id ORDER BY salary DESC) AS ranking
FROM
    employee_data;

-- COMMAND ----------

SELECT
    employee_id,
    department_id,
    position,
    salary,
    tenure_months,
    SUM(salary) OVER (PARTITION BY position ORDER BY tenure_months) AS Position_running_total
FROM
    employee_data;

-- COMMAND ----------

SELECT
    employee_id,
    department_id,
    position,
    salary,
    tenure_months,
    AVG(salary) OVER (PARTITION BY position ORDER BY tenure_months) AS avg_salary_per_group
FROM
    employee_data;

-- COMMAND ----------

SELECT
    employee_id,
    department_id,
    position,
    salary,
    tenure_months,
    LEAD(salary) OVER (PARTITION BY department_id ORDER BY tenure_months) AS upcoming_row_salary
FROM
    employee_data;

-- COMMAND ----------

SELECT
    employee_id,
    department_id,
    position,
    salary,
    tenure_months,
    LAG(salary) OVER (PARTITION BY department_id ORDER BY tenure_months) AS previous_row_salary
FROM
    employee_data;

-- COMMAND ----------

SELECT
    employee_id,
    department_id,
    position,
    salary,
    tenure_months,
    FIRST_VALUE(salary) OVER (PARTITION BY department_id ORDER BY tenure_months) AS first_salary_value
FROM
    employee_data;

-- COMMAND ----------

SELECT
    employee_id,
    department_id,
    position,
    salary,
    tenure_months,
    LAST_VALUE(salary) OVER (PARTITION BY department_id ORDER BY tenure_months) AS last_salary_value
FROM
    employee_data;

-- COMMAND ----------


