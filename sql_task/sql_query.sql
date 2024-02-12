-- Databricks notebook source
CREATE Table DemoTable (
  id INTEGER,
  firstName VARCHAR(255),
  lastName VARCHAR(255),
  gender VARCHAR(100),
  dob DATE
);

-- COMMAND ----------

INSERT INTO DemoTable
SELECT 5, "kshitiz", "Ulak", "Male", "1996-10-1" union all SELECT 6, "Mira", "Ulak", "Female", "1996-10-1"

-- COMMAND ----------

INSERT INTO DemoTable VALUES
(1, "Ammy", "Shrestha", "Male", "1997-09-25"),
(2, "Junu", "Shrestha", "Female", "1998-10-26")

-- COMMAND ----------

INSERT INTO DemoTable VALUES
(3, "sejal", "shrestha", "Male", "1997-09-25"),
(4, "asta", "thapa", "Female", "1998-10-26")

-- COMMAND ----------

Select * From DemoTable

-- COMMAND ----------

UPDATE DemoTable
SET dob = 
  CASE 
    WHEN id = 1 THEN '2000-10-25'
    WHEN id = 2 THEN '2000-10-25'
  END
WHERE id IN (1, 2);

-- COMMAND ----------

Select * From DemoTable

-- COMMAND ----------

SELECT dob lastName,
CASE
    WHEN lastName = 'Ulak' THEN 'Hello'
    WHEN lastName = 'Shrestha' THEN 'Hi'
    ELSE 'Nice day'
END AS Description
FROM DemoTable;

-- COMMAND ----------

UPDATE DemoTable
SET dob = 
  CASE 
    WHEN id = 1 THEN '2000-10-25'
    WHEN id = 2 THEN '2000-10-25'
    ELSE '1966-10-10'
  END
-- WHERE id IN (1, 2);

-- COMMAND ----------

-- Agregate functions

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

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/training_data

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE training_data;

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

select count(distinct trainer_name) from training_data;

-- COMMAND ----------

select sum( training_session_duration_hours), avg(training_session_duration_hours), max(training_session_duration_hours), min(training_session_duration_hours) from training_data;

-- COMMAND ----------

select trainer_name, count(*) as total from training_data group by trainer_name;

-- COMMAND ----------

select trainer_name, count(*) as total, sum(training_session_duration_hours) as totalSessionHours from training_data group by trainer_name;

-- COMMAND ----------

select trainer_name, participant_id, count(*) as total, sum(training_session_duration_hours) as totalSessionHours from training_data group by 1,2;

-- COMMAND ----------



-- select trainer_name, max(training_session_duration_hours) from training_data 
-- where max(training_session_duration_hours)
-- group by trainer_name ;

select trainer_name from training_data where training_session_duration_hours=(select max(training_session_duration_hours) from training_data) group by trainer_name;


-- COMMAND ----------


select trainer_name from training_data group by trainer_name having avg(training_session_duration_hours)>2;

-- COMMAND ----------

select trainer_name, avg(training_session_duration_hours) as avg from training_data group by trainer_name where avg(training_session_duration_hours)>2;

-- COMMAND ----------

select trainer_name, count(*) as total from training_data group by trainer_name having count(*)>3 

-- COMMAND ----------

select * from training_data

-- COMMAND ----------

-- subquery

-- COMMAND ----------

select trainer_name, count(*) as total from training_data group by trainer_name order by trainer_name;

-- COMMAND ----------

select participant_name from training_data where training_session_duration_hours > (select avg(training_session_duration_hours) from training_data)

-- COMMAND ----------

select * from training_data

-- COMMAND ----------

INSERT INTO training_data (participant_id, participant_name, training_session_date, training_session_duration_hours, trainer_name, training_location, training_subject)
VALUES
    (17, 'Erza', '2024-02-05', 2.5, 'Trainer A', 'Room 101', 'Introduction to Programming');

-- COMMAND ----------

-- finding session in same location by multiple 
SELECT DISTINCT training_location FROM training_data as main WHERE EXISTS (
  SELECT 1 FROM training_data as sub
  WHERE sub.training_location = main.training_location
  AND sub.trainer_name <> main.trainer_name
);


-- COMMAND ----------

 -- find participants sessions by trainne A

-- Find participants attending sessions by trainer A
SELECT DISTINCT participant_name
FROM training_data AS main
WHERE EXISTS (
  SELECT 1
  FROM training_data AS sub
where sub.trainer_name = 'Trainer A'
    AND main.participant_name = sub.participant_name
);

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

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/employee_data

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

SELECT * from employee_data;

-- COMMAND ----------

SELECT employee_id, department_id, position, salary, tenure_months,
  row_number() OVER (PARTITION BY department_id ORDER BY salary DESC) AS row_num FROM employee_data;

-- COMMAND ----------

SELECT employee_id, department_id, position, salary, tenure_months, dense_rank() OVER (PARTITION BY department_id ORDER BY salary DESC) as dense_rank_num from employee_data;

-- COMMAND ----------

-- COMMAND ----------

select *from employee_data;

-- COMMAND ----------

SELECT employee_id, department_id, position, salary, tenure_months, sum(salary) OVER (PARTITION BY department_id ORDER BY tenure_months) AS running_salary_total from employee_data;

-- COMMAND ----------

SELECT employee_id, department_id, position, salary, tenure_months, AVG(salary) OVER (PARTITION BY department_id ORDER BY tenure_months) AS avg_salary FROM employee_data

-- COMMAND ----------

select employee_id, department_id, position, salary, tenure_months, lag(salary) over (partition by department_id order by tenure_months) as prev_tenure_salaary from employee_data

-- COMMAND ----------

SELECT employee_id, department_id, position, salary, tenure_months, first_value(salary) OVER (PARTITION BY department_id ORDER BY tenure_months) as first_tenure_salary FROM employee_data; 

-- COMMAND ----------

-- CTE

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

-- List high salary employess having salary > 60000

WITH cte_name AS (
    SELECT employee_id, employee_name ,department, 
  salary 
    FROM employees
    WHERE salary>60000
)
-- Main query using the CTE
SELECT *
FROM cte_name;

-- COMMAND ----------

-- retrieve  information about employees in IT department


-- COMMAND ----------

--  identify top 3 earners of the company
-- WITH cte_name AS (
--     SELECT employee_id, employee_name ,department, max(salary) as highest,
--   salary 
--     FROM employees GROUP BY employee_id, employee_name ,department, salary
-- )
-- SELECT *
-- FROM cte_name WHERE highest <3;



-- COMMAND ----------


-- WITH cte_name AS ()

SELECT
   department,
    AVG(salary) OVER (PARTITION BY department) AS avg_per_group
FROM
    employees GROUP BY department, salary;

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

WITH dept_avg AS (
  SELECT department,
  AVG(salary) OVER (PARTITION BY department) as avg_salary
  FROM employees
)
 
SELECT DISTINCT *  FROM dept_avg;
