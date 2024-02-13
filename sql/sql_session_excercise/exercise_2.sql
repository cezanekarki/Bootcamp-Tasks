CREATE or replace TABLE employee_data (
    employee_id INT ,
    department_id INT,
    position VARCHAR(50),
    salary INT,
    tenure_months INT,
    job_experience_years INT
);

INSERT INTO employee_data VALUES
(1, 101, 'Software Engineer', 90000, 24, 2),
(2, 102, 'Data Analyst', 60000, 18, 1),
(3, 101, 'Project Manager', 90000, 36, 5),
(4, 103, 'UX Designer', 75000, 27, 3),
(5, 102, 'Business Analyst', 70000, 22, 2),
(6, 101, 'Software Engineer', 80000, 30, 4),
(7, 103, 'Product Manager', 95000, 42, 7),
(8, 102, 'Data Scientist', 100000, 48, 8);

--Some Window functions
SELECT employee_id, department_id, position, salary, job_experience_years, 
    RANK() over (PARTITION BY department_id ORDER BY salary DESC) as rank_skips, 
    dense_rank() over (PARTITION BY department_id ORDER BY salary DESC) as dense_rANK_dont_skips 
FROM employee_data


--Aggregate Functions with window
SELECT employee_id, department_id, position, salary, job_experience_years, 
    SUM(salary) over (PARTITION BY department_id ORDER BY tenure_months) as running_salary F
ROM employee_datas;

select employee_id,department_id,position,salary,tenure_months, 
    avg(salary) over(partition by department_id order by tenure_months) as average_salary
from employee_data;


select employee_id,department_id,position,salary,tenure_months, 
   first_value(salary) over(partition by department_id order by department_id) as average_salary
from employee_data;

--Lead and lag function
SELECT employee_id, department_id, position, salary, job_experience_years, 
    LEAD(salary) over (PARTITION BY department_id ORDER BY employee_id) as next_salary 
FROM employee_datas;

SELECT employee_id, department_id, position, salary, job_experience_years, 
    LAG(salary) over (PARTITION BY department_id ORDER BY employee_id) as LAG  
FROM employee_datas;