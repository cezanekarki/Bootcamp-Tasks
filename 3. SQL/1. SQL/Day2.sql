-- Databricks notebook source

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


SELECT count(*) AS total_data FROM training_data;

-- COMMAND ----------

SELECT count(*) AS trainer_data FROM training_data;

-- COMMAND ----------

SELECT count(DISTINCT(trainer_name)) AS unique_trainer_data FROM training_data;

-- COMMAND ----------

select sum(training_session_duration_hours), avg(training_session_duration_hours), min(training_session_duration_hours), max(training_session_duration_hours) from training_data

-- COMMAND ----------

select trainer_name, count(*), sum(training_session_duration_hours), avg(training_session_duration_hours) from training_data
group by trainer_name;

-- COMMAND ----------

select trainer_name from training_data
where training_session_duration_hours = (select max(training_session_duration_hours) from training_data)

-- COMMAND ----------

select trainer_name from training_data
group by trainer_name
having avg(training_session_duration_hours) > 2;


-- COMMAND ----------

select trainer_name from training_data
group by trainer_name
having count(trainer_name) > 3;

-- COMMAND ----------


