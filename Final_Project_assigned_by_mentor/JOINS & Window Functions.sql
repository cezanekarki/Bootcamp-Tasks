-- Databricks notebook source
CREATE Or Replace TABLE candidates (
    candidate_id INT,
    first_name VARCHAR(50),
    middle_name VARCHAR(50),
    last_name VARCHAR(50)
);

INSERT INTO candidates (candidate_id, first_name, middle_name, last_name)
VALUES
    (1, 'Francis', 'Hope', 'Christiansen'),
    (1 , 'Clinton', 'Theodore', 'Jast'),
    (1, 'Leon', 'Karsyn', 'Hoeger'),
    (3, 'Mario', 'Walter', 'Kovacek'),
    (4, 'Leland', 'Joseph', 'Jacobi'),
    (5, 'Beatrice', 'Rory', 'Mosciski'),
    (6, 'Eunice', '', 'Schulist'),
    (7, 'Bonnie', 'Xena', 'Hayes'),
    (8, 'Luz', 'Eyre', 'Thiel'),
    (8, 'Sheila', 'Helen', 'Baumbach'),
    (9, 'Stephen', 'Jacob', 'Hintz'),
    (10, '', 'Anise', 'Bauch'),
    (NULL, 'Noah', 'Jackson', 'Moore'),
    (NULL, 'Angela', 'Raven', 'Hegmann'),
    (10, 'Thomas', 'Gabriel', 'Barton');




-- COMMAND ----------

select * from candidates

-- COMMAND ----------

CREATE Or Replace TABLE Addresses (
    candidate_id INT,
    address VARCHAR(100),
    city VARCHAR(50)
);

INSERT INTO Addresses (candidate_id, address, city)
VALUES
    (1, '393 Marc Route', 'Goldnerfort'),
    (1, '2947 Mariana Unions', 'Lake Royalboro'),
    (2, '384 Graciela Island', 'West Kristin'),
    (2, '645 Dewitt Heights', 'Denesikview'),
    (3, '386 Coty Junctions', 'Bartellbury'),
    (6, '54455 Leone Spurs', 'West Lerabury'),
    (7, '24078 Pouros Fork', 'Christport'),
    (8, '3690 Ewald Passage', 'Tyreekchester'),
    (9, '489 Mable Gardens', 'New Tina'),
    (10, '751 Jenkins Green', 'Mount Prospect'),
    (11, '6729 Waelchi Spring', 'Lake Kyleighworth'),
    (12, '1335 Priscilla Station', 'Grand Prairie'),
    (13, '12549 Kacey Road', 'East Eunastad'),
    (NULL , '189 Willard Skyway', 'South Yolanda'),
    (NULL, '4094 Lindsay Flats', 'South Abbeyborough');


-- COMMAND ----------

select * from addresses

-- COMMAND ----------

-- DBTITLE 1,Inner Join
select * from candidates c inner join addresses a on c.candidate_id = a.candidate_id


--Inner Join = returns only the matching rows from both tables based on specified condition 

-- COMMAND ----------

-- DBTITLE 1,Left Join
select * from candidates c  left join addresses a on c.candidate_id = a.candidate_id


--Left Join = Inner Join + fetching any additional records from left table which are not present in right table 

-- COMMAND ----------

-- DBTITLE 1,Right Join
select * from candidates c  right join addresses a on c.candidate_id = a.candidate_id
--Left Join = Inner Join + fetching any additional records from right table which are not present in left table 

-- COMMAND ----------

-- DBTITLE 1,Full Join
select * from candidates c  full join addresses a on c.candidate_id = a.candidate_id
--Full Join = Inner Join + fetching any additional records from left table which are not present in right table 
--                         + fetching any additional records from right table which are not present in left table

-- in short all matching and non matching records from both the tables

-- COMMAND ----------

-- DBTITLE 1,Cross Join
select * from candidates cross join addresses

--Cross Join = combines each record from one table with each record from another table

-- COMMAND ----------

-- DBTITLE 1,Window Functions
-- ROW_NUMBER(): Assigns a unique sequential integer to each row within the partition of a result set.
SELECT *,
       ROW_NUMBER() OVER (ORDER BY candidate_id) AS row_num                                                           --PARTITION BY candidate_id 
FROM candidates;


-- COMMAND ----------

-- RANK(): Assigns a unique rank to each distinct row within the partition of a result set.
-- Rows having same value are assigned same rank. For the next rank after two same rank values, one rank value will be skipped. 
SELECT *,
       RANK() OVER (ORDER BY candidate_id ) AS rank
FROM addresses;


-- COMMAND ----------

-- DENSE_RANK(): Assigns a unique rank to each distinct row within the partition of a result set, but the ranks are consecutive without gaps.
-- for the next rank after two same rank, consecutive integer is used, no rank is skipped.
SELECT *,
       DENSE_RANK() OVER (ORDER BY candidate_id) AS dense_rank
FROM candidates;


-- COMMAND ----------

-- NTILE(n): Divides the result set into a specified number of equally-sized buckets or partitions.
SELECT *,
       NTILE(5) OVER (ORDER BY candidate_id) AS ntile
FROM candidates;

-- COMMAND ----------

-- LAG(column, offset): Accesses data from a previous row in the result set, relative to the current row.
SELECT candidate_id,
       first_name,
       LAG(candidate_id, 2) OVER (ORDER BY candidate_id) AS prev_candidate_id
FROM candidates;


-- COMMAND ----------

-- LEAD(column, offset): Accesses data from a following row in the result set, relative to the current row.
SELECT candidate_id,
       first_name,
       LEAD(candidate_id, 1) OVER (ORDER BY candidate_id) AS next_candidate_id
FROM candidates;


-- COMMAND ----------

-- FIRST_VALUE(column): Returns the value of the specified column from the first row in the window frame.
SELECT candidate_id,
       first_name,
       FIRST_VALUE(candidate_id) OVER (ORDER BY candidate_id) AS first_candidate_id
FROM candidates;


-- COMMAND ----------

-- LAST_VALUE(column): Returns the value of the specified column from the last row in the window frame.           ------ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
SELECT candidate_id,
       first_name,
       LAST_VALUE(candidate_id) OVER (ORDER BY candidate_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_candidate_id
FROM candidates;


-- COMMAND ----------

-- PERCENT_RANK(): percentage of rows in the partition that are less than or equal to the current row's value. The output ranges from 0 to 1, where 0 represents the lowest value and 1 represents the highest value in the partition.
SELECT
    candidate_id,
    first_name,
    middle_name,
    last_name,
    PERCENT_RANK() OVER (ORDER BY candidate_id) AS percent_rank
FROM
    candidates;



-- COMMAND ----------

-- CUME_DIST(): returns the cumulative probability of encountering a value less than or equal to the current row's value
SELECT
    candidate_id,
    first_name,
    middle_name,
    last_name,
    CUME_DIST() OVER (ORDER BY candidate_id) AS cumulative_distribution
FROM
    candidates;
