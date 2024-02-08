-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Overview
-- MAGIC
-- MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
-- MAGIC
-- MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define a list of sheet names
-- MAGIC sheet_names = ['Header', 'Detail', 'ContactInfo', 'Address']
-- MAGIC file_location = "/FileStore/tables/Project_1.xlsx"
-- MAGIC  
-- MAGIC  
-- MAGIC # Create an empty dictionary to store DataFrames
-- MAGIC dfs = {}
-- MAGIC  
-- MAGIC # Loop through each sheet name and read data into DataFrame
-- MAGIC for sheet_name in sheet_names:
-- MAGIC     df = spark.read.format("com.crealytics.spark.excel") \
-- MAGIC                .option("inferschema", True) \
-- MAGIC                .option("header", True) \
-- MAGIC                .option("dataAddress", f"{sheet_name}!") \
-- MAGIC                .option("sheetName", sheet_name) \
-- MAGIC                .load(file_location)
-- MAGIC     dfs[sheet_name] = df
-- MAGIC  
-- MAGIC # Display or further process the data as needed
-- MAGIC for sheet_name, df in dfs.items():
-- MAGIC     print(f"Sheet Name: {sheet_name}")
-- MAGIC     display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TO VIEW ALL THE SHEETS
-- MAGIC
-- MAGIC for sheet_name, df in dfs.items():
-- MAGIC     # Replace special characters with underscores
-- MAGIC     view_name = sheet_name.replace(" ", "_").replace("-", "_")
-- MAGIC     df.createOrReplaceTempView(view_name)
-- MAGIC  
-- MAGIC # Display contents of temporary views
-- MAGIC for sheet_name in sheet_names:
-- MAGIC     # Replace special characters with underscores
-- MAGIC     view_name = sheet_name.replace(" ", "_").replace("-", "_")
-- MAGIC     # print(f"Viewing contents of temporary table: {view_name}")
-- MAGIC     # spark.sql(f"SELECT * FROM {view_name}").show()

-- COMMAND ----------


CREATE TABLE BootcampNova(
    source_id STRING,
    subscriber_id STRING,
    first_name STRING,
    middle_name STRING,
    last_name STRING,
    prefix_name STRING,
    suffix_name STRING,
    name STRING,
    record_source STRING,
    record_created_ts TIMESTAMP,
    is_verified BOOLEAN,
    addresses ARRAY<STRUCT<
        address_type: STRING,
        address_line_1: STRING,
        address_line_2: STRING,
        city: STRING,
        state_province: STRING,
        postal_code: STRING,
        zip_code_extension: STRING,
        country: STRING
    >>,
    phones ARRAY<STRUCT<
        phone_type: STRING,
        number: STRING
    >>,
    email STRING,
    privacy_preference BOOLEAN,
    national_id STRING,
    gender STRING,
    maritial_status STRING,
    date_of_birth DATE,
    year_of_birth STRING,
    deceased_ind BOOLEAN,
    deceased_age STRING,
    deceased_date DATE,
    languages ARRAY<STRING>,
    employment STRUCT<
        employer_name: STRING,
        employee_role: STRING,
        employee_status: STRING,
        employee_hiredate: DATE
    >,
    additional_source_value MAP<STRING, STRING>
);

-- COMMAND ----------

INSERT INTO BootcampNova
SELECT
  source_id,
  subscriber_id,
  first_name,
  middle_name,
  last_name,
  prefix_name,
  suffix_name,
   CONCAT_WS(' ', first_name, middle_name, last_name) AS name,
  record_source,
  record_created_ts,
  is_verified,
  addresses,
  phones,
  email,
  privacy_preference,
  national_id,
  gender,
  marital_status,
  date_of_birth,
  year_of_birth,
  deceased_ind,
  deceased_age,
  deceased_date,
  languages,
  employment,
  additional_source_value
FROM (
  SELECT
    header.id AS source_id,
    header.insurer_id AS subscriber_id,
    detail.first_name AS first_name,
    detail.middle_name AS middle_name,
    detail.last_name AS last_name,
    CASE
      WHEN detail.gender = 'F' AND detail.marital_status IN ('Widowed', 'Divorced', 'Single') THEN 'Ms.'
      WHEN detail.gender = 'F' AND detail.marital_status = 'Married' THEN 'Mrs.'
      WHEN detail.gender = 'M' THEN 'Mr.'
      ELSE NULL
    END AS prefix_name,
  CASE
  WHEN detail.job_role IS NOT NULL AND POSITION(' ' IN detail.job_role) > 0 THEN
    CONCAT(
      SUBSTRING(detail.job_role, 1, 1),
      SUBSTRING(detail.job_role, POSITION(' ' IN detail.job_role) + 1, 1)
    )
 
  WHEN job_role LIKE '%Engineer%' THEN 'Er'
  WHEN job_role LIKE '%Nurse%' THEN 'RN'
  WHEN job_role LIKE '%Analyst%' THEN 'Analyst'
  WHEN job_role LIKE '%Manager%' THEN 'Mgr'
  WHEN job_role LIKE '%Administrator%' THEN 'Admin'
  WHEN job_role LIKE '%Developer%' THEN 'Dev'
  WHEN job_role LIKE '%Assistant%' THEN 'Asst'
  WHEN job_role LIKE '%Technician%' THEN 'Tech'
  WHEN job_role LIKE '%Account%' THEN 'Acct'
  WHEN job_role LIKE '%Biostatistician%' THEN 'BioStat'
  WHEN job_role LIKE '%Health Coach%' THEN 'HlthCoach'
  WHEN job_role LIKE '%Designer%' THEN 'Designer'
  WHEN job_role LIKE '%Statistician%' THEN 'Stat'
  WHEN job_role LIKE '%Programmer%' THEN 'Prog'
  WHEN job_role LIKE '%Professor%' THEN 'Prof'
  WHEN job_role LIKE '%Recruiter%' THEN 'RC'
  WHEN job_role LIKE '%Operator%' THEN 'OP'
  WHEN job_role LIKE '%Librarian%' THEN 'Lib'
  WHEN job_role LIKE '%Coordinator%' THEN 'Coord'
  WHEN job_role LIKE '%Automation Specialist%' THEN 'AutoSpec'
  WHEN job_role LIKE '%VP%' THEN 'VP'
  WHEN job_role LIKE '%Geologist%' THEN 'Geol'
  ELSE NULL
END AS suffix_name,

    CONCAT(detail.first_name, ' ', detail.middle_name, ' ', detail.last_name) AS name,
    'NOVA' AS record_source,
    CURRENT_TIMESTAMP() AS record_created_ts,
    true AS is_verified,
    ARRAY_AGG(STRUCT(
      address.address_type AS address_type,
      address.address_line_1 AS address_line_1,
      address.address_line_2 AS address_line_2,
      address.city AS city,
      address.state AS state_province,
      CASE
        WHEN LOCATE('-', address.zipcode) > 0 THEN SUBSTRING_INDEX(address.zipcode, '-', -1)
        WHEN LENGTH(address.zipcode) = 4 THEN address.zipcode
        ELSE NULL
      END AS postal_code,
      CASE
        WHEN LOCATE('-', address.zipcode) > 0 THEN SUBSTRING_INDEX(address.zipcode, '-', 1)
        WHEN LENGTH(address.zipcode) = 5 THEN address.zipcode
        ELSE NULL
      END AS zip_code_extension,
      'US' AS country
    )) OVER (PARTITION BY address.id) AS addresses,
    ARRAY_AGG(STRUCT(
      contactinfo.usage_type AS phone_type,
      CASE
        WHEN LENGTH(REGEXP_REPLACE(contactinfo.phone, '[^0-9]', '')) = 10 THEN contactinfo.phone
        ELSE NULL
      END AS number
    )) OVER (PARTITION BY contactinfo.id) AS phones,
    CASE
      WHEN LOCATE('@', detail.email) > 0 AND LOCATE('.', detail.email) > 0 THEN detail.email
      ELSE NULL
    END AS email,
    true AS privacy_preference,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(detail.ssn, '[^0-9]', '')) = 9 THEN REGEXP_REPLACE(detail.ssn, '[^0-9]', '')
      ELSE NULL
    END AS national_id,
    detail.gender AS gender,
    detail.marital_status AS marital_status,
    DATE(detail.date_of_birth) AS date_of_birth,
    YEAR(detail.date_of_birth) AS year_of_birth,
    CASE
      WHEN detail.deceased_date IS NULL THEN false
      ELSE true
    END AS deceased_ind,
    CASE
      WHEN detail.deceased_date IS NOT NULL THEN
        CASE
          WHEN YEAR(to_date(detail.deceased_date, 'M/d/yy')) - YEAR(detail.date_of_birth) > 122 THEN
            YEAR(to_date(detail.deceased_date, 'M/d/yy')) - YEAR(detail.date_of_birth) - 100
          ELSE
            YEAR(to_date(detail.deceased_date, 'M/d/yy')) - YEAR(detail.date_of_birth)
        END
      ELSE NULL
    END AS deceased_age,
    to_date(detail.deceased_date, 'M/d/yy') AS deceased_date,
    ARRAY(detail.spoken_language_1, detail.spoken_language_2) AS languages,
  STRUCT(
  detail.company AS employer_name,
  detail.job_role AS employee_role,
  NULL AS employee_status,
  detail.job_hiredate AS employee_hiredate
) AS employment,

    map('relationship', header.relationship, 'religion', detail.religion) AS additional_source_value,
    ROW_NUMBER() OVER (PARTITION BY header.id ORDER BY detail.id) AS row_num
  FROM header
  LEFT JOIN detail ON header.id = detail.id
  LEFT JOIN contactinfo ON header.id = contactinfo.id
  LEFT JOIN address ON header.id = address.id
) temp
WHERE row_num = 1
ORDER BY first_name ASC;

-- COMMAND ----------

select * from bootcampnova

-- COMMAND ----------


