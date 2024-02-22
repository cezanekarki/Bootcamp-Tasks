# Databricks notebook source
# MAGIC %sql
# MAGIC --trails
# MAGIC create or replace table nova
# MAGIC SELECT *
# MAGIC FROM Detail2_csv
# MAGIC INNER JOIN address3_xlsx ON Detail2_csv.id = address3_xlsx.id
# MAGIC INNER JOIN header2_json ON address3_xlsx.id = header2_json.id
# MAGIC INNER JOIN contactinfo3_txt ON header2_json.id = contactinfo3_txt.id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --trials
# MAGIC SELECT *
# MAGIC FROM Detail2_csv
# MAGIC JOIN address3_xlsx ON Detail2_csv.id = address3_xlsx.id
# MAGIC JOIN header2_json ON address3_xlsx.id = header2_json.id
# MAGIC JOIN contactinfo3_txt ON header2_json.id = contactinfo3_txt.id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --trails
# MAGIC create or replace table nova_target_table as select * from detail4_csv
# MAGIC NATURAL JOIN header4_json 
# MAGIC NATURAL JOIN contactinfo4_txt
# MAGIC NATURAL JOIN address4_xlsx 

# COMMAND ----------

# MAGIC %sql
# MAGIC --MAIN QUERY FOR CREATING AND ADDING NEW COLUMNS
# MAGIC CREATE OR REPLACE table nova_target AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     h.id as source_id,
# MAGIC     h.insurer_id as subscriber_id,
# MAGIC     d.first_name,
# MAGIC     d.middle_name,
# MAGIC     d.last_name,
# MAGIC     CASE
# MAGIC         WHEN d.gender = 'M' THEN 'Mr.'
# MAGIC         WHEN d.gender = 'F' AND d.marital_status ='Single' THEN 'Miss'
# MAGIC         WHEN d.gender = 'F' AND d.marital_status IS NULL THEN 'Miss'
# MAGIC         WHEN d.gender = 'F' AND d.marital_status IN ('Widowed', 'Married') THEN 'Mrs.'
# MAGIC         WHEN d.gender = 'F' AND d.marital_status ='Divorced' THEN 'Ms.'
# MAGIC         WHEN d.gender IS NULL AND d.marital_status ='Married' THEN Null
# MAGIC         ELSE NULL
# MAGIC     END AS prefix_name,
# MAGIC     CASE
# MAGIC         WHEN job_role LIKE '%Engineer%' THEN 'Er'
# MAGIC         WHEN job_role LIKE '%Analyst%' THEN 'Analyst'
# MAGIC         WHEN job_role LIKE '%Manager%' THEN 'Mgr'
# MAGIC         WHEN job_role LIKE '%Administrator%' THEN 'Admin'
# MAGIC         WHEN job_role LIKE '%Developer%' THEN 'Dev'
# MAGIC         WHEN job_role LIKE '%Assistant%' THEN 'Asst'
# MAGIC         WHEN job_role LIKE '%Technician%' THEN 'Tech'
# MAGIC         WHEN job_role LIKE '%Account%' THEN 'Acct'
# MAGIC         WHEN job_role LIKE '%Biostatistician%' THEN 'BioStat'
# MAGIC         WHEN job_role LIKE '%Health Coach%' THEN 'HlthCoach'
# MAGIC         WHEN job_role LIKE '%Designer%' THEN 'Designer'
# MAGIC         WHEN job_role LIKE '%Statistician%' THEN 'Stat'
# MAGIC         WHEN job_role LIKE '%Programmer%' THEN 'Prog'
# MAGIC         WHEN job_role LIKE '%Coordinator%' THEN 'Coord'
# MAGIC         WHEN job_role LIKE '%Automation Specialist%' THEN 'AutoSpec'
# MAGIC         WHEN job_role LIKE '%VP%' THEN 'VP'
# MAGIC         WHEN job_role LIKE '%Geologist%' THEN 'Geol'
# MAGIC         ELSE '(Other)'
# MAGIC       END as suffix_name,
# MAGIC      
# MAGIC
# MAGIC
# MAGIC   
# MAGIC      
# MAGIC     COALESCE(prefix_name,'') || ' ' || d.first_name || ' ' || COALESCE(d.middle_name, '') || ' ' || d.last_name || ' ' || COALESCE(suffix_name,'') AS name,
# MAGIC     'Nova Healthcare' as record_source,
# MAGIC     ARRAY_AGG(STRUCT(a.issued_date)) AS record_created_ts,
# MAGIC     True as is_verified,
# MAGIC     ARRAY_AGG(STRUCT(
# MAGIC       a.address_type,
# MAGIC        a.address_line_1,
# MAGIC         a.address_line_2,
# MAGIC          a.city, a.state,
# MAGIC         CASE
# MAGIC             WHEN a.zipcode LIKE '%-%' THEN SUBSTRING(a.zipcode FROM POSITION('-' IN a.zipcode) + 1)
# MAGIC             ELSE NULL
# MAGIC         END AS postal_code,
# MAGIC         SUBSTRING(a.zipcode, 1, CASE WHEN a.zipcode LIKE '%-%' THEN POSITION('-' IN a.zipcode) - 1 ELSE LENGTH(a.zipcode) END) AS zip_code_extension,
# MAGIC         'United States' AS country)) AS addresses,
# MAGIC     ARRAY_AGG(DISTINCT STRUCT(c.usage_type as phone_type, c.phone as p_number)) AS phones,
# MAGIC     d.email as email,
# MAGIC     False as privacy_preference,
# MAGIC     d.ssn as national_id,
# MAGIC     d.gender,
# MAGIC     d.marital_status,
# MAGIC     d.date_of_birth,
# MAGIC     YEAR(d.date_of_birth) AS year_of_birth,
# MAGIC     CASE
# MAGIC     WHEN email REGEXP '^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,4}$' THEN email
# MAGIC     ELSE 'Invalid'
# MAGIC   END AS email_id,
# MAGIC     CASE WHEN d.deceased_date IS NULL THEN false ELSE true END AS deceased_ind,
# MAGIC     YEAR(to_date(d.deceased_date,'M/d/yyyy')) - year(d.date_of_birth) AS deceased_age,
# MAGIC     to_date(d.deceased_date,'M/d/yyyy') as deceased_date,
# MAGIC     ARRAY(d.spoken_language_1, d.spoken_language_2) as languages,
# MAGIC     ARRAY(STRUCT(d.company as employer_name, d.job_role as employee_role,
# MAGIC       case
# MAGIC         when d.deceased_date is not null then 'Inactive'
# MAGIC         else 'Active'
# MAGIC       end as  employee_status,
# MAGIC       d.job_hiredate as employee_hiredate )) as Employeement,
# MAGIC     ARRAY(STRUCT(h.relationship, d.religion))AS additional_source_value
# MAGIC    
# MAGIC   FROM
# MAGIC     header4_json h
# MAGIC LEFT JOIN
# MAGIC     Detail4_csv d ON h.id = d.id
# MAGIC LEFT JOIN
# MAGIC     contactinfo4_txt c ON c.id = h.id
# MAGIC LEFT JOIN
# MAGIC     address4_xlsx a ON a.id = h.id
# MAGIC   GROUP BY
# MAGIC     h.id,
# MAGIC     h.insurer_id,
# MAGIC     d.first_name,
# MAGIC     d.middle_name,
# MAGIC     d.last_name,
# MAGIC     d.ssn,
# MAGIC     d.email,
# MAGIC     d.gender,
# MAGIC     d.marital_status,
# MAGIC     d.date_of_birth,
# MAGIC     d.deceased_date,
# MAGIC     d.spoken_language_1,
# MAGIC     d.spoken_language_2,
# MAGIC     d.company,
# MAGIC      d.job_role,
# MAGIC     d.job_hiredate,
# MAGIC     h.relationship,
# MAGIC     d.religion
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from nova_target;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe nova_target;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC   CASE
# MAGIC     WHEN email REGEXP '^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,4}$' THEN email
# MAGIC     ELSE 'Invalid'
# MAGIC   END AS email_id
# MAGIC FROM nova_target;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe nova_target;

# COMMAND ----------


