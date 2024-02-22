-- Databricks notebook source
SELECT 
    h.id as source_id,
    h.insurer_id as subscriber_id,
    d.first_name,
    d.middle_name,
    d.last_name,
CASE 
    WHEN d.gender = 'M' THEN 'Mr.'
    WHEN d.gender = 'F' AND d.marital_status IN ('Widowed', 'Married') THEN 'Mrs.' 
    WHEN d.gender = 'F' AND d.marital_status IS NULL THEN 'Miss'
    WHEN d.gender = 'F' AND d.marital_status='Single' THEN 'Miss' 
    WHEN d.gender = 'F' AND d.marital_status='Divorced' THEN 'Ms.' 
    WHEN d.gender IS NULL AND d.marital_status='Married' THEN 'Mrs'
    ELSE NULL 
END AS prefix_name, 

CASE 
    WHEN job_role LIKE '%Engineer%' THEN 'Er' 
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
    WHEN job_role LIKE '%Coordinator%' THEN 'Coord' 
    WHEN job_role LIKE '%Automation Specialist%' THEN 'AutoSpec' 
    WHEN job_role LIKE '%VP%' THEN 'VP' 
    WHEN job_role LIKE '%Geologist%' THEN 'Geol' 
    ELSE 'Other' 
END AS suffix_name, 
 
COALESCE(prefix_name,'') || ' ' || d.first_name || ' ' || COALESCE(d.middle_name, '') || ' ' || d.last_name || ' ' || '('||COALESCE(suffix_name,'')||')' AS name, 
'Nova Healthcare' as record_source,

array_agg(struct(a.issued_date)) as record_created_ts,

current_timestamp() AS record_created_ts,
True as is_verified,

ARRAY_AGG(STRUCT(
      a.address_type,
      a.address_line_1,
      a.address_line_2,
      a.city,
      a.state,
      (CASE WHEN len(zipcode) > 5 THEN left(zipcode,5)
            WHEN len(zipcode) = 5 THEN zipcode END) AS zip,
      (case WHEN len(zipcode) > 5 THEN right(zipcode,4)
            WHEN len(zipcode) = 4 THEN zipcode END) AS post,
      'United States' aS country )) AS addresses,

ARRAY_agg(DISTINCT STRUCT( c.usage_type, c.phone)) AS phones,

CASE 
    WHEN regexp_like(d.email,'.*@*\.*') THEN d.email 
    ELSE 'Invalid mail' 
END AS email,

false as privacy_preference,

CASE
    WHEN LEN(d.ssn) = 11 and regexp_like(d.ssn,'...-..-...') THEN d.ssn
    WHEN LEN(d.ssn)=9 then substr(d.ssn,1,3)||'-'||substr(d.ssn,4,2)||'-'||substr(d.ssn,6,4)
ELSE null end as national_id,

d.gender,
d.marital_status,
d.date_of_birth,

YEAR(d.date_of_birth) AS year_of_birth,

CASE 
    WHEN d.deceased_date IS NULL THEN false 
    ELSE true 
END AS deceased_ind,

YEAR(to_date(d.deceased_date,'M/d/yyyy')) - year(d.date_of_birth) AS deceased_age,
to_date(d.deceased_date,'M/d/yyyy') as deceased_date,

array(coalesce(split(d.spoken_langauge_1 || "," || d.spoken_language_2 , ","),
split(coalesce(d.spoken_language_1,d.spoken_language_2),","))) as languages,

array(struct(company as employer_name,
             job_role as employee_role,
            CASE WHEN d.deceased_date is not null THEN 'Inactive' 
            ELSE 'Active' 
            END AS employee_status,
                  
            CASE WHEN job_hiredate > d.date_of_birth THEN job_hiredate 
            ELSE null END AS employee_hiredate)) as employment,

array(struct(h.relationship,d.religion)) as additional_source_value
 
FROM header AS h 
JOIN details AS d on h.id = d.id
JOIN address as a on d.id = a.id
JOIN contactinfo AS c on  a.id=c.id
group by
h.id,
h.insurer_id,
d.first_name,
d.middle_name,
d.last_name,
d.email,
d.gender,
d.marital_status,
d.date_of_birth,
d.deceased_date,
d.spoken_language_1,
d.spoken_language_2,
d.company,
d.job_role,
d.job_hiredate,
h.relationship,
d.religion,
d.ssn


