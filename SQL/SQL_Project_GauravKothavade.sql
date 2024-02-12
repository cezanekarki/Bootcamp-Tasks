-- Databricks notebook source
create or replace table nova_target_table as
select * from detail4__csv_new
natural join
header1_json_new
natural join
contactinfo1_txt_new
natural join
address1_xlsx_new

-- COMMAND ----------

select *,
case
    when gender = 'M' then 'Mr.'
    when gender = 'F' and marital_status is null or marital_status = 'Single' then 'Miss'
    when gender = 'F' and marital_status in ('Married','Widowed') then 'Mrs.'
    when gender = 'F' and marital_status = 'Divorced' then 'Ms.'
    when gender is null and marital_status = 'Married' then Null
    else Null
end as prefix_name,
case
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
        ELSE null
      END as suffix_name,
      coalesce(prefix_name,'') || ' ' ||first_name||' '||
      coalesce(middle_name,'') || ' ' ||last_name||' '||
      coalesce(suffix_name,'') as name,
      'nova healthcare' as record_source,
      current_timestamp() as record_created_ts,
      true as is_verified,
      ARRAY_AGG(STRUCT(
      a.address_type,
      a.address_line_1,
      a.address_line_2,
      a.city,
      a.state,
      (case when len(zipcode) > 5 then left(zipcode,5)
            when len(zipcode) = 5 then zipcode end) as zip,
      (case when len(zipcode) > 5 then right(zipcode,4)
            when len(zipcode) = 4 then zipcode end) as post,
      'United States' as country )) AS addresses,
      ARRAY_AGG(DISTINCT STRUCT( usage_type, phone)) AS phones,
    case
        WHEN regexp_like(email,'.*@*\.*') THEN email
        ELSE 'Invalid mail'
        END AS email,
    false as privacy_preference,
 
    case
          WHEN len(ssn) = 11 and regexp_like(ssn,'...-..-...') THEN ssn
          WHEN len(ssn)=9 then substr(ssn,1,3)||'-'||substr(ssn,4,2)||'-'||substr(ssn,6,4),
          ELSE null end as national_id,
    YEAR(date_of_birth) AS year_of_birth,
    case
    WHEN deceased_date is NULL THEN false
    ELSE true
    END as deceased_ind,
    YEAR(to_date(deceased_date,'M/d/yyyy')) - year(date_of_birth) as deceased_age,
    to_date(deceased_date,'M/d/yyyy') as deceased_date,
    array(coalesce(split(spoken_langauge_1 || "," || spoken_language_2 , ","),split(coalesce(spoken_language_1,spoken_language_2),","))) as languages,
    array(struct(company as employer_name,
                  job_role as employee_role,
                  case WHEN deceased_date is not null THEN 'Inactive' ELSE 'Active' END as employee_status,
                  case WHEN job_hiredate > date_of_birth THEN job_hiredate ELSE null END as employee_hiredate)) as employment,
    array(struct(relationship,religion)) as additional_source_value
from nova_target_table;
