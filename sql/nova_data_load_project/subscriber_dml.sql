WITH VerifiedEmailCTE AS (
  SELECT
  D.id,
    CASE
      WHEN D.email LIKE '%@%.%'
        AND LENGTH(D.email) - LENGTH(REPLACE(D.email, '@', '')) = 1
        AND POSITION('@' IN D.email) < POSITION('.' IN D.email)
        AND SPLIT(D.email, '@')[0] RLIKE '^[a-zA-Z0-9._]+$'
        AND SPLIT(D.email, '@')[1] RLIKE '^[a-zA-Z0-9.-]+$'
      THEN true
      ELSE false
    END as is_verified
  from detailsinformation D
)

INSERT INTO subscribers

SELECT 
  H.id as source_id,
  H.insurer_id as subscriber_id,
  D.first_name,
  D.middle_name,
  D.last_name,
  CASE 
  WHEN D.gender="M"  THEN "Mr."-- Mrs. marrid women,Miss for unmarrid women, Mr for both marid and unmarrid men, Mx. for not known gender
  WHEN D.gender ="F" and (D.marital_status = "Single" or D.marital_status is null) THEN "miss"
  WHEN D.gender ="F"  and D.marital_status in ("Married","Widowed","Divorced") THEN "Mrs."
  WHEN D.gender is null THEN "Mx." --and (D.marital_status is null or D.marital_status in ("Divorced","Single","Widowed","Married")) THEN "Mx."
  ELSE "Not Known"
  END
as prefix,
  CASE --192 rows
    WHEN D.job_role LIKE "%Nurse%" then "RN"
    WHEN D.job_role LIKE "%Pharmacist%" THEN "Pharm.D."
    WHEN D.job_role LIKE "%Developer%" THEN "Dev"
    When D.job_role LIKE "%Manager%" THEN "manager"
    WHEN D.job_role LIKE "% Engineer%" THEN "PE"
    WHEN D.job_role like "%Clinical%" THEN "CLS"
    ELSE "Not Know" 
    END as suffix_name,
  COALESCE(D.first_name || ' ' || D.last_name || ' ' || D.middle_name, COALESCE(D.first_name, D.last_name, D.middle_name)) AS name,
  "Nova Health" as record_source,
  CURRENT_TIMESTAMP() as record_created_ts,
  VE.is_verified,
  COLLECT_LIST(STRUCT(
      A.address_type,
      A.address_line_1,
      A.address_line_2,
      A.city,
      A.state,
      "United State of America" as country,
      RIGHT(A.zipcode, 5) as postal_code,
      CASE 
      WHEN CHARINDEX('-', A.zipcode) > 0 THEN RIGHT(A.zipcode, LEN(A.zipcode) - CHARINDEX('-', A.zipcode))
      ELSE "No extension for zip"
    END AS zip_code_extension
  )) as addresses,
  COLLECT_LIST(STRUCT(
      C.phone,
      C.usage_type
  ))[0] as phones,
  CASE 
  WHEN VE.is_verified is true THEN D.email
  ELSE "Invalid email"
  END as email,
  false as privacy_preference,
  D.ssn as national_id,
  D.gender as gender, --COALESCE(D.gender, 'Other') as gender,
  D.religion as religion, --COALESCE(D.religion, 'Other') as religion,
  D.marital_status as marital_status, --COALESCE(D.marital_status, 'Single') as marital_status,
  to_date(D.date_of_birth,'M/d/yyyy') as date_of_birth, --2024-04-11
  year(TO_DATE(D.date_of_birth, 'm/d/yyyy')) as year_of_birth, 
  CASE
  WHEN D.deceased_date is NULL THEN false
  ELSE true
END as deceased_ind,
try_cast(date_diff(TO_DATE(D.deceased_date, 'm/d/yyyy'),TO_DATE(D.date_of_birth, 'm/d/yyyy'))/360 AS INT) as deceased_age,
to_date(D.deceased_date,'M/d/yyyy') as deceased_date,
  COALESCE(SPLIT(D.spoken_language_1 || ',' || D.spoken_language_2, ','), SPLIT(COALESCE(D.spoken_language_1, D.spoken_language_2), ',')) AS languages,
  STRUCT(
    COALESCE(D.first_name || ' ' || D.last_name || ' ' || D.middle_name, COALESCE(D.first_name, D.last_name, D.middle_name)) AS name,
    D.job_role,
    CASE WHEN
    D.job_hiredate IS NULL THEN "Inactive"
    ELSE "Active"
    END as employee_status,
    to_date(D.job_hiredate,'M/d/yyyy') as employee_hiredate
  ) as employment,
  collect_list(
    map("relation", H.relationship,
        "company", D.company,
        "issued_date",A.issued_date
    )
  ) as additional_source_value
FROM headertable H
LEFT JOIN contactinformation C ON H.id = C.id
LEFT JOIN VerifiedEmailCTE VE ON H.id = VE.id
LEFT JOIN address A ON H.id = A.id
LEFT JOIN detailsinformation D ON H.id = D.id
GROUP BY ALL;
