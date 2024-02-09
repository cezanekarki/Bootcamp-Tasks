WITH name_prefix AS (
  SELECT
    id,
    CASE 
      WHEN gender = 'M' THEN 'Mr.'
      WHEN gender = 'F' AND (marital_status = 'single' OR marital_status IS NULL) THEN 'Miss'
      WHEN gender = 'F' AND marital_status IN ('married', 'widowed', 'divorced') THEN 'Mrs.'
      WHEN gender IS NULL AND (marital_status IS NULL OR marital_status IN ('divorced', 'single', 'widowed', 'married')) THEN 'Mx.'
      ELSE 'not known'
    END AS prefix
  FROM Detail
),
emails_verified_CTE AS (
  SELECT
    id,
    email,
    CASE 
      WHEN CHARINDEX('@', email) > 0 
           AND CHARINDEX('.', email, CHARINDEX('@', email)) > CHARINDEX('@', email) THEN TRUE
      ELSE FALSE
    END AS is_verified
  FROM Detail
),


address_data AS (
  SELECT
    id,
    COLLECT_LIST(STRUCT(
      address_type,
      address_line_1,
      address_line_2,
      city,
      state AS state_province,
      LEFT(zipcode, 5) AS postal_code,
      SUBSTRING(zipcode FROM 6) AS zip_code_extension,
      'United States' AS country
    )) AS addresses
  FROM address
  GROUP BY id
),


phone_data AS (
  SELECT
    id,
    FIRST(STRUCT(phone, usage_type)) AS phones
  FROM contactinfo
  GROUP BY id
)

INSERT INTO member
SELECT 
  h.id AS source_id,
  h.insurer_id AS subscriber_id,
  d.first_name,
  d.middle_name,
  d.last_name,
  pn.prefix AS prefix_name,
  CASE 
    WHEN D.job_role LIKE "%Nurse%" then "RN"
    WHEN D.job_role LIKE "%Pharmacist%" THEN "Pharm.D."
    WHEN D.job_role LIKE "%Developer%" THEN "Dev"
    When D.job_role LIKE "%Manager%" THEN "Mgr"
    WHEN D.job_role LIKE "% Engineer%" THEN "Er"
    ELSE ""
  END AS suffix_name,

  d.first_name || ' ' || d.last_name || COALESCE(' ' || d.middle_name, '') AS name,

  'Nova Health Insurance' AS record_source,
  CURRENT_TIMESTAMP() AS record_created_ts,
  ve.is_verified,
  ad.addresses,
  pd.phones,
  d.email,
  FALSE AS privacy_preference,
  d.ssn AS national_id,
  d.gender AS gender,
  d.marital_status AS marital_status,
  TO_DATE(d.date_of_birth, 'M/d/yyyy') as date_of_birth, 
  YEAR(TO_DATE(D.date_of_birth, 'M/d/yyyy')) as year_of_birth,
  CASE 
        WHEN deceased_date IS NOT NULL THEN TRUE
        ELSE FALSE 
  END AS deceased_ind,
  YEAR(TO_DATE(d.deceased_date, 'M/d/yyyy')) - YEAR(TO_DATE(d.date_of_birth, 'M/d/yyyy')) AS deceased_age,
  TO_DATE(d.deceased_date, 'M/d/yyyy') AS deceased_date,

  ARRAY(d.spoken_language_1, d.spoken_language_2) AS languages,

  STRUCT(
    d.first_name || ' ' || d.last_name || ' ' || d.middle_name AS employee_name,
    d.job_role,
    CASE 
      WHEN d.job_hiredate IS NULL THEN 'inactive' 
      ELSE 'active' 
    END AS employee_status,
    TO_DATE(d.job_hiredate, 'M/d/yyyy') AS employee_hiredate
  ) AS employment
FROM header h
LEFT JOIN Detail d ON h.id = d.id
LEFT JOIN name_prefix pn ON h.id = pn.id
LEFT JOIN emails_verified_CTE ve ON d.id = ve.id
LEFT JOIN address_data ad ON h.id = ad.id
LEFT JOIN phone_data pd ON h.id = pd.id
GROUP BY ALL