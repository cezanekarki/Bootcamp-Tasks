with temp_add as(
    SELECT id, address_type, address_line_1, address_line_2, city, state, CASE
    WHEN POSITION('-' IN zipcode) > 0 THEN SPLIT_PART(zipcode, '-', 1)
    WHEN LENGTH(zipcode) = 5 THEN zipcode
  END AS ZipCode,
  CASE
    WHEN POSITION('-' IN zipcode) > 0 THEN SPLIT_PART(zipcode, '-', 2)
    WHEN LENGTH(zipcode) = 4 THEN zipcode
    ELSE NULL
  END AS PostalCode,
  "USA" as country
FROM
  address_info
),
temp_date as(
    select id, date_of_birth, SUBSTRING(date_of_birth, -4, 4) AS year_of_birth, deceased_date AS deceased_date,
    CASE 
        WHEN deceased_date IS NOT NULL THEN TRUE
        ELSE FALSE 
    END AS deceased_ind,
    CASE 
        WHEN deceased_date IS NOT NULL AND date_of_birth IS NOT NULL 
        THEN CAST(SUBSTRING(deceased_date, -4, 4) AS INT) - CAST(SUBSTRING(date_of_birth, -4, 4) AS INT)
        ELSE NULL 
    END AS deceased_age
    from detail_info
)
insert into final_result3
SELECT 
    d.id AS source_id, 
    h.insurer_id AS subscribe_id, 
    d.first_name AS first_name, 
    d.middle_name AS middle_name, 
    d.last_name AS last_name, 
    CASE 
    WHEN (d.gender = "F" and (d.marital_status = "Married" or d.marital_status = "Widowed")) THEN "Mrs."
    WHEN d.gender = "F" and d.marital_status = "Single" THEN "Miss"
    WHEN d.gender = "M" THEN "Mr."
    END as prefix,

    case 
    WHEN d.job_role like "%Nurse%" THEN "RN"
    when d.job_role like "%Doctor%" then "Dr."
    when d.job_role like "%Professor%" then "Prof."
    when d.job_role like "%VP%" then "VP"
    when d.job_role = "Clinical Specialist" then "CS"
    END as suffix, 
    Case 
    when d.middle_name is null then Concat(d.first_name,' ',d.last_name)
    else
    CONCAT(d.first_name,' ',d.middle_name,' ',d.last_name) end AS name,
    'Nova Health' AS record_source,
    CURRENT_TIMESTAMP AS recorded_ts,
    CASE 
        WHEN d.email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN TRUE
        when d.deceased_date < current_date() THEN True
        ELSE FALSE 
    END AS is_verified,
    ARRAY_AGG(
        STRUCT(
            t.address_type, 
            t.address_line_1, 
            t.address_line_2, 
            t.city, 
            t.state, 
            t.ZipCode,
            t.PostalCode,
            t.country
        )
    ) AS Address,
    ARRAY_AGG(STRUCT(c.phone,c.usage_type)) AS phones,
    case
        when d.email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN d.email
        else
            "Invalid Email"
    end AS email,
    FALSE as privacy_preference,
    d.ssn AS national_id,
    d.gender AS gender, 
    d.marital_status AS marital_status,
    td.date_of_birth AS date_of_birth,
    td.year_of_birth as year_of_birth, 
    td.deceased_ind as deceased_ind,
    td.deceased_age as deceased_age,
    td.deceased_date AS deceased_date,
    struct(d.spoken_language_1, d.spoken_language_2) as Languages,
    Struct(
        d.first_name, 
        d.job_role, 
        CASE WHEN d.job_hiredate IS NULL THEN 'Inactive' ELSE 'Active' END as Employment_status, 
        d.job_hiredate
    ) AS employment,
     MAP('relationship', h.relationship) AS additional_source_value
FROM header_info AS h
LEFT JOIN detail_info AS d ON h.id = d.id
LEFT JOIN contact_info AS c ON d.id = c.id
LEFT JOIN temp_add as t on d.id = t.id
left join temp_date as td on h.id = td.id
GROUP BY 
    d.id, h.insurer_id, d.first_name, d.middle_name, d.last_name, d.ssn, d.gender, td.date_of_birth, td.year_of_birth, 
	d.spoken_language_1, d.spoken_language_2, d.job_role, d.email, d.marital_status, d.deceased_date, td.deceased_date,
	td.deceased_ind, td.deceased_age, d.job_hiredate, d.company, d.religion, h.relationship;