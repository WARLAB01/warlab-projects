-- ============================================================================
-- L3 DIMENSION LOAD SCRIPT - SCD2 MERGE LOGIC
-- Schema: l3_workday
-- Source Schema: l1_workday
-- Purpose: Populate all dimension tables with SCD2 support
-- ============================================================================

-- ============================================================================
-- 1. LOAD DIM_DAY_D - Date Spine (SCD1, idempotent)
-- Generates calendar from 2020-01-01 to 2030-12-31
-- ============================================================================
BEGIN;

TRUNCATE TABLE l3_workday.dim_day_d;

INSERT INTO l3_workday.dim_day_d (
    day_sk, calendar_date, day_of_week, day_name, day_of_month, day_of_year, week_of_year,
    month_number, month_name, quarter_number, quarter_name, year_number, fiscal_year,
    fiscal_quarter, fiscal_quarter_name, is_weekend, is_month_end, is_quarter_end, is_year_end,
    insert_datetime
)
WITH date_spine AS (
    -- Generate all dates from 2020-01-01 to 2030-12-31
    SELECT CAST(date_col AS DATE) AS calendar_date
    FROM (
        SELECT dateadd(day, row_number() OVER (ORDER BY 1) - 1, '2020-01-01'::DATE) AS date_col
        FROM (
            SELECT 1 AS n
            UNION ALL
            SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
        ) s1
        CROSS JOIN (
            SELECT 1 AS n
            UNION ALL
            SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
        ) s2
        CROSS JOIN (
            SELECT 1 AS n
            UNION ALL
            SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
        ) s3
        CROSS JOIN (
            SELECT 1 AS n
            UNION ALL
            SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        ) s4
    ) all_dates
    WHERE calendar_date <= '2030-12-31'::DATE
),
calendar_data AS (
    SELECT
        calendar_date,
        CAST(to_char(calendar_date, 'YYYYMMDD') AS INTEGER) AS day_sk,
        CAST(to_char(calendar_date, 'D') AS INTEGER) AS day_of_week,
        to_char(calendar_date, 'Day') AS day_name,
        CAST(to_char(calendar_date, 'DD') AS INTEGER) AS day_of_month,
        CAST(to_char(calendar_date, 'DDD') AS INTEGER) AS day_of_year,
        CAST(to_char(calendar_date, 'IW') AS INTEGER) AS week_of_year,
        CAST(to_char(calendar_date, 'MM') AS INTEGER) AS month_number,
        to_char(calendar_date, 'Month') AS month_name,
        CAST(to_char(calendar_date, 'Q') AS INTEGER) AS quarter_number,
        CONCAT('Q', CAST(to_char(calendar_date, 'Q') AS INTEGER)) AS quarter_name,
        CAST(to_char(calendar_date, 'YYYY') AS INTEGER) AS year_number,
        -- Fiscal year: Nov-Oct (so Nov 2025 is FY2026)
        CASE
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) >= 11 THEN CAST(to_char(calendar_date, 'YYYY') AS INTEGER) + 1
            ELSE CAST(to_char(calendar_date, 'YYYY') AS INTEGER)
        END AS fiscal_year,
        -- Fiscal quarter: FQ1=Nov-Jan, FQ2=Feb-Apr, FQ3=May-Jul, FQ4=Aug-Oct
        CASE
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) IN (11, 12, 1) THEN 1
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) IN (2, 3, 4) THEN 2
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) IN (5, 6, 7) THEN 3
            ELSE 4
        END AS fiscal_quarter,
        CASE
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) IN (11, 12, 1) THEN 'FQ1'
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) IN (2, 3, 4) THEN 'FQ2'
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) IN (5, 6, 7) THEN 'FQ3'
            ELSE 'FQ4'
        END AS fiscal_quarter_name,
        CASE
            WHEN CAST(to_char(calendar_date, 'D') AS INTEGER) IN (6, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        -- Month end
        CASE
            WHEN calendar_date = dateadd(day, -1, dateadd(month, 1, dateadd(day, -EXTRACT(DAY FROM calendar_date) + 1, calendar_date)))
            THEN TRUE
            ELSE FALSE
        END AS is_month_end,
        -- Quarter end
        CASE
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) IN (3, 6, 9, 12) AND
                 calendar_date = dateadd(day, -1, dateadd(month, 1, dateadd(day, -EXTRACT(DAY FROM calendar_date) + 1, calendar_date)))
            THEN TRUE
            ELSE FALSE
        END AS is_quarter_end,
        -- Year end
        CASE
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) = 12 AND
                 CAST(to_char(calendar_date, 'DD') AS INTEGER) = 31
            THEN TRUE
            ELSE FALSE
        END AS is_year_end
    FROM date_spine
)
SELECT
    day_sk, calendar_date, day_of_week, day_name, day_of_month, day_of_year, week_of_year,
    month_number, month_name, quarter_number, quarter_name, year_number, fiscal_year,
    fiscal_quarter, fiscal_quarter_name, is_weekend, is_month_end, is_quarter_end, is_year_end,
    GETDATE()
FROM calendar_data;

COMMIT;


-- ============================================================================
-- 2. LOAD DIM_COMPANY_D - SCD2 from INT6024
-- ============================================================================
BEGIN;

CREATE TEMP TABLE stg_dim_company AS
SELECT
    company_id,
    company_wid,
    company_code,
    company_name,
    company_subtype,
    company_currency,
    business_unit,
    MD5(COALESCE(company_wid::VARCHAR, '') || COALESCE(company_code::VARCHAR, '') || COALESCE(company_name::VARCHAR, '') || COALESCE(company_subtype::VARCHAR, '') || COALESCE(company_currency::VARCHAR, '') || COALESCE(business_unit::VARCHAR, '')) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6024_company
WHERE company_id IS NOT NULL;

-- Step 1: Close changed records
UPDATE l3_workday.dim_company_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_company stg
WHERE tgt.company_id = stg.company_id
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Step 2: Insert new and changed records
INSERT INTO l3_workday.dim_company_d (
    company_id, company_wid, company_code, company_name, company_subtype,
    company_currency, business_unit, valid_from, valid_to, is_current,
    hash_diff, insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.company_id,
    stg.company_wid,
    stg.company_code,
    stg.company_name,
    stg.company_subtype,
    stg.company_currency,
    stg.business_unit,
    stg.valid_from,
    '9999-12-31'::DATE,
    TRUE,
    stg.hash_diff,
    GETDATE(),
    GETDATE(),
    '${ETL_BATCH_ID}'
FROM stg_dim_company stg
LEFT JOIN l3_workday.dim_company_d tgt
    ON stg.company_id = tgt.company_id AND tgt.is_current = TRUE
WHERE tgt.company_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Step 3: Handle deletes (records in dim but not in source)
UPDATE l3_workday.dim_company_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND company_id NOT IN (SELECT company_id FROM stg_dim_company);

DROP TABLE stg_dim_company;

COMMIT;


-- ============================================================================
-- 3. LOAD DIM_COST_CENTER_D - SCD2 from INT6025
-- ============================================================================
BEGIN;

CREATE TEMP TABLE stg_dim_cost_center AS
SELECT
    cost_center_id,
    cost_center_wid,
    cost_center_code,
    cost_center_name,
    hierarchy,
    subtype,
    MD5(COALESCE(cost_center_wid::VARCHAR, '') || COALESCE(cost_center_code::VARCHAR, '') || COALESCE(cost_center_name::VARCHAR, '') || COALESCE(hierarchy::VARCHAR, '') || COALESCE(subtype::VARCHAR, '')) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6025_cost_center
WHERE cost_center_id IS NOT NULL;

-- Step 1: Close changed records
UPDATE l3_workday.dim_cost_center_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_cost_center stg
WHERE tgt.cost_center_id = stg.cost_center_id
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Step 2: Insert new and changed records
INSERT INTO l3_workday.dim_cost_center_d (
    cost_center_id, cost_center_wid, cost_center_code, cost_center_name,
    hierarchy, subtype, valid_from, valid_to, is_current, hash_diff,
    insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.cost_center_id,
    stg.cost_center_wid,
    stg.cost_center_code,
    stg.cost_center_name,
    stg.hierarchy,
    stg.subtype,
    stg.valid_from,
    '9999-12-31'::DATE,
    TRUE,
    stg.hash_diff,
    GETDATE(),
    GETDATE(),
    '${ETL_BATCH_ID}'
FROM stg_dim_cost_center stg
LEFT JOIN l3_workday.dim_cost_center_d tgt
    ON stg.cost_center_id = tgt.cost_center_id AND tgt.is_current = TRUE
WHERE tgt.cost_center_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Step 3: Handle deletes
UPDATE l3_workday.dim_cost_center_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND cost_center_id NOT IN (SELECT cost_center_id FROM stg_dim_cost_center);

DROP TABLE stg_dim_cost_center;

COMMIT;


-- ============================================================================
-- 4. LOAD DIM_GRADE_PROFILE_D - SCD2 from INT6020
-- ============================================================================
BEGIN;

CREATE TEMP TABLE stg_dim_grade_profile AS
SELECT
    grade_profile_id,
    grade_id,
    grade_name,
    grade_profile_currency_code,
    effective_date,
    grade_profile_name,
    grade_profile_number_of_segements,
    grade_profile_salary_range_maximum,
    grade_profile_salary_range_midpoint,
    grade_profile_salary_range_minimjum,
    grade_profile_segement_1_top,
    grade_profile_segement_2_top,
    grade_profile_segement_3_top,
    grade_profile_segement_4_top,
    grade_profile_segement_5_top,
    MD5(COALESCE(grade_id::VARCHAR, '') || COALESCE(grade_name::VARCHAR, '') || COALESCE(grade_profile_currency_code::VARCHAR, '') || COALESCE(effective_date::VARCHAR, '') || COALESCE(grade_profile_name::VARCHAR, '') || COALESCE(grade_profile_number_of_segements::VARCHAR, '') || COALESCE(grade_profile_salary_range_maximum::VARCHAR, '') || COALESCE(grade_profile_salary_range_midpoint::VARCHAR, '') || COALESCE(grade_profile_salary_range_minimjum::VARCHAR, '') || COALESCE(grade_profile_segement_1_top::VARCHAR, '') || COALESCE(grade_profile_segement_2_top::VARCHAR, '') || COALESCE(grade_profile_segement_3_top::VARCHAR, '') || COALESCE(grade_profile_segement_4_top::VARCHAR, '') || COALESCE(grade_profile_segement_5_top::VARCHAR, '')) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6020_grade_profile
WHERE grade_profile_id IS NOT NULL;

-- Step 1: Close changed records
UPDATE l3_workday.dim_grade_profile_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_grade_profile stg
WHERE tgt.grade_profile_id = stg.grade_profile_id
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Step 2: Insert new and changed records
INSERT INTO l3_workday.dim_grade_profile_d (
    grade_profile_id, grade_id, grade_name, grade_profile_currency_code, effective_date,
    grade_profile_name, grade_profile_number_of_segements, grade_profile_salary_range_maximum,
    grade_profile_salary_range_midpoint, grade_profile_salary_range_minimjum,
    grade_profile_segement_1_top, grade_profile_segement_2_top, grade_profile_segement_3_top,
    grade_profile_segement_4_top, grade_profile_segement_5_top,
    valid_from, valid_to, is_current, hash_diff, insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.grade_profile_id, stg.grade_id, stg.grade_name, stg.grade_profile_currency_code, stg.effective_date,
    stg.grade_profile_name, stg.grade_profile_number_of_segements, stg.grade_profile_salary_range_maximum,
    stg.grade_profile_salary_range_midpoint, stg.grade_profile_salary_range_minimjum,
    stg.grade_profile_segement_1_top, stg.grade_profile_segement_2_top, stg.grade_profile_segement_3_top,
    stg.grade_profile_segement_4_top, stg.grade_profile_segement_5_top,
    stg.valid_from, '9999-12-31'::DATE, TRUE, stg.hash_diff, GETDATE(), GETDATE(), '${ETL_BATCH_ID}'
FROM stg_dim_grade_profile stg
LEFT JOIN l3_workday.dim_grade_profile_d tgt
    ON stg.grade_profile_id = tgt.grade_profile_id AND tgt.is_current = TRUE
WHERE tgt.grade_profile_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Step 3: Handle deletes
UPDATE l3_workday.dim_grade_profile_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND grade_profile_id NOT IN (SELECT grade_profile_id FROM stg_dim_grade_profile);

DROP TABLE stg_dim_grade_profile;

COMMIT;


-- ============================================================================
-- 5. LOAD DIM_JOB_PROFILE_D - SCD2 from INT6021 + INT6022
-- ============================================================================
BEGIN;

CREATE TEMP TABLE stg_dim_job_profile AS
SELECT
    jp21.job_profile_id,
    jp21.compensation_grade,
    jp21.critical_job_flag,
    jp21.difficult_to_fill_flag,
    jp21.inactive_flag::INT::VARCHAR,
    jp21.job_category_code,
    jp21.job_category_name,
    jp21.job_exempt_canada,
    jp21.job_exempt_us,
    jp21.job_family,
    jp21.job_family_group,
    jp21.job_family_group_name,
    jp21.job_family_name,
    jp21.job_level_code,
    jp21.job_level_name,
    jp21.job_profile_code,
    jp21.job_profile_description,
    jp21.job_profile_name,
    jp21.job_profile_summary,
    jp21.job_profile_wid,
    jp21.job_title,
    jp21.management_level_code,
    jp21.management_level_name,
    jp21.pay_rate_type,
    jp21.public_job::INT::VARCHAR,
    jp21.work_shift_required::INT::VARCHAR,
    jp21.job_matrix,
    jp21.is_people_manager::INT::VARCHAR,
    jp21.is_manager::INT::VARCHAR,
    jp21.frequency,
    jp22.aap_job_group,
    jp22.bonus_eligibility,
    jp22.customer_facing,
    jp22.eeo1_code,
    jp22.job_collection,
    jp22.loan_originator_code,
    jp22.national_occupation_code,
    jp22.occupation_code,
    jp22.recruitment_channel,
    jp22.standard_occupation_code,
    jp22.stock,
    MD5(COALESCE(jp21.compensation_grade::VARCHAR, '') || COALESCE(jp21.critical_job_flag::VARCHAR, '') || COALESCE(jp21.difficult_to_fill_flag::VARCHAR, '') || COALESCE(jp21.inactive_flag::INT::VARCHAR, '') || COALESCE(jp21.job_category_code::VARCHAR, '') || COALESCE(jp21.job_category_name::VARCHAR, '') || COALESCE(jp21.job_exempt_canada::VARCHAR, '') || COALESCE(jp21.job_exempt_us::VARCHAR, '') || COALESCE(jp21.job_family::VARCHAR, '') || COALESCE(jp21.job_family_group::VARCHAR, '') || COALESCE(jp21.job_family_group_name::VARCHAR, '') || COALESCE(jp21.job_family_name::VARCHAR, '') || COALESCE(jp21.job_level_code::VARCHAR, '') || COALESCE(jp21.job_level_name::VARCHAR, '') || COALESCE(jp21.job_profile_code::VARCHAR, '') || COALESCE(jp21.job_profile_description::VARCHAR, '') || COALESCE(jp21.job_profile_name::VARCHAR, '') || COALESCE(jp21.job_profile_summary::VARCHAR, '') || COALESCE(jp21.job_profile_wid::VARCHAR, '') || COALESCE(jp21.job_title::VARCHAR, '') || COALESCE(jp21.management_level_code::VARCHAR, '') || COALESCE(jp21.management_level_name::VARCHAR, '') || COALESCE(jp21.pay_rate_type::VARCHAR, '') || COALESCE(jp21.public_job::INT::VARCHAR, '') || COALESCE(jp21.work_shift_required::INT::VARCHAR, '') || COALESCE(jp21.job_matrix::VARCHAR, '') || COALESCE(jp21.is_people_manager::INT::VARCHAR, '') || COALESCE(jp21.is_manager::INT::VARCHAR, '') || COALESCE(jp21.frequency::VARCHAR, '') || COALESCE(jp22.aap_job_group::VARCHAR, '') || COALESCE(jp22.bonus_eligibility::VARCHAR, '') || COALESCE(jp22.customer_facing::VARCHAR, '') || COALESCE(jp22.eeo1_code::VARCHAR, '') || COALESCE(jp22.job_collection::VARCHAR, '') || COALESCE(jp22.loan_originator_code::VARCHAR, '') || COALESCE(jp22.national_occupation_code::VARCHAR, '') || COALESCE(jp22.occupation_code::VARCHAR, '') || COALESCE(jp22.recruitment_channel::VARCHAR, '') || COALESCE(jp22.standard_occupation_code::VARCHAR, '') || COALESCE(jp22.stock::VARCHAR, '')) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6021_job_profile jp21
LEFT JOIN l1_workday.int6022_job_classification jp22
    ON jp21.job_profile_id = jp22.job_profile_id
WHERE jp21.job_profile_id IS NOT NULL;

-- Step 1: Close changed records
UPDATE l3_workday.dim_job_profile_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_job_profile stg
WHERE tgt.job_profile_id = stg.job_profile_id
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Step 2: Insert new and changed records
INSERT INTO l3_workday.dim_job_profile_d (
    job_profile_id, compensation_grade, critical_job_flag, difficult_to_fill_flag, inactive_flag,
    job_category_code, job_category_name, job_exempt_canada, job_exempt_us, job_family,
    job_family_group, job_family_group_name, job_family_name, job_level_code, job_level_name,
    job_profile_code, job_profile_description, job_profile_name, job_profile_summary, job_profile_wid,
    job_title, management_level_code, management_level_name, pay_rate_type, public_job,
    work_shift_required, job_matrix, is_people_manager, is_manager, frequency,
    aap_job_group, bonus_eligibility, customer_facing, eeo1_code, job_collection,
    loan_originator_code, national_occupation_code, occupation_code, recruitment_channel,
    standard_occupation_code, stock,
    valid_from, valid_to, is_current, hash_diff, insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.job_profile_id, stg.compensation_grade, stg.critical_job_flag, stg.difficult_to_fill_flag, stg.inactive_flag,
    stg.job_category_code, stg.job_category_name, stg.job_exempt_canada, stg.job_exempt_us, stg.job_family,
    stg.job_family_group, stg.job_family_group_name, stg.job_family_name, stg.job_level_code, stg.job_level_name,
    stg.job_profile_code, stg.job_profile_description, stg.job_profile_name, stg.job_profile_summary, stg.job_profile_wid,
    stg.job_title, stg.management_level_code, stg.management_level_name, stg.pay_rate_type, stg.public_job,
    stg.work_shift_required, stg.job_matrix, stg.is_people_manager, stg.is_manager, stg.frequency,
    stg.aap_job_group, stg.bonus_eligibility, stg.customer_facing, stg.eeo1_code, stg.job_collection,
    stg.loan_originator_code, stg.national_occupation_code, stg.occupation_code, stg.recruitment_channel,
    stg.standard_occupation_code, stg.stock,
    stg.valid_from, '9999-12-31'::DATE, TRUE, stg.hash_diff, GETDATE(), GETDATE(), '${ETL_BATCH_ID}'
FROM stg_dim_job_profile stg
LEFT JOIN l3_workday.dim_job_profile_d tgt
    ON stg.job_profile_id = tgt.job_profile_id AND tgt.is_current = TRUE
WHERE tgt.job_profile_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Step 3: Handle deletes
UPDATE l3_workday.dim_job_profile_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND job_profile_id NOT IN (SELECT job_profile_id FROM stg_dim_job_profile);

DROP TABLE stg_dim_job_profile;

COMMIT;


-- ============================================================================
-- 6. LOAD DIM_LOCATION_D - SCD2 from INT6023
-- ============================================================================
BEGIN;

CREATE TEMP TABLE stg_dim_location AS
SELECT
    location_id,
    location_wid,
    location_name,
    inactive,
    address_line_1,
    address_line_2,
    city,
    region,
    region_name,
    country,
    country_name,
    location_postal_code,
    location_identifier,
    latitude,
    longitude,
    location_type,
    location_usage_type,
    trade_name,
    worksite_id_code,
    MD5(COALESCE(location_wid::VARCHAR, '') || COALESCE(location_name::VARCHAR, '') || COALESCE(inactive::VARCHAR, '') || COALESCE(address_line_1::VARCHAR, '') || COALESCE(address_line_2::VARCHAR, '') || COALESCE(city::VARCHAR, '') || COALESCE(region::VARCHAR, '') || COALESCE(region_name::VARCHAR, '') || COALESCE(country::VARCHAR, '') || COALESCE(country_name::VARCHAR, '') || COALESCE(location_postal_code::VARCHAR, '') || COALESCE(location_identifier::VARCHAR, '') || COALESCE(latitude::VARCHAR, '') || COALESCE(longitude::VARCHAR, '') || COALESCE(location_type::VARCHAR, '') || COALESCE(location_usage_type::VARCHAR, '') || COALESCE(trade_name::VARCHAR, '') || COALESCE(worksite_id_code::VARCHAR, '')) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6023_location
WHERE location_id IS NOT NULL;

-- Step 1: Close changed records
UPDATE l3_workday.dim_location_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_location stg
WHERE tgt.location_id = stg.location_id
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Step 2: Insert new and changed records
INSERT INTO l3_workday.dim_location_d (
    location_id, location_wid, location_name, inactive, address_line_1, address_line_2,
    city, region, region_name, country, country_name, location_postal_code, location_identifier,
    latitude, longitude, location_type, location_usage_type, trade_name, worksite_id_code,
    valid_from, valid_to, is_current, hash_diff, insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.location_id, stg.location_wid, stg.location_name, stg.inactive, stg.address_line_1, stg.address_line_2,
    stg.city, stg.region, stg.region_name, stg.country, stg.country_name, stg.location_postal_code, stg.location_identifier,
    stg.latitude, stg.longitude, stg.location_type, stg.location_usage_type, stg.trade_name, stg.worksite_id_code,
    stg.valid_from, '9999-12-31'::DATE, TRUE, stg.hash_diff, GETDATE(), GETDATE(), '${ETL_BATCH_ID}'
FROM stg_dim_location stg
LEFT JOIN l3_workday.dim_location_d tgt
    ON stg.location_id = tgt.location_id AND tgt.is_current = TRUE
WHERE tgt.location_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Step 3: Handle deletes
UPDATE l3_workday.dim_location_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND location_id NOT IN (SELECT location_id FROM stg_dim_location);

DROP TABLE stg_dim_location;

COMMIT;


-- ============================================================================
-- 7. LOAD DIM_DEPARTMENT_D - SCD2 from INT6028
-- ============================================================================
BEGIN;

CREATE TEMP TABLE stg_dim_department AS
SELECT
    department_id,
    department_wid,
    department_name,
    dept_name_with_manager_name,
    active::INT::VARCHAR,
    parent_dept_id,
    owner_ein,
    department_level,
    primary_location_code,
    type,
    subtype,
    MD5(COALESCE(department_wid::VARCHAR, '') || COALESCE(department_name::VARCHAR, '') || COALESCE(dept_name_with_manager_name::VARCHAR, '') || COALESCE(active::INT::VARCHAR, '') || COALESCE(parent_dept_id::VARCHAR, '') || COALESCE(owner_ein::VARCHAR, '') || COALESCE(department_level::VARCHAR, '') || COALESCE(primary_location_code::VARCHAR, '') || COALESCE(type::VARCHAR, '') || COALESCE(subtype::VARCHAR, '')) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6028_department_hierarchy
WHERE department_id IS NOT NULL;

-- Step 1: Close changed records
UPDATE l3_workday.dim_department_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_department stg
WHERE tgt.department_id = stg.department_id
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Step 2: Insert new and changed records
INSERT INTO l3_workday.dim_department_d (
    department_id, department_wid, department_name, dept_name_with_manager_name, active,
    parent_dept_id, owner_ein, department_level, primary_location_code, type, subtype,
    valid_from, valid_to, is_current, hash_diff, insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.department_id, stg.department_wid, stg.department_name, stg.dept_name_with_manager_name, stg.active,
    stg.parent_dept_id, stg.owner_ein, stg.department_level, stg.primary_location_code, stg.type, stg.subtype,
    stg.valid_from, '9999-12-31'::DATE, TRUE, stg.hash_diff, GETDATE(), GETDATE(), '${ETL_BATCH_ID}'
FROM stg_dim_department stg
LEFT JOIN l3_workday.dim_department_d tgt
    ON stg.department_id = tgt.department_id AND tgt.is_current = TRUE
WHERE tgt.department_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Step 3: Handle deletes
UPDATE l3_workday.dim_department_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND department_id NOT IN (SELECT department_id FROM stg_dim_department);

DROP TABLE stg_dim_department;

COMMIT;


-- ============================================================================
-- 8. LOAD DIM_POSITION_D - SCD2 from INT6032
-- ============================================================================
BEGIN;

CREATE TEMP TABLE stg_dim_position AS
SELECT
    position_id,
    supervisory_organization,
    effective_date,
    reason,
    worker_type,
    worker_sub_type,
    job_profile,
    job_title,
    business_title,
    time_type,
    location,
    MD5(COALESCE(supervisory_organization::VARCHAR, '') || COALESCE(effective_date::VARCHAR, '') || COALESCE(reason::VARCHAR, '') || COALESCE(worker_type::VARCHAR, '') || COALESCE(worker_sub_type::VARCHAR, '') || COALESCE(job_profile::VARCHAR, '') || COALESCE(job_title::VARCHAR, '') || COALESCE(business_title::VARCHAR, '') || COALESCE(time_type::VARCHAR, '') || COALESCE(location::VARCHAR, '')) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6032_positions
WHERE position_id IS NOT NULL;

-- Step 1: Close changed records
UPDATE l3_workday.dim_position_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_position stg
WHERE tgt.position_id = stg.position_id
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Step 2: Insert new and changed records
INSERT INTO l3_workday.dim_position_d (
    position_id, supervisory_organization, effective_date, reason, worker_type, worker_sub_type,
    job_profile, job_title, business_title, time_type, location,
    valid_from, valid_to, is_current, hash_diff, insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.position_id, stg.supervisory_organization, stg.effective_date, stg.reason, stg.worker_type, stg.worker_sub_type,
    stg.job_profile, stg.job_title, stg.business_title, stg.time_type, stg.location,
    stg.valid_from, '9999-12-31'::DATE, TRUE, stg.hash_diff, GETDATE(), GETDATE(), '${ETL_BATCH_ID}'
FROM stg_dim_position stg
LEFT JOIN l3_workday.dim_position_d tgt
    ON stg.position_id = tgt.position_id AND tgt.is_current = TRUE
WHERE tgt.position_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Step 3: Handle deletes
UPDATE l3_workday.dim_position_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND position_id NOT IN (SELECT position_id FROM stg_dim_position);

DROP TABLE stg_dim_position;

COMMIT;


-- ============================================================================
-- 9. LOAD DIM_WORKER_JOB_D - SCD2, BK = (employee_id, effective_date)
-- ALGORITHM 6.1: Complex multi-source as-of join
-- ============================================================================
BEGIN;

-- Step 6.1.1: Collect all effective dates from all three sources
CREATE TEMP TABLE tmp_effective_dates AS
SELECT DISTINCT employee_id, transaction_effective_date AS effective_date
FROM l3_workday.l3_workday_worker_job_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num
UNION
SELECT DISTINCT employee_id, transaction_effective_date AS effective_date
FROM l3_workday.l3_workday_worker_comp_dly_vw
WHERE idp_obsolete_date IS NULL
UNION
SELECT DISTINCT employee_id, transaction_effective_date AS effective_date
FROM l3_workday.l3_workday_worker_organization_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num
  AND organization_type IN ('Cost Centre', 'Company', 'Supervisory');

-- Step 6.1.2: Prepare filtered Worker Job data with row numbering
CREATE TEMP TABLE tmp_worker_job AS
SELECT
    employee_id,
    transaction_effective_date,
    position_id,
    worker_type,
    worker_sub_type,
    business_title,
    business_site_id,
    mailstop_floor,
    worker_status,
    active::INT::VARCHAR,
    first_day_of_work,
    expected_date_of_return,
    not_returning::INT::VARCHAR,
    return_unknown,
    probation_start_date,
    probation_end_date,
    academic_tenure_date,
    has_international_assignment::INT::VARCHAR,
    home_country,
    host_country,
    international_assignment_type,
    start_date_of_international_assignment,
    end_date_of_international_assignment,
    action,
    action_code,
    action_reason,
    action_reason_code,
    manager_id,
    soft_retirement_indicator::INT::VARCHAR,
    job_profile_id,
    sequence_number,
    planned_end_contract_date,
    job_entry_dt,
    stock_grants,
    time_type,
    supervisory_organization,
    location,
    job_title,
    french_job_title,
    shift_number,
    scheduled_weekly_hours,
    default_weekly_hours,
    scheduled_fte,
    work_model_start_date,
    work_model_type,
    worker_workday_id,
    idp_employee_status,
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_date DESC) AS rn
FROM l3_workday.l3_workday_worker_job_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num;

-- Step 6.1.3: Prepare filtered Worker Compensation data
CREATE TEMP TABLE tmp_worker_comp AS
SELECT
    employee_id,
    transaction_effective_date,
    compensation_package_proposed,
    compensation_grade_proposed,
    comp_grade_profile_proposed,
    compensation_step_proposed,
    pay_range_minimum,
    pay_range_midpoint,
    pay_range_maximum,
    base_pay_proposed_amount,
    base_pay_proposed_currency,
    base_pay_proposed_frequency,
    benefits_annual_rate_abbr,
    pay_rate_type,
    compensation,
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_moment DESC) AS rn
FROM l3_workday.l3_workday_worker_comp_dly_vw
WHERE idp_obsolete_date IS NULL;

-- Step 6.1.4: Prepare filtered Worker Organization data (3 pivots: Cost Centre, Company, Supervisory)
CREATE TEMP TABLE tmp_worker_org_cost_centre AS
SELECT
    employee_id,
    transaction_effective_date,
    organization_id AS cost_center_id,
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_date DESC) AS rn
FROM l3_workday.l3_workday_worker_organization_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num
  AND organization_type = 'Cost Centre';

CREATE TEMP TABLE tmp_worker_org_company AS
SELECT
    employee_id,
    transaction_effective_date,
    organization_id AS company_id,
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_date DESC) AS rn
FROM l3_workday.l3_workday_worker_organization_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num
  AND organization_type = 'Company';

CREATE TEMP TABLE tmp_worker_org_supervisory AS
SELECT
    employee_id,
    transaction_effective_date,
    organization_id AS sup_org_id,
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_date DESC) AS rn
FROM l3_workday.l3_workday_worker_organization_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num
  AND organization_type = 'Supervisory';

-- Step 6.1.5: Find the MAX effective date from each source that is <= each target date
-- This prevents the cartesian product that occurred when using (effective_date <=) directly
CREATE TEMP TABLE tmp_as_of_keys AS
SELECT
    ed.employee_id,
    ed.effective_date,
    -- Most recent worker_job row on or before this effective_date
    (SELECT MAX(wj2.transaction_effective_date)
     FROM tmp_worker_job wj2
     WHERE wj2.employee_id = ed.employee_id
       AND wj2.transaction_effective_date <= ed.effective_date
       AND wj2.rn = 1) AS wj_as_of_date,
    -- Most recent worker_comp row on or before this effective_date
    (SELECT MAX(wc2.transaction_effective_date)
     FROM tmp_worker_comp wc2
     WHERE wc2.employee_id = ed.employee_id
       AND wc2.transaction_effective_date <= ed.effective_date
       AND wc2.rn = 1) AS wc_as_of_date,
    -- Most recent cost_centre row on or before this effective_date
    (SELECT MAX(cc2.transaction_effective_date)
     FROM tmp_worker_org_cost_centre cc2
     WHERE cc2.employee_id = ed.employee_id
       AND cc2.transaction_effective_date <= ed.effective_date
       AND cc2.rn = 1) AS cc_as_of_date,
    -- Most recent company row on or before this effective_date
    (SELECT MAX(co2.transaction_effective_date)
     FROM tmp_worker_org_company co2
     WHERE co2.employee_id = ed.employee_id
       AND co2.transaction_effective_date <= ed.effective_date
       AND co2.rn = 1) AS co_as_of_date,
    -- Most recent supervisory row on or before this effective_date
    (SELECT MAX(so2.transaction_effective_date)
     FROM tmp_worker_org_supervisory so2
     WHERE so2.employee_id = ed.employee_id
       AND so2.transaction_effective_date <= ed.effective_date
       AND so2.rn = 1) AS so_as_of_date
FROM tmp_effective_dates ed;

-- Step 6.1.6: Assemble enriched row via exact as-of joins (no cartesian product)
CREATE TEMP TABLE tmp_assembled_rows AS
SELECT
    aok.employee_id,
    aok.effective_date,
    -- Worker Job attributes (exact as-of join)
    wj.position_id,
    wj.worker_type,
    wj.worker_sub_type,
    wj.business_title,
    wj.business_site_id,
    wj.mailstop_floor,
    wj.worker_status,
    wj.active,
    wj.first_day_of_work,
    wj.expected_date_of_return,
    wj.not_returning,
    wj.return_unknown,
    wj.probation_start_date,
    wj.probation_end_date,
    wj.academic_tenure_date,
    wj.has_international_assignment,
    wj.home_country,
    wj.host_country,
    wj.international_assignment_type,
    wj.start_date_of_international_assignment,
    wj.end_date_of_international_assignment,
    wj.action,
    wj.action_code,
    wj.action_reason,
    wj.action_reason_code,
    wj.manager_id,
    wj.soft_retirement_indicator,
    wj.job_profile_id,
    wj.sequence_number,
    wj.planned_end_contract_date,
    wj.job_entry_dt,
    wj.stock_grants,
    wj.time_type,
    wj.supervisory_organization,
    wj.location,
    wj.job_title,
    wj.french_job_title,
    wj.shift_number,
    wj.scheduled_weekly_hours,
    wj.default_weekly_hours,
    wj.scheduled_fte,
    wj.work_model_start_date,
    wj.work_model_type,
    wj.worker_workday_id,
    wj.idp_employee_status,
    -- Worker Compensation attributes (exact as-of join)
    wc.compensation_package_proposed,
    wc.compensation_grade_proposed,
    wc.comp_grade_profile_proposed,
    wc.compensation_step_proposed,
    wc.pay_range_minimum,
    wc.pay_range_midpoint,
    wc.pay_range_maximum,
    wc.base_pay_proposed_amount,
    wc.base_pay_proposed_currency,
    wc.base_pay_proposed_frequency,
    wc.benefits_annual_rate_abbr,
    wc.pay_rate_type,
    wc.compensation,
    -- Worker Organization attributes (exact as-of join)
    cc.cost_center_id,
    co.company_id,
    so.sup_org_id
FROM tmp_as_of_keys aok
LEFT JOIN tmp_worker_job wj
    ON aok.employee_id = wj.employee_id
    AND wj.transaction_effective_date = aok.wj_as_of_date
    AND wj.rn = 1
LEFT JOIN tmp_worker_comp wc
    ON aok.employee_id = wc.employee_id
    AND wc.transaction_effective_date = aok.wc_as_of_date
    AND wc.rn = 1
LEFT JOIN tmp_worker_org_cost_centre cc
    ON aok.employee_id = cc.employee_id
    AND cc.transaction_effective_date = aok.cc_as_of_date
    AND cc.rn = 1
LEFT JOIN tmp_worker_org_company co
    ON aok.employee_id = co.employee_id
    AND co.transaction_effective_date = aok.co_as_of_date
    AND co.rn = 1
LEFT JOIN tmp_worker_org_supervisory so
    ON aok.employee_id = so.employee_id
    AND so.transaction_effective_date = aok.so_as_of_date
    AND so.rn = 1;

-- Step 6.1.7: Frame SCD2 windows and compute hash_diff
CREATE TEMP TABLE stg_dim_worker_job AS
WITH windowed_rows AS (
    SELECT
        *,
        LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date) AS next_effective_date,
        effective_date AS effective_date_from,
        COALESCE(LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date), '9999-12-31'::DATE) - 1 AS effective_date_to
    FROM tmp_assembled_rows
)
SELECT
    employee_id,
    effective_date,
    position_id, worker_type, worker_sub_type, business_title, business_site_id, mailstop_floor,
    worker_status, active, first_day_of_work, expected_date_of_return, not_returning, return_unknown,
    probation_start_date, probation_end_date, academic_tenure_date, has_international_assignment,
    home_country, host_country, international_assignment_type, start_date_of_international_assignment,
    end_date_of_international_assignment, action, action_code, action_reason, action_reason_code,
    manager_id, soft_retirement_indicator, job_profile_id, sequence_number, planned_end_contract_date,
    job_entry_dt, stock_grants, time_type, supervisory_organization, location, job_title,
    french_job_title, shift_number, scheduled_weekly_hours, default_weekly_hours, scheduled_fte,
    work_model_start_date, work_model_type, worker_workday_id, idp_employee_status,
    compensation_package_proposed, compensation_grade_proposed, comp_grade_profile_proposed,
    compensation_step_proposed, pay_range_minimum, pay_range_midpoint, pay_range_maximum,
    base_pay_proposed_amount, base_pay_proposed_currency, base_pay_proposed_frequency,
    benefits_annual_rate_abbr, pay_rate_type, compensation,
    cost_center_id, company_id, sup_org_id,
    effective_date_from,
    effective_date_to,
    effective_date_from AS valid_from,
    MD5(COALESCE(position_id::VARCHAR, '') || COALESCE(worker_type::VARCHAR, '') || COALESCE(worker_sub_type::VARCHAR, '') || COALESCE(business_title::VARCHAR, '') || COALESCE(business_site_id::VARCHAR, '') || COALESCE(mailstop_floor::VARCHAR, '') || COALESCE(worker_status::VARCHAR, '') || COALESCE(active::INT::VARCHAR, '') || COALESCE(first_day_of_work::VARCHAR, '') || COALESCE(expected_date_of_return::VARCHAR, '') || COALESCE(not_returning::INT::VARCHAR, '') || COALESCE(return_unknown::VARCHAR, '') || COALESCE(probation_start_date::VARCHAR, '') || COALESCE(probation_end_date::VARCHAR, '') || COALESCE(academic_tenure_date::VARCHAR, '') || COALESCE(has_international_assignment::INT::VARCHAR, '') || COALESCE(home_country::VARCHAR, '') || COALESCE(host_country::VARCHAR, '') || COALESCE(international_assignment_type::VARCHAR, '') || COALESCE(start_date_of_international_assignment::VARCHAR, '') || COALESCE(end_date_of_international_assignment::VARCHAR, '') || COALESCE(action::VARCHAR, '') || COALESCE(action_code::VARCHAR, '') || COALESCE(action_reason::VARCHAR, '') || COALESCE(action_reason_code::VARCHAR, '') || COALESCE(manager_id::VARCHAR, '') || COALESCE(soft_retirement_indicator::INT::VARCHAR, '') || COALESCE(job_profile_id::VARCHAR, '') || COALESCE(sequence_number::VARCHAR, '') || COALESCE(planned_end_contract_date::VARCHAR, '') || COALESCE(job_entry_dt::VARCHAR, '') || COALESCE(stock_grants::VARCHAR, '') || COALESCE(time_type::VARCHAR, '') || COALESCE(supervisory_organization::VARCHAR, '') || COALESCE(location::VARCHAR, '') || COALESCE(job_title::VARCHAR, '') || COALESCE(french_job_title::VARCHAR, '') || COALESCE(shift_number::VARCHAR, '') || COALESCE(scheduled_weekly_hours::VARCHAR, '') || COALESCE(default_weekly_hours::VARCHAR, '') || COALESCE(scheduled_fte::VARCHAR, '') || COALESCE(work_model_start_date::VARCHAR, '') || COALESCE(work_model_type::VARCHAR, '') || COALESCE(worker_workday_id::VARCHAR, '') || COALESCE(idp_employee_status::VARCHAR, '') || COALESCE(compensation_package_proposed::VARCHAR, '') || COALESCE(compensation_grade_proposed::VARCHAR, '') || COALESCE(comp_grade_profile_proposed::VARCHAR, '') || COALESCE(compensation_step_proposed::VARCHAR, '') || COALESCE(pay_range_minimum::VARCHAR, '') || COALESCE(pay_range_midpoint::VARCHAR, '') || COALESCE(pay_range_maximum::VARCHAR, '') || COALESCE(base_pay_proposed_amount::VARCHAR, '') || COALESCE(base_pay_proposed_currency::VARCHAR, '') || COALESCE(base_pay_proposed_frequency::VARCHAR, '') || COALESCE(benefits_annual_rate_abbr::VARCHAR, '') || COALESCE(pay_rate_type::VARCHAR, '') || COALESCE(compensation::VARCHAR, '') || COALESCE(cost_center_id::VARCHAR, '') || COALESCE(company_id::VARCHAR, '') || COALESCE(sup_org_id::VARCHAR, '')) AS hash_diff
FROM windowed_rows;

-- Step 6.1.8: Mark is_current_job_row (most recent per employee)
CREATE TEMP TABLE stg_dim_worker_job_final AS
SELECT
    *,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date DESC) = 1
        THEN TRUE
        ELSE FALSE
    END AS is_current_job_row
FROM stg_dim_worker_job;

-- Step 6.1.9: SCD2 merge logic
-- Close changed records in target
UPDATE l3_workday.dim_worker_job_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_worker_job_final stg
WHERE tgt.employee_id = stg.employee_id
  AND tgt.effective_date = stg.effective_date
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Insert new and changed records
INSERT INTO l3_workday.dim_worker_job_d (
    employee_id, effective_date,
    position_id, worker_type, worker_sub_type, business_title, business_site_id, mailstop_floor,
    worker_status, active, first_day_of_work, expected_date_of_return, not_returning, return_unknown,
    probation_start_date, probation_end_date, academic_tenure_date, has_international_assignment,
    home_country, host_country, international_assignment_type, start_date_of_international_assignment,
    end_date_of_international_assignment, action, action_code, action_reason, action_reason_code,
    manager_id, soft_retirement_indicator, job_profile_id, sequence_number, planned_end_contract_date,
    job_entry_dt, stock_grants, time_type, supervisory_organization, location, job_title,
    french_job_title, shift_number, scheduled_weekly_hours, default_weekly_hours, scheduled_fte,
    work_model_start_date, work_model_type, worker_workday_id, idp_employee_status,
    compensation_package_proposed, compensation_grade_proposed, comp_grade_profile_proposed,
    compensation_step_proposed, pay_range_minimum, pay_range_midpoint, pay_range_maximum,
    base_pay_proposed_amount, base_pay_proposed_currency, base_pay_proposed_frequency,
    benefits_annual_rate_abbr, pay_rate_type, compensation,
    cost_center_id, company_id, sup_org_id,
    effective_date_from, effective_date_to, valid_from, valid_to, is_current, is_current_job_row,
    hash_diff, insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.employee_id, stg.effective_date,
    stg.position_id, stg.worker_type, stg.worker_sub_type, stg.business_title, stg.business_site_id, stg.mailstop_floor,
    stg.worker_status, stg.active, stg.first_day_of_work, stg.expected_date_of_return, stg.not_returning, stg.return_unknown,
    stg.probation_start_date, stg.probation_end_date, stg.academic_tenure_date, stg.has_international_assignment,
    stg.home_country, stg.host_country, stg.international_assignment_type, stg.start_date_of_international_assignment,
    stg.end_date_of_international_assignment, stg.action, stg.action_code, stg.action_reason, stg.action_reason_code,
    stg.manager_id, stg.soft_retirement_indicator, stg.job_profile_id, stg.sequence_number, stg.planned_end_contract_date,
    stg.job_entry_dt, stg.stock_grants, stg.time_type, stg.supervisory_organization, stg.location, stg.job_title,
    stg.french_job_title, stg.shift_number, stg.scheduled_weekly_hours, stg.default_weekly_hours, stg.scheduled_fte,
    stg.work_model_start_date, stg.work_model_type, stg.worker_workday_id, stg.idp_employee_status,
    stg.compensation_package_proposed, stg.compensation_grade_proposed, stg.comp_grade_profile_proposed,
    stg.compensation_step_proposed, stg.pay_range_minimum, stg.pay_range_midpoint, stg.pay_range_maximum,
    stg.base_pay_proposed_amount, stg.base_pay_proposed_currency, stg.base_pay_proposed_frequency,
    stg.benefits_annual_rate_abbr, stg.pay_rate_type, stg.compensation,
    stg.cost_center_id, stg.company_id, stg.sup_org_id,
    stg.effective_date_from, stg.effective_date_to, stg.valid_from, '9999-12-31'::DATE, TRUE, stg.is_current_job_row,
    stg.hash_diff, GETDATE(), GETDATE(), '${ETL_BATCH_ID}'
FROM stg_dim_worker_job_final stg
LEFT JOIN l3_workday.dim_worker_job_d tgt
    ON stg.employee_id = tgt.employee_id
    AND stg.effective_date = tgt.effective_date
    AND tgt.is_current = TRUE
WHERE tgt.employee_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Handle deletes: mark historical records not in new source
UPDATE l3_workday.dim_worker_job_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND (employee_id, effective_date) NOT IN (
    SELECT employee_id, effective_date FROM stg_dim_worker_job_final
  );

-- Cleanup temp tables
DROP TABLE tmp_effective_dates;
DROP TABLE tmp_worker_job;
DROP TABLE tmp_worker_comp;
DROP TABLE tmp_worker_org_cost_centre;
DROP TABLE tmp_worker_org_company;
DROP TABLE tmp_worker_org_supervisory;
DROP TABLE tmp_as_of_keys;
DROP TABLE tmp_assembled_rows;
DROP TABLE stg_dim_worker_job;
DROP TABLE stg_dim_worker_job_final;

COMMIT;


-- ============================================================================
-- 10. LOAD DIM_WORKER_STATUS_D - SCD2, BK = (employee_id, effective_date)
-- Status-related attributes only from Worker Job
-- ============================================================================
BEGIN;

-- Step 1: Collect all effective dates for status changes
CREATE TEMP TABLE tmp_status_effective_dates AS
SELECT DISTINCT employee_id, transaction_effective_date AS effective_date
FROM l3_workday.l3_workday_worker_job_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num;

-- Step 2: Prepare filtered Worker Job data for status attributes
CREATE TEMP TABLE tmp_worker_status_job AS
SELECT
    employee_id,
    transaction_effective_date,
    active_status_date,
    benefits_service_date,
    continuous_service_date,
    planned_end_contract_date,
    hire_date,
    eligible_for_rehire,
    not_eligible_for_hire::INT::VARCHAR,
    active::INT::VARCHAR,
    worker_status,
    employment_end_date,
    hire_reason,
    hire_rescinded::INT::VARCHAR,
    original_hire_date,
    primary_termination_category,
    primary_termination_reason,
    retired::INT::VARCHAR,
    retirement_eligibility_date,
    expected_retirement_date,
    seniority_date,
    termination_date,
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_date DESC) AS rn
FROM l3_workday.l3_workday_worker_job_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num;

-- Step 3: Compute as-of keys for status (fixed: separate temp table to avoid unsupported correlated subquery in JOIN ON)
CREATE TEMP TABLE tmp_status_as_of_keys AS
SELECT
    ed.employee_id,
    ed.effective_date,
    (SELECT MAX(wj2.transaction_effective_date)
     FROM tmp_worker_status_job wj2
     WHERE wj2.employee_id = ed.employee_id
       AND wj2.transaction_effective_date <= ed.effective_date
       AND wj2.rn = 1
    ) AS wj_as_of_date
FROM tmp_status_effective_dates ed;

-- Step 4: Assemble status rows via EXACT equality join (fixed: prevents cartesian product)
CREATE TEMP TABLE tmp_assembled_status_rows AS
SELECT
    aok.employee_id,
    aok.effective_date,
    wj.active_status_date,
    wj.benefits_service_date,
    wj.continuous_service_date,
    wj.planned_end_contract_date,
    wj.hire_date,
    wj.eligible_for_rehire,
    wj.not_eligible_for_hire,
    wj.active,
    wj.worker_status,
    wj.employment_end_date,
    wj.hire_reason,
    wj.hire_rescinded,
    wj.original_hire_date,
    wj.primary_termination_category,
    wj.primary_termination_reason,
    wj.retired,
    wj.retirement_eligibility_date,
    wj.expected_retirement_date,
    wj.seniority_date,
    wj.termination_date
FROM tmp_status_as_of_keys aok
LEFT JOIN tmp_worker_status_job wj
    ON aok.employee_id = wj.employee_id
    AND wj.transaction_effective_date = aok.wj_as_of_date
    AND wj.rn = 1;

-- Step 5: Frame SCD2 windows and compute hash_diff
CREATE TEMP TABLE stg_dim_worker_status AS
WITH windowed_rows AS (
    SELECT
        *,
        effective_date AS effective_date_from,
        COALESCE(LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date), '9999-12-31'::DATE) - 1 AS effective_date_to
    FROM tmp_assembled_status_rows
)
SELECT
    employee_id,
    effective_date,
    active_status_date, benefits_service_date, continuous_service_date,
    planned_end_contract_date, hire_date, eligible_for_rehire, not_eligible_for_hire,
    active::INT::VARCHAR, worker_status, employment_end_date, hire_reason, hire_rescinded,
    original_hire_date, primary_termination_category, primary_termination_reason,
    retired::INT::VARCHAR, retirement_eligibility_date, expected_retirement_date,
    seniority_date, termination_date,
    effective_date_from,
    effective_date_to,
    effective_date_from AS valid_from,
    MD5(COALESCE(active_status_date::VARCHAR, '') || COALESCE(benefits_service_date::VARCHAR, '') || COALESCE(continuous_service_date::VARCHAR, '') || COALESCE(planned_end_contract_date::VARCHAR, '') || COALESCE(hire_date::VARCHAR, '') || COALESCE(eligible_for_rehire::VARCHAR, '') || COALESCE(not_eligible_for_hire::INT::VARCHAR, '') || COALESCE(active::INT::VARCHAR, '') || COALESCE(worker_status::VARCHAR, '') || COALESCE(employment_end_date::VARCHAR, '') || COALESCE(hire_reason::VARCHAR, '') || COALESCE(hire_rescinded::INT::VARCHAR, '') || COALESCE(original_hire_date::VARCHAR, '') || COALESCE(primary_termination_category::VARCHAR, '') || COALESCE(primary_termination_reason::VARCHAR, '') || COALESCE(retired::INT::VARCHAR, '') || COALESCE(retirement_eligibility_date::VARCHAR, '') || COALESCE(expected_retirement_date::VARCHAR, '') || COALESCE(seniority_date::VARCHAR, '') || COALESCE(termination_date::VARCHAR, '')) AS hash_diff
FROM windowed_rows;

-- Step 6: SCD2 merge logic
-- Close changed records in target
UPDATE l3_workday.dim_worker_status_d tgt
SET valid_to = stg.valid_from - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
FROM stg_dim_worker_status stg
WHERE tgt.employee_id = stg.employee_id
  AND tgt.effective_date = stg.effective_date
  AND tgt.is_current = TRUE
  AND tgt.hash_diff <> stg.hash_diff;

-- Insert new and changed records
INSERT INTO l3_workday.dim_worker_status_d (
    employee_id, effective_date,
    active_status_date, benefits_service_date, continuous_service_date,
    planned_end_contract_date, hire_date, eligible_for_rehire, not_eligible_for_hire,
    active, worker_status, employment_end_date, hire_reason, hire_rescinded,
    original_hire_date, primary_termination_category, primary_termination_reason,
    retired, retirement_eligibility_date, expected_retirement_date,
    seniority_date, termination_date,
    effective_date_from, effective_date_to, valid_from, valid_to, is_current,
    hash_diff, insert_datetime, update_datetime, etl_batch_id
)
SELECT
    stg.employee_id, stg.effective_date,
    stg.active_status_date, stg.benefits_service_date, stg.continuous_service_date,
    stg.planned_end_contract_date, stg.hire_date, stg.eligible_for_rehire, stg.not_eligible_for_hire,
    stg.active, stg.worker_status, stg.employment_end_date, stg.hire_reason, stg.hire_rescinded,
    stg.original_hire_date, stg.primary_termination_category, stg.primary_termination_reason,
    stg.retired, stg.retirement_eligibility_date, stg.expected_retirement_date,
    stg.seniority_date, stg.termination_date,
    stg.effective_date_from, stg.effective_date_to, stg.valid_from, '9999-12-31'::DATE, TRUE,
    stg.hash_diff, GETDATE(), GETDATE(), '${ETL_BATCH_ID}'
FROM stg_dim_worker_status stg
LEFT JOIN l3_workday.dim_worker_status_d tgt
    ON stg.employee_id = tgt.employee_id
    AND stg.effective_date = tgt.effective_date
    AND tgt.is_current = TRUE
WHERE tgt.employee_id IS NULL
   OR tgt.hash_diff <> stg.hash_diff;

-- Handle deletes: mark historical records not in new source
UPDATE l3_workday.dim_worker_status_d
SET valid_to = CURRENT_DATE - 1,
    is_current = FALSE,
    update_datetime = GETDATE()
WHERE is_current = TRUE
  AND (employee_id, effective_date) NOT IN (
    SELECT employee_id, effective_date FROM stg_dim_worker_status
  );

-- Cleanup temp tables
DROP TABLE tmp_status_effective_dates;
DROP TABLE tmp_worker_status_job;
DROP TABLE tmp_status_as_of_keys;
DROP TABLE tmp_assembled_status_rows;
DROP TABLE stg_dim_worker_status;

COMMIT;

-- ============================================================================
-- END OF LOAD SCRIPT
-- ============================================================================
