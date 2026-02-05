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

WITH date_spine AS (
    -- Generate all dates from 2020-01-01 to 2030-12-31
    SELECT CAST(date_col AS DATE) AS calendar_date
    FROM (
        SELECT dateadd(day, row_number() OVER (ORDER BY 1) - 1, '2020-01-01'::DATE) AS date_col
        FROM (
            SELECT 1 AS n FROM (VALUES (1)) t(n)
            UNION ALL
            SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
        ) s1
        CROSS JOIN (
            SELECT 1 AS n FROM (VALUES (1)) t(n)
            UNION ALL
            SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
        ) s2
        CROSS JOIN (
            SELECT 1 AS n FROM (VALUES (1)) t(n)
            UNION ALL
            SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
        ) s3
        CROSS JOIN (
            SELECT 1 AS n FROM (VALUES (1)) t(n)
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
            WHEN calendar_date = dateadd(day, -1, dateadd(month, 1, dateadd(day, -day(calendar_date) + 1, calendar_date)))
            THEN TRUE
            ELSE FALSE
        END AS is_month_end,
        -- Quarter end
        CASE
            WHEN CAST(to_char(calendar_date, 'MM') AS INTEGER) IN (3, 6, 9, 12) AND
                 calendar_date = dateadd(day, -1, dateadd(month, 1, dateadd(day, -day(calendar_date) + 1, calendar_date)))
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
INSERT INTO l3_workday.dim_day_d (
    day_sk, calendar_date, day_of_week, day_name, day_of_month, day_of_year, week_of_year,
    month_number, month_name, quarter_number, quarter_name, year_number, fiscal_year,
    fiscal_quarter, fiscal_quarter_name, is_weekend, is_month_end, is_quarter_end, is_year_end,
    insert_datetime
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
    MD5(CONCAT(
        COALESCE(company_wid, ''),
        COALESCE(company_code, ''),
        COALESCE(company_name, ''),
        COALESCE(company_subtype, ''),
        COALESCE(company_currency, ''),
        COALESCE(business_unit, '')
    )) AS hash_diff,
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
    MD5(CONCAT(
        COALESCE(cost_center_wid, ''),
        COALESCE(cost_center_code, ''),
        COALESCE(cost_center_name, ''),
        COALESCE(hierarchy, ''),
        COALESCE(subtype, '')
    )) AS hash_diff,
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
    MD5(CONCAT(
        COALESCE(grade_id, ''),
        COALESCE(grade_name, ''),
        COALESCE(grade_profile_currency_code, ''),
        COALESCE(effective_date::VARCHAR, ''),
        COALESCE(grade_profile_name, ''),
        COALESCE(grade_profile_number_of_segements::VARCHAR, ''),
        COALESCE(grade_profile_salary_range_maximum::VARCHAR, ''),
        COALESCE(grade_profile_salary_range_midpoint::VARCHAR, ''),
        COALESCE(grade_profile_salary_range_minimjum::VARCHAR, ''),
        COALESCE(grade_profile_segement_1_top::VARCHAR, ''),
        COALESCE(grade_profile_segement_2_top::VARCHAR, ''),
        COALESCE(grade_profile_segement_3_top::VARCHAR, ''),
        COALESCE(grade_profile_segement_4_top::VARCHAR, ''),
        COALESCE(grade_profile_segement_5_top::VARCHAR, '')
    )) AS hash_diff,
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
    jp21.inactive_flag,
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
    jp21.public_job,
    jp21.work_shift_required,
    jp21.job_matrix,
    jp21.is_people_manager,
    jp21.is_manager,
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
    MD5(CONCAT(
        COALESCE(jp21.compensation_grade, ''),
        COALESCE(jp21.critical_job_flag, ''),
        COALESCE(jp21.difficult_to_fill_flag, ''),
        COALESCE(jp21.inactive_flag, ''),
        COALESCE(jp21.job_category_code, ''),
        COALESCE(jp21.job_category_name, ''),
        COALESCE(jp21.job_exempt_canada, ''),
        COALESCE(jp21.job_exempt_us, ''),
        COALESCE(jp21.job_family, ''),
        COALESCE(jp21.job_family_group, ''),
        COALESCE(jp21.job_family_group_name, ''),
        COALESCE(jp21.job_family_name, ''),
        COALESCE(jp21.job_level_code, ''),
        COALESCE(jp21.job_level_name, ''),
        COALESCE(jp21.job_profile_code, ''),
        COALESCE(jp21.job_profile_description, ''),
        COALESCE(jp21.job_profile_name, ''),
        COALESCE(jp21.job_profile_summary, ''),
        COALESCE(jp21.job_profile_wid, ''),
        COALESCE(jp21.job_title, ''),
        COALESCE(jp21.management_level_code, ''),
        COALESCE(jp21.management_level_name, ''),
        COALESCE(jp21.pay_rate_type, ''),
        COALESCE(jp21.public_job, ''),
        COALESCE(jp21.work_shift_required, ''),
        COALESCE(jp21.job_matrix, ''),
        COALESCE(jp21.is_people_manager, ''),
        COALESCE(jp21.is_manager, ''),
        COALESCE(jp21.frequency, ''),
        COALESCE(jp22.aap_job_group, ''),
        COALESCE(jp22.bonus_eligibility, ''),
        COALESCE(jp22.customer_facing, ''),
        COALESCE(jp22.eeo1_code, ''),
        COALESCE(jp22.job_collection, ''),
        COALESCE(jp22.loan_originator_code, ''),
        COALESCE(jp22.national_occupation_code, ''),
        COALESCE(jp22.occupation_code, ''),
        COALESCE(jp22.recruitment_channel, ''),
        COALESCE(jp22.standard_occupation_code, ''),
        COALESCE(jp22.stock, '')
    )) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6021_job_profile jp21
LEFT JOIN l1_workday.int6022_job_profile_details jp22
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
    MD5(CONCAT(
        COALESCE(location_wid, ''),
        COALESCE(location_name, ''),
        COALESCE(inactive, ''),
        COALESCE(address_line_1, ''),
        COALESCE(address_line_2, ''),
        COALESCE(city, ''),
        COALESCE(region, ''),
        COALESCE(region_name, ''),
        COALESCE(country, ''),
        COALESCE(country_name, ''),
        COALESCE(location_postal_code, ''),
        COALESCE(location_identifier, ''),
        COALESCE(latitude::VARCHAR, ''),
        COALESCE(longitude::VARCHAR, ''),
        COALESCE(location_type, ''),
        COALESCE(location_usage_type, ''),
        COALESCE(trade_name, ''),
        COALESCE(worksite_id_code, '')
    )) AS hash_diff,
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
    active,
    parent_dept_id,
    owner_ein,
    department_level,
    primary_location_code,
    type,
    subtype,
    MD5(CONCAT(
        COALESCE(department_wid, ''),
        COALESCE(department_name, ''),
        COALESCE(dept_name_with_manager_name, ''),
        COALESCE(active, ''),
        COALESCE(parent_dept_id, ''),
        COALESCE(owner_ein, ''),
        COALESCE(department_level, ''),
        COALESCE(primary_location_code, ''),
        COALESCE(type, ''),
        COALESCE(subtype, '')
    )) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6028_supervisory_organization
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
    MD5(CONCAT(
        COALESCE(supervisory_organization, ''),
        COALESCE(effective_date::VARCHAR, ''),
        COALESCE(reason, ''),
        COALESCE(worker_type, ''),
        COALESCE(worker_sub_type, ''),
        COALESCE(job_profile, ''),
        COALESCE(job_title, ''),
        COALESCE(business_title, ''),
        COALESCE(time_type, ''),
        COALESCE(location, '')
    )) AS hash_diff,
    CURRENT_DATE AS valid_from
FROM l1_workday.int6032_position
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
FROM l1_workday.l1_workday_worker_job_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num
UNION
SELECT DISTINCT employee_id, transaction_effective_date AS effective_date
FROM l1_workday.l1_workday_worker_comp_dly_vw
WHERE idp_obsolete_date IS NULL
UNION
SELECT DISTINCT employee_id, transaction_effective_date AS effective_date
FROM l1_workday.l1_workday_worker_organization_dly_vw
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
    active,
    first_day_of_work,
    expected_date_of_return,
    not_returning,
    return_unknown,
    probation_start_date,
    probation_end_date,
    academic_tenure_date,
    has_international_assignment,
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
    soft_retirement_indicator,
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
FROM l1_workday.l1_workday_worker_job_dly_vw
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
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_date DESC) AS rn
FROM l1_workday.l1_workday_worker_comp_dly_vw
WHERE idp_obsolete_date IS NULL;

-- Step 6.1.4: Prepare filtered Worker Organization data (3 pivots: Cost Centre, Company, Supervisory)
CREATE TEMP TABLE tmp_worker_org_cost_centre AS
SELECT
    employee_id,
    transaction_effective_date,
    organization_id AS cost_center_id,
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_date DESC) AS rn
FROM l1_workday.l1_workday_worker_organization_dly_vw
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
FROM l1_workday.l1_workday_worker_organization_dly_vw
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
FROM l1_workday.l1_workday_worker_organization_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num
  AND organization_type = 'Supervisory';

-- Step 6.1.5: Assemble enriched row via as-of joins
CREATE TEMP TABLE tmp_assembled_rows AS
SELECT
    ed.employee_id,
    ed.effective_date,
    -- Worker Job attributes (as-of join)
    COALESCE(wj.position_id, NULL) AS position_id,
    COALESCE(wj.worker_type, NULL) AS worker_type,
    COALESCE(wj.worker_sub_type, NULL) AS worker_sub_type,
    COALESCE(wj.business_title, NULL) AS business_title,
    COALESCE(wj.business_site_id, NULL) AS business_site_id,
    COALESCE(wj.mailstop_floor, NULL) AS mailstop_floor,
    COALESCE(wj.worker_status, NULL) AS worker_status,
    COALESCE(wj.active, NULL) AS active,
    COALESCE(wj.first_day_of_work, NULL) AS first_day_of_work,
    COALESCE(wj.expected_date_of_return, NULL) AS expected_date_of_return,
    COALESCE(wj.not_returning, NULL) AS not_returning,
    COALESCE(wj.return_unknown, NULL) AS return_unknown,
    COALESCE(wj.probation_start_date, NULL) AS probation_start_date,
    COALESCE(wj.probation_end_date, NULL) AS probation_end_date,
    COALESCE(wj.academic_tenure_date, NULL) AS academic_tenure_date,
    COALESCE(wj.has_international_assignment, NULL) AS has_international_assignment,
    COALESCE(wj.home_country, NULL) AS home_country,
    COALESCE(wj.host_country, NULL) AS host_country,
    COALESCE(wj.international_assignment_type, NULL) AS international_assignment_type,
    COALESCE(wj.start_date_of_international_assignment, NULL) AS start_date_of_international_assignment,
    COALESCE(wj.end_date_of_international_assignment, NULL) AS end_date_of_international_assignment,
    COALESCE(wj.action, NULL) AS action,
    COALESCE(wj.action_code, NULL) AS action_code,
    COALESCE(wj.action_reason, NULL) AS action_reason,
    COALESCE(wj.action_reason_code, NULL) AS action_reason_code,
    COALESCE(wj.manager_id, NULL) AS manager_id,
    COALESCE(wj.soft_retirement_indicator, NULL) AS soft_retirement_indicator,
    COALESCE(wj.job_profile_id, NULL) AS job_profile_id,
    COALESCE(wj.sequence_number, NULL) AS sequence_number,
    COALESCE(wj.planned_end_contract_date, NULL) AS planned_end_contract_date,
    COALESCE(wj.job_entry_dt, NULL) AS job_entry_dt,
    COALESCE(wj.stock_grants, NULL) AS stock_grants,
    COALESCE(wj.time_type, NULL) AS time_type,
    COALESCE(wj.supervisory_organization, NULL) AS supervisory_organization,
    COALESCE(wj.location, NULL) AS location,
    COALESCE(wj.job_title, NULL) AS job_title,
    COALESCE(wj.french_job_title, NULL) AS french_job_title,
    COALESCE(wj.shift_number, NULL) AS shift_number,
    COALESCE(wj.scheduled_weekly_hours, NULL) AS scheduled_weekly_hours,
    COALESCE(wj.default_weekly_hours, NULL) AS default_weekly_hours,
    COALESCE(wj.scheduled_fte, NULL) AS scheduled_fte,
    COALESCE(wj.work_model_start_date, NULL) AS work_model_start_date,
    COALESCE(wj.work_model_type, NULL) AS work_model_type,
    COALESCE(wj.worker_workday_id, NULL) AS worker_workday_id,
    COALESCE(wj.idp_employee_status, NULL) AS idp_employee_status,
    -- Worker Compensation attributes (as-of join)
    COALESCE(wc.compensation_package_proposed, NULL) AS compensation_package_proposed,
    COALESCE(wc.compensation_grade_proposed, NULL) AS compensation_grade_proposed,
    COALESCE(wc.comp_grade_profile_proposed, NULL) AS comp_grade_profile_proposed,
    COALESCE(wc.compensation_step_proposed, NULL) AS compensation_step_proposed,
    COALESCE(wc.pay_range_minimum, NULL) AS pay_range_minimum,
    COALESCE(wc.pay_range_midpoint, NULL) AS pay_range_midpoint,
    COALESCE(wc.pay_range_maximum, NULL) AS pay_range_maximum,
    COALESCE(wc.base_pay_proposed_amount, NULL) AS base_pay_proposed_amount,
    COALESCE(wc.base_pay_proposed_currency, NULL) AS base_pay_proposed_currency,
    COALESCE(wc.base_pay_proposed_frequency, NULL) AS base_pay_proposed_frequency,
    COALESCE(wc.benefits_annual_rate_abbr, NULL) AS benefits_annual_rate_abbr,
    COALESCE(wc.pay_rate_type, NULL) AS pay_rate_type,
    COALESCE(wc.compensation, NULL) AS compensation,
    -- Worker Organization attributes (resolved)
    COALESCE(cc.cost_center_id, NULL) AS cost_center_id,
    COALESCE(co.company_id, NULL) AS company_id,
    COALESCE(so.sup_org_id, NULL) AS sup_org_id
FROM tmp_effective_dates ed
LEFT JOIN tmp_worker_job wj
    ON ed.employee_id = wj.employee_id
    AND wj.transaction_effective_date <= ed.effective_date
    AND wj.rn = 1
LEFT JOIN tmp_worker_comp wc
    ON ed.employee_id = wc.employee_id
    AND wc.transaction_effective_date <= ed.effective_date
    AND wc.rn = 1
LEFT JOIN tmp_worker_org_cost_centre cc
    ON ed.employee_id = cc.employee_id
    AND cc.transaction_effective_date <= ed.effective_date
    AND cc.rn = 1
LEFT JOIN tmp_worker_org_company co
    ON ed.employee_id = co.employee_id
    AND co.transaction_effective_date <= ed.effective_date
    AND co.rn = 1
LEFT JOIN tmp_worker_org_supervisory so
    ON ed.employee_id = so.employee_id
    AND so.transaction_effective_date <= ed.effective_date
    AND so.rn = 1;

-- Step 6.1.6: Frame SCD2 windows and compute hash_diff
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
    MD5(CONCAT(
        COALESCE(position_id, ''),
        COALESCE(worker_type, ''),
        COALESCE(worker_sub_type, ''),
        COALESCE(business_title, ''),
        COALESCE(business_site_id, ''),
        COALESCE(mailstop_floor, ''),
        COALESCE(worker_status, ''),
        COALESCE(active::VARCHAR, ''),
        COALESCE(first_day_of_work::VARCHAR, ''),
        COALESCE(expected_date_of_return::VARCHAR, ''),
        COALESCE(not_returning::VARCHAR, ''),
        COALESCE(return_unknown, ''),
        COALESCE(probation_start_date::VARCHAR, ''),
        COALESCE(probation_end_date::VARCHAR, ''),
        COALESCE(academic_tenure_date::VARCHAR, ''),
        COALESCE(has_international_assignment::VARCHAR, ''),
        COALESCE(home_country, ''),
        COALESCE(host_country, ''),
        COALESCE(international_assignment_type, ''),
        COALESCE(start_date_of_international_assignment::VARCHAR, ''),
        COALESCE(end_date_of_international_assignment::VARCHAR, ''),
        COALESCE(action, ''),
        COALESCE(action_code, ''),
        COALESCE(action_reason, ''),
        COALESCE(action_reason_code, ''),
        COALESCE(manager_id, ''),
        COALESCE(soft_retirement_indicator::VARCHAR, ''),
        COALESCE(job_profile_id, ''),
        COALESCE(sequence_number::VARCHAR, ''),
        COALESCE(planned_end_contract_date::VARCHAR, ''),
        COALESCE(job_entry_dt::VARCHAR, ''),
        COALESCE(stock_grants, ''),
        COALESCE(time_type, ''),
        COALESCE(supervisory_organization, ''),
        COALESCE(location, ''),
        COALESCE(job_title, ''),
        COALESCE(french_job_title, ''),
        COALESCE(shift_number::VARCHAR, ''),
        COALESCE(scheduled_weekly_hours::VARCHAR, ''),
        COALESCE(default_weekly_hours::VARCHAR, ''),
        COALESCE(scheduled_fte::VARCHAR, ''),
        COALESCE(work_model_start_date::VARCHAR, ''),
        COALESCE(work_model_type, ''),
        COALESCE(worker_workday_id, ''),
        COALESCE(idp_employee_status, ''),
        COALESCE(compensation_package_proposed, ''),
        COALESCE(compensation_grade_proposed, ''),
        COALESCE(comp_grade_profile_proposed, ''),
        COALESCE(compensation_step_proposed, ''),
        COALESCE(pay_range_minimum::VARCHAR, ''),
        COALESCE(pay_range_midpoint::VARCHAR, ''),
        COALESCE(pay_range_maximum::VARCHAR, ''),
        COALESCE(base_pay_proposed_amount::VARCHAR, ''),
        COALESCE(base_pay_proposed_currency, ''),
        COALESCE(base_pay_proposed_frequency, ''),
        COALESCE(benefits_annual_rate_abbr::VARCHAR, ''),
        COALESCE(pay_rate_type, ''),
        COALESCE(compensation::VARCHAR, ''),
        COALESCE(cost_center_id, ''),
        COALESCE(company_id, ''),
        COALESCE(sup_org_id, '')
    )) AS hash_diff
FROM windowed_rows;

-- Step 6.1.7: Mark is_current_job_row (most recent per employee)
CREATE TEMP TABLE stg_dim_worker_job_final AS
SELECT
    *,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date DESC) = 1
        THEN TRUE
        ELSE FALSE
    END AS is_current_job_row
FROM stg_dim_worker_job;

-- Step 6.1.8: SCD2 merge logic
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
FROM l1_workday.l1_workday_worker_job_dly_vw
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
    not_eligible_for_hire,
    active,
    worker_status,
    employment_end_date,
    hire_reason,
    hire_rescinded,
    original_hire_date,
    primary_termination_category,
    primary_termination_reason,
    retired,
    retirement_eligibility_date,
    expected_retirement_date,
    seniority_date,
    termination_date,
    ROW_NUMBER() OVER (PARTITION BY employee_id, transaction_effective_date ORDER BY transaction_entry_date DESC) AS rn
FROM l1_workday.l1_workday_worker_job_dly_vw
WHERE idp_obsolete_date IS NULL
  AND transaction_entry_date = idp_max_entry_ts
  AND sequence_number = idp_min_seq_num;

-- Step 3: Assemble status rows via as-of join
CREATE TEMP TABLE tmp_assembled_status_rows AS
SELECT
    ed.employee_id,
    ed.effective_date,
    COALESCE(wj.active_status_date, NULL) AS active_status_date,
    COALESCE(wj.benefits_service_date, NULL) AS benefits_service_date,
    COALESCE(wj.continuous_service_date, NULL) AS continuous_service_date,
    COALESCE(wj.planned_end_contract_date, NULL) AS planned_end_contract_date,
    COALESCE(wj.hire_date, NULL) AS hire_date,
    COALESCE(wj.eligible_for_rehire, NULL) AS eligible_for_rehire,
    COALESCE(wj.not_eligible_for_hire, NULL) AS not_eligible_for_hire,
    COALESCE(wj.active, NULL) AS active,
    COALESCE(wj.worker_status, NULL) AS worker_status,
    COALESCE(wj.employment_end_date, NULL) AS employment_end_date,
    COALESCE(wj.hire_reason, NULL) AS hire_reason,
    COALESCE(wj.hire_rescinded, NULL) AS hire_rescinded,
    COALESCE(wj.original_hire_date, NULL) AS original_hire_date,
    COALESCE(wj.primary_termination_category, NULL) AS primary_termination_category,
    COALESCE(wj.primary_termination_reason, NULL) AS primary_termination_reason,
    COALESCE(wj.retired, NULL) AS retired,
    COALESCE(wj.retirement_eligibility_date, NULL) AS retirement_eligibility_date,
    COALESCE(wj.expected_retirement_date, NULL) AS expected_retirement_date,
    COALESCE(wj.seniority_date, NULL) AS seniority_date,
    COALESCE(wj.termination_date, NULL) AS termination_date
FROM tmp_status_effective_dates ed
LEFT JOIN tmp_worker_status_job wj
    ON ed.employee_id = wj.employee_id
    AND wj.transaction_effective_date <= ed.effective_date
    AND wj.rn = 1;

-- Step 4: Frame SCD2 windows and compute hash_diff
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
    active, worker_status, employment_end_date, hire_reason, hire_rescinded,
    original_hire_date, primary_termination_category, primary_termination_reason,
    retired, retirement_eligibility_date, expected_retirement_date,
    seniority_date, termination_date,
    effective_date_from,
    effective_date_to,
    effective_date_from AS valid_from,
    MD5(CONCAT(
        COALESCE(active_status_date::VARCHAR, ''),
        COALESCE(benefits_service_date::VARCHAR, ''),
        COALESCE(continuous_service_date::VARCHAR, ''),
        COALESCE(planned_end_contract_date::VARCHAR, ''),
        COALESCE(hire_date::VARCHAR, ''),
        COALESCE(eligible_for_rehire, ''),
        COALESCE(not_eligible_for_hire::VARCHAR, ''),
        COALESCE(active::VARCHAR, ''),
        COALESCE(worker_status, ''),
        COALESCE(employment_end_date::VARCHAR, ''),
        COALESCE(hire_reason, ''),
        COALESCE(hire_rescinded::VARCHAR, ''),
        COALESCE(original_hire_date::VARCHAR, ''),
        COALESCE(primary_termination_category, ''),
        COALESCE(primary_termination_reason, ''),
        COALESCE(retired::VARCHAR, ''),
        COALESCE(retirement_eligibility_date::VARCHAR, ''),
        COALESCE(expected_retirement_date::VARCHAR, ''),
        COALESCE(seniority_date::VARCHAR, ''),
        COALESCE(termination_date::VARCHAR, '')
    )) AS hash_diff
FROM windowed_rows;

-- Step 5: SCD2 merge logic
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
DROP TABLE tmp_assembled_status_rows;
DROP TABLE stg_dim_worker_status;

COMMIT;

-- ============================================================================
-- END OF LOAD SCRIPT
-- ============================================================================
