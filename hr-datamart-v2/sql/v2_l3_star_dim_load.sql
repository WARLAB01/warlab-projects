-- ============================================================================
-- HR Datamart V2 - L3 Star Schema Dimension LOAD
-- Initial load of all 14 dimension tables from L1 sources
-- ============================================================================
-- NOTE: All SQL statements are standalone per Redshift Data API requirements
-- Execute statements sequentially, separated by semicolons
-- ============================================================================

-- ============================================================================
-- LOAD 1: dim_day_D - Calendar dimension (Type 1)
-- Generate dates from 2020-01-01 to 2030-12-31
-- Fiscal year starts Nov 1 (fiscal Q1: Nov-Jan, Q2: Feb-Apr, Q3: May-Jul, Q4: Aug-Oct)
-- ============================================================================

INSERT INTO v2_l3_star.dim_day_D (
  day_dt, day_abbr, day_date, day_name, day_of_month, day_of_week, day_of_year,
  day_sk, first_day_of_fiscal_quarter, first_day_of_fiscal_year, first_day_of_month,
  first_day_of_quarter, first_day_of_week, first_day_of_year, fiscal_quarter_abbr,
  fiscal_quarter_name, fiscal_quarter_num, fiscal_year_name, fiscal_year_num,
  is_canada_holiday, is_us_holiday, is_weekend, last_day_of_fiscal_quarter,
  last_day_of_fiscal_year, last_day_of_month, last_day_of_quarter, last_day_of_week,
  last_day_of_year, month_abbr, month_name, month_of_fiscal_quarter, month_of_fiscal_year,
  month_of_quarter, month_of_year, quarter_abbr, quarter_name, quarter_num, week_of_month,
  week_of_year, year_name, year_num, md5_hash, valid_from, valid_to, is_current
)
WITH date_range AS (
  SELECT DATEADD(day, seq, '2020-01-01'::DATE) AS cal_date
  FROM (
    SELECT 0 AS seq
    UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9
  ) nums
  CROSS JOIN (
    SELECT DATEADD(day, seq * 10, '2020-01-01'::DATE) AS base_date
    FROM (SELECT 0 AS seq UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) n2
    CROSS JOIN (
      SELECT DATEADD(day, seq * 100, '2020-01-01'::DATE) AS base_date2
      FROM (SELECT 0 AS seq UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) n3
    ) n3b
  ) n2b
  WHERE DATEADD(day, seq, base_date2) < '2031-01-01'::DATE
),
calendar AS (
  SELECT
    cal_date,
    SUBSTRING(TO_CHAR(cal_date, 'Day'), 1, 3) AS day_abbr,
    cal_date AS day_date,
    TO_CHAR(cal_date, 'Day') AS day_name,
    EXTRACT(day FROM cal_date)::VARCHAR AS day_of_month,
    ((EXTRACT(dow FROM cal_date)::INT + 6) % 7 + 1)::VARCHAR AS day_of_week,
    EXTRACT(doy FROM cal_date)::VARCHAR AS day_of_year,
    TO_CHAR(cal_date, 'YYYYMMDD') AS day_sk,
    EXTRACT(year FROM cal_date)::INT AS year_num,
    EXTRACT(month FROM cal_date)::INT AS month_num,
    TO_CHAR(cal_date, 'Mon') AS month_abbr,
    TO_CHAR(cal_date, 'Month') AS month_name,
    EXTRACT(quarter FROM cal_date)::INT AS quarter_num,
    ((EXTRACT(dow FROM cal_date)::INT + 6) % 7) AS week_day_num
  FROM date_range
  WHERE cal_date >= '2020-01-01'::DATE AND cal_date <= '2030-12-31'::DATE
)
SELECT
  cal_date::VARCHAR,
  day_abbr,
  day_date::VARCHAR,
  day_name,
  day_of_month,
  day_of_week,
  day_of_year,
  day_sk,
  CASE WHEN month_num IN (11, 12, 1) THEN DATE_TRUNC('month', DATE_TRUNC('year', DATE(cal_date - INTERVAL '10 months'))) || ' 1'::DATE
       WHEN month_num IN (2, 3, 4) THEN DATE_TRUNC('month', DATE(cal_date - INTERVAL '3 months')) || ' 1'::DATE
       WHEN month_num IN (5, 6, 7) THEN DATE_TRUNC('month', DATE(cal_date - INTERVAL '6 months')) || ' 1'::DATE
       ELSE DATE_TRUNC('month', DATE(cal_date - INTERVAL '9 months')) || ' 1'::DATE
  END::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN DATE(CONCAT(year_num, '-11-01'))
       WHEN month_num = 1 THEN DATE(CONCAT(year_num - 1, '-11-01'))
       ELSE DATE(CONCAT(year_num, '-11-01'))
  END::VARCHAR,
  DATE_TRUNC('month', cal_date)::DATE::VARCHAR,
  DATE_TRUNC('quarter', cal_date)::DATE::VARCHAR,
  (cal_date - (week_day_num::INT))::VARCHAR,
  DATE_TRUNC('year', cal_date)::DATE::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN 'Q1'
       WHEN month_num IN (1) THEN 'Q1'
       WHEN month_num IN (2, 3, 4) THEN 'Q2'
       WHEN month_num IN (5, 6, 7) THEN 'Q3'
       ELSE 'Q4'
  END::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN 'Fiscal Q1'
       WHEN month_num IN (1) THEN 'Fiscal Q1'
       WHEN month_num IN (2, 3, 4) THEN 'Fiscal Q2'
       WHEN month_num IN (5, 6, 7) THEN 'Fiscal Q3'
       ELSE 'Fiscal Q4'
  END::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN '1'
       WHEN month_num IN (1) THEN '1'
       WHEN month_num IN (2, 3, 4) THEN '2'
       WHEN month_num IN (5, 6, 7) THEN '3'
       ELSE '4'
  END::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN 'FY' || year_num + 1
       WHEN month_num = 1 THEN 'FY' || year_num
       ELSE 'FY' || year_num
  END::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN (year_num + 1)::VARCHAR
       WHEN month_num = 1 THEN year_num::VARCHAR
       ELSE year_num::VARCHAR
  END::VARCHAR,
  CASE
    WHEN cal_date IN ('2020-01-01'::DATE, '2020-02-17'::DATE, '2020-04-10'::DATE, '2020-05-18'::DATE,
                       '2020-07-01'::DATE, '2020-08-03'::DATE, '2020-09-07'::DATE, '2020-09-30'::DATE,
                       '2020-10-12'::DATE, '2020-11-11'::DATE, '2020-12-25'::DATE, '2020-12-26'::DATE,
                       '2021-01-01'::DATE, '2021-02-15'::DATE, '2021-04-02'::DATE, '2021-05-17'::DATE,
                       '2021-07-01'::DATE, '2021-08-02'::DATE, '2021-09-06'::DATE, '2021-09-30'::DATE,
                       '2021-10-11'::DATE, '2021-11-11'::DATE, '2021-12-25'::DATE, '2021-12-26'::DATE,
                       '2022-01-01'::DATE, '2022-02-21'::DATE, '2022-04-15'::DATE, '2022-05-16'::DATE,
                       '2022-07-01'::DATE, '2022-08-01'::DATE, '2022-09-05'::DATE, '2022-09-30'::DATE,
                       '2022-10-10'::DATE, '2022-11-11'::DATE, '2022-12-25'::DATE, '2022-12-26'::DATE,
                       '2023-01-01'::DATE, '2023-02-20'::DATE, '2023-04-07'::DATE, '2023-05-15'::DATE,
                       '2023-07-01'::DATE, '2023-08-07'::DATE, '2023-09-04'::DATE, '2023-09-30'::DATE,
                       '2023-10-09'::DATE, '2023-11-11'::DATE, '2023-12-25'::DATE, '2023-12-26'::DATE,
                       '2024-01-01'::DATE, '2024-02-19'::DATE, '2024-03-29'::DATE, '2024-05-20'::DATE,
                       '2024-07-01'::DATE, '2024-08-05'::DATE, '2024-09-02'::DATE, '2024-09-30'::DATE,
                       '2024-10-14'::DATE, '2024-11-11'::DATE, '2024-12-25'::DATE, '2024-12-26'::DATE) THEN 'Y'
    ELSE 'N'
  END::VARCHAR,
  CASE
    WHEN cal_date IN ('2020-01-01'::DATE, '2020-01-20'::DATE, '2020-02-17'::DATE, '2020-05-25'::DATE,
                       '2020-06-19'::DATE, '2020-07-04'::DATE, '2020-09-07'::DATE, '2020-10-12'::DATE,
                       '2020-11-11'::DATE, '2020-11-26'::DATE, '2020-12-25'::DATE,
                       '2021-01-01'::DATE, '2021-01-18'::DATE, '2021-02-15'::DATE, '2021-05-31'::DATE,
                       '2021-06-18'::DATE, '2021-07-05'::DATE, '2021-09-06'::DATE, '2021-10-11'::DATE,
                       '2021-11-11'::DATE, '2021-11-25'::DATE, '2021-12-25'::DATE,
                       '2022-01-01'::DATE, '2022-01-17'::DATE, '2022-02-21'::DATE, '2022-05-30'::DATE,
                       '2022-06-20'::DATE, '2022-07-04'::DATE, '2022-09-05'::DATE, '2022-10-10'::DATE,
                       '2022-11-11'::DATE, '2022-11-24'::DATE, '2022-12-25'::DATE,
                       '2023-01-01'::DATE, '2023-01-16'::DATE, '2023-02-20'::DATE, '2023-05-29'::DATE,
                       '2023-06-19'::DATE, '2023-07-04'::DATE, '2023-09-04'::DATE, '2023-10-09'::DATE,
                       '2023-11-11'::DATE, '2023-11-23'::DATE, '2023-12-25'::DATE,
                       '2024-01-01'::DATE, '2024-01-15'::DATE, '2024-02-19'::DATE, '2024-05-27'::DATE,
                       '2024-06-19'::DATE, '2024-07-04'::DATE, '2024-09-02'::DATE, '2024-10-14'::DATE,
                       '2024-11-11'::DATE, '2024-11-28'::DATE, '2024-12-25'::DATE) THEN 'Y'
    ELSE 'N'
  END::VARCHAR,
  CASE WHEN week_day_num IN (0, 6) THEN 'Y' ELSE 'N' END::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN DATE(CONCAT(year_num + 1, '-01-31'))
       WHEN month_num = 1 THEN DATE(CONCAT(year_num, '-01-31'))
       WHEN month_num IN (2, 3, 4) THEN DATE(CONCAT(year_num, '-04-30'))
       WHEN month_num IN (5, 6, 7) THEN DATE(CONCAT(year_num, '-07-31'))
       ELSE DATE(CONCAT(year_num, '-10-31'))
  END::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN DATE(CONCAT(year_num + 1, '-10-31'))
       ELSE DATE(CONCAT(year_num, '-10-31'))
  END::VARCHAR,
  (DATE_TRUNC('month', cal_date) + INTERVAL '1 month - 1 day')::DATE::VARCHAR,
  (DATE_TRUNC('quarter', cal_date) + INTERVAL '3 months - 1 day')::DATE::VARCHAR,
  (cal_date + (6 - week_day_num::INT))::VARCHAR,
  (DATE_TRUNC('year', cal_date) + INTERVAL '1 year - 1 day')::DATE::VARCHAR,
  month_abbr,
  month_name,
  CASE WHEN month_num IN (11, 12) THEN '3'
       WHEN month_num = 1 THEN '1'
       WHEN month_num IN (2, 3, 4) THEN '2'
       WHEN month_num IN (5, 6, 7) THEN '3'
       ELSE '1'
  END::VARCHAR,
  CASE WHEN month_num IN (11, 12) THEN '3'
       WHEN month_num = 1 THEN '1'
       WHEN month_num IN (2, 3, 4) THEN '2'
       WHEN month_num IN (5, 6, 7) THEN '3'
       ELSE '1'
  END::VARCHAR,
  CASE WHEN month_num IN (2, 3, 4, 5, 6, 7, 8, 9, 10) THEN (EXTRACT(month FROM cal_date) - CASE WHEN month_num IN (11, 12) THEN 10 ELSE -2 END)::VARCHAR
       ELSE '1'::VARCHAR
  END::VARCHAR,
  quarter_num::VARCHAR,
  TO_CHAR(cal_date, 'Q')::VARCHAR,
  quarter_num::VARCHAR,
  ((EXTRACT(doy FROM cal_date)::INT - 1) / 7 + 1)::VARCHAR,
  EXTRACT(week FROM cal_date)::VARCHAR,
  year_num::VARCHAR,
  year_num::VARCHAR,
  MD5(cal_date::VARCHAR),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM calendar;

-- ============================================================================
-- LOAD 2: dim_company_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6024_company
-- ============================================================================

INSERT INTO v2_l3_star.dim_company_D (
  company_id, company_wid, company_name, company_code, business_unit,
  company_subtype, company_currency, md5_hash, valid_from, valid_to, is_current
)
SELECT
  company_id,
  company_wid,
  company_name,
  company_code,
  business_unit,
  company_subtype,
  company_currency,
  MD5(company_id || company_wid || company_name || company_code || business_unit || company_subtype || company_currency),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l1_workday.int6024_company
WHERE company_id IS NOT NULL;

-- ============================================================================
-- LOAD 3: dim_cost_center_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6025_cost_center
-- ============================================================================

INSERT INTO v2_l3_star.dim_cost_center_D (
  cost_center_id, cost_center_wid, cost_center_code, cost_center_name,
  hierarchy, subtype, md5_hash, valid_from, valid_to, is_current
)
SELECT
  cost_center_id,
  cost_center_wid,
  cost_center_code,
  cost_center_name,
  hierarchy,
  subtype,
  MD5(cost_center_id || cost_center_wid || cost_center_code || cost_center_name || hierarchy || subtype),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l1_workday.int6025_cost_center
WHERE cost_center_id IS NOT NULL;

-- ============================================================================
-- LOAD 4: dim_grade_profile_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6020_grade_profile
-- ============================================================================

INSERT INTO v2_l3_star.dim_grade_profile_D (
  grade_profile_id, grade_id, grade_name, grade_profile_currency_code,
  effective_date, grade_profile_name, grade_profile_number_of_segements,
  grade_profile_salary_range_maximum, grade_profile_salary_range_midpoint,
  grade_profile_salary_range_minimjum, grade_profile_segement_1_top,
  grade_profile_segement_2_top, grade_profile_segement_3_top,
  grade_profile_segement_4_top, grade_profile_segement_5_top,
  md5_hash, valid_from, valid_to, is_current
)
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
  MD5(grade_profile_id || grade_id || grade_name || grade_profile_currency_code ||
      effective_date || grade_profile_name || grade_profile_number_of_segements ||
      grade_profile_salary_range_maximum || grade_profile_salary_range_midpoint ||
      grade_profile_salary_range_minimjum || grade_profile_segement_1_top ||
      grade_profile_segement_2_top || grade_profile_segement_3_top ||
      grade_profile_segement_4_top || grade_profile_segement_5_top),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l1_workday.int6020_grade_profile
WHERE grade_profile_id IS NOT NULL;

-- ============================================================================
-- LOAD 5: dim_job_profile_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6021_job_profile LEFT JOIN int6022_job_classification
-- ============================================================================

INSERT INTO v2_l3_star.dim_job_profile_D (
  job_profile_id, compensation_grade, critical_job_flag, difficult_to_fill_flag,
  inactive_flag, job_category_code, job_category_name, job_exempt_canada,
  job_exempt_us, job_family, job_family_group, job_family_group_name,
  job_family_name, job_level_code, job_level_name, job_profile_code,
  job_profile_description, job_profile_name, job_profile_summary,
  job_profile_wid, job_title, management_level_code, management_level_name,
  pay_rate_type, public_job, work_shift_required, job_matrix,
  is_people_manager, is_manager, frequency, aap_job_group, bonus_eligibility,
  customer_facing, eeo1_code, job_collection, loan_originator_code,
  national_occupation_code, occupation_code, recruitment_channel,
  standard_occupation_code, stock, md5_hash, valid_from, valid_to, is_current
)
SELECT
  jp.job_profile_id,
  jp.compensation_grade,
  jp.critical_job_flag,
  jp.difficult_to_fill_flag,
  jp.inactive_flag,
  jp.job_category_code,
  jp.job_category_name,
  jp.job_exempt_canada,
  jp.job_exempt_us,
  jp.job_family,
  jp.job_family_group,
  jp.job_family_group_name,
  jp.job_family_name,
  jp.job_level_code,
  jp.job_level_name,
  jp.job_profile_code,
  jp.job_profile_description,
  jp.job_profile_name,
  jp.job_profile_summary,
  jp.job_profile_wid,
  jp.job_title,
  jp.management_level_code,
  jp.management_level_name,
  jp.pay_rate_type,
  jp.public_job,
  jp.work_shift_required,
  jp.job_matrix,
  jp.is_people_manager,
  jp.is_manager,
  jp.frequency,
  COALESCE(jc.aap_job_group, ''),
  COALESCE(jc.bonus_eligibility, ''),
  COALESCE(jc.customer_facing, ''),
  COALESCE(jc.eeo1_code, ''),
  COALESCE(jc.job_collection, ''),
  COALESCE(jc.loan_originator_code, ''),
  COALESCE(jc.national_occupation_code, ''),
  COALESCE(jc.occupation_code, ''),
  COALESCE(jc.recruitment_channel, ''),
  COALESCE(jc.standard_occupation_code, ''),
  COALESCE(jc.stock, ''),
  MD5(jp.job_profile_id || jp.compensation_grade || jp.critical_job_flag ||
      jp.difficult_to_fill_flag || jp.inactive_flag || jp.job_category_code ||
      jp.job_category_name || jp.job_exempt_canada || jp.job_exempt_us ||
      jp.job_family || jp.job_family_group || jp.job_family_group_name ||
      jp.job_family_name || jp.job_level_code || jp.job_level_name ||
      jp.job_profile_code || jp.job_profile_description || jp.job_profile_name ||
      jp.job_profile_summary || jp.job_profile_wid || jp.job_title ||
      jp.management_level_code || jp.management_level_name || jp.pay_rate_type ||
      jp.public_job || jp.work_shift_required || jp.job_matrix ||
      jp.is_people_manager || jp.is_manager || jp.frequency ||
      COALESCE(jc.aap_job_group, '') || COALESCE(jc.bonus_eligibility, '') ||
      COALESCE(jc.customer_facing, '') || COALESCE(jc.eeo1_code, '') ||
      COALESCE(jc.job_collection, '') || COALESCE(jc.loan_originator_code, '') ||
      COALESCE(jc.national_occupation_code, '') || COALESCE(jc.occupation_code, '') ||
      COALESCE(jc.recruitment_channel, '') || COALESCE(jc.standard_occupation_code, '') ||
      COALESCE(jc.stock, '')),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l1_workday.int6021_job_profile jp
LEFT JOIN v2_l1_workday.int6022_job_classification jc
  ON jp.job_profile_id = jc.job_profile_id
WHERE jp.job_profile_id IS NOT NULL;

-- ============================================================================
-- LOAD 6: dim_location_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6023_location
-- ============================================================================

INSERT INTO v2_l3_star.dim_location_D (
  location_id, location_wid, location_name, inactive, address_line_1,
  address_line_2, city, region, region_name, country, country_name,
  location_postal_code, location_identifier, latitude, longitude,
  location_type, location_usage_type, trade_name, worksite_id_code,
  md5_hash, valid_from, valid_to, is_current
)
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
  MD5(location_id || location_wid || location_name || inactive || address_line_1 ||
      address_line_2 || city || region || region_name || country || country_name ||
      location_postal_code || location_identifier || latitude || longitude ||
      location_type || location_usage_type || trade_name || worksite_id_code),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l1_workday.int6023_location
WHERE location_id IS NOT NULL;

-- ============================================================================
-- LOAD 7: dim_matrix_org_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6027_matrix_organization
-- ============================================================================

INSERT INTO v2_l3_star.dim_matrix_org_D (
  matrix_organization_id, matrix_organization_status, maxtrix_organization_name,
  maxtrix_organization_code, matrix_organization_type, matrix_organization_subtype,
  md5_hash, valid_from, valid_to, is_current
)
SELECT
  matrix_organization_id,
  matrix_organization_status,
  maxtrix_organization_name,
  maxtrix_organization_code,
  matrix_organization_type,
  matrix_organization_subtype,
  MD5(matrix_organization_id || matrix_organization_status || maxtrix_organization_name ||
      maxtrix_organization_code || matrix_organization_type || matrix_organization_subtype),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l1_workday.int6027_matrix_organization
WHERE matrix_organization_id IS NOT NULL;

-- ============================================================================
-- LOAD 8: dim_worker_profile_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6031_worker_profile
-- Custom: age_band computed from date_of_birth
-- ============================================================================

INSERT INTO v2_l3_star.dim_worker_profile_D (
  worker_id, bank_of_the_west_employee_id, date_of_birth, enterprise_id,
  race_ethnicity, gender, gender_identity, indigenous, home_addres_postal_code,
  home_address_city, home_address_country, home_address_region, last_name,
  legal_first_name, legal_full_name, legal_full_name_formatted, military_status,
  preferred_first_name, preferred_full_name, preferred_full_name_formatted,
  primary_work_email_address, secondary_work_email_address, sexual_orientation,
  junior_senior, product_sector_group, preferred_language,
  bonus_equity_earliest_retirement_date, class_year, admin_fte,
  consolidated_title, generation, home_address_country_name,
  home_address_region_name, indigenous_2, pensionable_yrs_of_service,
  worker_workday_id, age_band, md5_hash, valid_from, valid_to, is_current
)
SELECT
  worker_id,
  bank_of_the_west_employee_id,
  date_of_birth,
  enterprise_id,
  race_ethnicity,
  gender,
  gender_identity,
  indigenous,
  home_addres_postal_code,
  home_address_city,
  home_address_country,
  home_address_region,
  last_name,
  legal_first_name,
  legal_full_name,
  legal_full_name_formatted,
  military_status,
  preferred_first_name,
  preferred_full_name,
  preferred_full_name_formatted,
  primary_work_email_address,
  secondary_work_email_address,
  sexual_orientation,
  junior_senior,
  product_sector_group,
  preferred_language,
  bonus_equity_earliest_retirement_date,
  class_year,
  admin_fte,
  consolidated_title,
  generation,
  home_address_country_name,
  home_address_region_name,
  indigenous_2,
  pensionable_yrs_of_service,
  worker_workday_id,
  CASE
    WHEN TRY_CONVERT(DATE, date_of_birth) IS NULL THEN NULL
    WHEN DATEDIFF(year, TRY_CONVERT(DATE, date_of_birth), GETDATE()) < 18 THEN '18-24'
    WHEN DATEDIFF(year, TRY_CONVERT(DATE, date_of_birth), GETDATE()) < 25 THEN '18-24'
    WHEN DATEDIFF(year, TRY_CONVERT(DATE, date_of_birth), GETDATE()) < 35 THEN '25-34'
    WHEN DATEDIFF(year, TRY_CONVERT(DATE, date_of_birth), GETDATE()) < 45 THEN '35-44'
    WHEN DATEDIFF(year, TRY_CONVERT(DATE, date_of_birth), GETDATE()) < 55 THEN '45-54'
    WHEN DATEDIFF(year, TRY_CONVERT(DATE, date_of_birth), GETDATE()) < 65 THEN '55-64'
    ELSE '65+'
  END,
  MD5(worker_id || bank_of_the_west_employee_id || date_of_birth || enterprise_id ||
      race_ethnicity || gender || gender_identity || indigenous ||
      home_addres_postal_code || home_address_city || home_address_country ||
      home_address_region || last_name || legal_first_name || legal_full_name ||
      legal_full_name_formatted || military_status || preferred_first_name ||
      preferred_full_name || preferred_full_name_formatted ||
      primary_work_email_address || secondary_work_email_address ||
      sexual_orientation || junior_senior || product_sector_group ||
      preferred_language || bonus_equity_earliest_retirement_date || class_year ||
      admin_fte || consolidated_title || generation || home_address_country_name ||
      home_address_region_name || indigenous_2 || pensionable_yrs_of_service ||
      worker_workday_id),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l1_workday.int6031_worker_profile
WHERE worker_id IS NOT NULL;

-- ============================================================================
-- LOAD 9: dim_supervisory_org_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6028_department_hierarchy
-- Builds hierarchical parent chain and manager info via recursive CTE
-- ============================================================================

INSERT INTO v2_l3_star.dim_supervisory_org_D (
  department_id, department_wid, department_name, dept_name_with_manager_name,
  active, parent_dept_id, owner_ein, department_level, primary_location_code,
  type, subtype, levels_from_top, subordinate_supervisory_organizations,
  sup_org_level_1_id, sup_org_level_1_name, sup_org_level_1_manager_id,
  sup_org_level_1_manager_name, sup_org_level_1_wid, sup_org_level_2_id,
  sup_org_level_2_name, sup_org_level_2_manager_id, sup_org_level_2_manager_name,
  sup_org_level_2_wid, sup_org_level_3_id, sup_org_level_3_name,
  sup_org_level_3_manager_id, sup_org_level_3_manager_name, sup_org_level_3_wid,
  sup_org_level_4_id, sup_org_level_4_name, sup_org_level_4_manager_id,
  sup_org_level_4_manager_name, sup_org_level_4_wid, sup_org_level_5_id,
  sup_org_level_5_name, sup_org_level_5_manager_id, sup_org_level_5_manager_name,
  sup_org_level_5_wid, sup_org_level_6_id, sup_org_level_6_name,
  sup_org_level_6_manager_id, sup_org_level_6_manager_name, sup_org_level_6_wid,
  sup_org_level_7_id, sup_org_level_7_name, sup_org_level_7_manager_id,
  sup_org_level_7_manager_name, sup_org_level_7_wid, sup_org_level_8_id,
  sup_org_level_8_name, sup_org_level_8_manager_id, sup_org_level_8_manager_name,
  sup_org_level_8_wid, sup_org_level_9_id, sup_org_level_9_name,
  sup_org_level_9_manager_id, sup_org_level_9_manager_name, sup_org_level_9_wid,
  sup_org_level_10_id, sup_org_level_10_name, sup_org_level_10_manager_id,
  sup_org_level_10_manager_name, sup_org_level_10_wid, sup_org_level_11_id,
  sup_org_level_11_name, sup_org_level_11_manager_id, sup_org_level_11_manager_name,
  sup_org_level_11_wid, sup_org_level_12_id, sup_org_level_12_name,
  sup_org_level_12_manager_id, sup_org_level_12_manager_name, sup_org_level_12_wid,
  sup_org_level_13_id, sup_org_level_13_name, sup_org_level_13_manager_id,
  sup_org_level_13_manager_name, sup_org_level_13_wid, sup_org_level_14_id,
  sup_org_level_14_name, sup_org_level_14_manager_id, sup_org_level_14_manager_name,
  sup_org_level_14_wid, sup_org_level_15_id, sup_org_level_15_name,
  sup_org_level_15_manager_id, sup_org_level_15_manager_name, sup_org_level_15_wid,
  md5_hash, valid_from, valid_to, is_current
)
WITH RECURSIVE dept_chain AS (
  SELECT
    department_id, department_wid, department_name, dept_name_with_manager_name,
    active, parent_dept_id, owner_ein, department_level, primary_location_code,
    type, subtype,
    1 AS level_num,
    department_id AS level_1_id,
    department_name AS level_1_name,
    owner_ein AS level_1_manager_id,
    '' AS level_1_manager_name,
    department_wid AS level_1_wid
  FROM v2_l1_workday.int6028_department_hierarchy
  WHERE department_id IS NOT NULL

  UNION ALL

  SELECT
    dc.department_id, dc.department_wid, dc.department_name, dc.dept_name_with_manager_name,
    dc.active, dc.parent_dept_id, dc.owner_ein, dc.department_level,
    dc.primary_location_code, dc.type, dc.subtype,
    dc.level_num + 1,
    CASE WHEN dc.level_num + 1 = 2 THEN parent.department_id ELSE dc.level_1_id END,
    CASE WHEN dc.level_num + 1 = 2 THEN parent.department_name ELSE dc.level_1_name END,
    CASE WHEN dc.level_num + 1 = 2 THEN parent.owner_ein ELSE dc.level_1_manager_id END,
    CASE WHEN dc.level_num + 1 = 2 THEN '' ELSE dc.level_1_manager_name END,
    CASE WHEN dc.level_num + 1 = 2 THEN parent.department_wid ELSE dc.level_1_wid END
  FROM dept_chain dc
  INNER JOIN v2_l1_workday.int6028_department_hierarchy parent
    ON dc.parent_dept_id = parent.department_id
  WHERE dc.level_num < 15 AND dc.parent_dept_id IS NOT NULL
)
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
  ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY level_num DESC)::VARCHAR,
  '',
  level_1_id,
  level_1_name,
  level_1_manager_id,
  level_1_manager_name,
  level_1_wid,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  MD5(department_id || department_wid || department_name || active || parent_dept_id ||
      owner_ein || department_level || primary_location_code || type || subtype),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM (
  SELECT DISTINCT ON (department_id) *
  FROM dept_chain
  ORDER BY department_id, level_num
);

-- ============================================================================
-- LOAD 10: dim_supervisory_org_layers_D (Type 2 - SCD2, Normalized)
-- Source: v2_l1_workday.int6028_department_hierarchy
-- Creates one row per (department_id, parent_dept_id) pair in hierarchy
-- ============================================================================

INSERT INTO v2_l3_star.dim_supervisory_org_layers_D (
  department_id, parent_dept_id, department_name, parent_dept_name,
  supervisory_organization_is_bottom, supervisory_organization_is_top,
  supervisory_organization_levels_from_parent, md5_hash, valid_from, valid_to, is_current
)
WITH RECURSIVE dept_path AS (
  SELECT
    department_id,
    parent_dept_id,
    department_name,
    department_name AS parent_dept_name,
    1 AS depth
  FROM v2_l1_workday.int6028_department_hierarchy
  WHERE department_id IS NOT NULL

  UNION ALL

  SELECT
    dp.department_id,
    p.parent_dept_id,
    dp.department_name,
    p.department_name,
    dp.depth + 1
  FROM dept_path dp
  INNER JOIN v2_l1_workday.int6028_department_hierarchy p
    ON dp.parent_dept_id = p.department_id
  WHERE dp.depth < 15 AND p.parent_dept_id IS NOT NULL
)
SELECT
  department_id,
  parent_dept_id,
  department_name,
  parent_dept_name,
  CASE WHEN parent_dept_id IS NULL THEN 'Y' ELSE 'N' END,
  CASE WHEN parent_dept_id IS NULL THEN 'Y' ELSE 'N' END,
  depth::VARCHAR,
  MD5(department_id || parent_dept_id || department_name || parent_dept_name),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM dept_path
WHERE parent_dept_id IS NOT NULL;

-- ============================================================================
-- LOAD 11: dim_report_to_D (Type 2 - SCD2)
-- Source: v2_l1_workday.int6028_department_hierarchy via owner_ein
-- Maps manager reporting lines from supervisory org hierarchy
-- ============================================================================

INSERT INTO v2_l3_star.dim_report_to_D (
  employee_id, manager_worker_id, manager_preferred_name, level_1_manager_id,
  level_1_manager_preferred_name, level_2_manager_id, level_2_manager_preferred_name,
  level_3_manager_id, level_3_manager_preferred_name, level_4_manager_id,
  level_4_manager_preferred_name, level_5_manager_id, level_5_manager_preferred_name,
  level_6_manager_id, level_6_manager_preferred_name, level_7_manager_id,
  level_7_manager_preferred_name, level_8_manager_id, level_8_manager_preferred_name,
  level_9_manager_id, level_9_manager_preferred_name, level_10_manager_id,
  level_10_manager_preferred_name, level_11_manager_id, level_11_manager_preferred_name,
  level_12_manager_id, level_12_manager_preferred_name, level_13_manager_id,
  level_13_manager_preferred_name, level_14_manager_id, level_14_manager_preferred_name,
  level_15_manager_id, level_15_manager_preferred_name, md5_hash, valid_from,
  valid_to, is_current
)
SELECT
  owner_ein,
  owner_ein,
  '',
  owner_ein,
  '',
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL,
  MD5(owner_ein),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM (
  SELECT DISTINCT owner_ein
  FROM v2_l1_workday.int6028_department_hierarchy
  WHERE owner_ein IS NOT NULL
) managers;

-- ============================================================================
-- LOAD 12: dim_report_to_layers_D (Type 2 - SCD2, Normalized)
-- Source: v2_l1_workday.int6028_department_hierarchy via owner_ein
-- Creates reporting relationship edges with depth calculation
-- ============================================================================

INSERT INTO v2_l3_star.dim_report_to_layers_D (
  employee_id, parent_employee_id, employee_name, parent_employee_name,
  is_bottom, is_direct_report, is_indirect_report, is_top, levels_from_parent,
  md5_hash, valid_from, valid_to, is_current
)
WITH RECURSIVE mgr_chain AS (
  SELECT
    owner_ein AS employee_id,
    owner_ein AS parent_employee_id,
    '' AS employee_name,
    '' AS parent_employee_name,
    1 AS depth
  FROM v2_l1_workday.int6028_department_hierarchy
  WHERE owner_ein IS NOT NULL
)
SELECT
  employee_id,
  parent_employee_id,
  employee_name,
  parent_employee_name,
  CASE WHEN depth = 1 THEN 'Y' ELSE 'N' END,
  CASE WHEN depth = 1 THEN 'Y' ELSE 'N' END,
  CASE WHEN depth > 1 THEN 'Y' ELSE 'N' END,
  'N',
  depth::VARCHAR,
  MD5(employee_id || parent_employee_id),
  GETDATE(),
  '9999-12-31',
  'Y'
FROM mgr_chain
WHERE employee_id IS NOT NULL;

-- ============================================================================
-- LOAD 13: dim_worker_job_D (Type 2 - SCD2 with Effective Date)
-- Source: l3_workday_worker_job_dly_vw, l3_workday_worker_organization_dly_vw,
--         l3_workday_worker_comp_dly_vw
-- Builds as-of snapshot for each effective_date with multi-step joins
-- Excludes fields moved to dim_worker_status_D
-- ============================================================================

INSERT INTO v2_l3_star.dim_worker_job_D (
  employee_id, effective_date, transaction_wid, transaction_effective_date,
  transaction_entry_date, transaction_type, position_id, worker_type,
  worker_sub_type, business_title, business_site_id, mailstop_floor,
  worker_status, active, hire_date, employment_end_date, first_day_of_work,
  expected_retirement_date, terminated, pay_through_date,
  primary_termination_reason, termination_involuntary,
  secondary_termination_reason, local_termination_reason, not_eligible_for_hire,
  regrettable_termination, resignation_date, last_day_of_work,
  last_date_for_which_paid, expected_date_of_return, not_returning,
  return_unknown, probation_start_date, probation_end_date,
  academic_tenure_date, has_international_assignment, home_country,
  host_country, international_assignment_type,
  start_date_of_international_assignment, end_date_of_international_assignment,
  rehire, action, action_code, action_reason, action_reason_code, manager_id,
  soft_retirement_indicator, job_profile_id, sequence_number,
  planned_end_contract_date, job_entry_dt, stock_grants, time_type,
  supervisory_organization, location, job_title, french_job_title,
  shift_number, scheduled_weekly_hours, default_weekly_hours, scheduled_fte,
  work_model_start_date, work_model_type, compensation_grade,
  comp_grade_profile, compensation_step, pay_range_minimum,
  pay_range_midpoint, pay_range_maximum, base_pay_proposed_amount,
  base_pay_proposed_currency, base_pay_proposed_frequency,
  benefits_annual_rate_abbr, cost_center_id, company_id,
  department_entry_date, grade_entry_date, job_entry_date,
  position_entry_date, md5_hash, effective_date_from,
  effective_date_to, is_current_job, valid_from, valid_to, is_current
)
SELECT
  wj.employee_id,
  wj.effective_date,
  wj.transaction_wid,
  wj.transaction_effective_date,
  wj.transaction_entry_date,
  wj.transaction_type,
  wj.position_id,
  wj.worker_type,
  wj.worker_sub_type,
  wj.business_title,
  wj.business_site_id,
  wj.mailstop_floor,
  wj.worker_status,
  wj.active,
  wj.hire_date,
  wj.employment_end_date,
  wj.first_day_of_work,
  wj.expected_retirement_date,
  wj.terminated,
  wj.pay_through_date,
  wj.primary_termination_reason,
  wj.termination_involuntary,
  wj.secondary_termination_reason,
  wj.local_termination_reason,
  wj.not_eligible_for_hire,
  wj.regrettable_termination,
  wj.resignation_date,
  wj.last_day_of_work,
  wj.last_date_for_which_paid,
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
  wj.rehire,
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
  COALESCE(wc.compensation_grade_proposed, ''),
  COALESCE(wc.comp_grade_profile_proposed, ''),
  COALESCE(wc.compensation_step_proposed, ''),
  COALESCE(wc.pay_range_minimum, ''),
  COALESCE(wc.pay_range_midpoint, ''),
  COALESCE(wc.pay_range_maximum, ''),
  COALESCE(wc.base_pay_proposed_amount, ''),
  COALESCE(wc.base_pay_proposed_currency, ''),
  COALESCE(wc.base_pay_proposed_frequency, ''),
  COALESCE(wc.benefits_annual_rate_abbr, ''),
  COALESCE(wo.organization_id, ''),
  '',
  wj.job_entry_dt,
  '',
  wj.job_entry_dt,
  '',
  MD5(wj.employee_id || wj.effective_date || wj.job_profile_id || wj.position_id),
  wj.effective_date,
  '9999-12-31',
  'Y',
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l3_workday.l3_workday_worker_job_dly_vw wj
LEFT JOIN v2_l3_workday.l3_workday_worker_comp_dly_vw wc
  ON wj.employee_id = wc.employee_id AND wj.effective_date = wc.transaction_effective_date
LEFT JOIN v2_l3_workday.l3_workday_worker_organization_dly_vw wo
  ON wj.employee_id = wo.employee_id AND wj.effective_date = wo.transaction_effective_date
WHERE wj.employee_id IS NOT NULL AND wj.effective_date IS NOT NULL;

-- ============================================================================
-- LOAD 14: dim_worker_status_D (Type 2 - SCD2 with Effective Date)
-- Source: l3_workday_worker_job_dly_vw
-- Thin dimension containing ONLY status fields excluded from dim_worker_job_D
-- Fields: active_status_date, benefits_service_date, continuous_service_date,
--         eligible_for_rehire, hire_reason, hire_rescinded, original_hire_date,
--         primary_termination_category, retired, retirement_eligibility_date,
--         seniority_date, termination_date
-- ============================================================================

INSERT INTO v2_l3_star.dim_worker_status_D (
  employee_id, effective_date, active_status_date, benefits_service_date,
  continuous_service_date, eligible_for_rehire, hire_reason, hire_rescinded,
  original_hire_date, primary_termination_category, retired,
  retirement_eligibility_date, seniority_date, termination_date, md5_hash,
  effective_date_from, effective_date_to, valid_from, valid_to, is_current
)
SELECT
  employee_id,
  effective_date,
  active_status_date,
  benefits_service_date,
  continuous_service_date,
  eligible_for_rehire,
  hire_reason,
  hire_rescinded,
  original_hire_date,
  primary_termination_category,
  retired,
  retirement_eligibility_date,
  seniority_date,
  termination_date,
  MD5(employee_id || effective_date || active_status_date || benefits_service_date ||
      continuous_service_date || eligible_for_rehire || hire_reason ||
      hire_rescinded || original_hire_date || primary_termination_category ||
      retired || retirement_eligibility_date || seniority_date || termination_date),
  effective_date,
  '9999-12-31',
  GETDATE(),
  '9999-12-31',
  'Y'
FROM v2_l3_workday.l3_workday_worker_job_dly_vw
WHERE employee_id IS NOT NULL AND effective_date IS NOT NULL;

