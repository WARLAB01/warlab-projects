-- ============================================================================
-- HR Datamart V2 - L3 Star Schema Dimension DDL
-- Creates v2_l3_star schema and 14 dimension tables
-- ============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS v2_l3_star;

-- ============================================================================
-- DIM 1: dim_day_D (Type 1 - Calendar/Time)
-- Business Key: day_dt (DATE)
-- Attributes: day_name, day_of_month, fiscal_quarter_name, is_weekend, holidays, etc.
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_day_D;

CREATE TABLE v2_l3_star.dim_day_D (
  dim_sk BIGINT IDENTITY(1,1),
  day_dt VARCHAR(1000),
  day_abbr VARCHAR(1000),
  day_date VARCHAR(1000),
  day_name VARCHAR(1000),
  day_of_month VARCHAR(1000),
  day_of_week VARCHAR(1000),
  day_of_year VARCHAR(1000),
  day_sk VARCHAR(1000),
  first_day_of_fiscal_quarter VARCHAR(1000),
  first_day_of_fiscal_year VARCHAR(1000),
  first_day_of_month VARCHAR(1000),
  first_day_of_quarter VARCHAR(1000),
  first_day_of_week VARCHAR(1000),
  first_day_of_year VARCHAR(1000),
  fiscal_quarter_abbr VARCHAR(1000),
  fiscal_quarter_name VARCHAR(1000),
  fiscal_quarter_num VARCHAR(1000),
  fiscal_year_name VARCHAR(1000),
  fiscal_year_num VARCHAR(1000),
  is_canada_holiday VARCHAR(1000),
  is_us_holiday VARCHAR(1000),
  is_weekend VARCHAR(1000),
  last_day_of_fiscal_quarter VARCHAR(1000),
  last_day_of_fiscal_year VARCHAR(1000),
  last_day_of_month VARCHAR(1000),
  last_day_of_quarter VARCHAR(1000),
  last_day_of_week VARCHAR(1000),
  last_day_of_year VARCHAR(1000),
  month_abbr VARCHAR(1000),
  month_name VARCHAR(1000),
  month_of_fiscal_quarter VARCHAR(1000),
  month_of_fiscal_year VARCHAR(1000),
  month_of_quarter VARCHAR(1000),
  month_of_year VARCHAR(1000),
  quarter_abbr VARCHAR(1000),
  quarter_name VARCHAR(1000),
  quarter_num VARCHAR(1000),
  week_of_month VARCHAR(1000),
  week_of_year VARCHAR(1000),
  year_name VARCHAR(1000),
  year_num VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 2: dim_company_D (Type 2 - SCD2)
-- Business Key: company_id
-- Source: v2_l1_workday.int6024_company
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_company_D;

CREATE TABLE v2_l3_star.dim_company_D (
  dim_sk BIGINT IDENTITY(1,1),
  company_id VARCHAR(1000),
  company_wid VARCHAR(1000),
  company_name VARCHAR(1000),
  company_code VARCHAR(1000),
  business_unit VARCHAR(1000),
  company_subtype VARCHAR(1000),
  company_currency VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 3: dim_cost_center_D (Type 2 - SCD2)
-- Business Key: cost_center_id
-- Source: v2_l1_workday.int6025_cost_center
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_cost_center_D;

CREATE TABLE v2_l3_star.dim_cost_center_D (
  dim_sk BIGINT IDENTITY(1,1),
  cost_center_id VARCHAR(1000),
  cost_center_wid VARCHAR(1000),
  cost_center_code VARCHAR(1000),
  cost_center_name VARCHAR(1000),
  hierarchy VARCHAR(1000),
  subtype VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 4: dim_grade_profile_D (Type 2 - SCD2)
-- Business Key: grade_profile_id
-- Source: v2_l1_workday.int6020_grade_profile
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_grade_profile_D;

CREATE TABLE v2_l3_star.dim_grade_profile_D (
  dim_sk BIGINT IDENTITY(1,1),
  grade_profile_id VARCHAR(1000),
  grade_id VARCHAR(1000),
  grade_name VARCHAR(1000),
  grade_profile_currency_code VARCHAR(1000),
  effective_date VARCHAR(1000),
  grade_profile_name VARCHAR(1000),
  grade_profile_number_of_segements VARCHAR(1000),
  grade_profile_salary_range_maximum VARCHAR(1000),
  grade_profile_salary_range_midpoint VARCHAR(1000),
  grade_profile_salary_range_minimjum VARCHAR(1000),
  grade_profile_segement_1_top VARCHAR(1000),
  grade_profile_segement_2_top VARCHAR(1000),
  grade_profile_segement_3_top VARCHAR(1000),
  grade_profile_segement_4_top VARCHAR(1000),
  grade_profile_segement_5_top VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 5: dim_job_profile_D (Type 2 - SCD2)
-- Business Key: job_profile_id
-- Source: v2_l1_workday.int6021_job_profile LEFT JOIN int6022_job_classification
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_job_profile_D;

CREATE TABLE v2_l3_star.dim_job_profile_D (
  dim_sk BIGINT IDENTITY(1,1),
  job_profile_id VARCHAR(1000),
  compensation_grade VARCHAR(1000),
  critical_job_flag VARCHAR(1000),
  difficult_to_fill_flag VARCHAR(1000),
  inactive_flag VARCHAR(1000),
  job_category_code VARCHAR(1000),
  job_category_name VARCHAR(1000),
  job_exempt_canada VARCHAR(1000),
  job_exempt_us VARCHAR(1000),
  job_family VARCHAR(1000),
  job_family_group VARCHAR(1000),
  job_family_group_name VARCHAR(1000),
  job_family_name VARCHAR(1000),
  job_level_code VARCHAR(1000),
  job_level_name VARCHAR(1000),
  job_profile_code VARCHAR(1000),
  job_profile_description VARCHAR(1000),
  job_profile_name VARCHAR(1000),
  job_profile_summary VARCHAR(1000),
  job_profile_wid VARCHAR(1000),
  job_title VARCHAR(1000),
  management_level_code VARCHAR(1000),
  management_level_name VARCHAR(1000),
  pay_rate_type VARCHAR(1000),
  public_job VARCHAR(1000),
  work_shift_required VARCHAR(1000),
  job_matrix VARCHAR(1000),
  is_people_manager VARCHAR(1000),
  is_manager VARCHAR(1000),
  frequency VARCHAR(1000),
  aap_job_group VARCHAR(1000),
  bonus_eligibility VARCHAR(1000),
  customer_facing VARCHAR(1000),
  eeo1_code VARCHAR(1000),
  job_collection VARCHAR(1000),
  loan_originator_code VARCHAR(1000),
  national_occupation_code VARCHAR(1000),
  occupation_code VARCHAR(1000),
  recruitment_channel VARCHAR(1000),
  standard_occupation_code VARCHAR(1000),
  stock VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 6: dim_location_D (Type 2 - SCD2)
-- Business Key: location_id
-- Source: v2_l1_workday.int6023_location
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_location_D;

CREATE TABLE v2_l3_star.dim_location_D (
  dim_sk BIGINT IDENTITY(1,1),
  location_id VARCHAR(1000),
  location_wid VARCHAR(1000),
  location_name VARCHAR(1000),
  inactive VARCHAR(1000),
  address_line_1 VARCHAR(1000),
  address_line_2 VARCHAR(1000),
  city VARCHAR(1000),
  region VARCHAR(1000),
  region_name VARCHAR(1000),
  country VARCHAR(1000),
  country_name VARCHAR(1000),
  location_postal_code VARCHAR(1000),
  location_identifier VARCHAR(1000),
  latitude VARCHAR(1000),
  longitude VARCHAR(1000),
  location_type VARCHAR(1000),
  location_usage_type VARCHAR(1000),
  trade_name VARCHAR(1000),
  worksite_id_code VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 7: dim_matrix_org_D (Type 2 - SCD2)
-- Business Key: matrix_organization_id
-- Source: v2_l1_workday.int6027_matrix_organization
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_matrix_org_D;

CREATE TABLE v2_l3_star.dim_matrix_org_D (
  dim_sk BIGINT IDENTITY(1,1),
  matrix_organization_id VARCHAR(1000),
  matrix_organization_status VARCHAR(1000),
  maxtrix_organization_name VARCHAR(1000),
  maxtrix_organization_code VARCHAR(1000),
  matrix_organization_type VARCHAR(1000),
  matrix_organization_subtype VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 8: dim_worker_profile_D (Type 2 - SCD2)
-- Business Key: worker_id (employee_id in business context)
-- Source: v2_l1_workday.int6031_worker_profile
-- Custom: age_band (computed from date_of_birth)
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_worker_profile_D;

CREATE TABLE v2_l3_star.dim_worker_profile_D (
  dim_sk BIGINT IDENTITY(1,1),
  worker_id VARCHAR(1000),
  bank_of_the_west_employee_id VARCHAR(1000),
  date_of_birth VARCHAR(1000),
  enterprise_id VARCHAR(1000),
  race_ethnicity VARCHAR(1000),
  gender VARCHAR(1000),
  gender_identity VARCHAR(1000),
  indigenous VARCHAR(1000),
  home_addres_postal_code VARCHAR(1000),
  home_address_city VARCHAR(1000),
  home_address_country VARCHAR(1000),
  home_address_region VARCHAR(1000),
  last_name VARCHAR(1000),
  legal_first_name VARCHAR(1000),
  legal_full_name VARCHAR(1000),
  legal_full_name_formatted VARCHAR(1000),
  military_status VARCHAR(1000),
  preferred_first_name VARCHAR(1000),
  preferred_full_name VARCHAR(1000),
  preferred_full_name_formatted VARCHAR(1000),
  primary_work_email_address VARCHAR(1000),
  secondary_work_email_address VARCHAR(1000),
  sexual_orientation VARCHAR(1000),
  junior_senior VARCHAR(1000),
  product_sector_group VARCHAR(1000),
  preferred_language VARCHAR(1000),
  bonus_equity_earliest_retirement_date VARCHAR(1000),
  class_year VARCHAR(1000),
  admin_fte VARCHAR(1000),
  consolidated_title VARCHAR(1000),
  generation VARCHAR(1000),
  home_address_country_name VARCHAR(1000),
  home_address_region_name VARCHAR(1000),
  indigenous_2 VARCHAR(1000),
  pensionable_yrs_of_service VARCHAR(1000),
  worker_workday_id VARCHAR(1000),
  age_band VARCHAR(20),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 9: dim_supervisory_org_D (Type 2 - SCD2)
-- Business Key: department_id
-- Source: v2_l1_workday.int6028_department_hierarchy
-- Custom: levels_from_top, subordinate_supervisory_organizations, sup_org_level_*_id,
--         sup_org_level_*_name, sup_org_level_*_manager_id, sup_org_level_*_manager_name,
--         sup_org_level_*_wid
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_supervisory_org_D;

CREATE TABLE v2_l3_star.dim_supervisory_org_D (
  dim_sk BIGINT IDENTITY(1,1),
  department_id VARCHAR(1000),
  department_wid VARCHAR(1000),
  department_name VARCHAR(1000),
  dept_name_with_manager_name VARCHAR(1000),
  active VARCHAR(1000),
  parent_dept_id VARCHAR(1000),
  owner_ein VARCHAR(1000),
  department_level VARCHAR(1000),
  primary_location_code VARCHAR(1000),
  type VARCHAR(1000),
  subtype VARCHAR(1000),
  levels_from_top VARCHAR(1000),
  subordinate_supervisory_organizations VARCHAR(1000),
  sup_org_level_1_id VARCHAR(1000),
  sup_org_level_1_name VARCHAR(1000),
  sup_org_level_1_manager_id VARCHAR(1000),
  sup_org_level_1_manager_name VARCHAR(1000),
  sup_org_level_1_wid VARCHAR(1000),
  sup_org_level_2_id VARCHAR(1000),
  sup_org_level_2_name VARCHAR(1000),
  sup_org_level_2_manager_id VARCHAR(1000),
  sup_org_level_2_manager_name VARCHAR(1000),
  sup_org_level_2_wid VARCHAR(1000),
  sup_org_level_3_id VARCHAR(1000),
  sup_org_level_3_name VARCHAR(1000),
  sup_org_level_3_manager_id VARCHAR(1000),
  sup_org_level_3_manager_name VARCHAR(1000),
  sup_org_level_3_wid VARCHAR(1000),
  sup_org_level_4_id VARCHAR(1000),
  sup_org_level_4_name VARCHAR(1000),
  sup_org_level_4_manager_id VARCHAR(1000),
  sup_org_level_4_manager_name VARCHAR(1000),
  sup_org_level_4_wid VARCHAR(1000),
  sup_org_level_5_id VARCHAR(1000),
  sup_org_level_5_name VARCHAR(1000),
  sup_org_level_5_manager_id VARCHAR(1000),
  sup_org_level_5_manager_name VARCHAR(1000),
  sup_org_level_5_wid VARCHAR(1000),
  sup_org_level_6_id VARCHAR(1000),
  sup_org_level_6_name VARCHAR(1000),
  sup_org_level_6_manager_id VARCHAR(1000),
  sup_org_level_6_manager_name VARCHAR(1000),
  sup_org_level_6_wid VARCHAR(1000),
  sup_org_level_7_id VARCHAR(1000),
  sup_org_level_7_name VARCHAR(1000),
  sup_org_level_7_manager_id VARCHAR(1000),
  sup_org_level_7_manager_name VARCHAR(1000),
  sup_org_level_7_wid VARCHAR(1000),
  sup_org_level_8_id VARCHAR(1000),
  sup_org_level_8_name VARCHAR(1000),
  sup_org_level_8_manager_id VARCHAR(1000),
  sup_org_level_8_manager_name VARCHAR(1000),
  sup_org_level_8_wid VARCHAR(1000),
  sup_org_level_9_id VARCHAR(1000),
  sup_org_level_9_name VARCHAR(1000),
  sup_org_level_9_manager_id VARCHAR(1000),
  sup_org_level_9_manager_name VARCHAR(1000),
  sup_org_level_9_wid VARCHAR(1000),
  sup_org_level_10_id VARCHAR(1000),
  sup_org_level_10_name VARCHAR(1000),
  sup_org_level_10_manager_id VARCHAR(1000),
  sup_org_level_10_manager_name VARCHAR(1000),
  sup_org_level_10_wid VARCHAR(1000),
  sup_org_level_11_id VARCHAR(1000),
  sup_org_level_11_name VARCHAR(1000),
  sup_org_level_11_manager_id VARCHAR(1000),
  sup_org_level_11_manager_name VARCHAR(1000),
  sup_org_level_11_wid VARCHAR(1000),
  sup_org_level_12_id VARCHAR(1000),
  sup_org_level_12_name VARCHAR(1000),
  sup_org_level_12_manager_id VARCHAR(1000),
  sup_org_level_12_manager_name VARCHAR(1000),
  sup_org_level_12_wid VARCHAR(1000),
  sup_org_level_13_id VARCHAR(1000),
  sup_org_level_13_name VARCHAR(1000),
  sup_org_level_13_manager_id VARCHAR(1000),
  sup_org_level_13_manager_name VARCHAR(1000),
  sup_org_level_13_wid VARCHAR(1000),
  sup_org_level_14_id VARCHAR(1000),
  sup_org_level_14_name VARCHAR(1000),
  sup_org_level_14_manager_id VARCHAR(1000),
  sup_org_level_14_manager_name VARCHAR(1000),
  sup_org_level_14_wid VARCHAR(1000),
  sup_org_level_15_id VARCHAR(1000),
  sup_org_level_15_name VARCHAR(1000),
  sup_org_level_15_manager_id VARCHAR(1000),
  sup_org_level_15_manager_name VARCHAR(1000),
  sup_org_level_15_wid VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 10: dim_supervisory_org_layers_D (Type 2 - SCD2, Normalized)
-- Business Key: (department_id, parent_dept_id)
-- Source: v2_l1_workday.int6028_department_hierarchy
-- Custom: supervisory_organization_is_bottom, supervisory_organization_is_top,
--         supervisory_organization_levels_from_parent
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_supervisory_org_layers_D;

CREATE TABLE v2_l3_star.dim_supervisory_org_layers_D (
  dim_sk BIGINT IDENTITY(1,1),
  department_id VARCHAR(1000),
  parent_dept_id VARCHAR(1000),
  department_name VARCHAR(1000),
  parent_dept_name VARCHAR(1000),
  supervisory_organization_is_bottom VARCHAR(1),
  supervisory_organization_is_top VARCHAR(1),
  supervisory_organization_levels_from_parent VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 11: dim_report_to_D (Type 2 - SCD2)
-- Business Key: employee_id (owner_ein from int6028)
-- Source: v2_l1_workday.int6028_department_hierarchy via owner_ein
-- Custom: manager_worker_id, manager_preferred_name, level_1_manager_id through
--         level_15_manager_id, level_1_manager_preferred_name through level_15_manager_preferred_name
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_report_to_D;

CREATE TABLE v2_l3_star.dim_report_to_D (
  dim_sk BIGINT IDENTITY(1,1),
  employee_id VARCHAR(1000),
  manager_worker_id VARCHAR(1000),
  manager_preferred_name VARCHAR(1000),
  level_1_manager_id VARCHAR(1000),
  level_1_manager_preferred_name VARCHAR(1000),
  level_2_manager_id VARCHAR(1000),
  level_2_manager_preferred_name VARCHAR(1000),
  level_3_manager_id VARCHAR(1000),
  level_3_manager_preferred_name VARCHAR(1000),
  level_4_manager_id VARCHAR(1000),
  level_4_manager_preferred_name VARCHAR(1000),
  level_5_manager_id VARCHAR(1000),
  level_5_manager_preferred_name VARCHAR(1000),
  level_6_manager_id VARCHAR(1000),
  level_6_manager_preferred_name VARCHAR(1000),
  level_7_manager_id VARCHAR(1000),
  level_7_manager_preferred_name VARCHAR(1000),
  level_8_manager_id VARCHAR(1000),
  level_8_manager_preferred_name VARCHAR(1000),
  level_9_manager_id VARCHAR(1000),
  level_9_manager_preferred_name VARCHAR(1000),
  level_10_manager_id VARCHAR(1000),
  level_10_manager_preferred_name VARCHAR(1000),
  level_11_manager_id VARCHAR(1000),
  level_11_manager_preferred_name VARCHAR(1000),
  level_12_manager_id VARCHAR(1000),
  level_12_manager_preferred_name VARCHAR(1000),
  level_13_manager_id VARCHAR(1000),
  level_13_manager_preferred_name VARCHAR(1000),
  level_14_manager_id VARCHAR(1000),
  level_14_manager_preferred_name VARCHAR(1000),
  level_15_manager_id VARCHAR(1000),
  level_15_manager_preferred_name VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 12: dim_report_to_layers_D (Type 2 - SCD2, Normalized)
-- Business Key: (employee_id, parent_employee_id)
-- Source: v2_l1_workday.int6028_department_hierarchy via owner_ein
-- Custom: is_bottom, is_direct_report, is_indirect_report, is_top, levels_from_parent
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_report_to_layers_D;

CREATE TABLE v2_l3_star.dim_report_to_layers_D (
  dim_sk BIGINT IDENTITY(1,1),
  employee_id VARCHAR(1000),
  parent_employee_id VARCHAR(1000),
  employee_name VARCHAR(1000),
  parent_employee_name VARCHAR(1000),
  is_bottom VARCHAR(1),
  is_direct_report VARCHAR(1),
  is_indirect_report VARCHAR(1),
  is_top VARCHAR(1),
  levels_from_parent VARCHAR(1000),
  md5_hash VARCHAR(32),
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 13: dim_worker_job_D (Type 2 - SCD2 with Effective Date)
-- Business Key: (employee_id, effective_date)
-- Source: l3_workday_worker_job_dly_vw, l3_workday_worker_organization_dly_vw,
--         l3_workday_worker_comp_dly_vw, int6031_worker_profile
-- Custom: department_entry_date, grade_entry_date, job_entry_date,
--         position_entry_date, work_model_start_date
-- Excluded Fields (moved to dim_worker_status_D): active_status_date, benefits_service_date,
--   continuous_service_date, eligible_for_rehire, hire_reason, hire_rescinded,
--   original_hire_date, primary_termination_category, retired, retirement_eligibility_date,
--   seniority_date, termination_date
-- SCD2: effective_date_from, effective_date_to, is_current_job
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_worker_job_D;

CREATE TABLE v2_l3_star.dim_worker_job_D (
  dim_sk BIGINT IDENTITY(1,1),
  employee_id VARCHAR(1000),
  effective_date VARCHAR(1000),
  transaction_wid VARCHAR(1000),
  transaction_effective_date VARCHAR(1000),
  transaction_entry_date VARCHAR(1000),
  transaction_type VARCHAR(1000),
  position_id VARCHAR(1000),
  worker_type VARCHAR(1000),
  worker_sub_type VARCHAR(1000),
  business_title VARCHAR(1000),
  business_site_id VARCHAR(1000),
  mailstop_floor VARCHAR(1000),
  worker_status VARCHAR(1000),
  active VARCHAR(1000),
  hire_date VARCHAR(1000),
  employment_end_date VARCHAR(1000),
  first_day_of_work VARCHAR(1000),
  expected_retirement_date VARCHAR(1000),
  terminated VARCHAR(1000),
  pay_through_date VARCHAR(1000),
  primary_termination_reason VARCHAR(1000),
  termination_involuntary VARCHAR(1000),
  secondary_termination_reason VARCHAR(1000),
  local_termination_reason VARCHAR(1000),
  not_eligible_for_hire VARCHAR(1000),
  regrettable_termination VARCHAR(1000),
  resignation_date VARCHAR(1000),
  last_day_of_work VARCHAR(1000),
  last_date_for_which_paid VARCHAR(1000),
  expected_date_of_return VARCHAR(1000),
  not_returning VARCHAR(1000),
  return_unknown VARCHAR(1000),
  probation_start_date VARCHAR(1000),
  probation_end_date VARCHAR(1000),
  academic_tenure_date VARCHAR(1000),
  has_international_assignment VARCHAR(1000),
  home_country VARCHAR(1000),
  host_country VARCHAR(1000),
  international_assignment_type VARCHAR(1000),
  start_date_of_international_assignment VARCHAR(1000),
  end_date_of_international_assignment VARCHAR(1000),
  rehire VARCHAR(1000),
  action VARCHAR(1000),
  action_code VARCHAR(1000),
  action_reason VARCHAR(1000),
  action_reason_code VARCHAR(1000),
  manager_id VARCHAR(1000),
  soft_retirement_indicator VARCHAR(1000),
  job_profile_id VARCHAR(1000),
  sequence_number VARCHAR(1000),
  planned_end_contract_date VARCHAR(1000),
  job_entry_dt VARCHAR(1000),
  stock_grants VARCHAR(1000),
  time_type VARCHAR(1000),
  supervisory_organization VARCHAR(1000),
  location VARCHAR(1000),
  job_title VARCHAR(1000),
  french_job_title VARCHAR(1000),
  shift_number VARCHAR(1000),
  scheduled_weekly_hours VARCHAR(1000),
  default_weekly_hours VARCHAR(1000),
  scheduled_fte VARCHAR(1000),
  work_model_start_date VARCHAR(1000),
  work_model_type VARCHAR(1000),
  compensation_grade VARCHAR(1000),
  comp_grade_profile VARCHAR(1000),
  compensation_step VARCHAR(1000),
  pay_range_minimum VARCHAR(1000),
  pay_range_midpoint VARCHAR(1000),
  pay_range_maximum VARCHAR(1000),
  base_pay_proposed_amount VARCHAR(1000),
  base_pay_proposed_currency VARCHAR(1000),
  base_pay_proposed_frequency VARCHAR(1000),
  benefits_annual_rate_abbr VARCHAR(1000),
  cost_center_id VARCHAR(1000),
  company_id VARCHAR(1000),
  department_entry_date VARCHAR(1000),
  grade_entry_date VARCHAR(1000),
  job_entry_date VARCHAR(1000),
  position_entry_date VARCHAR(1000),
  md5_hash VARCHAR(32),
  effective_date_from VARCHAR(1000),
  effective_date_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current_job VARCHAR(1) DEFAULT 'Y',
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);

-- ============================================================================
-- DIM 14: dim_worker_status_D (Type 2 - SCD2 with Effective Date)
-- Business Key: (employee_id, effective_date)
-- Source: l3_workday_worker_job_dly_vw
-- Description: Thin dimension containing only status/eligibility fields EXCLUDED
--              from dim_worker_job_D
-- Fields: active_status_date, benefits_service_date, continuous_service_date,
--         eligible_for_rehire, hire_reason, hire_rescinded, original_hire_date,
--         primary_termination_category, retired, retirement_eligibility_date,
--         seniority_date, termination_date
-- SCD2: effective_date_from, effective_date_to
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.dim_worker_status_D;

CREATE TABLE v2_l3_star.dim_worker_status_D (
  dim_sk BIGINT IDENTITY(1,1),
  employee_id VARCHAR(1000),
  effective_date VARCHAR(1000),
  active_status_date VARCHAR(1000),
  benefits_service_date VARCHAR(1000),
  continuous_service_date VARCHAR(1000),
  eligible_for_rehire VARCHAR(1000),
  hire_reason VARCHAR(1000),
  hire_rescinded VARCHAR(1000),
  original_hire_date VARCHAR(1000),
  primary_termination_category VARCHAR(1000),
  retired VARCHAR(1000),
  retirement_eligibility_date VARCHAR(1000),
  seniority_date VARCHAR(1000),
  termination_date VARCHAR(1000),
  md5_hash VARCHAR(32),
  effective_date_from VARCHAR(1000),
  effective_date_to VARCHAR(1000) DEFAULT '9999-12-31',
  valid_from VARCHAR(1000) DEFAULT GETDATE(),
  valid_to VARCHAR(1000) DEFAULT '9999-12-31',
  is_current VARCHAR(1) DEFAULT 'Y',
  insert_datetime VARCHAR(1000) DEFAULT GETDATE(),
  update_datetime VARCHAR(1000) DEFAULT GETDATE()
);
