-- ============================================================================
-- HR Datamart V2 - L3 Source Schema DDL
-- Creates v2_l3_workday schema and 3 fact tables + 3 views
-- ============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS v2_l3_workday;

-- ============================================================================
-- TABLE 1: l3_workday_worker_job_dly
-- Mirrors int0095e_worker_job with 5 warehouse cols + 5 IDP columns
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_workday.l3_workday_worker_job_dly;

CREATE TABLE v2_l3_workday.l3_workday_worker_job_dly (
  -- Source L1 columns (81 columns)
  employee_id VARCHAR(1000),
  transaction_wid VARCHAR(1000),
  transaction_effective_date VARCHAR(1000),
  transaction_entry_date VARCHAR(1000),
  transaction_type VARCHAR(1000),
  position_id VARCHAR(1000),
  effective_date VARCHAR(1000),
  worker_type VARCHAR(1000),
  worker_sub_type VARCHAR(1000),
  business_title VARCHAR(1000),
  business_site_id VARCHAR(1000),
  mailstop_floor VARCHAR(1000),
  worker_status VARCHAR(1000),
  active VARCHAR(1000),
  active_status_date VARCHAR(1000),
  hire_date VARCHAR(1000),
  original_hire_date VARCHAR(1000),
  hire_reason VARCHAR(1000),
  employment_end_date VARCHAR(1000),
  continuous_service_date VARCHAR(1000),
  first_day_of_work VARCHAR(1000),
  expected_retirement_date VARCHAR(1000),
  retirement_eligibility_date VARCHAR(1000),
  retired VARCHAR(1000),
  seniority_date VARCHAR(1000),
  severance_date VARCHAR(1000),
  benefits_service_date VARCHAR(1000),
  company_service_date VARCHAR(1000),
  time_off_service_date VARCHAR(1000),
  vesting_date VARCHAR(1000),
  terminated VARCHAR(1000),
  termination_date VARCHAR(1000),
  pay_through_date VARCHAR(1000),
  primary_termination_reason VARCHAR(1000),
  primary_termination_category VARCHAR(1000),
  termination_involuntary VARCHAR(1000),
  secondary_termination_reason VARCHAR(1000),
  local_termination_reason VARCHAR(1000),
  not_eligible_for_hire VARCHAR(1000),
  regrettable_termination VARCHAR(1000),
  hire_rescinded VARCHAR(1000),
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
  eligible_for_rehire VARCHAR(1000),
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
  worker_workday_id VARCHAR(1000),
  -- Warehouse columns (5 columns)
  ingest_timestamp VARCHAR(1000),
  source_file_name VARCHAR(1000),
  etl_batch_id VARCHAR(1000),
  insert_datetime VARCHAR(1000),
  update_datetime VARCHAR(1000),
  -- IDP columns (5 columns)
  idp_calc_end_date VARCHAR(1000),
  idp_obsolete_date VARCHAR(1000),
  idp_max_entry_ts VARCHAR(1000),
  idp_min_seq_num VARCHAR(1000),
  idp_employee_status VARCHAR(10)
);

-- ============================================================================
-- TABLE 2: l3_workday_worker_organization_dly
-- Mirrors int0096_worker_organization with 5 warehouse cols + 4 IDP columns
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_workday.l3_workday_worker_organization_dly;

CREATE TABLE v2_l3_workday.l3_workday_worker_organization_dly (
  -- Source L1 columns (9 columns)
  employee_id VARCHAR(1000),
  transaction_wid VARCHAR(1000),
  transaction_effective_date VARCHAR(1000),
  transaction_entry_date VARCHAR(1000),
  transaction_type VARCHAR(1000),
  organization_id VARCHAR(1000),
  organization_type VARCHAR(1000),
  sequence_number VARCHAR(1000),
  worker_workday_id VARCHAR(1000),
  -- Warehouse columns (5 columns)
  ingest_timestamp VARCHAR(1000),
  source_file_name VARCHAR(1000),
  etl_batch_id VARCHAR(1000),
  insert_datetime VARCHAR(1000),
  update_datetime VARCHAR(1000),
  -- IDP columns (4 columns)
  idp_calc_end_date VARCHAR(1000),
  idp_obsolete_date VARCHAR(1000),
  idp_max_entry_ts VARCHAR(1000),
  idp_min_seq_num VARCHAR(1000)
);

-- ============================================================================
-- TABLE 3: l3_workday_worker_comp_dly
-- Mirrors int0098_worker_compensation with 5 warehouse cols + 1 IDP column
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_workday.l3_workday_worker_comp_dly;

CREATE TABLE v2_l3_workday.l3_workday_worker_comp_dly (
  -- Source L1 columns (19 columns)
  employee_id VARCHAR(1000),
  transaction_wid VARCHAR(1000),
  transaction_effective_date VARCHAR(1000),
  transaction_entry_moment VARCHAR(1000),
  transaction_type VARCHAR(1000),
  compensation_package_proposed VARCHAR(1000),
  compensation_grade_proposed VARCHAR(1000),
  comp_grade_profile_proposed VARCHAR(1000),
  compensation_step_proposed VARCHAR(1000),
  pay_range_minimum VARCHAR(1000),
  pay_range_midpoint VARCHAR(1000),
  pay_range_maximum VARCHAR(1000),
  base_pay_proposed_amount VARCHAR(1000),
  base_pay_proposed_currency VARCHAR(1000),
  base_pay_proposed_frequency VARCHAR(1000),
  benefits_annual_rate_abbr VARCHAR(1000),
  pay_rate_type VARCHAR(1000),
  compensation VARCHAR(1000),
  worker_workday_id VARCHAR(1000),
  -- Warehouse columns (5 columns)
  ingest_timestamp VARCHAR(1000),
  source_file_name VARCHAR(1000),
  etl_batch_id VARCHAR(1000),
  insert_datetime VARCHAR(1000),
  update_datetime VARCHAR(1000),
  -- IDP columns (1 column only)
  idp_obsolete_date VARCHAR(1000)
);

-- ============================================================================
-- VIEW 1: l3_workday_worker_job_dly_vw
-- Unfiltered view of worker job daily snapshot
-- ============================================================================

DROP VIEW IF EXISTS v2_l3_workday.l3_workday_worker_job_dly_vw;

CREATE VIEW v2_l3_workday.l3_workday_worker_job_dly_vw AS
SELECT * FROM v2_l3_workday.l3_workday_worker_job_dly;

-- ============================================================================
-- VIEW 2: l3_workday_worker_organization_dly_vw
-- Unfiltered view of worker organization daily snapshot
-- ============================================================================

DROP VIEW IF EXISTS v2_l3_workday.l3_workday_worker_organization_dly_vw;

CREATE VIEW v2_l3_workday.l3_workday_worker_organization_dly_vw AS
SELECT * FROM v2_l3_workday.l3_workday_worker_organization_dly;

-- ============================================================================
-- VIEW 3: l3_workday_worker_comp_dly_vw
-- Unfiltered view of worker compensation daily snapshot
-- ============================================================================

DROP VIEW IF EXISTS v2_l3_workday.l3_workday_worker_comp_dly_vw;

CREATE VIEW v2_l3_workday.l3_workday_worker_comp_dly_vw AS
SELECT * FROM v2_l3_workday.l3_workday_worker_comp_dly;
