-- ============================================================================
-- HR Datamart V2 - L3 Source Data Load
-- Populates v2_l3_workday tables from L1 source with IDP calculations
-- ============================================================================

-- ============================================================================
-- WORKER JOB (int0095e_worker_job) -> l3_workday_worker_job_dly
-- ============================================================================

-- STEP 1: TRUNCATE table
TRUNCATE TABLE v2_l3_workday.l3_workday_worker_job_dly;

-- STEP 2: INSERT all L1 data + idp_obsolete_date from INT270 rescinded join
INSERT INTO v2_l3_workday.l3_workday_worker_job_dly (
  employee_id,
  transaction_wid,
  transaction_effective_date,
  transaction_entry_date,
  transaction_type,
  position_id,
  effective_date,
  worker_type,
  worker_sub_type,
  business_title,
  business_site_id,
  mailstop_floor,
  worker_status,
  active,
  active_status_date,
  hire_date,
  original_hire_date,
  hire_reason,
  employment_end_date,
  continuous_service_date,
  first_day_of_work,
  expected_retirement_date,
  retirement_eligibility_date,
  retired,
  seniority_date,
  severance_date,
  benefits_service_date,
  company_service_date,
  time_off_service_date,
  vesting_date,
  terminated,
  termination_date,
  pay_through_date,
  primary_termination_reason,
  primary_termination_category,
  termination_involuntary,
  secondary_termination_reason,
  local_termination_reason,
  not_eligible_for_hire,
  regrettable_termination,
  hire_rescinded,
  resignation_date,
  last_day_of_work,
  last_date_for_which_paid,
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
  rehire,
  eligible_for_rehire,
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
  ingest_timestamp,
  source_file_name,
  etl_batch_id,
  insert_datetime,
  update_datetime,
  idp_obsolete_date,
  idp_calc_end_date,
  idp_max_entry_ts,
  idp_min_seq_num,
  idp_employee_status
)
SELECT
  l1.employee_id,
  l1.transaction_wid,
  l1.transaction_effective_date,
  l1.transaction_entry_date,
  l1.transaction_type,
  l1.position_id,
  l1.effective_date,
  l1.worker_type,
  l1.worker_sub_type,
  l1.business_title,
  l1.business_site_id,
  l1.mailstop_floor,
  l1.worker_status,
  l1.active,
  l1.active_status_date,
  l1.hire_date,
  l1.original_hire_date,
  l1.hire_reason,
  l1.employment_end_date,
  l1.continuous_service_date,
  l1.first_day_of_work,
  l1.expected_retirement_date,
  l1.retirement_eligibility_date,
  l1.retired,
  l1.seniority_date,
  l1.severance_date,
  l1.benefits_service_date,
  l1.company_service_date,
  l1.time_off_service_date,
  l1.vesting_date,
  l1.terminated,
  l1.termination_date,
  l1.pay_through_date,
  l1.primary_termination_reason,
  l1.primary_termination_category,
  l1.termination_involuntary,
  l1.secondary_termination_reason,
  l1.local_termination_reason,
  l1.not_eligible_for_hire,
  l1.regrettable_termination,
  l1.hire_rescinded,
  l1.resignation_date,
  l1.last_day_of_work,
  l1.last_date_for_which_paid,
  l1.expected_date_of_return,
  l1.not_returning,
  l1.return_unknown,
  l1.probation_start_date,
  l1.probation_end_date,
  l1.academic_tenure_date,
  l1.has_international_assignment,
  l1.home_country,
  l1.host_country,
  l1.international_assignment_type,
  l1.start_date_of_international_assignment,
  l1.end_date_of_international_assignment,
  l1.rehire,
  l1.eligible_for_rehire,
  l1.action,
  l1.action_code,
  l1.action_reason,
  l1.action_reason_code,
  l1.manager_id,
  l1.soft_retirement_indicator,
  l1.job_profile_id,
  l1.sequence_number,
  l1.planned_end_contract_date,
  l1.job_entry_dt,
  l1.stock_grants,
  l1.time_type,
  l1.supervisory_organization,
  l1.location,
  l1.job_title,
  l1.french_job_title,
  l1.shift_number,
  l1.scheduled_weekly_hours,
  l1.default_weekly_hours,
  l1.scheduled_fte,
  l1.work_model_start_date,
  l1.work_model_type,
  l1.worker_workday_id,
  l1.ingest_timestamp,
  l1.source_file_name,
  l1.etl_batch_id,
  l1.insert_datetime,
  l1.update_datetime,
  r270.rescinded_moment,
  NULL,
  NULL,
  NULL,
  NULL
FROM v2_l1_workday.int0095e_worker_job l1
LEFT JOIN v2_l1_workday.int270_rescinded r270
  ON l1.transaction_wid = r270.workday_id
  AND r270.idp_table = 'INT095E';

-- STEP 3: UPDATE idp_max_entry_ts = MAX(transaction_entry_date) per (employee_id, transaction_effective_date)
UPDATE v2_l3_workday.l3_workday_worker_job_dly
SET idp_max_entry_ts = (
  SELECT MAX(transaction_entry_date)
  FROM v2_l3_workday.l3_workday_worker_job_dly t2
  WHERE t2.employee_id = v2_l3_workday.l3_workday_worker_job_dly.employee_id
    AND t2.transaction_effective_date = v2_l3_workday.l3_workday_worker_job_dly.transaction_effective_date
);

-- STEP 4: UPDATE idp_min_seq_num = MIN(sequence_number) WHERE transaction_entry_date = idp_max_entry_ts AND idp_obsolete_date IS NULL per (employee_id, transaction_effective_date)
UPDATE v2_l3_workday.l3_workday_worker_job_dly
SET idp_min_seq_num = (
  SELECT MIN(sequence_number)
  FROM v2_l3_workday.l3_workday_worker_job_dly t2
  WHERE t2.employee_id = v2_l3_workday.l3_workday_worker_job_dly.employee_id
    AND t2.transaction_effective_date = v2_l3_workday.l3_workday_worker_job_dly.transaction_effective_date
    AND t2.transaction_entry_date = v2_l3_workday.l3_workday_worker_job_dly.idp_max_entry_ts
    AND t2.idp_obsolete_date IS NULL
);

-- STEP 5: UPDATE idp_calc_end_date = next effective date - 1 day for winning rows (obsolete IS NULL AND entry = max_ts AND seq = min_seq), default to '9999-12-31'
UPDATE v2_l3_workday.l3_workday_worker_job_dly
SET idp_calc_end_date = COALESCE(
  (
    SELECT (MIN(t2.transaction_effective_date::DATE) - 1)::VARCHAR
    FROM v2_l3_workday.l3_workday_worker_job_dly t2
    WHERE t2.employee_id = v2_l3_workday.l3_workday_worker_job_dly.employee_id
      AND t2.transaction_effective_date::DATE > v2_l3_workday.l3_workday_worker_job_dly.transaction_effective_date::DATE
      AND t2.idp_obsolete_date IS NULL
      AND t2.transaction_entry_date = t2.idp_max_entry_ts
      AND t2.sequence_number = t2.idp_min_seq_num
  ),
  '9999-12-31'
);

-- STEP 6: UPDATE idp_employee_status with status logic
UPDATE v2_l3_workday.l3_workday_worker_job_dly
SET idp_employee_status = CASE
  WHEN worker_status = 'Active' THEN 'A'
  WHEN worker_status = 'On Leave' THEN 'L'
  WHEN worker_status = 'Terminated' THEN
    CASE
      WHEN transaction_effective_date::DATE <= pay_through_date::DATE
           AND termination_date::DATE < pay_through_date::DATE THEN 'U'
      WHEN retired = '1' THEN 'R'
      WHEN primary_termination_reason = 'TER-DEA' THEN 'D'
      ELSE 'T'
    END
  ELSE ''
END;

-- ============================================================================
-- WORKER ORGANIZATION (int0096_worker_organization) -> l3_workday_worker_organization_dly
-- ============================================================================

-- STEP 1: TRUNCATE table
TRUNCATE TABLE v2_l3_workday.l3_workday_worker_organization_dly;

-- STEP 2: INSERT all L1 data + idp_obsolete_date from INT270 rescinded join
INSERT INTO v2_l3_workday.l3_workday_worker_organization_dly (
  employee_id,
  transaction_wid,
  transaction_effective_date,
  transaction_entry_date,
  transaction_type,
  organization_id,
  organization_type,
  sequence_number,
  worker_workday_id,
  ingest_timestamp,
  source_file_name,
  etl_batch_id,
  insert_datetime,
  update_datetime,
  idp_obsolete_date,
  idp_calc_end_date,
  idp_max_entry_ts,
  idp_min_seq_num
)
SELECT
  l1.employee_id,
  l1.transaction_wid,
  l1.transaction_effective_date,
  l1.transaction_entry_date,
  l1.transaction_type,
  l1.organization_id,
  l1.organization_type,
  l1.sequence_number,
  l1.worker_workday_id,
  l1.ingest_timestamp,
  l1.source_file_name,
  l1.etl_batch_id,
  l1.insert_datetime,
  l1.update_datetime,
  r270.rescinded_moment,
  NULL,
  NULL,
  NULL
FROM v2_l1_workday.int0096_worker_organization l1
LEFT JOIN v2_l1_workday.int270_rescinded r270
  ON l1.transaction_wid = r270.workday_id
  AND r270.idp_table = 'INT096';

-- STEP 3: UPDATE idp_max_entry_ts = MAX(transaction_entry_date) per (employee_id, transaction_effective_date)
UPDATE v2_l3_workday.l3_workday_worker_organization_dly
SET idp_max_entry_ts = (
  SELECT MAX(transaction_entry_date)
  FROM v2_l3_workday.l3_workday_worker_organization_dly t2
  WHERE t2.employee_id = v2_l3_workday.l3_workday_worker_organization_dly.employee_id
    AND t2.transaction_effective_date = v2_l3_workday.l3_workday_worker_organization_dly.transaction_effective_date
);

-- STEP 4: UPDATE idp_min_seq_num = MIN(sequence_number) WHERE transaction_entry_date = idp_max_entry_ts AND idp_obsolete_date IS NULL per (employee_id, transaction_effective_date)
UPDATE v2_l3_workday.l3_workday_worker_organization_dly
SET idp_min_seq_num = (
  SELECT MIN(sequence_number)
  FROM v2_l3_workday.l3_workday_worker_organization_dly t2
  WHERE t2.employee_id = v2_l3_workday.l3_workday_worker_organization_dly.employee_id
    AND t2.transaction_effective_date = v2_l3_workday.l3_workday_worker_organization_dly.transaction_effective_date
    AND t2.transaction_entry_date = v2_l3_workday.l3_workday_worker_organization_dly.idp_max_entry_ts
    AND t2.idp_obsolete_date IS NULL
);

-- STEP 5: UPDATE idp_calc_end_date = next effective date - 1 day for winning rows (obsolete IS NULL AND entry = max_ts AND seq = min_seq), default to '9999-12-31'
UPDATE v2_l3_workday.l3_workday_worker_organization_dly
SET idp_calc_end_date = COALESCE(
  (
    SELECT (MIN(t2.transaction_effective_date::DATE) - 1)::VARCHAR
    FROM v2_l3_workday.l3_workday_worker_organization_dly t2
    WHERE t2.employee_id = v2_l3_workday.l3_workday_worker_organization_dly.employee_id
      AND t2.transaction_effective_date::DATE > v2_l3_workday.l3_workday_worker_organization_dly.transaction_effective_date::DATE
      AND t2.idp_obsolete_date IS NULL
      AND t2.transaction_entry_date = t2.idp_max_entry_ts
      AND t2.sequence_number = t2.idp_min_seq_num
  ),
  '9999-12-31'
);

-- ============================================================================
-- WORKER COMPENSATION (int0098_worker_compensation) -> l3_workday_worker_comp_dly
-- ============================================================================

-- STEP 1: TRUNCATE table
TRUNCATE TABLE v2_l3_workday.l3_workday_worker_comp_dly;

-- STEP 2: INSERT all L1 data + idp_obsolete_date from INT270 rescinded join
INSERT INTO v2_l3_workday.l3_workday_worker_comp_dly (
  employee_id,
  transaction_wid,
  transaction_effective_date,
  transaction_entry_moment,
  transaction_type,
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
  worker_workday_id,
  ingest_timestamp,
  source_file_name,
  etl_batch_id,
  insert_datetime,
  update_datetime,
  idp_obsolete_date
)
SELECT
  l1.employee_id,
  l1.transaction_wid,
  l1.transaction_effective_date,
  l1.transaction_entry_moment,
  l1.transaction_type,
  l1.compensation_package_proposed,
  l1.compensation_grade_proposed,
  l1.comp_grade_profile_proposed,
  l1.compensation_step_proposed,
  l1.pay_range_minimum,
  l1.pay_range_midpoint,
  l1.pay_range_maximum,
  l1.base_pay_proposed_amount,
  l1.base_pay_proposed_currency,
  l1.base_pay_proposed_frequency,
  l1.benefits_annual_rate_abbr,
  l1.pay_rate_type,
  l1.compensation,
  l1.worker_workday_id,
  l1.ingest_timestamp,
  l1.source_file_name,
  l1.etl_batch_id,
  l1.insert_datetime,
  l1.update_datetime,
  r270.rescinded_moment
FROM v2_l1_workday.int0098_worker_compensation l1
LEFT JOIN v2_l1_workday.int270_rescinded r270
  ON l1.transaction_wid = r270.workday_id
  AND r270.idp_table = 'INT098';
