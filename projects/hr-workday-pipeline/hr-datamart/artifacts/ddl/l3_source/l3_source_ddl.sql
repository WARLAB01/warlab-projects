-- ============================================================================
-- L3 SOURCE DAILY TABLES - DDL
-- HR Datamart - Workday Integration Layer
-- ============================================================================
-- Purpose: Create L3 source tables with IDP columns for daily snapshots
-- Schema: l3_workday
-- Source Schema: l1_workday
-- ============================================================================

-- ============================================================================
-- TABLE 1: l3_workday_worker_job_dly
-- Description: Daily snapshot of worker job information with IDP tracking
-- Source: l1_workday.int0095e_worker_job + l1_workday.int270_rescinded
-- ============================================================================

CREATE TABLE IF NOT EXISTS l3_workday.l3_workday_worker_job_dly (
    -- Source columns from int0095e_worker_job
    employee_id VARCHAR(15),
    transaction_wid VARCHAR(32),
    transaction_effective_date DATE,
    transaction_entry_date TIMESTAMP,
    transaction_type VARCHAR(256),
    position_id VARCHAR(15),
    effective_date DATE,
    worker_type VARCHAR(30),
    worker_sub_type VARCHAR(30),
    business_title VARCHAR(200),
    business_site_id VARCHAR(30),
    mailstop_floor VARCHAR(30),
    worker_status VARCHAR(30),
    active BOOLEAN,
    active_status_date DATE,
    hire_date DATE,
    original_hire_date DATE,
    hire_reason VARCHAR(256),
    employment_end_date DATE,
    continuous_service_date DATE,
    first_day_of_work DATE,
    expected_retirement_date DATE,
    retirement_eligibility_date DATE,
    retired BOOLEAN,
    seniority_date DATE,
    severance_date DATE,
    benefits_service_date DATE,
    company_service_date DATE,
    time_off_service_date DATE,
    vesting_date DATE,
    terminated BOOLEAN,
    termination_date DATE,
    pay_through_date DATE,
    primary_termination_reason VARCHAR(256),
    primary_termination_category VARCHAR(256),
    termination_involuntary BOOLEAN,
    secondary_termination_reason VARCHAR(256),
    local_termination_reason VARCHAR(256),
    not_eligible_for_hire BOOLEAN,
    regrettable_termination BOOLEAN,
    hire_rescinded BOOLEAN,
    resignation_date DATE,
    last_day_of_work DATE,
    last_date_for_which_paid DATE,
    expected_date_of_return DATE,
    not_returning BOOLEAN,
    return_unknown VARCHAR(10),
    probation_start_date DATE,
    probation_end_date DATE,
    academic_tenure_date DATE,
    has_international_assignment BOOLEAN,
    home_country VARCHAR(50),
    host_country VARCHAR(50),
    international_assignment_type VARCHAR(30),
    start_date_of_international_assignment DATE,
    end_date_of_international_assignment DATE,
    rehire BOOLEAN,
    eligible_for_rehire CHAR(1),
    action VARCHAR(256),
    action_code VARCHAR(256),
    action_reason VARCHAR(256),
    action_reason_code VARCHAR(256),
    manager_id VARCHAR(15),
    soft_retirement_indicator BOOLEAN,
    job_profile_id VARCHAR(30),
    sequence_number INTEGER,
    planned_end_contract_date DATE,
    job_entry_dt DATE,
    stock_grants VARCHAR(65535),
    time_type VARCHAR(256),
    supervisory_organization VARCHAR(200),
    location VARCHAR(100),
    job_title VARCHAR(200),
    french_job_title VARCHAR(200),
    shift_number INTEGER,
    scheduled_weekly_hours DECIMAL(3,1),
    default_weekly_hours DECIMAL(3,1),
    scheduled_fte DECIMAL(5,2),
    work_model_start_date DATE,
    work_model_type VARCHAR(15),
    worker_workday_id VARCHAR(32),

    -- IDP Columns
    idp_calc_end_date DATE,
    idp_obsolete_date TIMESTAMP,
    idp_max_entry_ts TIMESTAMP,
    idp_min_seq_num INTEGER,
    idp_employee_status VARCHAR(5),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;

COMMENT ON TABLE l3_workday.l3_workday_worker_job_dly IS
'L3 source daily table for worker job information. Contains all active and historical job records with IDP tracking columns for data quality and versioning.';

-- ============================================================================
-- VIEW 1: l3_workday_worker_job_dly_vw
-- Description: View of all worker job daily records
-- ============================================================================

CREATE OR REPLACE VIEW l3_workday.l3_workday_worker_job_dly_vw AS
SELECT *
FROM l3_workday.l3_workday_worker_job_dly;

COMMENT ON VIEW l3_workday.l3_workday_worker_job_dly_vw IS
'View providing full access to worker job daily snapshot table.';

-- ============================================================================
-- TABLE 2: l3_workday_worker_organization_dly
-- Description: Daily snapshot of worker organization assignments with IDP tracking
-- Source: l1_workday.int0096_worker_organization + l1_workday.int270_rescinded
-- ============================================================================

CREATE TABLE IF NOT EXISTS l3_workday.l3_workday_worker_organization_dly (
    -- Source columns from int0096_worker_organization
    employee_id VARCHAR(15),
    transaction_wid VARCHAR(32),
    transaction_effective_date DATE,
    transaction_entry_date TIMESTAMP,
    transaction_type VARCHAR(256),
    organization_id VARCHAR(15),
    organization_type VARCHAR(15),
    sequence_number INTEGER,
    worker_workday_id VARCHAR(32),

    -- IDP Columns
    idp_calc_end_date DATE,
    idp_obsolete_date TIMESTAMP,
    idp_max_entry_ts TIMESTAMP,
    idp_min_seq_num INTEGER,
    idp_employee_status VARCHAR(5),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;

COMMENT ON TABLE l3_workday.l3_workday_worker_organization_dly IS
'L3 source daily table for worker organization assignments. Contains all organization hierarchy assignments for employees with IDP tracking.';

-- ============================================================================
-- VIEW 2: l3_workday_worker_organization_dly_vw
-- Description: View of all worker organization daily records
-- ============================================================================

CREATE OR REPLACE VIEW l3_workday.l3_workday_worker_organization_dly_vw AS
SELECT *
FROM l3_workday.l3_workday_worker_organization_dly;

COMMENT ON VIEW l3_workday.l3_workday_worker_organization_dly_vw IS
'View providing full access to worker organization daily snapshot table.';

-- ============================================================================
-- TABLE 3: l3_workday_worker_comp_dly
-- Description: Daily snapshot of worker compensation with minimal IDP tracking
-- Source: l1_workday.int0098_worker_compensation + l1_workday.int270_rescinded
-- Note: This table includes ONLY idp_obsolete_date, no other IDP columns
-- ============================================================================

CREATE TABLE IF NOT EXISTS l3_workday.l3_workday_worker_comp_dly (
    -- Source columns from int0098_worker_compensation
    employee_id VARCHAR(15),
    transaction_wid VARCHAR(32),
    transaction_effective_date DATE,
    transaction_entry_moment TIMESTAMP,
    transaction_type VARCHAR(256),
    compensation_package_proposed VARCHAR(15),
    compensation_grade_proposed VARCHAR(15),
    comp_grade_profile_proposed VARCHAR(30),
    compensation_step_proposed VARCHAR(30),
    pay_range_minimum DECIMAL(19,4),
    pay_range_midpoint DECIMAL(19,4),
    pay_range_maximum DECIMAL(19,4),
    base_pay_proposed_amount DECIMAL(19,4),
    base_pay_proposed_currency VARCHAR(3),
    base_pay_proposed_frequency VARCHAR(30),
    benefits_annual_rate_abbr DECIMAL(19,4),
    pay_rate_type VARCHAR(30),
    compensation DECIMAL(19,4),
    worker_workday_id VARCHAR(32),

    -- IDP Columns (ONLY idp_obsolete_date for this table)
    idp_obsolete_date TIMESTAMP,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;

COMMENT ON TABLE l3_workday.l3_workday_worker_comp_dly IS
'L3 source daily table for worker compensation data. Contains compensation details with minimal IDP tracking (obsolescence only).';

-- ============================================================================
-- VIEW 3: l3_workday_worker_comp_dly_vw
-- Description: View of all worker compensation daily records
-- ============================================================================

CREATE OR REPLACE VIEW l3_workday.l3_workday_worker_comp_dly_vw AS
SELECT *
FROM l3_workday.l3_workday_worker_comp_dly;

COMMENT ON VIEW l3_workday.l3_workday_worker_comp_dly_vw IS
'View providing full access to worker compensation daily snapshot table.';

-- ============================================================================
-- END OF DDL
-- ============================================================================
