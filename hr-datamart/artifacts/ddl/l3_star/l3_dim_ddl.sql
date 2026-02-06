-- ============================================================================
-- L3 STAR SCHEMA DIMENSION DDL
-- Schema: l3_workday
-- Purpose: Create all dimension tables with SCD2 support (except dim_day_d)
-- ============================================================================

-- ============================================================================
-- 1. DIM_DAY_D - SCD1 Date Spine (NOT SCD2)
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_day_d;

CREATE TABLE l3_workday.dim_day_d (
    day_sk INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(256),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(256),
    quarter_number INTEGER,
    quarter_name VARCHAR(5),
    year_number INTEGER,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_quarter_name VARCHAR(5),
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    is_quarter_end BOOLEAN,
    is_year_end BOOLEAN,
    insert_datetime TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO;



-- ============================================================================
-- 2. DIM_COMPANY_D - SCD2 from INT6024
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_company_d;

CREATE TABLE l3_workday.dim_company_d (
    company_sk BIGINT IDENTITY(1,1),
    company_id VARCHAR(15) NOT NULL,
    company_wid VARCHAR(32),
    company_code VARCHAR(15),
    company_name VARCHAR(100),
    company_subtype VARCHAR(15),
    company_currency VARCHAR(3),
    business_unit VARCHAR(30),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;



-- ============================================================================
-- 3. DIM_COST_CENTER_D - SCD2 from INT6025
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_cost_center_d;

CREATE TABLE l3_workday.dim_cost_center_d (
    cost_center_sk BIGINT IDENTITY(1,1),
    cost_center_id VARCHAR(15) NOT NULL,
    cost_center_wid VARCHAR(32),
    cost_center_code VARCHAR(15),
    cost_center_name VARCHAR(100),
    hierarchy VARCHAR(2000),
    subtype VARCHAR(15),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;



-- ============================================================================
-- 4. DIM_GRADE_PROFILE_D - SCD2 from INT6020
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_grade_profile_d;

CREATE TABLE l3_workday.dim_grade_profile_d (
    grade_profile_sk BIGINT IDENTITY(1,1),
    grade_profile_id VARCHAR(30) NOT NULL,
    grade_id VARCHAR(15),
    grade_name VARCHAR(200),
    grade_profile_currency_code VARCHAR(3),
    effective_date DATE,
    grade_profile_name VARCHAR(200),
    grade_profile_number_of_segements INTEGER,
    grade_profile_salary_range_maximum DECIMAL(19,4),
    grade_profile_salary_range_midpoint DECIMAL(19,4),
    grade_profile_salary_range_minimjum DECIMAL(19,4),
    grade_profile_segement_1_top DECIMAL(19,4),
    grade_profile_segement_2_top DECIMAL(19,4),
    grade_profile_segement_3_top DECIMAL(19,4),
    grade_profile_segement_4_top DECIMAL(19,4),
    grade_profile_segement_5_top DECIMAL(19,4),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;



-- ============================================================================
-- 5. DIM_JOB_PROFILE_D - SCD2 from INT6021 + INT6022
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_job_profile_d;

CREATE TABLE l3_workday.dim_job_profile_d (
    job_profile_sk BIGINT IDENTITY(1,1),
    job_profile_id VARCHAR(30) NOT NULL,
    -- From INT6021
    compensation_grade VARCHAR(30),
    critical_job_flag VARCHAR(256),
    difficult_to_fill_flag VARCHAR(256),
    inactive_flag VARCHAR(256),
    job_category_code VARCHAR(30),
    job_category_name VARCHAR(200),
    job_exempt_canada VARCHAR(30),
    job_exempt_us VARCHAR(30),
    job_family VARCHAR(100),
    job_family_group VARCHAR(100),
    job_family_group_name VARCHAR(200),
    job_family_name VARCHAR(200),
    job_level_code VARCHAR(30),
    job_level_name VARCHAR(100),
    job_profile_code VARCHAR(30),
    job_profile_description VARCHAR(2000),
    job_profile_name VARCHAR(200),
    job_profile_summary VARCHAR(2000),
    job_profile_wid VARCHAR(32),
    job_title VARCHAR(200),
    management_level_code VARCHAR(30),
    management_level_name VARCHAR(100),
    pay_rate_type VARCHAR(30),
    public_job VARCHAR(256),
    work_shift_required VARCHAR(256),
    job_matrix VARCHAR(100),
    is_people_manager VARCHAR(256),
    is_manager VARCHAR(256),
    frequency VARCHAR(30),
    -- From INT6022
    aap_job_group VARCHAR(100),
    bonus_eligibility VARCHAR(256),
    customer_facing VARCHAR(256),
    eeo1_code VARCHAR(30),
    job_collection VARCHAR(100),
    loan_originator_code VARCHAR(30),
    national_occupation_code VARCHAR(30),
    occupation_code VARCHAR(30),
    recruitment_channel VARCHAR(100),
    standard_occupation_code VARCHAR(30),
    stock VARCHAR(100),
    -- SCD2
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;



-- ============================================================================
-- 6. DIM_LOCATION_D - SCD2 from INT6023
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_location_d;

CREATE TABLE l3_workday.dim_location_d (
    location_sk BIGINT IDENTITY(1,1),
    location_id VARCHAR(30) NOT NULL,
    location_wid VARCHAR(32),
    location_name VARCHAR(200),
    inactive VARCHAR(256),
    address_line_1 VARCHAR(256),
    address_line_2 VARCHAR(256),
    city VARCHAR(100),
    region VARCHAR(100),
    region_name VARCHAR(100),
    country VARCHAR(100),
    country_name VARCHAR(100),
    location_postal_code VARCHAR(30),
    location_identifier VARCHAR(100),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    location_type VARCHAR(100),
    location_usage_type VARCHAR(100),
    trade_name VARCHAR(200),
    worksite_id_code VARCHAR(100),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;



-- ============================================================================
-- 7. DIM_DEPARTMENT_D - SCD2 from INT6028 (Supervisory Organizations)
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_department_d;

CREATE TABLE l3_workday.dim_department_d (
    department_sk BIGINT IDENTITY(1,1),
    department_id VARCHAR(15) NOT NULL,
    department_wid VARCHAR(32),
    department_name VARCHAR(200),
    dept_name_with_manager_name VARCHAR(400),
    active VARCHAR(256),
    parent_dept_id VARCHAR(15),
    owner_ein VARCHAR(30),
    department_level VARCHAR(30),
    primary_location_code VARCHAR(30),
    type VARCHAR(100),
    subtype VARCHAR(100),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;



-- ============================================================================
-- 8. DIM_POSITION_D - SCD2 from INT6032
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_position_d;

CREATE TABLE l3_workday.dim_position_d (
    position_sk BIGINT IDENTITY(1,1),
    position_id VARCHAR(30) NOT NULL,
    supervisory_organization VARCHAR(200),
    effective_date DATE,
    reason VARCHAR(256),
    worker_type VARCHAR(30),
    worker_sub_type VARCHAR(30),
    job_profile VARCHAR(30),
    job_title VARCHAR(200),
    business_title VARCHAR(200),
    time_type VARCHAR(30),
    location VARCHAR(100),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;



-- ============================================================================
-- 9. DIM_WORKER_JOB_D - SCD2, BK = (employee_id, effective_date)
-- This is the most complex dimension, combining Worker Job, Compensation, Organization
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_worker_job_d;

CREATE TABLE l3_workday.dim_worker_job_d (
    worker_job_sk BIGINT IDENTITY(1,1),
    employee_id VARCHAR(15) NOT NULL,
    effective_date DATE NOT NULL,
    -- Worker Job attributes
    position_id VARCHAR(15),
    worker_type VARCHAR(30),
    worker_sub_type VARCHAR(30),
    business_title VARCHAR(200),
    business_site_id VARCHAR(30),
    mailstop_floor VARCHAR(30),
    worker_status VARCHAR(30),
    active VARCHAR(256),
    first_day_of_work DATE,
    expected_date_of_return DATE,
    not_returning VARCHAR(256),
    return_unknown VARCHAR(256),
    probation_start_date DATE,
    probation_end_date DATE,
    academic_tenure_date DATE,
    has_international_assignment VARCHAR(256),
    home_country VARCHAR(50),
    host_country VARCHAR(50),
    international_assignment_type VARCHAR(30),
    start_date_of_international_assignment DATE,
    end_date_of_international_assignment DATE,
    action VARCHAR(256),
    action_code VARCHAR(256),
    action_reason VARCHAR(256),
    action_reason_code VARCHAR(256),
    manager_id VARCHAR(15),
    soft_retirement_indicator VARCHAR(256),
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
    idp_employee_status VARCHAR(5),
    -- Worker Compensation attributes
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
    -- Worker Organization attributes (resolved)
    cost_center_id VARCHAR(15),
    company_id VARCHAR(15),
    sup_org_id VARCHAR(15),
    -- SCD2 and time-series columns
    effective_date_from DATE NOT NULL,
    effective_date_to DATE NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    is_current_job_row BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;



-- ============================================================================
-- 10. DIM_WORKER_STATUS_D - SCD2, BK = (employee_id, effective_date)
-- Contains only status-related attributes from Worker Job
-- ============================================================================
DROP TABLE IF EXISTS l3_workday.dim_worker_status_d;

CREATE TABLE l3_workday.dim_worker_status_d (
    worker_status_sk BIGINT IDENTITY(1,1),
    employee_id VARCHAR(15) NOT NULL,
    effective_date DATE NOT NULL,
    -- Status-related attributes from Worker Job
    active_status_date DATE,
    benefits_service_date DATE,
    continuous_service_date DATE,
    planned_end_contract_date DATE,
    hire_date DATE,
    eligible_for_rehire VARCHAR(256),
    not_eligible_for_hire VARCHAR(256),
    active VARCHAR(256),
    worker_status VARCHAR(30),
    employment_end_date DATE,
    hire_reason VARCHAR(256),
    hire_rescinded VARCHAR(256),
    original_hire_date DATE,
    primary_termination_category VARCHAR(256),
    primary_termination_reason VARCHAR(256),
    retired VARCHAR(256),
    retirement_eligibility_date DATE,
    expected_retirement_date DATE,
    seniority_date DATE,
    termination_date DATE,
    -- SCD2 and time-series columns
    effective_date_from DATE NOT NULL,
    effective_date_to DATE NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(64),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO;


-- ============================================================================
-- END OF DDL
-- ============================================================================