-- ============================================================================
-- L1_WORKDAY SCHEMA - RAW STAGING LAYER
-- Generated: Data Engineering Automation
-- ============================================================================
-- Raw staging tables for Workday HRDP feeds
-- All tables include standard warehouse audit columns
-- Primary keys documented as comments (Redshift doesn't enforce them)
-- ============================================================================


-- ============================================================================
-- Table: l1_workday.int6020_grade_profile
-- Feed: Grade Profile Master Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int6020_grade_profile;

CREATE TABLE l1_workday.int6020_grade_profile (
    grade_id VARCHAR(15) ENCODE lzo,
    grade_name VARCHAR(200) ENCODE lzo,
    grade_profile_currency_code VARCHAR(3) ENCODE raw,
    grade_profile_id VARCHAR(30) ENCODE raw,
    effective_date DATE,
    grade_profile_name VARCHAR(200) ENCODE lzo,
    grade_profile_number_of_segements INTEGER,
    grade_profile_salary_range_maximum DECIMAL(19,4),
    grade_profile_salary_range_midpoint DECIMAL(19,4),
    grade_profile_salary_range_minimjum DECIMAL(19,4),
    grade_profile_segement_1_top DECIMAL(19,4),
    grade_profile_segement_2_top DECIMAL(19,4),
    grade_profile_segement_3_top DECIMAL(19,4),
    grade_profile_segement_4_top DECIMAL(19,4),
    grade_profile_segement_5_top DECIMAL(19,4),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: grade_profile_id
;


-- ============================================================================
-- Table: l1_workday.int6021_job_profile
-- Feed: Job Profile Master Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int6021_job_profile;

CREATE TABLE l1_workday.int6021_job_profile (
    compensation_grade VARCHAR(15) ENCODE lzo,
    critical_job_flag CHAR(1) ENCODE raw,
    difficult_to_fill_flag CHAR(1) ENCODE raw,
    inactive_flag BOOLEAN,
    job_category_code VARCHAR(30) ENCODE lzo,
    job_category_name VARCHAR(100) ENCODE lzo,
    job_exempt_canada VARCHAR(50) ENCODE lzo,
    job_exempt_us VARCHAR(50) ENCODE lzo,
    job_family VARCHAR(15) ENCODE lzo,
    job_family_group VARCHAR(100) ENCODE lzo,
    job_family_group_name VARCHAR(50) ENCODE lzo,
    job_family_name VARCHAR(100) ENCODE lzo,
    job_level_code VARCHAR(15) ENCODE lzo,
    job_level_name VARCHAR(15) ENCODE lzo,
    job_profile_code VARCHAR(30) ENCODE lzo,
    job_profile_description VARCHAR(65535) ENCODE lzo,
    job_profile_id VARCHAR(30) ENCODE raw,
    job_profile_name VARCHAR(200) ENCODE lzo,
    job_profile_summary VARCHAR(65535) ENCODE lzo,
    job_profile_wid VARCHAR(32) ENCODE raw,
    job_title VARCHAR(200) ENCODE lzo,
    management_level_code VARCHAR(30) ENCODE lzo,
    management_level_name VARCHAR(200) ENCODE lzo,
    pay_rate_type VARCHAR(30) ENCODE lzo,
    public_job BOOLEAN,
    work_shift_required BOOLEAN,
    job_matrix VARCHAR(100) ENCODE lzo,
    is_people_manager BOOLEAN,
    is_manager BOOLEAN,
    frequency VARCHAR(15) ENCODE lzo,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: job_profile_id
;


-- ============================================================================
-- Table: l1_workday.int6022_job_classification
-- Feed: Job Classification Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int6022_job_classification;

CREATE TABLE l1_workday.int6022_job_classification (
    job_profile_id VARCHAR(30) ENCODE raw,
    job_profile_wid VARCHAR(32) ENCODE raw,
    aap_job_group VARCHAR(200) ENCODE lzo,
    bonus_eligibility VARCHAR(100) ENCODE lzo,
    customer_facing VARCHAR(15) ENCODE lzo,
    eeo1_code VARCHAR(100) ENCODE lzo,
    job_collection VARCHAR(100) ENCODE lzo,
    loan_originator_code VARCHAR(30) ENCODE lzo,
    national_occupation_code VARCHAR(200) ENCODE lzo,
    occupation_code VARCHAR(200) ENCODE lzo,
    recruitment_channel VARCHAR(15) ENCODE lzo,
    standard_occupation_code VARCHAR(200) ENCODE lzo,
    stock VARCHAR(100) ENCODE lzo,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: job_profile_id
;


-- ============================================================================
-- Table: l1_workday.int6023_location
-- Feed: Location Master Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int6023_location;

CREATE TABLE l1_workday.int6023_location (
    location_id VARCHAR(30) ENCODE raw,
    location_wid VARCHAR(30) ENCODE raw,
    location_name VARCHAR(100) ENCODE lzo,
    inactive VARCHAR(5) ENCODE lzo,
    address_line_1 VARCHAR(200) ENCODE lzo,
    address_line_2 VARCHAR(200) ENCODE lzo,
    city VARCHAR(100) ENCODE lzo,
    region VARCHAR(2) ENCODE raw,
    region_name VARCHAR(50) ENCODE lzo,
    country VARCHAR(2) ENCODE raw,
    country_name VARCHAR(50) ENCODE lzo,
    location_postal_code VARCHAR(15) ENCODE lzo,
    location_identifier VARCHAR(30) ENCODE lzo,
    latitude DECIMAL(11,8),
    longitude DECIMAL(11,8),
    location_type VARCHAR(30) ENCODE lzo,
    location_usage_type VARCHAR(30) ENCODE lzo,
    trade_name VARCHAR(30) ENCODE lzo,
    worksite_id_code VARCHAR(30) ENCODE lzo,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: location_id
;


-- ============================================================================
-- Table: l1_workday.int6024_company
-- Feed: Company Master Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int6024_company;

CREATE TABLE l1_workday.int6024_company (
    company_id VARCHAR(15) ENCODE raw,
    company_wid VARCHAR(32) ENCODE raw,
    company_name VARCHAR(100) ENCODE lzo,
    company_code VARCHAR(15) ENCODE lzo,
    business_unit VARCHAR(30) ENCODE lzo,
    company_subtype VARCHAR(15) ENCODE lzo,
    company_currency VARCHAR(3) ENCODE raw,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: company_id
;


-- ============================================================================
-- Table: l1_workday.int6025_cost_center
-- Feed: Cost Center Master Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int6025_cost_center;

CREATE TABLE l1_workday.int6025_cost_center (
    cost_center_id VARCHAR(15) ENCODE raw,
    cost_center_wid VARCHAR(32) ENCODE raw,
    cost_center_code VARCHAR(15) ENCODE lzo,
    cost_center_name VARCHAR(100) ENCODE lzo,
    hierarchy VARCHAR(2000) ENCODE lzo,
    subtype VARCHAR(15) ENCODE lzo,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: cost_center_id
;


-- ============================================================================
-- Table: l1_workday.int0095e_worker_job
-- Feed: Worker Job Information
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int0095e_worker_job;

CREATE TABLE l1_workday.int0095e_worker_job (
    employee_id VARCHAR(15) ENCODE raw,
    transaction_wid VARCHAR(32) ENCODE raw,
    transaction_effective_date DATE,
    transaction_entry_date TIMESTAMP,
    transaction_type VARCHAR(256) ENCODE lzo,
    position_id VARCHAR(15) ENCODE raw,
    effective_date DATE,
    worker_type VARCHAR(30) ENCODE lzo,
    worker_sub_type VARCHAR(30) ENCODE lzo,
    business_title VARCHAR(200) ENCODE lzo,
    business_site_id VARCHAR(30) ENCODE lzo,
    mailstop_floor VARCHAR(30) ENCODE lzo,
    worker_status VARCHAR(30) ENCODE lzo,
    active BOOLEAN,
    active_status_date DATE,
    hire_date DATE,
    original_hire_date DATE,
    hire_reason VARCHAR(256) ENCODE lzo,
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
    primary_termination_reason VARCHAR(256) ENCODE lzo,
    primary_termination_category VARCHAR(256) ENCODE lzo,
    termination_involuntary BOOLEAN,
    secondary_termination_reason VARCHAR(256) ENCODE lzo,
    local_termination_reason VARCHAR(256) ENCODE lzo,
    not_eligible_for_hire BOOLEAN,
    regrettable_termination BOOLEAN,
    hire_rescinded BOOLEAN,
    resignation_date DATE,
    last_day_of_work DATE,
    last_date_for_which_paid DATE,
    expected_date_of_return DATE,
    not_returning BOOLEAN,
    return_unknown VARCHAR(10) ENCODE lzo,
    probation_start_date DATE,
    probation_end_date DATE,
    academic_tenure_date DATE,
    has_international_assignment BOOLEAN,
    home_country VARCHAR(50) ENCODE lzo,
    host_country VARCHAR(50) ENCODE lzo,
    international_assignment_type VARCHAR(30) ENCODE lzo,
    start_date_of_international_assignment DATE,
    end_date_of_international_assignment DATE,
    rehire BOOLEAN,
    eligible_for_rehire CHAR(1) ENCODE raw,
    action VARCHAR(256) ENCODE lzo,
    action_code VARCHAR(256) ENCODE lzo,
    action_reason VARCHAR(256) ENCODE lzo,
    action_reason_code VARCHAR(256) ENCODE lzo,
    manager_id VARCHAR(15) ENCODE raw,
    soft_retirement_indicator BOOLEAN,
    job_profile_id VARCHAR(30) ENCODE raw,
    sequence_number INTEGER,
    planned_end_contract_date DATE,
    job_entry_dt DATE,
    stock_grants VARCHAR(65535) ENCODE lzo,
    time_type VARCHAR(256) ENCODE lzo,
    supervisory_organization VARCHAR(200) ENCODE lzo,
    location VARCHAR(100) ENCODE lzo,
    job_title VARCHAR(200) ENCODE lzo,
    french_job_title VARCHAR(200) ENCODE lzo,
    shift_number INTEGER,
    scheduled_weekly_hours DECIMAL(3,1),
    default_weekly_hours DECIMAL(3,1),
    scheduled_fte DECIMAL(5,2),
    work_model_start_date DATE,
    work_model_type VARCHAR(15) ENCODE lzo,
    worker_workday_id VARCHAR(32) ENCODE raw,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: employee_id, transaction_wid, position_id, effective_date
;


-- ============================================================================
-- Table: l1_workday.int0096_worker_organization
-- Feed: Worker Organization Assignment
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int0096_worker_organization;

CREATE TABLE l1_workday.int0096_worker_organization (
    employee_id VARCHAR(15) ENCODE raw,
    transaction_wid VARCHAR(32) ENCODE raw,
    transaction_effective_date DATE,
    transaction_entry_date TIMESTAMP,
    transaction_type VARCHAR(256) ENCODE lzo,
    organization_id VARCHAR(15) ENCODE raw,
    organization_type VARCHAR(15) ENCODE lzo,
    sequence_number INTEGER,
    worker_workday_id VARCHAR(32) ENCODE raw,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: employee_id, transaction_wid, organization_id, organization_type
;


-- ============================================================================
-- Table: l1_workday.int0098_worker_compensation
-- Feed: Worker Compensation Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int0098_worker_compensation;

CREATE TABLE l1_workday.int0098_worker_compensation (
    employee_id VARCHAR(15) ENCODE raw,
    transaction_wid VARCHAR(32) ENCODE raw,
    transaction_effective_date DATE,
    transaction_entry_moment TIMESTAMP,
    transaction_type VARCHAR(256) ENCODE lzo,
    compensation_package_proposed VARCHAR(15) ENCODE lzo,
    compensation_grade_proposed VARCHAR(15) ENCODE lzo,
    comp_grade_profile_proposed VARCHAR(30) ENCODE lzo,
    compensation_step_proposed VARCHAR(30) ENCODE lzo,
    pay_range_minimum DECIMAL(19,4),
    pay_range_midpoint DECIMAL(19,4),
    pay_range_maximum DECIMAL(19,4),
    base_pay_proposed_amount DECIMAL(19,4),
    base_pay_proposed_currency VARCHAR(3) ENCODE raw,
    base_pay_proposed_frequency VARCHAR(30) ENCODE lzo,
    benefits_annual_rate_abbr DECIMAL(19,4),
    pay_rate_type VARCHAR(30) ENCODE lzo,
    compensation DECIMAL(19,4),
    worker_workday_id VARCHAR(32) ENCODE raw,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: employee_id, transaction_wid
;


-- ============================================================================
-- Table: l1_workday.int6032_positions
-- Feed: Position Master Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int6032_positions;

CREATE TABLE l1_workday.int6032_positions (
    position_id VARCHAR(30) ENCODE raw,
    supervisory_organization VARCHAR(100) ENCODE lzo,
    effective_date DATE,
    reason VARCHAR(256) ENCODE lzo,
    worker_type VARCHAR(30) ENCODE lzo,
    worker_sub_type VARCHAR(30) ENCODE lzo,
    job_profile VARCHAR(30) ENCODE lzo,
    job_title VARCHAR(200) ENCODE lzo,
    business_title VARCHAR(200) ENCODE lzo,
    time_type VARCHAR(256) ENCODE lzo,
    location VARCHAR(100) ENCODE lzo,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: position_id
;


-- ============================================================================
-- Table: l1_workday.int6028_department_hierarchy
-- Feed: Department Hierarchy Data
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int6028_department_hierarchy;

CREATE TABLE l1_workday.int6028_department_hierarchy (
    department_id VARCHAR(15) ENCODE raw,
    department_wid VARCHAR(32) ENCODE raw,
    department_name VARCHAR(200) ENCODE lzo,
    dept_name_with_manager_name VARCHAR(200) ENCODE lzo,
    active BOOLEAN,
    parent_dept_id VARCHAR(15) ENCODE raw,
    owner_ein VARCHAR(15) ENCODE lzo,
    department_level INTEGER,
    primary_location_code VARCHAR(15) ENCODE lzo,
    type VARCHAR(30) ENCODE lzo,
    subtype VARCHAR(30) ENCODE lzo,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: department_id
;


-- ============================================================================
-- Table: l1_workday.int270_rescinded
-- Feed: Rescinded Records
-- ============================================================================
DROP TABLE IF EXISTS l1_workday.int270_rescinded;

CREATE TABLE l1_workday.int270_rescinded (
    workday_id VARCHAR(32) ENCODE raw,
    idp_table VARCHAR(256) ENCODE lzo,
    rescinded_moment TIMESTAMP,
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50) ENCODE lzo,
    source_file_name VARCHAR(500) ENCODE lzo,
    ingest_timestamp TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
-- PK: workday_id
;

-- ============================================================================
-- End of L1_WORKDAY Schema DDL
-- ============================================================================
