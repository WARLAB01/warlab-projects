CREATE SCHEMA IF NOT EXISTS v2_l1_workday;
-- int0095e_worker_job
DROP TABLE IF EXISTS v2_l1_workday.int0095e_worker_job;
CREATE TABLE v2_l1_workday.int0095e_worker_job (
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
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int0096_worker_organization
DROP TABLE IF EXISTS v2_l1_workday.int0096_worker_organization;
CREATE TABLE v2_l1_workday.int0096_worker_organization (
    employee_id VARCHAR(1000),
    transaction_wid VARCHAR(1000),
    transaction_effective_date VARCHAR(1000),
    transaction_entry_date VARCHAR(1000),
    transaction_type VARCHAR(1000),
    organization_id VARCHAR(1000),
    organization_type VARCHAR(1000),
    sequence_number VARCHAR(1000),
    worker_workday_id VARCHAR(1000),
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int0098_worker_compensation
DROP TABLE IF EXISTS v2_l1_workday.int0098_worker_compensation;
CREATE TABLE v2_l1_workday.int0098_worker_compensation (
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
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int270_rescinded
DROP TABLE IF EXISTS v2_l1_workday.int270_rescinded;
CREATE TABLE v2_l1_workday.int270_rescinded (
    workday_id VARCHAR(1000),
    idp_table VARCHAR(1000),
    rescinded_moment VARCHAR(1000),
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6020_grade_profile
DROP TABLE IF EXISTS v2_l1_workday.int6020_grade_profile;
CREATE TABLE v2_l1_workday.int6020_grade_profile (
    grade_id VARCHAR(1000),
    grade_name VARCHAR(1000),
    grade_profile_currency_code VARCHAR(1000),
    grade_profile_id VARCHAR(1000),
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
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6021_job_profile
DROP TABLE IF EXISTS v2_l1_workday.int6021_job_profile;
CREATE TABLE v2_l1_workday.int6021_job_profile (
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
    job_profile_id VARCHAR(1000),
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
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6022_job_classification
DROP TABLE IF EXISTS v2_l1_workday.int6022_job_classification;
CREATE TABLE v2_l1_workday.int6022_job_classification (
    job_profile_id VARCHAR(1000),
    job_profile_wid VARCHAR(1000),
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
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6023_location
DROP TABLE IF EXISTS v2_l1_workday.int6023_location;
CREATE TABLE v2_l1_workday.int6023_location (
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
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6024_company
DROP TABLE IF EXISTS v2_l1_workday.int6024_company;
CREATE TABLE v2_l1_workday.int6024_company (
    company_id VARCHAR(1000),
    company_wid VARCHAR(1000),
    company_name VARCHAR(1000),
    company_code VARCHAR(1000),
    business_unit VARCHAR(1000),
    company_subtype VARCHAR(1000),
    company_currency VARCHAR(1000),
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6025_cost_center
DROP TABLE IF EXISTS v2_l1_workday.int6025_cost_center;
CREATE TABLE v2_l1_workday.int6025_cost_center (
    cost_center_id VARCHAR(1000),
    cost_center_wid VARCHAR(1000),
    cost_center_code VARCHAR(1000),
    cost_center_name VARCHAR(1000),
    hierarchy VARCHAR(1000),
    subtype VARCHAR(1000),
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6027_matrix_organization
DROP TABLE IF EXISTS v2_l1_workday.int6027_matrix_organization;
CREATE TABLE v2_l1_workday.int6027_matrix_organization (
    matrix_organization_id VARCHAR(1000),
    matrix_organization_status VARCHAR(1000),
    maxtrix_organization_name VARCHAR(1000),
    maxtrix_organization_code VARCHAR(1000),
    matrix_organization_type VARCHAR(1000),
    matrix_organization_subtype VARCHAR(1000),
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6028_department_hierarchy
DROP TABLE IF EXISTS v2_l1_workday.int6028_department_hierarchy;
CREATE TABLE v2_l1_workday.int6028_department_hierarchy (
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
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6031_worker_profile
DROP TABLE IF EXISTS v2_l1_workday.int6031_worker_profile;
CREATE TABLE v2_l1_workday.int6031_worker_profile (
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
    worker_id VARCHAR(1000),
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
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

-- int6032_positions
DROP TABLE IF EXISTS v2_l1_workday.int6032_positions;
CREATE TABLE v2_l1_workday.int6032_positions (
    position_id VARCHAR(1000),
    supervisory_organization VARCHAR(1000),
    effective_date VARCHAR(1000),
    reason VARCHAR(1000),
    worker_type VARCHAR(1000),
    worker_sub_type VARCHAR(1000),
    job_profile VARCHAR(1000),
    job_title VARCHAR(1000),
    business_title VARCHAR(1000),
    time_type VARCHAR(1000),
    location VARCHAR(1000),
    ingest_timestamp TIMESTAMP DEFAULT GETDATE(),
    source_file_name VARCHAR(500),
    etl_batch_id VARCHAR(100),
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
);

