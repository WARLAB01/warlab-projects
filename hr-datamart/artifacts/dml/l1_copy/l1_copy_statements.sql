-- ============================================================================
-- L1_WORKDAY COPY STATEMENTS
-- Generated: Data Engineering Automation
-- ============================================================================
-- Full refresh COPY statements for all L1 staging tables
-- Pattern: TRUNCATE + COPY with parameterized S3 paths
-- All statements use transactional wrapping for consistency
-- ============================================================================
-- ============================================================================
-- INT6020_GRADE_PROFILE: Grade Profile Master Data
-- Feed: workday/hrdp/int6020_grade_profile/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int6020_grade_profile;
COPY l1_workday.int6020_grade_profile (
    grade_id,
    grade_name,
    grade_profile_currency_code,
    grade_profile_id,
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
    grade_profile_segement_5_top
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int6020_grade_profile/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT6021_JOB_PROFILE: Job Profile Master Data
-- Feed: workday/hrdp/int6021_job_profile/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int6021_job_profile;
COPY l1_workday.int6021_job_profile (
    compensation_grade,
    critical_job_flag,
    difficult_to_fill_flag,
    inactive_flag,
    job_category_code,
    job_category_name,
    job_exempt_canada,
    job_exempt_us,
    job_family,
    job_family_group,
    job_family_group_name,
    job_family_name,
    job_level_code,
    job_level_name,
    job_profile_code,
    job_profile_description,
    job_profile_id,
    job_profile_name,
    job_profile_summary,
    job_profile_wid,
    job_title,
    management_level_code,
    management_level_name,
    pay_rate_type,
    public_job,
    work_shift_required,
    job_matrix,
    is_people_manager,
    is_manager,
    frequency
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int6021_job_profile/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT6022_JOB_CLASSIFICATION: Job Classification Data
-- Feed: workday/hrdp/int6022_job_classification/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int6022_job_classification;
COPY l1_workday.int6022_job_classification (
    job_profile_id,
    job_profile_wid,
    aap_job_group,
    bonus_eligibility,
    customer_facing,
    eeo1_code,
    job_collection,
    loan_originator_code,
    national_occupation_code,
    occupation_code,
    recruitment_channel,
    standard_occupation_code,
    stock
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int6022_job_classification/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT6023_LOCATION: Location Master Data
-- Feed: workday/hrdp/int6023_location/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int6023_location;
COPY l1_workday.int6023_location (
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
    worksite_id_code
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int6023_location/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT6024_COMPANY: Company Master Data
-- Feed: workday/hrdp/int6024_company/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int6024_company;
COPY l1_workday.int6024_company (
    company_id,
    company_wid,
    company_name,
    company_code,
    business_unit,
    company_subtype,
    company_currency
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int6024_company/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT6025_COST_CENTER: Cost Center Master Data
-- Feed: workday/hrdp/int6025_cost_center/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int6025_cost_center;
COPY l1_workday.int6025_cost_center (
    cost_center_id,
    cost_center_wid,
    cost_center_code,
    cost_center_name,
    hierarchy,
    subtype
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int6025_cost_center/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT0095E_WORKER_JOB: Worker Job Information
-- Feed: workday/hrdp/int0095e_worker_job/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int0095e_worker_job;
COPY l1_workday.int0095e_worker_job (
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
    worker_workday_id
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int0095e_worker_job/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT0096_WORKER_ORGANIZATION: Worker Organization Assignment
-- Feed: workday/hrdp/int0096_worker_organization/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int0096_worker_organization;
COPY l1_workday.int0096_worker_organization (
    employee_id,
    transaction_wid,
    transaction_effective_date,
    transaction_entry_date,
    transaction_type,
    organization_id,
    organization_type,
    sequence_number,
    worker_workday_id
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int0096_worker_organization/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT0098_WORKER_COMPENSATION: Worker Compensation Data
-- Feed: workday/hrdp/int0098_worker_compensation/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int0098_worker_compensation;
COPY l1_workday.int0098_worker_compensation (
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
    worker_workday_id
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int0098_worker_compensation/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT6032_POSITIONS: Position Master Data
-- Feed: workday/hrdp/int6032_positions/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int6032_positions;
COPY l1_workday.int6032_positions (
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
    location
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int6032_positions/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT6028_DEPARTMENT_HIERARCHY: Department Hierarchy Data
-- Feed: workday/hrdp/int6028_department_hierarchy/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int6028_department_hierarchy;
COPY l1_workday.int6028_department_hierarchy (
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
    subtype
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int6028_department_hierarchy/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- INT270_RESCINDED: Rescinded Records
-- Feed: workday/hrdp/int270_rescinded/
-- ============================================================================
BEGIN;
TRUNCATE TABLE l1_workday.int270_rescinded;
COPY l1_workday.int270_rescinded (
    workday_id,
    idp_table,
    rescinded_moment
)
FROM 's3://${S3_BUCKET}/workday/hrdp/int270_rescinded/dt=YYYY-MM-DD/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
DELIMITER '|'
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
ACCEPTINVCHARS
MAXERROR 0
TRUNCATECOLUMNS;
COMMIT;
-- ============================================================================
-- End of L1_WORKDAY COPY Statements
-- ============================================================================