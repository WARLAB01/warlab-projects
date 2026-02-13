-- ============================================================================
-- HR Datamart V2 - L3 Star Schema Fact Tables DDL
-- Schema: v2_l3_star
-- ============================================================================

-- ============================================================================
-- FACT TABLE 1: fct_worker_movement_f
-- Grain: Transaction - 1 row per employee_id per effective_date
-- Source: dim_worker_job_d (no direct L1 joins)
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.fct_worker_movement_f CASCADE;

CREATE TABLE v2_l3_star.fct_worker_movement_f (
    -- Surrogate key
    worker_movement_sk BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Natural key
    employee_id VARCHAR(256) NOT NULL,
    effective_date DATE NOT NULL,

    -- Current row dimension FKs
    day_sk BIGINT,
    company_sk BIGINT,
    cost_center_sk BIGINT,
    grade_profile_sk BIGINT,
    job_profile_sk BIGINT,
    location_sk BIGINT,
    supervisory_org_sk BIGINT,
    matrix_org_sk BIGINT,
    worker_profile_sk BIGINT,
    worker_job_sk BIGINT,
    worker_status_sk BIGINT,
    report_to_sk BIGINT,

    -- Prior row dimension FKs
    prior_day_sk BIGINT,
    prior_company_sk BIGINT,
    prior_cost_center_sk BIGINT,
    prior_grade_profile_sk BIGINT,
    prior_job_profile_sk BIGINT,
    prior_location_sk BIGINT,
    prior_supervisory_org_sk BIGINT,
    prior_matrix_org_sk BIGINT,
    prior_worker_job_sk BIGINT,
    prior_worker_status_sk BIGINT,
    prior_report_to_sk BIGINT,

    -- Current row natural keys / attributes
    company_id VARCHAR(256),
    cost_center_id VARCHAR(256),
    grade_id VARCHAR(256),
    job_profile_id VARCHAR(256),
    location_id VARCHAR(256),
    sup_org_id VARCHAR(256),
    work_model_type VARCHAR(256),
    base_pay_proposed_amount VARCHAR(256),
    idp_employee_status VARCHAR(256),
    action VARCHAR(256),
    action_reason VARCHAR(256),
    primary_termination_reason VARCHAR(256),
    worker_status VARCHAR(256),

    -- Prior row natural keys / attributes
    prior_company_id VARCHAR(256),
    prior_cost_center_id VARCHAR(256),
    prior_grade_id VARCHAR(256),
    prior_job_profile_id VARCHAR(256),
    prior_location_id VARCHAR(256),
    prior_sup_org_id VARCHAR(256),
    prior_work_model_type VARCHAR(256),
    prior_base_pay_proposed_amount VARCHAR(256),
    prior_idp_employee_status VARCHAR(256),
    prior_effective_date DATE,

    -- ====================================================================
    -- Metrics (28 total)
    -- ====================================================================
    base_pay_change_count INTEGER DEFAULT 0,
    company_change_count INTEGER DEFAULT 0,
    cost_center_change_count INTEGER DEFAULT 0,
    demotion_count INTEGER DEFAULT 0,
    external_hire_count INTEGER DEFAULT 0,
    grade_change_count INTEGER DEFAULT 0,
    grade_decrease_count INTEGER DEFAULT 0,
    grade_increase_count INTEGER DEFAULT 0,
    hire_count INTEGER DEFAULT 0,
    internal_hire_count INTEGER DEFAULT 0,
    involuntary_termination_count INTEGER DEFAULT 0,
    job_change_count INTEGER DEFAULT 0,
    lateral_move_count INTEGER DEFAULT 0,
    location_change_count INTEGER DEFAULT 0,
    management_level_change_count INTEGER DEFAULT 0,
    management_level_decrease_count INTEGER DEFAULT 0,
    management_level_increase_count INTEGER DEFAULT 0,
    matrix_organization_change_count INTEGER DEFAULT 0,
    promotion_count INTEGER DEFAULT 0,
    promotion_count_business_process INTEGER DEFAULT 0,
    regrettable_termination_count INTEGER DEFAULT 0,
    rehire_count INTEGER DEFAULT 0,
    structured_termination_count INTEGER DEFAULT 0,
    supervisory_organization_change_count INTEGER DEFAULT 0,
    termination_count INTEGER DEFAULT 0,
    unstructured_termination_count INTEGER DEFAULT 0,
    voluntary_termination_count INTEGER DEFAULT 0,
    worker_model_change_count INTEGER DEFAULT 0,

    -- Audit
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY(employee_id, effective_date);

-- ============================================================================
-- FACT TABLE 2: fct_worker_headcount_restat_f
-- Grain: Snapshot - 1 row per employee_id per month-end date
-- Restate last 24 months; active (incl. leave) workers at month-end
-- ============================================================================

DROP TABLE IF EXISTS v2_l3_star.fct_worker_headcount_restat_f CASCADE;

CREATE TABLE v2_l3_star.fct_worker_headcount_restat_f (
    -- Surrogate key
    headcount_restat_sk BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Natural key
    snapshot_date DATE NOT NULL,
    employee_id VARCHAR(256) NOT NULL,

    -- Dimension FKs (resolved as-of snapshot_date)
    day_sk BIGINT,
    company_sk BIGINT,
    cost_center_sk BIGINT,
    grade_profile_sk BIGINT,
    job_profile_sk BIGINT,
    location_sk BIGINT,
    supervisory_org_sk BIGINT,
    matrix_org_sk BIGINT,
    worker_profile_sk BIGINT,
    worker_job_sk BIGINT,
    worker_status_sk BIGINT,
    report_to_sk BIGINT,

    -- Natural keys for slicing
    company_id VARCHAR(256),
    cost_center_id VARCHAR(256),
    job_profile_id VARCHAR(256),
    location_id VARCHAR(256),
    sup_org_id VARCHAR(256),
    idp_employee_status VARCHAR(256),

    -- Metric
    headcount INTEGER DEFAULT 0,

    -- Audit
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY(snapshot_date, employee_id);
