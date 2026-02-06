-- ============================================================================
-- L3 Star Schema - HR Datamart Fact Tables DDL
-- Schema: l3_workday
-- Naming Convention: fct_<name>_f (lowercase snake_case, _f suffix)
-- ============================================================================

-- ============================================================================
-- FACT TABLE 1: fct_worker_movement_f
-- ============================================================================
-- Grain: Transaction - 1 row per employee_id per effective_date
-- Description: Tracks worker movements (job changes, transfers, promotions, etc.)
--              with both current and prior row attributes for change detection.
-- ============================================================================

DROP TABLE IF EXISTS l3_workday.fct_worker_movement_f CASCADE;

CREATE TABLE l3_workday.fct_worker_movement_f (
    -- Surrogate keys
    worker_movement_sk BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Natural key identifiers
    employee_id VARCHAR(15) NOT NULL,
    effective_date DATE NOT NULL,

    -- Current row dimension foreign keys
    day_sk INTEGER,
    company_sk BIGINT,
    cost_center_sk BIGINT,
    grade_profile_sk BIGINT,
    job_profile_sk BIGINT,
    location_sk BIGINT,
    department_sk BIGINT,
    position_sk BIGINT,
    worker_job_sk BIGINT,
    worker_status_sk BIGINT,

    -- Prior row dimension foreign keys
    prior_day_sk INTEGER,
    prior_company_sk BIGINT,
    prior_cost_center_sk BIGINT,
    prior_grade_profile_sk BIGINT,
    prior_job_profile_sk BIGINT,
    prior_location_sk BIGINT,
    prior_department_sk BIGINT,
    prior_position_sk BIGINT,
    prior_worker_job_sk BIGINT,
    prior_worker_status_sk BIGINT,

    -- Current row natural keys / attributes
    company_id VARCHAR(15),
    cost_center_id VARCHAR(15),
    grade_id VARCHAR(15),
    job_profile_id VARCHAR(30),
    location_id VARCHAR(30),
    management_level_code VARCHAR(30),
    matrix_org_id VARCHAR(100),
    sup_org_id VARCHAR(15),
    work_model_type VARCHAR(15),
    base_pay_proposed_amount DECIMAL(19,4),
    idp_employee_status VARCHAR(5),
    -- Business process attributes (for hire/term/promotion metrics)
    action VARCHAR(256),
    action_reason VARCHAR(256),
    primary_termination_category VARCHAR(256),
    primary_termination_reason VARCHAR(256),

    -- Prior row natural keys / attributes
    prior_company_id VARCHAR(15),
    prior_cost_center_id VARCHAR(15),
    prior_grade_id VARCHAR(15),
    prior_job_profile_id VARCHAR(30),
    prior_location_id VARCHAR(30),
    prior_management_level_code VARCHAR(30),
    prior_matrix_org_id VARCHAR(100),
    prior_sup_org_id VARCHAR(15),
    prior_work_model_type VARCHAR(15),
    prior_base_pay_proposed_amount DECIMAL(19,4),
    prior_idp_employee_status VARCHAR(5),
    prior_effective_date DATE,

    -- ====================================================================
    -- Metrics (28 total - alphabetical)
    -- ====================================================================
    base_pay_change_count INTEGER,
    company_change_count INTEGER,
    cost_center_change_count INTEGER,
    demotion_count INTEGER,
    external_hire_count INTEGER,
    grade_change_count INTEGER,
    grade_decrease_count INTEGER,
    grade_increase_count INTEGER,
    hire_count INTEGER,
    internal_hire_count INTEGER,
    involuntary_termination_count INTEGER,
    job_change_count INTEGER,
    lateral_move_count INTEGER,
    location_change_count INTEGER,
    management_level_change_count INTEGER,
    management_level_decrease_count INTEGER,
    management_level_increase_count INTEGER,
    matrix_organization_change_count INTEGER,
    promotion_count INTEGER,
    promotion_count_business_process INTEGER,
    regrettable_termination_count INTEGER,
    rehire_count INTEGER,
    structured_termination_count INTEGER,
    supervisory_organization_change_count INTEGER,
    termination_count INTEGER,
    unstructured_termination_count INTEGER,
    voluntary_termination_count INTEGER,
    worker_model_change_count INTEGER,

    -- Audit columns
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO
SORTKEY(employee_id, effective_date);

-- Create indexes on common query patterns
CREATE INDEX ix_fct_worker_movement_company ON l3_workday.fct_worker_movement_f(company_sk, effective_date);
CREATE INDEX ix_fct_worker_movement_date ON l3_workday.fct_worker_movement_f(effective_date);
CREATE INDEX ix_fct_worker_movement_employee ON l3_workday.fct_worker_movement_f(employee_id);

-- ============================================================================
-- FACT TABLE 2: fct_worker_headcount_restat_f
-- ============================================================================
-- Grain: Snapshot - 1 row per employee_id per month-end date
-- Description: Monthly headcount snapshot for all active workers. Supports
--              restating prior periods for reconciliation and audit.
-- ============================================================================

DROP TABLE IF EXISTS l3_workday.fct_worker_headcount_restat_f CASCADE;

CREATE TABLE l3_workday.fct_worker_headcount_restat_f (
    -- Surrogate keys
    headcount_restat_sk BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Natural key identifiers
    snapshot_date DATE NOT NULL,
    employee_id VARCHAR(15) NOT NULL,

    -- Dimension foreign keys (resolved as-of snapshot_date)
    day_sk INTEGER,
    company_sk BIGINT,
    cost_center_sk BIGINT,
    grade_profile_sk BIGINT,
    job_profile_sk BIGINT,
    location_sk BIGINT,
    department_sk BIGINT,
    position_sk BIGINT,
    worker_job_sk BIGINT,
    worker_status_sk BIGINT,

    -- Natural keys / attributes for slicing
    company_id VARCHAR(15),
    cost_center_id VARCHAR(15),
    job_profile_id VARCHAR(30),
    location_id VARCHAR(30),
    sup_org_id VARCHAR(15),
    idp_employee_status VARCHAR(5),

    -- Metric
    headcount INTEGER,

    -- Audit columns
    insert_datetime TIMESTAMP DEFAULT GETDATE(),
    update_datetime TIMESTAMP DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
)
DISTSTYLE AUTO
SORTKEY(snapshot_date, employee_id);

-- Create indexes on common query patterns
CREATE INDEX ix_fct_worker_headcount_snapshot ON l3_workday.fct_worker_headcount_restat_f(snapshot_date);
CREATE INDEX ix_fct_worker_headcount_employee ON l3_workday.fct_worker_headcount_restat_f(employee_id, snapshot_date);
CREATE INDEX ix_fct_worker_headcount_company ON l3_workday.fct_worker_headcount_restat_f(company_sk, snapshot_date);
