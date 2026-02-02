-- =============================================================================
-- HR Workday Data Warehouse Schema
-- Database: hr_workday_db
-- =============================================================================

-- Create schema for HR data
CREATE SCHEMA IF NOT EXISTS hr_workday;

-- Set search path
SET search_path TO hr_workday, public;

-- =============================================================================
-- CORE HR EMPLOYEES TABLE (Non-transactional master data)
-- =============================================================================
DROP TABLE IF EXISTS hr_workday.core_hr_employees CASCADE;

CREATE TABLE hr_workday.core_hr_employees (
    employee_id             VARCHAR(20) NOT NULL,
    worker_id               VARCHAR(20),
    first_name              VARCHAR(100),
    last_name               VARCHAR(100),
    preferred_name          VARCHAR(100),
    legal_full_name         VARCHAR(200),
    email_work              VARCHAR(200),
    gender                  VARCHAR(10),
    original_hire_date      DATE,
    hire_date               DATE,
    termination_date        DATE,
    worker_status           VARCHAR(50),
    worker_type             VARCHAR(50),
    business_title          VARCHAR(200),
    job_profile             VARCHAR(200),
    job_family              VARCHAR(100),
    job_level               INTEGER,
    management_level        VARCHAR(50),
    supervisory_organization VARCHAR(200),
    manager_employee_id     VARCHAR(20),
    business_unit           VARCHAR(100),
    division                VARCHAR(100),
    department              VARCHAR(100),
    team                    VARCHAR(200),
    cost_center             VARCHAR(50),
    location                VARCHAR(100),
    country                 VARCHAR(50),
    region                  VARCHAR(50),
    pay_rate_type           VARCHAR(50),
    fte                     DECIMAL(5,2),
    base_salary             DECIMAL(15,2),
    bonus_target_percent    DECIMAL(5,4),
    bonus_target_amount     DECIMAL(15,2),
    annual_equity_grant     DECIMAL(15,2),
    total_compensation      DECIMAL(15,2),
    currency                VARCHAR(10),
    car_allowance           DECIMAL(15,2),
    phone_allowance         DECIMAL(15,2),
    executive_perquisite    DECIMAL(15,2),
    last_performance_rating VARCHAR(50),
    years_of_service        INTEGER,
    time_in_position        INTEGER,
    is_manager              BOOLEAN,
    -- Audit columns
    loaded_at               TIMESTAMP DEFAULT GETDATE(),
    source_file             VARCHAR(500),
    PRIMARY KEY (employee_id)
)
DISTSTYLE KEY
DISTKEY (employee_id)
SORTKEY (employee_id, hire_date);

-- =============================================================================
-- JOB MOVEMENT TRANSACTIONS TABLE
-- =============================================================================
DROP TABLE IF EXISTS hr_workday.job_movement_transactions CASCADE;

CREATE TABLE hr_workday.job_movement_transactions (
    transaction_id          VARCHAR(20) NOT NULL,
    employee_id             VARCHAR(20) NOT NULL,
    worker_id               VARCHAR(20),
    effective_date          DATE NOT NULL,
    transaction_type        VARCHAR(50),
    transaction_status      VARCHAR(50),
    reason_code             VARCHAR(100),
    prior_job_profile       VARCHAR(200),
    new_job_profile         VARCHAR(200),
    prior_job_level         INTEGER,
    new_job_level           INTEGER,
    prior_business_unit     VARCHAR(100),
    new_business_unit       VARCHAR(100),
    prior_division          VARCHAR(100),
    new_division            VARCHAR(100),
    prior_department        VARCHAR(100),
    new_department          VARCHAR(100),
    prior_manager_id        VARCHAR(20),
    new_manager_id          VARCHAR(20),
    prior_location          VARCHAR(100),
    new_location            VARCHAR(100),
    prior_worker_type       VARCHAR(50),
    new_worker_type         VARCHAR(50),
    initiated_by            VARCHAR(100),
    initiated_date          DATE,
    completed_date          DATE,
    comments                VARCHAR(500),
    -- Audit columns
    loaded_at               TIMESTAMP DEFAULT GETDATE(),
    source_file             VARCHAR(500),
    PRIMARY KEY (transaction_id)
)
DISTSTYLE KEY
DISTKEY (employee_id)
SORTKEY (employee_id, effective_date);

-- =============================================================================
-- COMPENSATION CHANGE TRANSACTIONS TABLE
-- =============================================================================
DROP TABLE IF EXISTS hr_workday.compensation_change_transactions CASCADE;

CREATE TABLE hr_workday.compensation_change_transactions (
    transaction_id              VARCHAR(20) NOT NULL,
    employee_id                 VARCHAR(20) NOT NULL,
    worker_id                   VARCHAR(20),
    effective_date              DATE NOT NULL,
    transaction_type            VARCHAR(50),
    transaction_status          VARCHAR(50),
    reason_code                 VARCHAR(100),
    prior_base_salary           DECIMAL(15,2),
    new_base_salary             DECIMAL(15,2),
    base_change_amount          DECIMAL(15,2),
    base_change_percent         DECIMAL(8,2),
    prior_bonus_target_percent  DECIMAL(5,4),
    new_bonus_target_percent    DECIMAL(5,4),
    prior_bonus_target_amount   DECIMAL(15,2),
    new_bonus_target_amount     DECIMAL(15,2),
    prior_annual_equity         DECIMAL(15,2),
    new_annual_equity           DECIMAL(15,2),
    allowance_type              VARCHAR(100),
    allowance_amount            DECIMAL(15,2),
    currency                    VARCHAR(10),
    performance_rating          VARCHAR(50),
    compa_ratio_prior           DECIMAL(8,4),
    compa_ratio_new             DECIMAL(8,4),
    initiated_by                VARCHAR(100),
    approved_by                 VARCHAR(100),
    initiated_date              DATE,
    completed_date              DATE,
    comments                    VARCHAR(500),
    -- Audit columns
    loaded_at                   TIMESTAMP DEFAULT GETDATE(),
    source_file                 VARCHAR(500),
    PRIMARY KEY (transaction_id)
)
DISTSTYLE KEY
DISTKEY (employee_id)
SORTKEY (employee_id, effective_date);

-- =============================================================================
-- WORKER MOVEMENT TRANSACTIONS TABLE
-- =============================================================================
DROP TABLE IF EXISTS hr_workday.worker_movement_transactions CASCADE;

CREATE TABLE hr_workday.worker_movement_transactions (
    transaction_id          VARCHAR(20) NOT NULL,
    employee_id             VARCHAR(20) NOT NULL,
    worker_id               VARCHAR(20),
    effective_date          DATE NOT NULL,
    movement_type           VARCHAR(50),
    movement_status         VARCHAR(50),
    reason_code             VARCHAR(100),
    prior_location          VARCHAR(100),
    new_location            VARCHAR(100),
    prior_country           VARCHAR(50),
    new_country             VARCHAR(50),
    prior_region            VARCHAR(50),
    new_region              VARCHAR(50),
    prior_business_unit     VARCHAR(100),
    new_business_unit       VARCHAR(100),
    prior_division          VARCHAR(100),
    new_division            VARCHAR(100),
    prior_department        VARCHAR(100),
    new_department          VARCHAR(100),
    prior_team              VARCHAR(200),
    new_team                VARCHAR(200),
    prior_cost_center       VARCHAR(50),
    new_cost_center         VARCHAR(50),
    prior_manager_id        VARCHAR(20),
    new_manager_id          VARCHAR(20),
    prior_supervisory_org   VARCHAR(200),
    new_supervisory_org     VARCHAR(200),
    relocation_package      VARCHAR(100),
    remote_work_arrangement VARCHAR(50),
    initiated_by            VARCHAR(100),
    approved_by             VARCHAR(100),
    initiated_date          DATE,
    completed_date          DATE,
    comments                VARCHAR(500),
    -- Audit columns
    loaded_at               TIMESTAMP DEFAULT GETDATE(),
    source_file             VARCHAR(500),
    PRIMARY KEY (transaction_id)
)
DISTSTYLE KEY
DISTKEY (employee_id)
SORTKEY (employee_id, effective_date);

-- =============================================================================
-- CREATE INDEXES FOR COMMON QUERY PATTERNS
-- =============================================================================

-- Note: Redshift uses sort keys instead of traditional indexes
-- The SORTKEY definitions above optimize for employee_id + effective_date queries

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================
-- Uncomment and modify as needed for your users/groups

-- GRANT USAGE ON SCHEMA hr_workday TO GROUP analysts;
-- GRANT SELECT ON ALL TABLES IN SCHEMA hr_workday TO GROUP analysts;

-- GRANT ALL ON SCHEMA hr_workday TO GROUP data_engineers;
-- GRANT ALL ON ALL TABLES IN SCHEMA hr_workday TO GROUP data_engineers;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- Run these after loading data to verify the schema

/*
-- Check table sizes
SELECT
    schemaname,
    tablename,
    tbl_rows as row_count,
    size as size_mb
FROM svv_table_info
WHERE schemaname = 'hr_workday'
ORDER BY tablename;

-- Check column definitions
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'hr_workday'
ORDER BY table_name, ordinal_position;
*/

COMMENT ON SCHEMA hr_workday IS 'HR Workday data warehouse - Employee master data and transactional history';
COMMENT ON TABLE hr_workday.core_hr_employees IS 'Non-transactional employee master data snapshot';
COMMENT ON TABLE hr_workday.job_movement_transactions IS 'Employee job changes: hires, promotions, terminations, lateral moves';
COMMENT ON TABLE hr_workday.compensation_change_transactions IS 'Compensation changes: merit, promotions, market adjustments, equity';
COMMENT ON TABLE hr_workday.worker_movement_transactions IS 'Worker movement: transfers, relocations, org changes, work arrangements';
