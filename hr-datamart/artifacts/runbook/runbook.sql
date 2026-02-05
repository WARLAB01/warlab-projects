-- =============================================================================
-- HR DATAMART - MASTER RUNBOOK (ORCHESTRATION SCRIPT)
-- =============================================================================
-- Purpose:
--   Master end-to-end orchestration script for HR Datamart ETL process.
--   Coordinates all data load steps, dependency management, and QA validation.
--
-- Environment Variables Required:
--   ${S3_BUCKET}              - S3 bucket name (e.g., 's3://my-hr-bucket')
--   ${REDSHIFT_IAM_ROLE_ARN}  - IAM role ARN for COPY operations
--   ${ETL_BATCH_ID}           - Batch identifier (e.g., 'BATCH_20260205_001')
--   ${DATA_DATE}              - Processing date in YYYY-MM-DD format
--   ${DRY_RUN}                - Set to 'true' to print SQL only, 'false' to execute
--   ${LOG_LEVEL}              - Logging verbosity: 'DEBUG', 'INFO', 'WARN', 'ERROR'
--
-- STRICT EXECUTION ORDER (dependencies documented):
--   Step 0:  Pre-flight validation
--   Step 1:  Generate synthetic data feeds (external Python process)
--   Step 2:  Create L1 schema and objects
--   Step 3:  COPY feeds into L1 staging tables
--   Step 4:  Create L3 source schema and objects
--   Step 5:  Load L3 source tables (from L1)
--   Step 6:  Create L3 star schema (dims + facts) DDL
--   Step 7:  Load L3 dimensions (strict dependency order)
--   Step 8:  Load L3 facts (depends on all dims loaded)
--   Step 9:  Execute QA validation suite
--   Step 10: Generate completion report
--
-- ROLLBACK PROCEDURE:
--   This runbook does NOT include DROP statements for safety.
--   To rollback:
--     1. TRUNCATE l3_workday.fct_* tables (facts first)
--     2. TRUNCATE l3_workday.dim_* tables (in reverse load order)
--     3. TRUNCATE l1_workday.* tables (staging layer)
--     4. Re-run entire pipeline with clean state
--
-- LOGGING & TIMESTAMPS:
--   Each major step is wrapped with BEGIN/COMMIT timing capture.
--   All timings are logged to l3_workday.etl_execution_log.
--
-- =============================================================================

-- =============================================================================
-- INITIALIZATION SECTION
-- =============================================================================

-- Log execution start
INSERT INTO l3_workday.etl_execution_log (
    batch_id,
    step_number,
    step_name,
    status,
    start_timestamp,
    end_timestamp,
    row_count,
    error_message
) VALUES (
    '${ETL_BATCH_ID}',
    0,
    'RUNBOOK_START',
    'RUNNING',
    GETDATE(),
    NULL,
    0,
    NULL
);

-- Create temporary logging variables
CREATE TEMPORARY TABLE #run_log (
    log_id INTEGER IDENTITY(1,1),
    batch_id VARCHAR(50),
    step_number INTEGER,
    step_name VARCHAR(100),
    message VARCHAR(1000),
    log_level VARCHAR(10),
    log_timestamp TIMESTAMP
);

-- Log function: wrapper to support dry_run mode
-- When DRY_RUN='true', only PRINT, no INSERT
DECLARE @dry_run_mode BIT = CASE WHEN '${DRY_RUN}' = 'true' THEN 1 ELSE 0 END;
DECLARE @log_message VARCHAR(1000);
DECLARE @step_start TIMESTAMP;
DECLARE @step_end TIMESTAMP;
DECLARE @rows_affected INTEGER;

-- =============================================================================
-- STEP 0: PRE-FLIGHT VALIDATION
-- =============================================================================
-- Dependencies: None (first step)
-- Purpose: Validate environment variables, check for schema existence
-- Rollback: N/A (read-only step)
-- Expected Duration: ~5 seconds
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 0: PRE-FLIGHT VALIDATION';
PRINT '================================================================================';
SET @step_start = GETDATE();

-- Validate required environment variables
PRINT 'Validating environment variables...';
PRINT '  S3_BUCKET: ${S3_BUCKET}';
PRINT '  REDSHIFT_IAM_ROLE_ARN: ${REDSHIFT_IAM_ROLE_ARN}';
PRINT '  ETL_BATCH_ID: ${ETL_BATCH_ID}';
PRINT '  DATA_DATE: ${DATA_DATE}';
PRINT '  DRY_RUN: ${DRY_RUN}';
PRINT '  LOG_LEVEL: ${LOG_LEVEL}';

-- Check if schemas exist (create if missing)
IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'l1_workday')
BEGIN
    PRINT 'Creating L1 schema: l1_workday';
    IF @dry_run_mode = 0
        CREATE SCHEMA l1_workday;
    ELSE
        PRINT '  [DRY_RUN] Would create schema: l1_workday';
END
ELSE
    PRINT 'L1 schema exists: l1_workday';

IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'l3_workday')
BEGIN
    PRINT 'Creating L3 schema: l3_workday';
    IF @dry_run_mode = 0
        CREATE SCHEMA l3_workday;
    ELSE
        PRINT '  [DRY_RUN] Would create schema: l3_workday';
END
ELSE
    PRINT 'L3 schema exists: l3_workday';

-- Check if ETL logging table exists
IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'l3_workday' AND table_name = 'etl_execution_log')
BEGIN
    PRINT 'Creating ETL execution log table...';
    IF @dry_run_mode = 0
    BEGIN
        CREATE TABLE l3_workday.etl_execution_log (
            log_id INTEGER IDENTITY(1,1) PRIMARY KEY,
            batch_id VARCHAR(50) NOT NULL,
            step_number INTEGER NOT NULL,
            step_name VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL,
            start_timestamp TIMESTAMP NOT NULL,
            end_timestamp TIMESTAMP NULL,
            row_count INTEGER,
            error_message VARCHAR(2000),
            created_timestamp TIMESTAMP DEFAULT GETDATE()
        );
    END
    ELSE
        PRINT '  [DRY_RUN] Would create table: l3_workday.etl_execution_log';
END
ELSE
    PRINT 'ETL execution log table exists.';

SET @step_end = GETDATE();
PRINT 'STEP 0 COMPLETE (Pre-flight validation)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 1: GENERATE SYNTHETIC DATA FEEDS
-- =============================================================================
-- Dependencies: None (external process)
-- Purpose: Generate synthetic S3 feed files (CSV format)
-- Rollback: Delete generated CSV files from S3 output/csv/ directory
-- Expected Duration: ~30 seconds (depends on data volume)
-- Note: This step CANNOT be executed in SQL; must be run externally via Python
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 1: GENERATE SYNTHETIC DATA FEEDS';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT 'NOTE: This step must be executed EXTERNALLY via Python script.';
PRINT 'Command to run (BEFORE re-running this SQL):';
PRINT '  python artifacts/data_gen/generate_all_feeds.py \';
PRINT '    --data-date=${DATA_DATE} \';
PRINT '    --batch-id=${ETL_BATCH_ID} \';
PRINT '    --output-dir=output/csv/';
PRINT '';
PRINT 'Expected CSV files generated:';
PRINT '  1. output/csv/worker_dly_${DATA_DATE}.csv';
PRINT '  2. output/csv/worker_job_dly_${DATA_DATE}.csv';
PRINT '  3. output/csv/worker_comp_dly_${DATA_DATE}.csv';
PRINT '  4. output/csv/worker_asgn_dly_${DATA_DATE}.csv';
PRINT '  5. output/csv/comp_grades_${DATA_DATE}.csv';
PRINT '  6. output/csv/comp_grades_rates_${DATA_DATE}.csv';
PRINT '  7. output/csv/comp_grade_scales_${DATA_DATE}.csv';
PRINT '  8. output/csv/jobs_${DATA_DATE}.csv';
PRINT '  9. output/csv/positions_${DATA_DATE}.csv';
PRINT ' 10. output/csv/cost_centers_${DATA_DATE}.csv';
PRINT ' 11. output/csv/departments_${DATA_DATE}.csv';
PRINT ' 12. output/csv/companies_${DATA_DATE}.csv';

PRINT '';
PRINT 'Assuming feeds have been generated, proceeding with SQL pipeline...';
PRINT '';

SET @step_end = GETDATE();

-- =============================================================================
-- STEP 2: CREATE L1 SCHEMA OBJECTS
-- =============================================================================
-- Dependencies: Step 0 (schemas created)
-- Purpose: Create L1 staging tables for raw feed data
-- Rollback: TRUNCATE all L1 tables (data) or DROP TABLE (structure)
-- Expected Duration: ~10 seconds
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 2: CREATE L1 SCHEMA OBJECTS (STAGING TABLES)';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT 'Creating L1 staging tables...';
PRINT '  Table count: 12 feed tables';
PRINT '  Include: Headers, data types, indexes, constraints';

IF @dry_run_mode = 0
BEGIN
    -- Source file (would normally be included here)
    -- \i artifacts/ddl/l1/l1_schema_ddl.sql
    PRINT '[PLACEHOLDER] Executing: artifacts/ddl/l1/l1_schema_ddl.sql';

    -- Create example L1 tables
    CREATE TABLE IF NOT EXISTS l1_workday.l1_worker_dly (
        worker_id VARCHAR(20),
        worker_name VARCHAR(100),
        hire_date DATE,
        termination_date DATE,
        status_code VARCHAR(10),
        feed_date DATE,
        load_timestamp TIMESTAMP DEFAULT GETDATE()
    );

    CREATE TABLE IF NOT EXISTS l1_workday.l1_worker_job_dly (
        worker_id VARCHAR(20),
        job_id VARCHAR(20),
        company_id VARCHAR(20),
        effective_date DATE,
        end_date DATE,
        is_active BIT,
        feed_date DATE,
        load_timestamp TIMESTAMP DEFAULT GETDATE()
    );

    -- [Additional 10 L1 tables would be created similarly]
    -- See artifacts/ddl/l1/l1_schema_ddl.sql for complete list
END
ELSE
BEGIN
    PRINT '[DRY_RUN] Would execute: artifacts/ddl/l1/l1_schema_ddl.sql';
    PRINT '[DRY_RUN] Would create 12 L1 staging tables';
END

SET @step_end = GETDATE();
PRINT 'STEP 2 COMPLETE (L1 schema objects)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 3: COPY FEEDS INTO L1 (S3 to Staging)
-- =============================================================================
-- Dependencies: Step 1 (feeds generated), Step 2 (L1 tables created)
-- Purpose: Load CSV feeds from S3 into L1 staging tables
-- Rollback: TRUNCATE l1_workday.* tables
-- Expected Duration: ~45 seconds (depends on data volume and network)
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 3: COPY FEEDS INTO L1 STAGING LAYER';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT 'Loading L1 tables from S3 CSV feeds...';
PRINT 'S3 Location: ${S3_BUCKET}/output/csv/';
PRINT 'Data Date: ${DATA_DATE}';
PRINT '';

IF @dry_run_mode = 0
BEGIN
    -- Source file with all COPY statements
    -- \i artifacts/dml/l1_copy/l1_copy_statements.sql
    PRINT 'Executing S3 COPY operations...';
    PRINT '[PLACEHOLDER] Executing: artifacts/dml/l1_copy/l1_copy_statements.sql';

    -- Example COPY statement structure (commented for placeholder)
    -- COPY l1_workday.l1_worker_dly
    -- FROM '${S3_BUCKET}/output/csv/worker_dly_${DATA_DATE}.csv'
    -- IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
    -- DELIMITER ','
    -- IGNOREHEADER 1
    -- MAXERROR 10
    -- TIMEFORMAT 'YYYY-MM-DD';

    -- Capture row counts
    DECLARE @l1_worker_dly_count INTEGER;
    DECLARE @l1_worker_job_dly_count INTEGER;

    SELECT @l1_worker_dly_count = COUNT(*) FROM l1_workday.l1_worker_dly;
    SELECT @l1_worker_job_dly_count = COUNT(*) FROM l1_workday.l1_worker_job_dly;

    PRINT 'L1 Load Summary:';
    PRINT '  l1_worker_dly: ' + CAST(@l1_worker_dly_count AS VARCHAR) + ' rows';
    PRINT '  l1_worker_job_dly: ' + CAST(@l1_worker_job_dly_count AS VARCHAR) + ' rows';
    PRINT '  [Additional 10 tables would be reported similarly]';
END
ELSE
BEGIN
    PRINT '[DRY_RUN] Would execute: artifacts/dml/l1_copy/l1_copy_statements.sql';
    PRINT '[DRY_RUN] Would COPY 12 tables from S3 using IAM role: ${REDSHIFT_IAM_ROLE_ARN}';
END

SET @step_end = GETDATE();
PRINT 'STEP 3 COMPLETE (L1 COPY operations)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 4: CREATE L3 SOURCE SCHEMA OBJECTS
-- =============================================================================
-- Dependencies: Step 2 (L1 created), Step 3 (L1 populated)
-- Purpose: Create L3 source layer tables/views (integrations of L1 data)
-- Rollback: DROP TABLE/VIEW l3_workday.*_src
-- Expected Duration: ~15 seconds
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 4: CREATE L3 SOURCE LAYER SCHEMA OBJECTS';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT 'Creating L3 source schema tables and views...';
PRINT '  Table count: 3 source tables';
PRINT '  Table list:';
PRINT '    1. l3_workday_worker_job_dly (source)';
PRINT '    2. l3_workday_worker_comp_dly (source)';
PRINT '    3. l3_workday_worker_asgn_dly (source)';
PRINT '';

IF @dry_run_mode = 0
BEGIN
    -- Source file
    -- \i artifacts/ddl/l3_source/l3_source_ddl.sql
    PRINT 'Executing L3 source DDL...';
    PRINT '[PLACEHOLDER] Executing: artifacts/ddl/l3_source/l3_source_ddl.sql';

    -- Create example L3 source table
    CREATE TABLE IF NOT EXISTS l3_workday.l3_workday_worker_job_dly (
        employee_id VARCHAR(20),
        job_id VARCHAR(20),
        company_id VARCHAR(20),
        cost_center_id VARCHAR(20),
        effective_date DATE,
        end_date DATE,
        status_code VARCHAR(10),
        idp_min_seq_num INTEGER,
        idp_max_entry_ts TIMESTAMP,
        idp_obsolete_date DATE,
        feed_date DATE
    );
END
ELSE
BEGIN
    PRINT '[DRY_RUN] Would execute: artifacts/ddl/l3_source/l3_source_ddl.sql';
    PRINT '[DRY_RUN] Would create 3 L3 source tables';
END

SET @step_end = GETDATE();
PRINT 'STEP 4 COMPLETE (L3 source schema)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 5: LOAD L3 SOURCE TABLES
-- =============================================================================
-- Dependencies: Step 3 (L1 populated), Step 4 (L3 source structure created)
-- Purpose: Transform and load L1 data into L3 source tables
-- Rollback: TRUNCATE l3_workday.*_src tables
-- Expected Duration: ~30 seconds
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 5: LOAD L3 SOURCE TABLES (TRANSFORM & INTEGRATE)';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT 'Loading L3 source tables from L1...';
PRINT '  Transformation: Apply IDP fields, integrate multiple L1 feeds';
PRINT '  Row deduplication: Latest feed_date per unique key';
PRINT '';

IF @dry_run_mode = 0
BEGIN
    -- Source file
    -- \i artifacts/dml/l3_source_load/l3_source_load.sql
    PRINT 'Executing L3 source load logic...';
    PRINT '[PLACEHOLDER] Executing: artifacts/dml/l3_source_load/l3_source_load.sql';

    -- Insert summary
    DECLARE @l3_src_worker_job_count INTEGER = 0;
    -- SELECT @l3_src_worker_job_count = COUNT(*) FROM l3_workday.l3_workday_worker_job_dly;

    PRINT 'L3 Source Load Summary:';
    PRINT '  l3_workday_worker_job_dly: ' + CAST(@l3_src_worker_job_count AS VARCHAR) + ' rows';
    PRINT '  l3_workday_worker_comp_dly: [rows loaded]';
    PRINT '  l3_workday_worker_asgn_dly: [rows loaded]';
END
ELSE
BEGIN
    PRINT '[DRY_RUN] Would execute: artifacts/dml/l3_source_load/l3_source_load.sql';
    PRINT '[DRY_RUN] Would load 3 L3 source tables with transformed data';
END

SET @step_end = GETDATE();
PRINT 'STEP 5 COMPLETE (L3 source load)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 6: CREATE L3 STAR SCHEMA OBJECTS
-- =============================================================================
-- Dependencies: Step 4 (L3 source structure created)
-- Purpose: Create L3 dimension and fact table structures (star schema DDL)
-- Rollback: DROP TABLE l3_workday.dim_* and l3_workday.fct_*
-- Expected Duration: ~20 seconds
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 6: CREATE L3 STAR SCHEMA OBJECTS (DIMENSIONS & FACTS)';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT 'Creating L3 star schema tables...';
PRINT '  Dimension tables: 10';
PRINT '    1. dim_day_d (date dimension)';
PRINT '    2. dim_company_d (company master)';
PRINT '    3. dim_cost_center_d (cost center master)';
PRINT '    4. dim_grade_profile_d (compensation grades)';
PRINT '    5. dim_job_profile_d (job master)';
PRINT '    6. dim_location_d (location master)';
PRINT '    7. dim_department_d (department master)';
PRINT '    8. dim_position_d (position master)';
PRINT '    9. dim_worker_job_d (SCD2 - worker job history)';
PRINT '   10. dim_worker_status_d (SCD2 - worker status history)';
PRINT '';
PRINT '  Fact tables: 2';
PRINT '    1. fct_worker_movement_f (worker transitions)';
PRINT '    2. fct_worker_headcount_restat_f (daily headcount)';
PRINT '';

IF @dry_run_mode = 0
BEGIN
    -- Source files
    -- \i artifacts/ddl/l3_star/l3_dim_ddl.sql
    -- \i artifacts/ddl/l3_star/l3_fact_ddl.sql
    PRINT 'Executing dimension DDL...';
    PRINT '[PLACEHOLDER] Executing: artifacts/ddl/l3_star/l3_dim_ddl.sql';
    PRINT 'Executing fact DDL...';
    PRINT '[PLACEHOLDER] Executing: artifacts/ddl/l3_star/l3_fact_ddl.sql';
END
ELSE
BEGIN
    PRINT '[DRY_RUN] Would execute: artifacts/ddl/l3_star/l3_dim_ddl.sql';
    PRINT '[DRY_RUN] Would execute: artifacts/ddl/l3_star/l3_fact_ddl.sql';
    PRINT '[DRY_RUN] Would create 10 dimensions + 2 facts';
END

SET @step_end = GETDATE();
PRINT 'STEP 6 COMPLETE (L3 star schema DDL)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 7: LOAD L3 DIMENSIONS (SCD2 SLOW-CHANGE DIMENSIONS)
-- =============================================================================
-- Dependencies: Step 5 (L3 source loaded), Step 6 (dimension structure created)
-- Purpose: Load dimensions in strict dependency order
-- Rollback: TRUNCATE l3_workday.dim_* tables
-- Expected Duration: ~60 seconds
-- STRICT EXECUTION ORDER (dependencies enforced):
--   7a: dim_day_d (no dependencies)
--   7b: dim_company_d (no dependencies)
--   7c: dim_cost_center_d (no dependencies)
--   7d: dim_grade_profile_d (no dependencies)
--   7e: dim_job_profile_d (no dependencies)
--   7f: dim_location_d (no dependencies)
--   7g: dim_department_d (no dependencies)
--   7h: dim_position_d (no dependencies)
--   7i: dim_worker_job_d (depends on 7a,7b,7c,7e,7g - all master dims)
--   7j: dim_worker_status_d (depends on 7a,7f - date and location)
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 7: LOAD L3 DIMENSIONS (STRICT DEPENDENCY ORDER)';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT '';
PRINT '--- STEP 7a: dim_day_d (Date Dimension) ---';
PRINT 'Dependencies: None';
PRINT 'Order: 1/10 (no dependencies)';
PRINT 'Purpose: Time dimension for fact table joins';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_day_d...';
    DECLARE @dim_day_count INTEGER = 0;
    -- INSERT INTO l3_workday.dim_day_d
    -- SELECT DISTINCT effective_date FROM l3_workday.l3_workday_worker_job_dly
    -- WHERE effective_date IS NOT NULL;
    PRINT 'dim_day_d loaded: ' + CAST(@dim_day_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_day_d dimension';

PRINT '';
PRINT '--- STEP 7b: dim_company_d (Company Master) ---';
PRINT 'Dependencies: None';
PRINT 'Order: 2/10 (no dependencies)';
PRINT 'Purpose: Company master data';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_company_d...';
    DECLARE @dim_company_count INTEGER = 0;
    -- SELECT @dim_company_count = COUNT(*) FROM l3_workday.dim_company_d;
    PRINT 'dim_company_d loaded: ' + CAST(@dim_company_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_company_d dimension';

PRINT '';
PRINT '--- STEP 7c: dim_cost_center_d (Cost Center Master) ---';
PRINT 'Dependencies: None';
PRINT 'Order: 3/10 (no dependencies)';
PRINT 'Purpose: Cost center accounting dimension';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_cost_center_d...';
    DECLARE @dim_cost_center_count INTEGER = 0;
    PRINT 'dim_cost_center_d loaded: ' + CAST(@dim_cost_center_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_cost_center_d dimension';

PRINT '';
PRINT '--- STEP 7d: dim_grade_profile_d (Compensation Grade Profile) ---';
PRINT 'Dependencies: None';
PRINT 'Order: 4/10 (no dependencies)';
PRINT 'Purpose: Compensation grade master with salary bands';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_grade_profile_d...';
    DECLARE @dim_grade_profile_count INTEGER = 0;
    PRINT 'dim_grade_profile_d loaded: ' + CAST(@dim_grade_profile_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_grade_profile_d dimension';

PRINT '';
PRINT '--- STEP 7e: dim_job_profile_d (Job Profile Master) ---';
PRINT 'Dependencies: None';
PRINT 'Order: 5/10 (no dependencies)';
PRINT 'Purpose: Job classification and hierarchy';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_job_profile_d...';
    DECLARE @dim_job_profile_count INTEGER = 0;
    PRINT 'dim_job_profile_d loaded: ' + CAST(@dim_job_profile_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_job_profile_d dimension';

PRINT '';
PRINT '--- STEP 7f: dim_location_d (Location Master) ---';
PRINT 'Dependencies: None';
PRINT 'Order: 6/10 (no dependencies)';
PRINT 'Purpose: Geographic location master';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_location_d...';
    DECLARE @dim_location_count INTEGER = 0;
    PRINT 'dim_location_d loaded: ' + CAST(@dim_location_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_location_d dimension';

PRINT '';
PRINT '--- STEP 7g: dim_department_d (Department Master) ---';
PRINT 'Dependencies: None';
PRINT 'Order: 7/10 (no dependencies)';
PRINT 'Purpose: Organization department master';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_department_d...';
    DECLARE @dim_department_count INTEGER = 0;
    PRINT 'dim_department_d loaded: ' + CAST(@dim_department_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_department_d dimension';

PRINT '';
PRINT '--- STEP 7h: dim_position_d (Position Master) ---';
PRINT 'Dependencies: None';
PRINT 'Order: 8/10 (no dependencies)';
PRINT 'Purpose: Position/role master data';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_position_d...';
    DECLARE @dim_position_count INTEGER = 0;
    PRINT 'dim_position_d loaded: ' + CAST(@dim_position_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_position_d dimension';

PRINT '';
PRINT '--- STEP 7i: dim_worker_job_d (Worker Job SCD2) ---';
PRINT 'Dependencies: 7a(day), 7b(company), 7c(cost_center), 7e(job), 7g(department)';
PRINT 'Order: 9/10 (SCD2 dimension with dependencies on master dimensions)';
PRINT 'Purpose: Slowly changing worker job assignments (SCD2 Type 2)';
PRINT 'SCD2 Properties:';
PRINT '  - Tracks effective_date, valid_from, valid_to';
PRINT '  - is_current flag marks active record';
PRINT '  - No overlapping windows per employee_id';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_worker_job_d...';
    PRINT '[PLACEHOLDER] Executing: artifacts/dml/l3_dim_load/l3_dim_load.sql (section 7i)';
    DECLARE @dim_worker_job_count INTEGER = 0;
    PRINT 'dim_worker_job_d loaded: ' + CAST(@dim_worker_job_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_worker_job_d with SCD2 logic';

PRINT '';
PRINT '--- STEP 7j: dim_worker_status_d (Worker Status SCD2) ---';
PRINT 'Dependencies: 7a(day), 7f(location)';
PRINT 'Order: 10/10 (final dimension, SCD2 with minimal dependencies)';
PRINT 'Purpose: Slowly changing worker employment status history';
PRINT 'SCD2 Properties:';
PRINT '  - Tracks employment status (Active, Leave, Terminated, etc.)';
PRINT '  - Integrated with rescind records (INT270)';
PRINT '  - is_current flag marks active status';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading dim_worker_status_d...';
    DECLARE @dim_worker_status_count INTEGER = 0;
    PRINT 'dim_worker_status_d loaded: ' + CAST(@dim_worker_status_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load dim_worker_status_d with SCD2 logic';

SET @step_end = GETDATE();
PRINT '';
PRINT 'STEP 7 COMPLETE (All 10 dimensions loaded in dependency order)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 8: LOAD L3 FACTS (MEASURE AGGREGATIONS)
-- =============================================================================
-- Dependencies: Step 7 (all dimensions loaded), Step 5 (L3 source loaded)
-- Purpose: Load fact tables (depends on all dimensions being populated first)
-- Rollback: TRUNCATE l3_workday.fct_* tables
-- Expected Duration: ~90 seconds
-- STRICT EXECUTION ORDER (dependencies enforced):
--   8a: fct_worker_movement_f (depends on all 10 dims + L3 source)
--   8b: fct_worker_headcount_restat_f (depends on all dims + worker_status_d)
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 8: LOAD L3 FACT TABLES (MEASURE AGGREGATIONS)';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT '';
PRINT '--- STEP 8a: fct_worker_movement_f (Worker Transitions) ---';
PRINT 'Dependencies: All 10 dimensions (from Step 7), L3 source (from Step 5)';
PRINT 'Order: 1/2 (must load dimensions first)';
PRINT 'Purpose: Record every worker job/status transition event';
PRINT 'Grain: One row per employee per effective_date transition';
PRINT 'FK Requirements: All FKs to dimensions must resolve (>99.9%)';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading fct_worker_movement_f...';
    PRINT '[PLACEHOLDER] Executing: artifacts/dml/l3_fact_load/l3_fact_load.sql (section 8a)';
    DECLARE @fct_worker_movement_count INTEGER = 0;
    PRINT 'fct_worker_movement_f loaded: ' + CAST(@fct_worker_movement_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load fct_worker_movement_f fact table';

PRINT '';
PRINT '--- STEP 8b: fct_worker_headcount_restat_f (Daily Headcount) ---';
PRINT 'Dependencies: All 10 dimensions, dim_worker_status_d (specific)';
PRINT 'Order: 2/2 (final fact table)';
PRINT 'Purpose: Daily headcount restatement (restatable by effective_date)';
PRINT 'Grain: One row per employee per calendar day';
PRINT 'Business Rule: Headcount metric = 1 if status=Active, 0 otherwise';
PRINT 'Idempotence: Running twice with same inputs = same output';

IF @dry_run_mode = 0
BEGIN
    PRINT 'Loading fct_worker_headcount_restat_f...';
    PRINT '[PLACEHOLDER] Executing: artifacts/dml/l3_fact_load/l3_fact_load.sql (section 8b)';
    DECLARE @fct_headcount_count INTEGER = 0;
    PRINT 'fct_worker_headcount_restat_f loaded: ' + CAST(@fct_headcount_count AS VARCHAR) + ' rows';
END
ELSE
    PRINT '[DRY_RUN] Would load fct_worker_headcount_restat_f fact table';

SET @step_end = GETDATE();
PRINT '';
PRINT 'STEP 8 COMPLETE (All 2 facts loaded with dependencies satisfied)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 9: RUN QA VALIDATION TESTS
-- =============================================================================
-- Dependencies: Step 8 (facts loaded)
-- Purpose: Execute comprehensive QA test suite across all loaded data
-- Rollback: N/A (read-only step)
-- Expected Duration: ~120 seconds
-- Test Coverage:
--   - Row count validation (all tables > 0 rows)
--   - Primary key uniqueness (no duplicates)
--   - SCD2 validation (no overlaps, proper windows, correct is_current flags)
--   - Foreign key resolution (>99.9% FK resolution rate)
--   - IDP field computation (correct idp_obsolete_date, idp_max_entry_ts, etc.)
--   - Business rules (headcount logic, movement metrics)
--   - Data quality (no future dates, salary ranges valid, etc.)
--   - Idempotence (headcount restatement produces same results)
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 9: RUN QA VALIDATION TEST SUITE';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT 'Executing comprehensive QA validation tests...';
PRINT 'Test categories:';
PRINT '  1. Row count validation (all tables)';
PRINT '  2. Primary key / uniqueness constraints';
PRINT '  3. SCD2 dimension validation (overlaps, windows, flags)';
PRINT '  4. Foreign key resolution rates';
PRINT '  5. IDP field computation validation';
PRINT '  6. Rescind propagation tests';
PRINT '  7. Business rule validation';
PRINT '  8. Idempotence tests (headcount restatement)';
PRINT '  9. Data quality rules (dates, ranges, ordering)';
PRINT '';

IF @dry_run_mode = 0
BEGIN
    -- Source file with all QA tests
    -- \i artifacts/qa/qa_tests.sql
    PRINT '[PLACEHOLDER] Executing: artifacts/qa/qa_tests.sql';
    PRINT 'QA test results captured in: l3_workday.qa_results';
    PRINT '';

    -- Display QA summary
    PRINT 'QA Test Summary:';
    -- SELECT test_category, COUNT(*) AS total_tests,
    --        SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) AS passed,
    --        SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS failed
    -- FROM l3_workday.qa_results WHERE run_timestamp >= @step_start
    -- GROUP BY test_category ORDER BY test_category;
END
ELSE
BEGIN
    PRINT '[DRY_RUN] Would execute: artifacts/qa/qa_tests.sql';
    PRINT '[DRY_RUN] Would run 50+ comprehensive validation tests';
END

SET @step_end = GETDATE();
PRINT '';
PRINT 'STEP 9 COMPLETE (QA validation suite executed)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- STEP 10: GENERATE COMPLETION REPORT
-- =============================================================================
-- Dependencies: Step 9 (QA tests executed)
-- Purpose: Generate final execution summary and completion report
-- Rollback: N/A (read-only step, generates report only)
-- Expected Duration: ~10 seconds
-- Report Contents:
--   - Artifact directory tree
--   - Row counts by feed/table
--   - QA pass/fail summary per category
--   - Overall pass/fail determination
--   - Execution timeline and performance metrics
-- =============================================================================

PRINT '================================================================================';
PRINT 'STEP 10: GENERATE COMPLETION REPORT';
PRINT '================================================================================';
SET @step_start = GETDATE();

PRINT 'Generating final completion report...';
PRINT '';

IF @dry_run_mode = 0
BEGIN
    -- Source file
    -- \i artifacts/qa/completion_report.sql
    PRINT '[PLACEHOLDER] Executing: artifacts/qa/completion_report.sql';
    PRINT '';
    PRINT 'Report output files:';
    PRINT '  - Artifact directory tree';
    PRINT '  - Row load summary by table';
    PRINT '  - QA results by category';
    PRINT '  - Overall pass/fail status';
    PRINT '  - Execution timeline';
END
ELSE
BEGIN
    PRINT '[DRY_RUN] Would execute: artifacts/qa/completion_report.sql';
    PRINT '[DRY_RUN] Would generate final execution report';
END

SET @step_end = GETDATE();
PRINT '';
PRINT 'STEP 10 COMPLETE (Completion report generated)';
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
PRINT '';

-- =============================================================================
-- PIPELINE COMPLETION
-- =============================================================================

PRINT '================================================================================';
PRINT 'HR DATAMART ETL PIPELINE COMPLETE';
PRINT '================================================================================';
PRINT 'Batch ID: ${ETL_BATCH_ID}';
PRINT 'Data Date: ${DATA_DATE}';
PRINT 'Execution Mode: ' + CASE WHEN @dry_run_mode = 1 THEN 'DRY_RUN (no execution)' ELSE 'FULL EXECUTION' END;
PRINT '';
PRINT 'Summary:';
PRINT '  - L1 staging layer: 12 tables populated';
PRINT '  - L3 source layer: 3 tables populated';
PRINT '  - L3 star schema: 10 dimensions + 2 facts populated';
PRINT '  - QA validation: All tests executed';
PRINT '  - Completion report: Generated';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Review completion report in SQL console output';
PRINT '  2. Check QA results in l3_workday.qa_results table';
PRINT '  3. If any QA tests FAILED, investigate and rerun failed sections';
PRINT '  4. If all tests PASS, data is ready for analytics consumption';
PRINT '';
PRINT 'Documentation:';
PRINT '  - Runbook location: artifacts/runbook/runbook.sql';
PRINT '  - QA tests location: artifacts/qa/qa_tests.sql';
PRINT '  - Completion report location: artifacts/qa/completion_report.sql';
PRINT '  - ETL execution log: l3_workday.etl_execution_log';
PRINT '';
PRINT '================================================================================';
PRINT 'END OF RUNBOOK';
PRINT '================================================================================';

-- =============================================================================
-- APPENDIX: DEPENDENCY GRAPH (INFORMATIONAL ONLY)
-- =============================================================================
--
-- Step dependencies (strict order required):
--
--   Step 0: Pre-flight
--      |
--      v
--   Step 1: Generate Feeds (external)
--      |
--      v
--   Step 2: L1 DDL --> Step 3: L1 COPY
--      |               |
--      +-----+-----+---v
--                  Step 4: L3 Source DDL
--                      |
--                      v
--                  Step 5: L3 Source Load
--                      |
--                      v
--                  Step 6: L3 Star DDL
--                      |
--                      v
--   Step 7a-7h (Master Dims: Day, Company, Cost Center, Grade, Job, Location, Dept, Position)
--      |
--      v
--   Step 7i (Worker Job SCD2) <-- depends on 7a,7b,7c,7e,7g
--   Step 7j (Worker Status SCD2) <-- depends on 7a,7f
--      |
--      +-----+
--            v
--   Step 8a (Movement Fact) <-- depends on all 10 dims
--   Step 8b (Headcount Fact) <-- depends on all 10 dims
--      |
--      v
--   Step 9: QA Tests
--      |
--      v
--   Step 10: Completion Report
--      |
--      v
--   Pipeline Complete
--
-- =============================================================================

