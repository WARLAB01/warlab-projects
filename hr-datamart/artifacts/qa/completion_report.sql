-- =============================================================================
-- HR DATAMART - COMPLETION REPORT
-- =============================================================================
-- Purpose:
--   Generate final execution summary report after ETL pipeline completion.
--   Outputs artifact inventory, row load summary, QA results, and pass/fail status.
--
-- Report Sections:
--   1. Directory tree artifact listing
--   2. Execution metadata (batch ID, dates, timing)
--   3. Row counts by feed and layer
--   4. QA test results summary
--   5. Overall pass/fail determination
--
-- =============================================================================

PRINT '================================================================================';
PRINT 'HR DATAMART - COMPLETION REPORT';
PRINT '================================================================================';
PRINT '';
PRINT 'Report Generated: ' + CAST(GETDATE() AS VARCHAR);
PRINT '';

-- =============================================================================
-- SECTION 1: ARTIFACT DIRECTORY TREE
-- =============================================================================

PRINT '================================================================================';
PRINT 'SECTION 1: ARTIFACT INVENTORY';
PRINT '================================================================================';
PRINT '';
PRINT 'Directory Structure:';
PRINT '';
PRINT 'hr-datamart/';
PRINT '├── artifacts/';
PRINT '│   ├── config/';
PRINT '│   │   ├── constants.sql           (ETL constants and configuration)';
PRINT '│   │   └── logging.sql             (Logging configuration)';
PRINT '│   ├── ddl/';
PRINT '│   │   ├── l1/';
PRINT '│   │   │   └── l1_schema_ddl.sql   (L1 staging tables: 12 tables)';
PRINT '│   │   ├── l3_source/';
PRINT '│   │   │   └── l3_source_ddl.sql   (L3 source tables: 3 tables)';
PRINT '│   │   └── l3_star/';
PRINT '│   │       ├── l3_dim_ddl.sql      (L3 dimensions: 10 dimensions)';
PRINT '│   │       └── l3_fact_ddl.sql     (L3 facts: 2 fact tables)';
PRINT '│   ├── dml/';
PRINT '│   │   ├── l1_copy/';
PRINT '│   │   │   └── l1_copy_statements.sql  (COPY from S3 into L1)';
PRINT '│   │   ├── l3_source_load/';
PRINT '│   │   │   └── l3_source_load.sql      (Transform L1 -> L3 source)';
PRINT '│   │   ├── l3_dim_load/';
PRINT '│   │   │   └── l3_dim_load.sql         (Load all 10 dimensions)';
PRINT '│   │   └── l3_fact_load/';
PRINT '│   │       └── l3_fact_load.sql        (Load 2 fact tables)';
PRINT '│   ├── data_gen/';
PRINT '│   │   ├── generate_all_feeds.py    (Python: Generate synthetic CSV feeds)';
PRINT '│   │   ├── data_generator.py        (Core data generation logic)';
PRINT '│   │   └── config.json              (Feed generation configuration)';
PRINT '│   ├── glue/';
PRINT '│   │   ├── etl_job.py               (AWS Glue job entrypoint)';
PRINT '│   │   └── requirements.txt         (Python dependencies)';
PRINT '│   ├── qa/';
PRINT '│   │   ├── qa_tests.sql             (Comprehensive QA test suite)';
PRINT '│   │   └── completion_report.sql    (This file)';
PRINT '│   ├── runbook/';
PRINT '│   │   └── runbook.sql              (Master orchestration script)';
PRINT '│   ├── docs/';
PRINT '│   │   ├── README.md                (HR datamart overview)';
PRINT '│   │   ├── ARCHITECTURE.md          (System architecture doc)';
PRINT '│   │   └── OPERATION_GUIDE.md       (Operational procedures)';
PRINT '│   └── output/';
PRINT '│       ├── csv/                     (Generated CSV feeds)';
PRINT '│       └── logs/                    (Execution logs)';
PRINT '';

-- Count artifact files
DECLARE @total_sql_files INTEGER;
DECLARE @total_python_files INTEGER;
DECLARE @total_config_files INTEGER;

SELECT @total_sql_files = 10;  -- Approximate count
SELECT @total_python_files = 3;
SELECT @total_config_files = 3;

PRINT 'Artifact Summary:';
PRINT '  DDL files: 3 (L1, L3 source, L3 star)';
PRINT '  DML files: 4 (L1 copy, L3 source load, L3 dim load, L3 fact load)';
PRINT '  QA files: 2 (QA tests, Completion report)';
PRINT '  Runbook: 1 (Master orchestration)';
PRINT '  Python scripts: 3 (Data generation)';
PRINT '  Configuration files: 3';
PRINT '  Total: 16+ files';
PRINT '';

-- =============================================================================
-- SECTION 2: EXECUTION METADATA
-- =============================================================================

PRINT '================================================================================';
PRINT 'SECTION 2: EXECUTION METADATA';
PRINT '================================================================================';
PRINT '';

DECLARE @batch_id VARCHAR(50);
DECLARE @execution_start_time TIMESTAMP;
DECLARE @execution_end_time TIMESTAMP;
DECLARE @execution_duration_seconds INTEGER;

-- Retrieve latest batch execution metadata
SELECT TOP 1
    @batch_id = batch_id,
    @execution_start_time = MIN(start_timestamp),
    @execution_end_time = MAX(end_timestamp)
FROM l3_workday.etl_execution_log
GROUP BY batch_id
ORDER BY batch_id DESC;

IF @execution_end_time IS NOT NULL
    SET @execution_duration_seconds = DATEDIFF(SECOND, @execution_start_time, @execution_end_time);
ELSE
    SET @execution_duration_seconds = 0;

PRINT 'Batch ID: ' + ISNULL(@batch_id, 'Not available');
PRINT 'Execution Start: ' + ISNULL(CAST(@execution_start_time AS VARCHAR), 'Not available');
PRINT 'Execution End: ' + ISNULL(CAST(@execution_end_time AS VARCHAR), 'Not available');
PRINT 'Total Duration: ' + CAST(@execution_duration_seconds AS VARCHAR) + ' seconds';
PRINT '';

-- Show step-by-step execution log
PRINT 'Step-by-Step Execution Log:';
PRINT '';

SELECT
    step_number,
    step_name,
    status,
    CAST(start_timestamp AS VARCHAR) AS start_time,
    DATEDIFF(SECOND, start_timestamp, ISNULL(end_timestamp, GETDATE())) AS duration_seconds,
    ISNULL(CAST(row_count AS VARCHAR), 'N/A') AS rows_affected,
    ISNULL(error_message, 'OK') AS status_detail
FROM l3_workday.etl_execution_log
WHERE batch_id = @batch_id
ORDER BY step_number;

PRINT '';

-- =============================================================================
-- SECTION 3: ROW COUNT SUMMARY BY LAYER
-- =============================================================================

PRINT '================================================================================';
PRINT 'SECTION 3: ROW COUNTS BY LAYER';
PRINT '================================================================================';
PRINT '';

-- L1 Layer Row Counts
PRINT 'L1 LAYER (STAGING):';
PRINT '';

DECLARE @l1_worker_dly_count INT = 0;
DECLARE @l1_worker_job_dly_count INT = 0;
DECLARE @l1_worker_comp_dly_count INT = 0;
DECLARE @l1_worker_asgn_dly_count INT = 0;
DECLARE @l1_comp_grades_count INT = 0;
DECLARE @l1_comp_grades_rates_count INT = 0;
DECLARE @l1_comp_grade_scales_count INT = 0;
DECLARE @l1_jobs_count INT = 0;
DECLARE @l1_positions_count INT = 0;
DECLARE @l1_cost_centers_count INT = 0;
DECLARE @l1_departments_count INT = 0;
DECLARE @l1_companies_count INT = 0;

SELECT @l1_worker_dly_count = COUNT(*) FROM l1_workday.l1_worker_dly;
SELECT @l1_worker_job_dly_count = COUNT(*) FROM l1_workday.l1_worker_job_dly;
SELECT @l1_worker_comp_dly_count = COUNT(*) FROM l1_workday.l1_worker_comp_dly;
SELECT @l1_worker_asgn_dly_count = COUNT(*) FROM l1_workday.l1_worker_asgn_dly;
SELECT @l1_comp_grades_count = COUNT(*) FROM l1_workday.l1_comp_grades;
SELECT @l1_comp_grades_rates_count = COUNT(*) FROM l1_workday.l1_comp_grades_rates;
SELECT @l1_comp_grade_scales_count = COUNT(*) FROM l1_workday.l1_comp_grade_scales;
SELECT @l1_jobs_count = COUNT(*) FROM l1_workday.l1_jobs;
SELECT @l1_positions_count = COUNT(*) FROM l1_workday.l1_positions;
SELECT @l1_cost_centers_count = COUNT(*) FROM l1_workday.l1_cost_centers;
SELECT @l1_departments_count = COUNT(*) FROM l1_workday.l1_departments;
SELECT @l1_companies_count = COUNT(*) FROM l1_workday.l1_companies;

PRINT 'Feed Tables:';
PRINT '  l1_worker_dly........................' + CAST(@l1_worker_dly_count AS VARCHAR) + ' rows';
PRINT '  l1_worker_job_dly....................' + CAST(@l1_worker_job_dly_count AS VARCHAR) + ' rows';
PRINT '  l1_worker_comp_dly...................' + CAST(@l1_worker_comp_dly_count AS VARCHAR) + ' rows';
PRINT '  l1_worker_asgn_dly...................' + CAST(@l1_worker_asgn_dly_count AS VARCHAR) + ' rows';
PRINT '';
PRINT 'Master Data Tables:';
PRINT '  l1_comp_grades........................' + CAST(@l1_comp_grades_count AS VARCHAR) + ' rows';
PRINT '  l1_comp_grades_rates.................' + CAST(@l1_comp_grades_rates_count AS VARCHAR) + ' rows';
PRINT '  l1_comp_grade_scales.................' + CAST(@l1_comp_grade_scales_count AS VARCHAR) + ' rows';
PRINT '  l1_jobs..............................' + CAST(@l1_jobs_count AS VARCHAR) + ' rows';
PRINT '  l1_positions..........................' + CAST(@l1_positions_count AS VARCHAR) + ' rows';
PRINT '  l1_cost_centers.......................' + CAST(@l1_cost_centers_count AS VARCHAR) + ' rows';
PRINT '  l1_departments........................' + CAST(@l1_departments_count AS VARCHAR) + ' rows';
PRINT '  l1_companies...........................' + CAST(@l1_companies_count AS VARCHAR) + ' rows';

DECLARE @l1_total_rows INT;
SET @l1_total_rows = @l1_worker_dly_count + @l1_worker_job_dly_count + @l1_worker_comp_dly_count + @l1_worker_asgn_dly_count
                   + @l1_comp_grades_count + @l1_comp_grades_rates_count + @l1_comp_grade_scales_count + @l1_jobs_count
                   + @l1_positions_count + @l1_cost_centers_count + @l1_departments_count + @l1_companies_count;

PRINT '';
PRINT 'L1 TOTAL: ' + CAST(@l1_total_rows AS VARCHAR) + ' rows across 12 tables';
PRINT '';

-- L3 Source Layer Row Counts
PRINT 'L3 SOURCE LAYER (TRANSFORMED):';
PRINT '';

DECLARE @l3_src_worker_job_count INT = 0;
DECLARE @l3_src_worker_comp_count INT = 0;
DECLARE @l3_src_worker_asgn_count INT = 0;

SELECT @l3_src_worker_job_count = COUNT(*) FROM l3_workday.l3_workday_worker_job_dly;
SELECT @l3_src_worker_comp_count = COUNT(*) FROM l3_workday.l3_workday_worker_comp_dly;
SELECT @l3_src_worker_asgn_count = COUNT(*) FROM l3_workday.l3_workday_worker_asgn_dly;

PRINT '  l3_workday_worker_job_dly............' + CAST(@l3_src_worker_job_count AS VARCHAR) + ' rows';
PRINT '  l3_workday_worker_comp_dly...........' + CAST(@l3_src_worker_comp_count AS VARCHAR) + ' rows';
PRINT '  l3_workday_worker_asgn_dly...........' + CAST(@l3_src_worker_asgn_count AS VARCHAR) + ' rows';

DECLARE @l3_src_total_rows INT;
SET @l3_src_total_rows = @l3_src_worker_job_count + @l3_src_worker_comp_count + @l3_src_worker_asgn_count;

PRINT '';
PRINT 'L3 SOURCE TOTAL: ' + CAST(@l3_src_total_rows AS VARCHAR) + ' rows across 3 tables';
PRINT '';

-- L3 Dimension Layer Row Counts
PRINT 'L3 DIMENSION LAYER (STAR SCHEMA):';
PRINT '';

DECLARE @dim_day_count INT = 0;
DECLARE @dim_company_count INT = 0;
DECLARE @dim_cost_center_count INT = 0;
DECLARE @dim_grade_profile_count INT = 0;
DECLARE @dim_job_profile_count INT = 0;
DECLARE @dim_location_count INT = 0;
DECLARE @dim_department_count INT = 0;
DECLARE @dim_position_count INT = 0;
DECLARE @dim_worker_job_count INT = 0;
DECLARE @dim_worker_status_count INT = 0;

SELECT @dim_day_count = COUNT(*) FROM l3_workday.dim_day_d;
SELECT @dim_company_count = COUNT(*) FROM l3_workday.dim_company_d;
SELECT @dim_cost_center_count = COUNT(*) FROM l3_workday.dim_cost_center_d;
SELECT @dim_grade_profile_count = COUNT(*) FROM l3_workday.dim_grade_profile_d;
SELECT @dim_job_profile_count = COUNT(*) FROM l3_workday.dim_job_profile_d;
SELECT @dim_location_count = COUNT(*) FROM l3_workday.dim_location_d;
SELECT @dim_department_count = COUNT(*) FROM l3_workday.dim_department_d;
SELECT @dim_position_count = COUNT(*) FROM l3_workday.dim_position_d;
SELECT @dim_worker_job_count = COUNT(*) FROM l3_workday.dim_worker_job_d;
SELECT @dim_worker_status_count = COUNT(*) FROM l3_workday.dim_worker_status_d;

PRINT 'Reference Dimensions:';
PRINT '  dim_day_d..............................' + CAST(@dim_day_count AS VARCHAR) + ' rows';
PRINT '  dim_company_d..........................' + CAST(@dim_company_count AS VARCHAR) + ' rows';
PRINT '  dim_cost_center_d......................' + CAST(@dim_cost_center_count AS VARCHAR) + ' rows';
PRINT '  dim_grade_profile_d....................' + CAST(@dim_grade_profile_count AS VARCHAR) + ' rows';
PRINT '  dim_job_profile_d...................... ' + CAST(@dim_job_profile_count AS VARCHAR) + ' rows';
PRINT '  dim_location_d.........................' + CAST(@dim_location_count AS VARCHAR) + ' rows';
PRINT '  dim_department_d.......................' + CAST(@dim_department_count AS VARCHAR) + ' rows';
PRINT '  dim_position_d.........................' + CAST(@dim_position_count AS VARCHAR) + ' rows';
PRINT '';
PRINT 'SCD2 Dimensions (Slowly Changing):';
PRINT '  dim_worker_job_d (SCD2)................' + CAST(@dim_worker_job_count AS VARCHAR) + ' rows';
PRINT '  dim_worker_status_d (SCD2).............' + CAST(@dim_worker_status_count AS VARCHAR) + ' rows';

DECLARE @l3_dim_total_rows INT;
SET @l3_dim_total_rows = @dim_day_count + @dim_company_count + @dim_cost_center_count + @dim_grade_profile_count
                       + @dim_job_profile_count + @dim_location_count + @dim_department_count + @dim_position_count
                       + @dim_worker_job_count + @dim_worker_status_count;

PRINT '';
PRINT 'L3 DIMENSION TOTAL: ' + CAST(@l3_dim_total_rows AS VARCHAR) + ' rows across 10 dimensions';
PRINT '';

-- L3 Fact Layer Row Counts
PRINT 'L3 FACT LAYER (STAR SCHEMA):';
PRINT '';

DECLARE @fct_worker_movement_count INT = 0;
DECLARE @fct_worker_headcount_count INT = 0;

SELECT @fct_worker_movement_count = COUNT(*) FROM l3_workday.fct_worker_movement_f;
SELECT @fct_worker_headcount_count = COUNT(*) FROM l3_workday.fct_worker_headcount_restat_f;

PRINT '  fct_worker_movement_f.................' + CAST(@fct_worker_movement_count AS VARCHAR) + ' rows';
PRINT '  fct_worker_headcount_restat_f.........' + CAST(@fct_worker_headcount_count AS VARCHAR) + ' rows';

DECLARE @l3_fact_total_rows INT;
SET @l3_fact_total_rows = @fct_worker_movement_count + @fct_worker_headcount_count;

PRINT '';
PRINT 'L3 FACT TOTAL: ' + CAST(@l3_fact_total_rows AS VARCHAR) + ' rows across 2 fact tables';
PRINT '';

-- Grand Total
DECLARE @grand_total_rows INT;
SET @grand_total_rows = @l1_total_rows + @l3_src_total_rows + @l3_dim_total_rows + @l3_fact_total_rows;

PRINT '================================================================================';
PRINT 'GRAND TOTAL: ' + CAST(@grand_total_rows AS VARCHAR) + ' rows';
PRINT 'Layer Breakdown:';
PRINT '  L1 Staging: ' + CAST(@l1_total_rows AS VARCHAR) + ' rows (12 tables)';
PRINT '  L3 Source: ' + CAST(@l3_src_total_rows AS VARCHAR) + ' rows (3 tables)';
PRINT '  L3 Dimensions: ' + CAST(@l3_dim_total_rows AS VARCHAR) + ' rows (10 dimensions)';
PRINT '  L3 Facts: ' + CAST(@l3_fact_total_rows AS VARCHAR) + ' rows (2 facts)';
PRINT '================================================================================';
PRINT '';

-- =============================================================================
-- SECTION 4: QA TEST RESULTS SUMMARY
-- =============================================================================

PRINT '================================================================================';
PRINT 'SECTION 4: QA TEST RESULTS SUMMARY';
PRINT '================================================================================';
PRINT '';

DECLARE @total_qa_tests INT;
DECLARE @passed_qa_tests INT;
DECLARE @failed_qa_tests INT;
DECLARE @qa_pass_rate DECIMAL(5, 1);

SELECT @total_qa_tests = COUNT(*)
FROM l3_workday.qa_results;

SELECT @passed_qa_tests = COUNT(*)
FROM l3_workday.qa_results
WHERE status = 'PASS';

SELECT @failed_qa_tests = COUNT(*)
FROM l3_workday.qa_results
WHERE status = 'FAIL';

SET @qa_pass_rate = CASE
    WHEN @total_qa_tests > 0 THEN (@passed_qa_tests * 100.0 / @total_qa_tests)
    ELSE 100.0
END;

PRINT 'Overall QA Results:';
PRINT '  Total Tests: ' + CAST(@total_qa_tests AS VARCHAR);
PRINT '  Passed: ' + CAST(@passed_qa_tests AS VARCHAR);
PRINT '  Failed: ' + CAST(@failed_qa_tests AS VARCHAR);
PRINT '  Pass Rate: ' + CAST(@qa_pass_rate AS VARCHAR) + '%';
PRINT '';

-- QA Results by Category
PRINT 'QA Results by Category:';
PRINT '';

SELECT
    '  ' + test_category AS category,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    CAST(SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5, 1)) AS pass_pct
FROM l3_workday.qa_results
GROUP BY test_category
ORDER BY test_category;

PRINT '';

-- List failed tests (if any)
IF @failed_qa_tests > 0
BEGIN
    PRINT 'FAILED TESTS (detailed):';
    PRINT '';

    SELECT
        '  [' + test_category + '] ' + test_name AS test_name,
        'Expected: ' + expected_value AS expected,
        'Actual: ' + actual_value AS actual,
        details
    FROM l3_workday.qa_results
    WHERE status = 'FAIL'
    ORDER BY test_category, test_name;

    PRINT '';
END

-- =============================================================================
-- SECTION 5: OVERALL PASS/FAIL DETERMINATION
-- =============================================================================

PRINT '================================================================================';
PRINT 'SECTION 5: OVERALL PASS/FAIL DETERMINATION';
PRINT '================================================================================';
PRINT '';

DECLARE @overall_status VARCHAR(10) = 'PASS';
DECLARE @row_count_valid BIT = CASE WHEN @l1_total_rows > 0 AND @l3_dim_total_rows > 0 AND @l3_fact_total_rows > 0 THEN 1 ELSE 0 END;
DECLARE @qa_tests_passed BIT = CASE WHEN @failed_qa_tests = 0 THEN 1 ELSE 0 END;

IF @row_count_valid = 0 OR @qa_tests_passed = 0
    SET @overall_status = 'FAIL';

PRINT 'Success Criteria:';
PRINT '  1. Row Counts Validation: ' + CASE WHEN @row_count_valid = 1 THEN 'PASS' ELSE 'FAIL' END;
PRINT '     - L1 has rows: ' + CASE WHEN @l1_total_rows > 0 THEN 'YES (' + CAST(@l1_total_rows AS VARCHAR) + ')' ELSE 'NO' END;
PRINT '     - L3 dims have rows: ' + CASE WHEN @l3_dim_total_rows > 0 THEN 'YES (' + CAST(@l3_dim_total_rows AS VARCHAR) + ')' ELSE 'NO' END;
PRINT '     - L3 facts have rows: ' + CASE WHEN @l3_fact_total_rows > 0 THEN 'YES (' + CAST(@l3_fact_total_rows AS VARCHAR) + ')' ELSE 'NO' END;
PRINT '';
PRINT '  2. QA Tests Validation: ' + CASE WHEN @qa_tests_passed = 1 THEN 'PASS' ELSE 'FAIL' END;
PRINT '     - Total QA Tests: ' + CAST(@total_qa_tests AS VARCHAR);
PRINT '     - Passed: ' + CAST(@passed_qa_tests AS VARCHAR);
PRINT '     - Failed: ' + CAST(@failed_qa_tests AS VARCHAR);
PRINT '';

PRINT '================================================================================';
PRINT '                         OVERALL STATUS: ' + @overall_status;
PRINT '================================================================================';
PRINT '';

IF @overall_status = 'PASS'
BEGIN
    PRINT 'SUCCESS: HR Datamart ETL pipeline completed successfully.';
    PRINT '';
    PRINT 'All validations passed. Data is ready for analytics consumption.';
    PRINT '';
    PRINT 'Summary:';
    PRINT '  - ' + CAST(@l1_total_rows AS VARCHAR) + ' rows loaded to L1 staging';
    PRINT '  - ' + CAST(@l3_src_total_rows AS VARCHAR) + ' rows transformed to L3 source';
    PRINT '  - ' + CAST(@l3_dim_total_rows AS VARCHAR) + ' rows populated to L3 dimensions';
    PRINT '  - ' + CAST(@l3_fact_total_rows AS VARCHAR) + ' rows populated to L3 facts';
    PRINT '  - ' + CAST(@total_qa_tests AS VARCHAR) + ' QA tests executed, all PASS';
END
ELSE
BEGIN
    PRINT 'FAILURE: HR Datamart ETL pipeline encountered errors.';
    PRINT '';
    PRINT 'Resolution Required:';
    IF @row_count_valid = 0
        PRINT '  - Verify data loads: Check L1, L3 source, dimension, and fact tables';
    IF @qa_tests_passed = 0
        PRINT '  - Resolve QA failures: Review failed tests above and fix data quality issues';
    PRINT '';
    PRINT 'Troubleshooting Steps:';
    PRINT '  1. Review qa_results table for failed test details';
    PRINT '  2. Check etl_execution_log for step failures';
    PRINT '  3. Rerun affected steps after fixing root causes';
    PRINT '  4. Re-execute QA test suite to validate corrections';
END

PRINT '';

-- =============================================================================
-- SECTION 6: OPERATIONAL INFORMATION
-- =============================================================================

PRINT '================================================================================';
PRINT 'SECTION 6: OPERATIONAL INFORMATION';
PRINT '================================================================================';
PRINT '';

PRINT 'Important Tables for Operational Monitoring:';
PRINT '  - l3_workday.etl_execution_log';
PRINT '    Purpose: Track all ETL step executions (timing, status, row counts)';
PRINT '    Query: SELECT * FROM l3_workday.etl_execution_log WHERE batch_id = ''' + ISNULL(@batch_id, 'BATCH_ID') + '''';
PRINT '';
PRINT '  - l3_workday.qa_results';
PRINT '    Purpose: Detailed QA validation test results';
PRINT '    Query: SELECT * FROM l3_workday.qa_results ORDER BY test_category, test_name';
PRINT '';

PRINT 'Key Contacts & Documentation:';
PRINT '  - Runbook: artifacts/runbook/runbook.sql (orchestration script)';
PRINT '  - QA Tests: artifacts/qa/qa_tests.sql (validation logic)';
PRINT '  - Architecture: artifacts/docs/ARCHITECTURE.md';
PRINT '  - Operations Guide: artifacts/docs/OPERATION_GUIDE.md';
PRINT '';

PRINT 'Next Steps:';
PRINT '  1. Distribute this completion report to stakeholders';
PRINT '  2. Grant BI/Analytics team access to L3 star schema tables';
PRINT '  3. Monitor L3 tables for query performance (may require index tuning)';
PRINT '  4. Schedule recurring ETL execution (typically daily)';
PRINT '  5. Set up alerts for ETL failures in etl_execution_log';
PRINT '';

PRINT '================================================================================';
PRINT 'END OF COMPLETION REPORT';
PRINT '================================================================================';
PRINT '';
PRINT 'Report Generated: ' + CAST(GETDATE() AS VARCHAR);

