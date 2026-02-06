-- HR DATAMART - QA VALIDATION TEST SUITE (Redshift-Compatible)
-- Each test is a standalone INSERT...SELECT statement
-- Total: ~38 tests across 7 categories

-- ============================================================================
-- SETUP
-- ============================================================================

DROP TABLE IF EXISTS l3_workday.qa_results;

CREATE TABLE l3_workday.qa_results (
    test_id INTEGER IDENTITY(1,1),
    test_name VARCHAR(200) NOT NULL,
    test_category VARCHAR(50) NOT NULL,
    status VARCHAR(10) NOT NULL,
    expected_value VARCHAR(200),
    actual_value VARCHAR(200),
    details VARCHAR(1000),
    run_timestamp TIMESTAMP DEFAULT GETDATE()
);

-- ============================================================================
-- CATEGORY 1: L1 ROW COUNTS (12 tests)
-- ============================================================================

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int6020_grade_profile row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Grade profile staging data'
FROM l1_workday.int6020_grade_profile;
INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int6021_job_profile row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Job profile staging data'
FROM l1_workday.int6021_job_profile;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int6022_job_classification row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Job profile details staging data'
FROM l1_workday.int6022_job_classification;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int6023_location row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Location staging data'
FROM l1_workday.int6023_location;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int6024_company row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Company staging data'
FROM l1_workday.int6024_company;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int6025_cost_center row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Cost center staging data'
FROM l1_workday.int6025_cost_center;
INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int0095e_worker_job row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker job staging data'
FROM l1_workday.int0095e_worker_job;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int0096_worker_organization row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker organization staging data'
FROM l1_workday.int0096_worker_organization;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int0098_worker_compensation row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker compensation staging data'
FROM l1_workday.int0098_worker_compensation;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int6032_positions row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Position staging data'
FROM l1_workday.int6032_positions;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int6028_department_hierarchy row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Supervisory organization staging data'
FROM l1_workday.int6028_department_hierarchy;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L1: int270_rescinded row count > 0', 'ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Rescinded staging data'
FROM l1_workday.int270_rescinded;
-- ============================================================================
-- CATEGORY 2: L3 SOURCE ROW COUNTS (3 tests)
-- ============================================================================

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L3: l3_workday_worker_job_dly row count > 0', 'SOURCE_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker job daily source data'
FROM l3_workday.l3_workday_worker_job_dly;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L3: l3_workday_worker_organization_dly row count > 0', 'SOURCE_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker organization daily source data'
FROM l3_workday.l3_workday_worker_organization_dly;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'L3: l3_workday_worker_comp_dly row count > 0', 'SOURCE_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker compensation daily source data'
FROM l3_workday.l3_workday_worker_comp_dly;

-- ============================================================================
-- CATEGORY 3: L3 DIMENSION ROW COUNTS (10 tests)
-- ============================================================================

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_day_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Day dimension data'
FROM l3_workday.dim_day_d;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_company_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Company dimension data'
FROM l3_workday.dim_company_d;
INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_cost_center_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Cost center dimension data'
FROM l3_workday.dim_cost_center_d;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_grade_profile_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Grade profile dimension data'
FROM l3_workday.dim_grade_profile_d;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_job_profile_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Job profile dimension data'
FROM l3_workday.dim_job_profile_d;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_location_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Location dimension data'
FROM l3_workday.dim_location_d;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_department_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Department dimension data'
FROM l3_workday.dim_department_d;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_position_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Position dimension data'
FROM l3_workday.dim_position_d;
INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_worker_job_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker job dimension data'
FROM l3_workday.dim_worker_job_d;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DIM: dim_worker_status_d row count > 0', 'DIMENSION_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker status dimension data'
FROM l3_workday.dim_worker_status_d;

-- ============================================================================
-- CATEGORY 4: L3 FACT ROW COUNTS (2 tests)
-- ============================================================================

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'FCT: fct_worker_movement_f row count > 0', 'FACT_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker movement fact table data'
FROM l3_workday.fct_worker_movement_f;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'FCT: fct_worker_headcount_restat_f row count > 0', 'FACT_ROW_COUNT',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       '> 0', CAST(COUNT(*) AS VARCHAR), 'Worker headcount restatement fact table data'
FROM l3_workday.fct_worker_headcount_restat_f;

-- ============================================================================
-- CATEGORY 5: PK UNIQUENESS (4 tests)
-- ============================================================================

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'PK: dim_company_d uniqueness (company_sk)', 'PK_UNIQUENESS',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 duplicates', CAST(COUNT(*) AS VARCHAR), 'Company SK must be unique'
FROM (SELECT company_sk, COUNT(*) FROM l3_workday.dim_company_d GROUP BY company_sk HAVING COUNT(*) > 1);
INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'PK: dim_worker_job_d uniqueness (employee_id, effective_date, valid_from)', 'PK_UNIQUENESS',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 duplicates', CAST(COUNT(*) AS VARCHAR), 'Worker job composite key must be unique'
FROM (SELECT employee_id, effective_date, valid_from, COUNT(*) FROM l3_workday.dim_worker_job_d 
      GROUP BY employee_id, effective_date, valid_from HAVING COUNT(*) > 1);

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'PK: dim_worker_status_d uniqueness (employee_id, effective_date, valid_from)', 'PK_UNIQUENESS',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 duplicates', CAST(COUNT(*) AS VARCHAR), 'Worker status composite key must be unique'
FROM (SELECT employee_id, effective_date, valid_from, COUNT(*) FROM l3_workday.dim_worker_status_d 
      GROUP BY employee_id, effective_date, valid_from HAVING COUNT(*) > 1);

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'PK: fct_worker_movement_f uniqueness (employee_id, effective_date)', 'PK_UNIQUENESS',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 duplicates', CAST(COUNT(*) AS VARCHAR), 'Worker movement composite key must be unique'
FROM (SELECT employee_id, effective_date, COUNT(*) FROM l3_workday.fct_worker_movement_f 
      GROUP BY employee_id, effective_date HAVING COUNT(*) > 1);

-- ============================================================================
-- CATEGORY 6: SCD2 VALIDATION (4 tests)
-- ============================================================================

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'SCD2: dim_company_d current records have valid_to=9999-12-31', 'SCD2_VALIDATION',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 bad records', CAST(COUNT(*) AS VARCHAR), 'Current flag must have valid_to=9999-12-31'
FROM l3_workday.dim_company_d WHERE is_current = TRUE AND valid_to <> '9999-12-31';
INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'SCD2: dim_cost_center_d current records have valid_to=9999-12-31', 'SCD2_VALIDATION',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 bad records', CAST(COUNT(*) AS VARCHAR), 'Current flag must have valid_to=9999-12-31'
FROM l3_workday.dim_cost_center_d WHERE is_current = TRUE AND valid_to <> '9999-12-31';

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'SCD2: dim_grade_profile_d current records have valid_to=9999-12-31', 'SCD2_VALIDATION',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 bad records', CAST(COUNT(*) AS VARCHAR), 'Current flag must have valid_to=9999-12-31'
FROM l3_workday.dim_grade_profile_d WHERE is_current = TRUE AND valid_to <> '9999-12-31';

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'SCD2: dim_worker_job_d current records have valid_to=9999-12-31', 'SCD2_VALIDATION',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 bad records', CAST(COUNT(*) AS VARCHAR), 'Current flag must have valid_to=9999-12-31'
FROM l3_workday.dim_worker_job_d WHERE is_current = TRUE AND valid_to <> '9999-12-31';

-- ============================================================================
-- CATEGORY 7: DATA QUALITY (3 tests)
-- ============================================================================

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DQ: dim_grade_profile_d salary ranges valid (min <= mid <= max)', 'DATA_QUALITY',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       '0 invalid ranges', CAST(COUNT(*) AS VARCHAR), 'Salary range midpoint must be between min and max'
FROM l3_workday.dim_grade_profile_d 
WHERE grade_profile_salary_range_minimjum > grade_profile_salary_range_midpoint 
   OR grade_profile_salary_range_midpoint > grade_profile_salary_range_maximum;
INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DQ: dim_day_d row count approximately 4018 (2020-01-01 to 2030-12-31)', 'DATA_QUALITY',
       CASE WHEN COUNT(*) BETWEEN 4000 AND 4100 THEN 'PASS' ELSE 'FAIL' END,
       '~4018 days', CAST(COUNT(*) AS VARCHAR), 'Day dimension should cover 11 years'
FROM l3_workday.dim_day_d;

INSERT INTO l3_workday.qa_results (test_name, test_category, status, expected_value, actual_value, details)
SELECT 'DQ: dim_day_d calendar_date range (2020-01-01 to 2030-12-31)', 'DATA_QUALITY',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       'min >= 2020-01-01 AND max <= 2030-12-31', CAST(COUNT(*) AS VARCHAR), 'Calendar dates must be within expected range'
FROM l3_workday.dim_day_d 
WHERE calendar_date < '2020-01-01' OR calendar_date > '2030-12-31';

-- ============================================================================
-- SUMMARY REPORTS
-- ============================================================================

-- Summary by test category
SELECT test_category, COUNT(*) AS total, 
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
FROM l3_workday.qa_results GROUP BY test_category ORDER BY test_category;

-- Overall summary
SELECT COUNT(*) AS total_tests,
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
FROM l3_workday.qa_results;

-- Failed tests detail
SELECT test_name, status, expected_value, actual_value, details
FROM l3_workday.qa_results WHERE status = 'FAIL' ORDER BY test_category;