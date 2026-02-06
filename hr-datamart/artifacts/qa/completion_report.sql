-- HR DATAMART - COMPLETION REPORT (Redshift-Compatible)
-- Each query runs independently via Redshift Data API

-- SECTION 1: L1 Row Counts
SELECT 'L1_ROW_COUNTS' AS report_section,
       table_name, row_count
FROM (
    SELECT 'int6020_grade_profile' AS table_name, COUNT(*) AS row_count FROM l1_workday.int6020_grade_profile
    UNION ALL SELECT 'int6021_job_profile', COUNT(*) FROM l1_workday.int6021_job_profile
    UNION ALL SELECT 'int6022_job_classification', COUNT(*) FROM l1_workday.int6022_job_classification
    UNION ALL SELECT 'int6023_location', COUNT(*) FROM l1_workday.int6023_location
    UNION ALL SELECT 'int6024_company', COUNT(*) FROM l1_workday.int6024_company
    UNION ALL SELECT 'int6025_cost_center', COUNT(*) FROM l1_workday.int6025_cost_center
    UNION ALL SELECT 'int0095e_worker_job', COUNT(*) FROM l1_workday.int0095e_worker_job
    UNION ALL SELECT 'int0096_worker_organization', COUNT(*) FROM l1_workday.int0096_worker_organization
    UNION ALL SELECT 'int0098_worker_compensation', COUNT(*) FROM l1_workday.int0098_worker_compensation
    UNION ALL SELECT 'int6032_positions', COUNT(*) FROM l1_workday.int6032_positions
    UNION ALL SELECT 'int6028_department_hierarchy', COUNT(*) FROM l1_workday.int6028_department_hierarchy
    UNION ALL SELECT 'int270_rescinded', COUNT(*) FROM l1_workday.int270_rescinded
) l1_counts ORDER BY table_name;

-- SECTION 2: L3 Source Row Counts  
SELECT 'L3_SOURCE_ROW_COUNTS' AS report_section,
       table_name, row_count
FROM (
    SELECT 'l3_workday_worker_job_dly' AS table_name, COUNT(*) AS row_count FROM l3_workday.l3_workday_worker_job_dly
    UNION ALL SELECT 'l3_workday_worker_organization_dly', COUNT(*) FROM l3_workday.l3_workday_worker_organization_dly
    UNION ALL SELECT 'l3_workday_worker_comp_dly', COUNT(*) FROM l3_workday.l3_workday_worker_comp_dly
) l3_src ORDER BY table_name;

-- SECTION 3: L3 Dimension Row Counts
SELECT 'L3_DIMENSION_ROW_COUNTS' AS report_section,
       table_name, row_count
FROM (
    SELECT 'dim_day_d' AS table_name, COUNT(*) AS row_count FROM l3_workday.dim_day_d
    UNION ALL SELECT 'dim_company_d', COUNT(*) FROM l3_workday.dim_company_d
    UNION ALL SELECT 'dim_cost_center_d', COUNT(*) FROM l3_workday.dim_cost_center_d
    UNION ALL SELECT 'dim_grade_profile_d', COUNT(*) FROM l3_workday.dim_grade_profile_d
    UNION ALL SELECT 'dim_job_profile_d', COUNT(*) FROM l3_workday.dim_job_profile_d
    UNION ALL SELECT 'dim_location_d', COUNT(*) FROM l3_workday.dim_location_d
    UNION ALL SELECT 'dim_department_d', COUNT(*) FROM l3_workday.dim_department_d
    UNION ALL SELECT 'dim_position_d', COUNT(*) FROM l3_workday.dim_position_d
    UNION ALL SELECT 'dim_worker_job_d', COUNT(*) FROM l3_workday.dim_worker_job_d
    UNION ALL SELECT 'dim_worker_status_d', COUNT(*) FROM l3_workday.dim_worker_status_d
) l3_dims ORDER BY table_name;

-- SECTION 4: L3 Fact Row Counts
SELECT 'L3_FACT_ROW_COUNTS' AS report_section,
       table_name, row_count
FROM (
    SELECT 'fct_worker_movement_f' AS table_name, COUNT(*) AS row_count FROM l3_workday.fct_worker_movement_f
    UNION ALL SELECT 'fct_worker_headcount_restat_f', COUNT(*) FROM l3_workday.fct_worker_headcount_restat_f
) l3_facts ORDER BY table_name;

-- SECTION 5: Grand Total
SELECT 'GRAND_TOTAL' AS report_section,
       layer, table_count, total_rows
FROM (
    SELECT 'L1_Staging' AS layer, 12 AS table_count,
           (SELECT COUNT(*) FROM l1_workday.int6020_grade_profile) +
           (SELECT COUNT(*) FROM l1_workday.int6021_job_profile) +
           (SELECT COUNT(*) FROM l1_workday.int6022_job_classification) +
           (SELECT COUNT(*) FROM l1_workday.int6023_location) +
           (SELECT COUNT(*) FROM l1_workday.int6024_company) +
           (SELECT COUNT(*) FROM l1_workday.int6025_cost_center) +
           (SELECT COUNT(*) FROM l1_workday.int0095e_worker_job) +
           (SELECT COUNT(*) FROM l1_workday.int0096_worker_organization) +
           (SELECT COUNT(*) FROM l1_workday.int0098_worker_compensation) +
           (SELECT COUNT(*) FROM l1_workday.int6032_positions) +
           (SELECT COUNT(*) FROM l1_workday.int6028_department_hierarchy) +
           (SELECT COUNT(*) FROM l1_workday.int270_rescinded) AS total_rows
    UNION ALL
    SELECT 'L3_Source', 3,
           (SELECT COUNT(*) FROM l3_workday.l3_workday_worker_job_dly) +
           (SELECT COUNT(*) FROM l3_workday.l3_workday_worker_organization_dly) +
           (SELECT COUNT(*) FROM l3_workday.l3_workday_worker_comp_dly)
    UNION ALL
    SELECT 'L3_Dimensions', 10,
           (SELECT COUNT(*) FROM l3_workday.dim_day_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_company_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_cost_center_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_grade_profile_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_job_profile_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_location_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_department_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_position_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_worker_job_d) +
           (SELECT COUNT(*) FROM l3_workday.dim_worker_status_d)
    UNION ALL
    SELECT 'L3_Facts', 2,
           (SELECT COUNT(*) FROM l3_workday.fct_worker_movement_f) +
           (SELECT COUNT(*) FROM l3_workday.fct_worker_headcount_restat_f)
) summary ORDER BY layer;

-- SECTION 6: QA Results Summary  
SELECT 'QA_SUMMARY' AS report_section,
       test_category, 
       COUNT(*) AS total_tests,
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
FROM l3_workday.qa_results
GROUP BY test_category
ORDER BY test_category;

-- SECTION 7: Overall Status
SELECT 'OVERALL_STATUS' AS report_section,
       COUNT(*) AS total_tests,
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
       CASE WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS overall_status
FROM l3_workday.qa_results;

-- SECTION 8: Failed Tests (if any)
SELECT 'FAILED_TESTS' AS report_section,
       test_name, test_category, expected_value, actual_value, details
FROM l3_workday.qa_results
WHERE status = 'FAIL'
ORDER BY test_category, test_name;