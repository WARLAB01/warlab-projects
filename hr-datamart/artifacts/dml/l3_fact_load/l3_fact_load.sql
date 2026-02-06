-- ============================================================================
-- L3 Star Schema - HR Datamart Fact Table Load Logic
-- Schema: l3_workday
-- ============================================================================
-- FACT 1: fct_worker_movement_f - Load from dim_worker_job_d
-- FACT 2: fct_worker_headcount_restat_f - Monthly snapshot restatement
-- ============================================================================

-- ============================================================================
-- FACT TABLE 1: fct_worker_movement_f
-- ============================================================================
-- Load Logic:
--   1. Source entirely from dim_worker_job_d (no direct L1 joins)
--   2. Use LAG() to get prior row attributes by employee and effective_date
--   3. Resolve all current and prior row dimension FKs as-of their dates
--   4. Compute change metrics (only when both rows are active: status in ('A','L'))
-- ============================================================================

BEGIN;

TRUNCATE TABLE l3_workday.fct_worker_movement_f;

INSERT INTO l3_workday.fct_worker_movement_f (
    employee_id,
    effective_date,
    day_sk,
    company_sk,
    cost_center_sk,
    grade_profile_sk,
    job_profile_sk,
    location_sk,
    department_sk,
    position_sk,
    worker_job_sk,
    worker_status_sk,
    prior_day_sk,
    prior_company_sk,
    prior_cost_center_sk,
    prior_grade_profile_sk,
    prior_job_profile_sk,
    prior_location_sk,
    prior_department_sk,
    prior_position_sk,
    prior_worker_job_sk,
    prior_worker_status_sk,
    company_id,
    cost_center_id,
    grade_id,
    job_profile_id,
    location_id,
    management_level_code,
    matrix_org_id,
    sup_org_id,
    work_model_type,
    base_pay_proposed_amount,
    idp_employee_status,
    prior_company_id,
    prior_cost_center_id,
    prior_grade_id,
    prior_job_profile_id,
    prior_location_id,
    prior_management_level_code,
    prior_matrix_org_id,
    prior_sup_org_id,
    prior_work_model_type,
    prior_base_pay_proposed_amount,
    prior_idp_employee_status,
    prior_effective_date,
    base_pay_change_count,
    company_change_count,
    cost_center_change_count,
    grade_change_count,
    grade_decrease_count,
    grade_increase_count,
    job_change_count,
    location_change_count,
    management_level_change_count,
    management_level_decrease_count,
    management_level_increase_count,
    matrix_organization_change_count,
    regrettable_termination_count,
    supervisory_organization_change_count,
    worker_model_change_count,
    insert_datetime,
    update_datetime,
    etl_batch_id
)
WITH source_data AS (
    -- Extract all worker job records with prior row info using LAG()
    SELECT
        dwj.employee_id,
        dwj.effective_date,
        dwj.worker_job_sk,
        dwj.company_id,
        dwj.cost_center_id,
        dwj.compensation_grade_proposed AS grade_id,
        dwj.job_profile_id,
        dwj.location AS location_id,
        djp.management_level_code,
        djp.job_matrix AS matrix_org_id,
        dwj.sup_org_id,
        dwj.work_model_type,
        dwj.base_pay_proposed_amount,
        dwj.idp_employee_status,
        dwj.position_id,
        -- Prior row values using LAG() window function
        LAG(dwj.effective_date) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_effective_date,
        LAG(dwj.company_id) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_company_id,
        LAG(dwj.cost_center_id) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_cost_center_id,
        LAG(dwj.compensation_grade_proposed) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_grade_id,
        LAG(dwj.job_profile_id) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_job_profile_id,
        LAG(dwj.location) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_location_id,
        LAG(djp.management_level_code) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_management_level_code,
        LAG(djp.job_matrix) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_matrix_org_id,
        LAG(dwj.sup_org_id) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_sup_org_id,
        LAG(dwj.work_model_type) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_work_model_type,
        LAG(dwj.base_pay_proposed_amount) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_base_pay_proposed_amount,
        LAG(dwj.idp_employee_status) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_idp_employee_status,
        LAG(dwj.position_id) OVER (
            PARTITION BY dwj.employee_id
            ORDER BY dwj.effective_date ASC
        ) AS prior_position_id
    FROM l3_workday.dim_worker_job_d dwj
    LEFT JOIN l3_workday.dim_job_profile_d djp
        ON dwj.job_profile_id = djp.job_profile_id
        AND djp.is_current = true
    WHERE dwj.is_current = true
),
-- Resolve current row dimension foreign keys as-of effective_date
with_current_fks AS (
    SELECT
        sd.employee_id,
        sd.effective_date,
        sd.worker_job_sk,
        sd.company_id,
        sd.cost_center_id,
        sd.grade_id,
        sd.job_profile_id,
        sd.location_id,
        sd.management_level_code,
        sd.matrix_org_id,
        sd.sup_org_id,
        sd.work_model_type,
        sd.base_pay_proposed_amount,
        sd.idp_employee_status,
        sd.position_id,
        sd.prior_effective_date,
        sd.prior_company_id,
        sd.prior_cost_center_id,
        sd.prior_grade_id,
        sd.prior_job_profile_id,
        sd.prior_location_id,
        sd.prior_management_level_code,
        sd.prior_matrix_org_id,
        sd.prior_sup_org_id,
        sd.prior_work_model_type,
        sd.prior_base_pay_proposed_amount,
        sd.prior_idp_employee_status,
        sd.prior_position_id,
        -- Day SK for current effective_date
        CAST(TO_CHAR(sd.effective_date, 'YYYYMMDD') AS INTEGER) AS day_sk,
        -- Current row FKs: dimension lookups as-of effective_date
        dc.company_sk,
        dcc.cost_center_sk,
        dgp.grade_profile_sk,
        djp.job_profile_sk,
        dl.location_sk,
        dd.department_sk,
        dp.position_sk,
        dws.worker_status_sk
    FROM source_data sd
    LEFT JOIN l3_workday.dim_company_d dc
        ON sd.company_id = dc.company_id
        AND sd.effective_date BETWEEN dc.valid_from AND dc.valid_to
    LEFT JOIN l3_workday.dim_cost_center_d dcc
        ON sd.cost_center_id = dcc.cost_center_id
        AND sd.effective_date BETWEEN dcc.valid_from AND dcc.valid_to
    LEFT JOIN l3_workday.dim_grade_profile_d dgp
        ON sd.grade_id = dgp.grade_id
        AND sd.effective_date BETWEEN dgp.valid_from AND dgp.valid_to
    LEFT JOIN l3_workday.dim_job_profile_d djp
        ON sd.job_profile_id = djp.job_profile_id
        AND sd.effective_date BETWEEN djp.valid_from AND djp.valid_to
    LEFT JOIN l3_workday.dim_location_d dl
        ON sd.location_id = dl.location_id
        AND sd.effective_date BETWEEN dl.valid_from AND dl.valid_to
    LEFT JOIN l3_workday.dim_department_d dd
        ON sd.sup_org_id = dd.department_id
        AND sd.effective_date BETWEEN dd.valid_from AND dd.valid_to
    LEFT JOIN l3_workday.dim_position_d dp
        ON sd.position_id = dp.position_id
        AND sd.effective_date BETWEEN dp.valid_from AND dp.valid_to
    LEFT JOIN l3_workday.dim_worker_status_d dws
        ON sd.employee_id = dws.employee_id
        AND sd.effective_date = dws.effective_date
        AND dws.is_current = true
),
-- Resolve prior row dimension foreign keys as-of prior_effective_date
with_prior_fks AS (
    SELECT
        wcf.employee_id,
        wcf.effective_date,
        wcf.worker_job_sk,
        wcf.company_id,
        wcf.cost_center_id,
        wcf.grade_id,
        wcf.job_profile_id,
        wcf.location_id,
        wcf.management_level_code,
        wcf.matrix_org_id,
        wcf.sup_org_id,
        wcf.work_model_type,
        wcf.base_pay_proposed_amount,
        wcf.idp_employee_status,
        wcf.position_id,
        wcf.prior_effective_date,
        wcf.prior_company_id,
        wcf.prior_cost_center_id,
        wcf.prior_grade_id,
        wcf.prior_job_profile_id,
        wcf.prior_location_id,
        wcf.prior_management_level_code,
        wcf.prior_matrix_org_id,
        wcf.prior_sup_org_id,
        wcf.prior_work_model_type,
        wcf.prior_base_pay_proposed_amount,
        wcf.prior_idp_employee_status,
        wcf.prior_position_id,
        wcf.day_sk,
        wcf.company_sk,
        wcf.cost_center_sk,
        wcf.grade_profile_sk,
        wcf.job_profile_sk,
        wcf.location_sk,
        wcf.department_sk,
        wcf.position_sk,
        wcf.worker_status_sk,
        -- Day SK for prior effective_date
        CAST(TO_CHAR(wcf.prior_effective_date, 'YYYYMMDD') AS INTEGER) AS prior_day_sk,
        -- Prior row FKs: dimension lookups as-of prior_effective_date
        dc2.company_sk AS prior_company_sk,
        dcc2.cost_center_sk AS prior_cost_center_sk,
        dgp2.grade_profile_sk AS prior_grade_profile_sk,
        djp2.job_profile_sk AS prior_job_profile_sk,
        dl2.location_sk AS prior_location_sk,
        dd2.department_sk AS prior_department_sk,
        dp2.position_sk AS prior_position_sk,
        dwj2.worker_job_sk AS prior_worker_job_sk,
        dws2.worker_status_sk AS prior_worker_status_sk
    FROM with_current_fks wcf
    LEFT JOIN l3_workday.dim_company_d dc2
        ON wcf.prior_company_id = dc2.company_id
        AND wcf.prior_effective_date BETWEEN dc2.valid_from AND dc2.valid_to
    LEFT JOIN l3_workday.dim_cost_center_d dcc2
        ON wcf.prior_cost_center_id = dcc2.cost_center_id
        AND wcf.prior_effective_date BETWEEN dcc2.valid_from AND dcc2.valid_to
    LEFT JOIN l3_workday.dim_grade_profile_d dgp2
        ON wcf.prior_grade_id = dgp2.grade_id
        AND wcf.prior_effective_date BETWEEN dgp2.valid_from AND dgp2.valid_to
    LEFT JOIN l3_workday.dim_job_profile_d djp2
        ON wcf.prior_job_profile_id = djp2.job_profile_id
        AND wcf.prior_effective_date BETWEEN djp2.valid_from AND djp2.valid_to
    LEFT JOIN l3_workday.dim_location_d dl2
        ON wcf.prior_location_id = dl2.location_id
        AND wcf.prior_effective_date BETWEEN dl2.valid_from AND dl2.valid_to
    LEFT JOIN l3_workday.dim_department_d dd2
        ON wcf.prior_sup_org_id = dd2.department_id
        AND wcf.prior_effective_date BETWEEN dd2.valid_from AND dd2.valid_to
    LEFT JOIN l3_workday.dim_position_d dp2
        ON wcf.prior_position_id = dp2.position_id
        AND wcf.prior_effective_date BETWEEN dp2.valid_from AND dp2.valid_to
    LEFT JOIN l3_workday.dim_worker_job_d dwj2
        ON wcf.employee_id = dwj2.employee_id
        AND wcf.prior_effective_date = dwj2.effective_date
        AND dwj2.is_current = true
    LEFT JOIN l3_workday.dim_worker_status_d dws2
        ON wcf.employee_id = dws2.employee_id
        AND wcf.prior_effective_date = dws2.effective_date
        AND dws2.is_current = true
)
-- Final SELECT: output all columns with computed change metrics
SELECT
    wpf.employee_id,
    wpf.effective_date,
    wpf.day_sk,
    wpf.company_sk,
    wpf.cost_center_sk,
    wpf.grade_profile_sk,
    wpf.job_profile_sk,
    wpf.location_sk,
    wpf.department_sk,
    wpf.position_sk,
    wpf.worker_job_sk,
    wpf.worker_status_sk,
    wpf.prior_day_sk,
    wpf.prior_company_sk,
    wpf.prior_cost_center_sk,
    wpf.prior_grade_profile_sk,
    wpf.prior_job_profile_sk,
    wpf.prior_location_sk,
    wpf.prior_department_sk,
    wpf.prior_position_sk,
    wpf.prior_worker_job_sk,
    wpf.prior_worker_status_sk,
    wpf.company_id,
    wpf.cost_center_id,
    wpf.grade_id,
    wpf.job_profile_id,
    wpf.location_id,
    wpf.management_level_code,
    wpf.matrix_org_id,
    wpf.sup_org_id,
    wpf.work_model_type,
    wpf.base_pay_proposed_amount,
    wpf.idp_employee_status,
    wpf.prior_company_id,
    wpf.prior_cost_center_id,
    wpf.prior_grade_id,
    wpf.prior_job_profile_id,
    wpf.prior_location_id,
    wpf.prior_management_level_code,
    wpf.prior_matrix_org_id,
    wpf.prior_sup_org_id,
    wpf.prior_work_model_type,
    wpf.prior_base_pay_proposed_amount,
    wpf.prior_idp_employee_status,
    wpf.prior_effective_date,
    -- CHANGE METRICS: Computed only when both current and prior rows are active (status in ('A','L'))
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.base_pay_proposed_amount, 0) <> COALESCE(wpf.prior_base_pay_proposed_amount, 0)
        THEN 1
        ELSE 0
    END AS base_pay_change_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.company_id, '') <> COALESCE(wpf.prior_company_id, '')
        THEN 1
        ELSE 0
    END AS company_change_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.cost_center_id, '') <> COALESCE(wpf.prior_cost_center_id, '')
        THEN 1
        ELSE 0
    END AS cost_center_change_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.grade_id, '') <> COALESCE(wpf.prior_grade_id, '')
        THEN 1
        ELSE 0
    END AS grade_change_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND wpf.grade_id < wpf.prior_grade_id
        THEN 1
        ELSE 0
    END AS grade_decrease_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND wpf.grade_id > wpf.prior_grade_id
        THEN 1
        ELSE 0
    END AS grade_increase_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.job_profile_id, '') <> COALESCE(wpf.prior_job_profile_id, '')
        THEN 1
        ELSE 0
    END AS job_change_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.location_id, '') <> COALESCE(wpf.prior_location_id, '')
        THEN 1
        ELSE 0
    END AS location_change_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.management_level_code, '') <> COALESCE(wpf.prior_management_level_code, '')
        THEN 1
        ELSE 0
    END AS management_level_change_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND wpf.management_level_code < wpf.prior_management_level_code
        THEN 1
        ELSE 0
    END AS management_level_decrease_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND wpf.management_level_code > wpf.prior_management_level_code
        THEN 1
        ELSE 0
    END AS management_level_increase_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.matrix_org_id, '') <> COALESCE(wpf.prior_matrix_org_id, '')
        THEN 1
        ELSE 0
    END AS matrix_organization_change_count,
    NULL::INTEGER AS regrettable_termination_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.sup_org_id, '') <> COALESCE(wpf.prior_sup_org_id, '')
        THEN 1
        ELSE 0
    END AS supervisory_organization_change_count,
    CASE
        WHEN wpf.idp_employee_status IN ('A', 'L')
         AND wpf.prior_idp_employee_status IN ('A', 'L')
         AND COALESCE(wpf.work_model_type, '') <> COALESCE(wpf.prior_work_model_type, '')
        THEN 1
        ELSE 0
    END AS worker_model_change_count,
    GETDATE() AS insert_datetime,
    GETDATE() AS update_datetime,
    '${ETL_BATCH_ID}' AS etl_batch_id
FROM with_prior_fks wpf;

COMMIT;


-- ============================================================================
-- FACT TABLE 2: fct_worker_headcount_restat_f
-- ============================================================================
-- Load Logic:
--   1. Generate month-end dates for the last 24 months
--   2. For each month-end, find all active workers from dim_worker_job_d
--      A worker is active at month-end if they have a current SCD2 row where
--      effective_date_from <= snapshot_date AND effective_date_to >= snapshot_date
--      AND idp_employee_status IN ('A', 'L')
--   3. Resolve all dimension FKs as-of snapshot_date
--   4. Compute headcount metric (1 if active and not on leave)
-- ============================================================================

BEGIN;

-- Delete existing data for restatement window (last 24 months)
DELETE FROM l3_workday.fct_worker_headcount_restat_f
WHERE snapshot_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '24 months';

-- Generate month-end dates for last 24 months
INSERT INTO l3_workday.fct_worker_headcount_restat_f (
    snapshot_date,
    employee_id,
    day_sk,
    company_sk,
    cost_center_sk,
    grade_profile_sk,
    job_profile_sk,
    location_sk,
    department_sk,
    position_sk,
    worker_job_sk,
    worker_status_sk,
    company_id,
    cost_center_id,
    job_profile_id,
    location_id,
    sup_org_id,
    idp_employee_status,
    headcount,
    insert_datetime,
    update_datetime,
    etl_batch_id
)
WITH month_ends AS (
    -- Generate 24 month-end dates going backward from current date
    SELECT
        LAST_DAY(DATEADD(month, -month_offset, DATE_TRUNC('month', CURRENT_DATE))) AS snapshot_date
    FROM (
        SELECT
            ROW_NUMBER() OVER (ORDER BY 1) - 1 AS month_offset
        FROM (
            SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
            UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
            UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20
            UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23 UNION ALL SELECT 24
        ) months
    ) month_generator
    WHERE LAST_DAY(DATEADD(month, -month_offset, DATE_TRUNC('month', CURRENT_DATE))) < CURRENT_DATE
),
-- For each month-end, find all active workers from dim_worker_job_d
-- A worker is active if they have a valid current row as-of that date
active_workers AS (
    SELECT
        me.snapshot_date,
        dwj.employee_id,
        dwj.company_id,
        dwj.cost_center_id,
        dwj.job_profile_id,
        dwj.location AS location_id,
        dwj.sup_org_id,
        dwj.supervisory_organization,
        dwj.idp_employee_status,
        dwj.compensation_grade_proposed AS grade_id,
        dwj.position_id,
        dwj.worker_job_sk
    FROM month_ends me
    CROSS JOIN l3_workday.dim_worker_job_d dwj
    WHERE dwj.effective_date_from <= me.snapshot_date
      AND dwj.effective_date_to >= me.snapshot_date
      AND dwj.is_current = true
      AND dwj.idp_employee_status IN ('A', 'L')
),
-- Resolve all dimension FKs as-of snapshot_date
with_dimension_fks AS (
    SELECT
        aw.snapshot_date,
        aw.employee_id,
        aw.company_id,
        aw.cost_center_id,
        aw.job_profile_id,
        aw.location_id,
        aw.sup_org_id,
        aw.idp_employee_status,
        aw.grade_id,
        aw.position_id,
        aw.worker_job_sk,
        -- Day SK for snapshot_date
        CAST(TO_CHAR(aw.snapshot_date, 'YYYYMMDD') AS INTEGER) AS day_sk,
        -- Dimension FKs resolved as-of snapshot_date
        dc.company_sk,
        dcc.cost_center_sk,
        dgp.grade_profile_sk,
        djp.job_profile_sk,
        dl.location_sk,
        dd.department_sk,
        dp.position_sk,
        dws.worker_status_sk
    FROM active_workers aw
    LEFT JOIN l3_workday.dim_company_d dc
        ON aw.company_id = dc.company_id
        AND aw.snapshot_date BETWEEN dc.valid_from AND dc.valid_to
    LEFT JOIN l3_workday.dim_cost_center_d dcc
        ON aw.cost_center_id = dcc.cost_center_id
        AND aw.snapshot_date BETWEEN dcc.valid_from AND dcc.valid_to
    LEFT JOIN l3_workday.dim_grade_profile_d dgp
        ON aw.grade_id = dgp.grade_id
        AND aw.snapshot_date BETWEEN dgp.valid_from AND dgp.valid_to
    LEFT JOIN l3_workday.dim_job_profile_d djp
        ON aw.job_profile_id = djp.job_profile_id
        AND aw.snapshot_date BETWEEN djp.valid_from AND djp.valid_to
    LEFT JOIN l3_workday.dim_location_d dl
        ON aw.location_id = dl.location_id
        AND aw.snapshot_date BETWEEN dl.valid_from AND dl.valid_to
    -- Bridge CC-format supervisory_organization to DPT-format department_id
    LEFT JOIN l3_workday.dim_department_d dd
        ON REPLACE(aw.supervisory_organization, 'CC', 'DPT') = dd.department_id
        AND aw.snapshot_date BETWEEN dd.valid_from AND dd.valid_to
    LEFT JOIN l3_workday.dim_position_d dp
        ON aw.position_id = dp.position_id
        AND aw.snapshot_date BETWEEN dp.valid_from AND dp.valid_to
    LEFT JOIN l3_workday.dim_worker_status_d dws
        ON aw.employee_id = dws.employee_id
        AND aw.snapshot_date BETWEEN dws.effective_date_from AND dws.effective_date_to
        AND dws.is_current = true
)
-- Final SELECT: output all columns with headcount metric
SELECT
    wdf.snapshot_date,
    wdf.employee_id,
    wdf.day_sk,
    wdf.company_sk,
    wdf.cost_center_sk,
    wdf.grade_profile_sk,
    wdf.job_profile_sk,
    wdf.location_sk,
    wdf.department_sk,
    wdf.position_sk,
    wdf.worker_job_sk,
    wdf.worker_status_sk,
    wdf.company_id,
    wdf.cost_center_id,
    wdf.job_profile_id,
    wdf.location_id,
    wdf.sup_org_id,
    wdf.idp_employee_status,
    -- METRIC: headcount = 1 if active (not on leave), 0 if on leave
    CASE
        WHEN wdf.idp_employee_status = 'A' THEN 1
        ELSE 0
    END AS headcount,
    GETDATE() AS insert_datetime,
    GETDATE() AS update_datetime,
    '${ETL_BATCH_ID}' AS etl_batch_id
FROM with_dimension_fks wdf;

COMMIT;
