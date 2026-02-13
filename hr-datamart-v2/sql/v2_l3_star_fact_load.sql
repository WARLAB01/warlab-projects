-- ============================================================
-- HR Datamart V2 - Fact Table Load SQL
-- Step 6: L3 Star Schema Fact Tables
-- ============================================================
-- Depends on: All dimension tables loaded and deduplicated
-- Key patterns:
--   - dim_worker_job_d.is_current = 'Y' (VARCHAR)
--   - All date columns in dim_worker_job_d are VARCHAR, must ::DATE cast
--   - FK resolution uses simple natural key joins (dims deduplicated to 1 row per NK)
--   - Movement fact uses LAG() for prior row comparison
--   - Headcount uses monthly snapshots with ROW_NUMBER for as-of resolution
-- ============================================================

-- ============================================================
-- 1. FACT: fct_worker_movement_f
-- Transaction grain: one row per employee per effective_date
-- 28 computed metrics comparing current vs prior row
-- ============================================================

TRUNCATE TABLE v2_l3_star.fct_worker_movement_f;

INSERT INTO v2_l3_star.fct_worker_movement_f (
    employee_id, effective_date,
    day_sk, company_sk, cost_center_sk, grade_profile_sk, job_profile_sk,
    location_sk, supervisory_org_sk, worker_job_sk, worker_status_sk,
    company_id, cost_center_id, grade_id, job_profile_id, location_id,
    sup_org_id, work_model_type, base_pay_proposed_amount,
    idp_employee_status, action, action_reason, primary_termination_reason, worker_status,
    prior_day_sk, prior_company_sk, prior_cost_center_sk, prior_grade_profile_sk,
    prior_job_profile_sk, prior_location_sk, prior_supervisory_org_sk,
    prior_worker_job_sk, prior_worker_status_sk,
    prior_company_id, prior_cost_center_id, prior_grade_id, prior_job_profile_id,
    prior_location_id, prior_sup_org_id, prior_work_model_type,
    prior_base_pay_proposed_amount, prior_idp_employee_status, prior_effective_date,
    -- 28 metric columns
    base_pay_change_count, company_change_count, cost_center_change_count,
    demotion_count, external_hire_count, grade_change_count, grade_decrease_count,
    grade_increase_count, hire_count, internal_hire_count,
    involuntary_termination_count, job_change_count, lateral_move_count,
    location_change_count, management_level_change_count,
    management_level_decrease_count, management_level_increase_count,
    matrix_organization_change_count, promotion_count,
    promotion_count_business_process, regrettable_termination_count,
    rehire_count, structured_termination_count,
    supervisory_organization_change_count, termination_count,
    unstructured_termination_count, voluntary_termination_count,
    worker_model_change_count
)
WITH base AS (
    SELECT
        dwj.employee_id,
        dwj.effective_date::DATE AS effective_date,
        dwj.dim_sk AS worker_job_sk,
        dwj.company_id, dwj.cost_center_id,
        dwj.compensation_grade AS grade_id,
        dwj.job_profile_id,
        dwj.location AS location_id,
        dwj.supervisory_organization AS sup_org_id,
        dwj.work_model_type, dwj.base_pay_proposed_amount,
        dwj.action, dwj.action_reason, dwj.primary_termination_reason, dwj.worker_status,
        CASE WHEN dwj.worker_status = 'Active' THEN 'A'
             WHEN dwj.worker_status = 'Leave' THEN 'L'
             WHEN dwj.worker_status = 'Terminated' THEN 'T'
             ELSE 'U' END AS idp_employee_status,
        -- Prior row via LAG (inline OVER - no WINDOW clause in Redshift Data API)
        LAG(dwj.effective_date::DATE) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_effective_date,
        LAG(dwj.dim_sk) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_worker_job_sk,
        LAG(dwj.company_id) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_company_id,
        LAG(dwj.cost_center_id) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_cost_center_id,
        LAG(dwj.compensation_grade) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_grade_id,
        LAG(dwj.job_profile_id) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_job_profile_id,
        LAG(dwj.location) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_location_id,
        LAG(dwj.supervisory_organization) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_sup_org_id,
        LAG(dwj.work_model_type) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_work_model_type,
        LAG(dwj.base_pay_proposed_amount) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_base_pay_proposed_amount,
        LAG(CASE WHEN dwj.worker_status = 'Active' THEN 'A'
                 WHEN dwj.worker_status = 'Leave' THEN 'L'
                 WHEN dwj.worker_status = 'Terminated' THEN 'T'
                 ELSE 'U' END) OVER (PARTITION BY dwj.employee_id ORDER BY dwj.effective_date::DATE) AS prior_idp_employee_status
    FROM v2_l3_star.dim_worker_job_d dwj
    WHERE dwj.is_current = 'Y'
),
with_fks AS (
    SELECT b.*,
        -- Current FKs (simple NK join - dims are deduplicated)
        dd.dim_sk AS day_sk,
        dc.dim_sk AS company_sk, dcc.dim_sk AS cost_center_sk,
        dg.dim_sk AS grade_profile_sk, djp.dim_sk AS job_profile_sk,
        dl.dim_sk AS location_sk, dso.dim_sk AS supervisory_org_sk,
        dws.dim_sk AS worker_status_sk,
        -- Prior FKs
        pdd.dim_sk AS prior_day_sk,
        pdc.dim_sk AS prior_company_sk, pdcc.dim_sk AS prior_cost_center_sk,
        pdg.dim_sk AS prior_grade_profile_sk, pdjp.dim_sk AS prior_job_profile_sk,
        pdl.dim_sk AS prior_location_sk, pdso.dim_sk AS prior_supervisory_org_sk,
        pdws.dim_sk AS prior_worker_status_sk
    FROM base b
    LEFT JOIN v2_l3_star.dim_day_d dd ON b.effective_date = dd.day_dt
    LEFT JOIN v2_l3_star.dim_company_d dc ON b.company_id = dc.company_id
    LEFT JOIN v2_l3_star.dim_cost_center_d dcc ON b.cost_center_id = dcc.cost_center_id
    LEFT JOIN v2_l3_star.dim_grade_profile_d dg ON b.grade_id = dg.grade_id
    LEFT JOIN v2_l3_star.dim_job_profile_d djp ON b.job_profile_id = djp.job_profile_id
    LEFT JOIN v2_l3_star.dim_location_d dl ON b.location_id = dl.location_id
    LEFT JOIN v2_l3_star.dim_supervisory_org_d dso ON b.sup_org_id = dso.department_id
    LEFT JOIN v2_l3_star.dim_worker_status_d dws ON b.employee_id = dws.employee_id AND b.effective_date = dws.effective_date::DATE
    LEFT JOIN v2_l3_star.dim_day_d pdd ON b.prior_effective_date = pdd.day_dt
    LEFT JOIN v2_l3_star.dim_company_d pdc ON b.prior_company_id = pdc.company_id
    LEFT JOIN v2_l3_star.dim_cost_center_d pdcc ON b.prior_cost_center_id = pdcc.cost_center_id
    LEFT JOIN v2_l3_star.dim_grade_profile_d pdg ON b.prior_grade_id = pdg.grade_id
    LEFT JOIN v2_l3_star.dim_job_profile_d pdjp ON b.prior_job_profile_id = pdjp.job_profile_id
    LEFT JOIN v2_l3_star.dim_location_d pdl ON b.prior_location_id = pdl.location_id
    LEFT JOIN v2_l3_star.dim_supervisory_org_d pdso ON b.prior_sup_org_id = pdso.department_id
    LEFT JOIN v2_l3_star.dim_worker_status_d pdws ON b.employee_id = pdws.employee_id AND b.prior_effective_date = pdws.effective_date::DATE
)
SELECT f.employee_id, f.effective_date,
    f.day_sk, f.company_sk, f.cost_center_sk, f.grade_profile_sk, f.job_profile_sk,
    f.location_sk, f.supervisory_org_sk, f.worker_job_sk, f.worker_status_sk,
    f.company_id, f.cost_center_id, f.grade_id, f.job_profile_id, f.location_id,
    f.sup_org_id, f.work_model_type, f.base_pay_proposed_amount,
    f.idp_employee_status, f.action, f.action_reason, f.primary_termination_reason, f.worker_status,
    f.prior_day_sk, f.prior_company_sk, f.prior_cost_center_sk, f.prior_grade_profile_sk,
    f.prior_job_profile_sk, f.prior_location_sk, f.prior_supervisory_org_sk,
    f.prior_worker_job_sk, f.prior_worker_status_sk,
    f.prior_company_id, f.prior_cost_center_id, f.prior_grade_id, f.prior_job_profile_id,
    f.prior_location_id, f.prior_sup_org_id, f.prior_work_model_type,
    f.prior_base_pay_proposed_amount, f.prior_idp_employee_status, f.prior_effective_date,
    -- base_pay_change_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.base_pay_proposed_amount,'0') <> COALESCE(f.prior_base_pay_proposed_amount,'0') THEN 1 ELSE 0 END,
    -- company_change_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.company_id,'') <> COALESCE(f.prior_company_id,'') THEN 1 ELSE 0 END,
    -- cost_center_change_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.cost_center_id,'') <> COALESCE(f.prior_cost_center_id,'') THEN 1 ELSE 0 END,
    -- demotion_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.job_profile_id,'') <> COALESCE(f.prior_job_profile_id,'')
         AND f.grade_id < f.prior_grade_id THEN 1 ELSE 0 END,
    -- external_hire_count
    CASE WHEN UPPER(COALESCE(f.action_reason,'')) = 'HIRE' THEN 1 ELSE 0 END,
    -- grade_change_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.grade_id,'') <> COALESCE(f.prior_grade_id,'') THEN 1 ELSE 0 END,
    -- grade_decrease_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND f.grade_id < f.prior_grade_id THEN 1 ELSE 0 END,
    -- grade_increase_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND f.grade_id > f.prior_grade_id THEN 1 ELSE 0 END,
    -- hire_count (external + internal)
    CASE WHEN UPPER(COALESCE(f.action_reason,'')) = 'HIRE' THEN 1 ELSE 0 END
      + CASE WHEN UPPER(COALESCE(f.action_reason,'')) = 'CHANGE JOB'
              AND UPPER(COALESCE(f.action,'')) LIKE '%JOB APPLICATION%' THEN 1 ELSE 0 END,
    -- internal_hire_count
    CASE WHEN UPPER(COALESCE(f.action_reason,'')) = 'CHANGE JOB'
         AND UPPER(COALESCE(f.action,'')) LIKE '%JOB APPLICATION%' THEN 1 ELSE 0 END,
    -- involuntary_termination_count (INV- prefix)
    CASE WHEN UPPER(COALESCE(f.primary_termination_reason,'')) LIKE 'INV-%' THEN 1 ELSE 0 END,
    -- job_change_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.job_profile_id,'') <> COALESCE(f.prior_job_profile_id,'') THEN 1 ELSE 0 END,
    -- lateral_move_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.job_profile_id,'') <> COALESCE(f.prior_job_profile_id,'')
         AND COALESCE(f.grade_id,'') = COALESCE(f.prior_grade_id,'') THEN 1 ELSE 0 END,
    -- location_change_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.location_id,'') <> COALESCE(f.prior_location_id,'') THEN 1 ELSE 0 END,
    -- management_level_change_count (placeholder)
    0,
    -- management_level_decrease_count (placeholder)
    0,
    -- management_level_increase_count (placeholder)
    0,
    -- matrix_organization_change_count (placeholder)
    0,
    -- promotion_count (job change + grade increase)
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.job_profile_id,'') <> COALESCE(f.prior_job_profile_id,'')
         AND f.grade_id > f.prior_grade_id THEN 1 ELSE 0 END,
    -- promotion_count_business_process (action_reason based)
    CASE WHEN UPPER(COALESCE(f.action_reason,'')) LIKE '%PROMOTION%' THEN 1 ELSE 0 END,
    -- regrettable_termination_count (voluntary terms)
    CASE WHEN UPPER(COALESCE(f.primary_termination_reason,'')) LIKE 'VOL-%' THEN 1 ELSE 0 END,
    -- rehire_count
    CASE WHEN UPPER(COALESCE(f.action_reason,'')) LIKE '%REHIRE%' THEN 1 ELSE 0 END,
    -- structured_termination_count (placeholder)
    0,
    -- supervisory_organization_change_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.sup_org_id,'') <> COALESCE(f.prior_sup_org_id,'') THEN 1 ELSE 0 END,
    -- termination_count
    CASE WHEN UPPER(COALESCE(f.action,'')) LIKE '%TERMINAT%' THEN 1 ELSE 0 END,
    -- unstructured_termination_count (placeholder)
    0,
    -- voluntary_termination_count (VOL- prefix)
    CASE WHEN UPPER(COALESCE(f.primary_termination_reason,'')) LIKE 'VOL-%' THEN 1 ELSE 0 END,
    -- worker_model_change_count
    CASE WHEN f.idp_employee_status IN ('A','L') AND f.prior_idp_employee_status IN ('A','L')
         AND COALESCE(f.work_model_type,'') <> COALESCE(f.prior_work_model_type,'') THEN 1 ELSE 0 END
FROM with_fks f;


-- ============================================================
-- 2. FACT: fct_worker_headcount_restat_f
-- Monthly snapshot: 24 months, one row per active employee per month
-- ============================================================

TRUNCATE TABLE v2_l3_star.fct_worker_headcount_restat_f;

-- Replace {max_date} with: SELECT MAX(effective_date)::DATE FROM v2_l3_star.dim_worker_job_d
-- Example: '2026-02-28'

INSERT INTO v2_l3_star.fct_worker_headcount_restat_f (
    snapshot_date, employee_id,
    day_sk, company_sk, cost_center_sk, grade_profile_sk, job_profile_sk,
    location_sk, supervisory_org_sk, worker_job_sk, worker_status_sk,
    company_id, cost_center_id, job_profile_id, location_id, sup_org_id,
    idp_employee_status, headcount
)
WITH month_ends AS (
    SELECT LAST_DAY(DATEADD(month, -n, '{max_date}'::DATE)) AS snapshot_dt
    FROM (SELECT 0::INT AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
        UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
        UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
        UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
        UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19
        UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23
    ) m
),
active_workers AS (
    SELECT me.snapshot_dt, dwj.employee_id, dwj.dim_sk AS worker_job_sk,
        dwj.company_id, dwj.cost_center_id, dwj.compensation_grade AS grade_id,
        dwj.job_profile_id, dwj.location AS location_id,
        dwj.supervisory_organization AS sup_org_id, dwj.worker_status,
        CASE WHEN dwj.worker_status = 'Active' THEN 'A'
             WHEN dwj.worker_status = 'Leave' THEN 'L'
             ELSE 'U' END AS idp_employee_status,
        ROW_NUMBER() OVER (PARTITION BY me.snapshot_dt, dwj.employee_id
                           ORDER BY dwj.effective_date::DATE DESC) AS rn
    FROM month_ends me
    JOIN v2_l3_star.dim_worker_job_d dwj
        ON dwj.is_current = 'Y'
        AND dwj.effective_date::DATE <= me.snapshot_dt
        AND dwj.worker_status IN ('Active', 'Leave')
)
SELECT aw.snapshot_dt, aw.employee_id,
    dd.dim_sk, dc.dim_sk, dcc.dim_sk, dg.dim_sk, djp.dim_sk, dl.dim_sk, dso.dim_sk,
    aw.worker_job_sk, dws.dim_sk,
    aw.company_id, aw.cost_center_id, aw.job_profile_id, aw.location_id, aw.sup_org_id,
    aw.idp_employee_status,
    CASE WHEN aw.worker_status = 'Active' THEN 1 ELSE 0 END
FROM active_workers aw
LEFT JOIN v2_l3_star.dim_day_d dd ON aw.snapshot_dt = dd.day_dt
LEFT JOIN v2_l3_star.dim_company_d dc ON aw.company_id = dc.company_id
LEFT JOIN v2_l3_star.dim_cost_center_d dcc ON aw.cost_center_id = dcc.cost_center_id
LEFT JOIN v2_l3_star.dim_grade_profile_d dg ON aw.grade_id = dg.grade_id
LEFT JOIN v2_l3_star.dim_job_profile_d djp ON aw.job_profile_id = djp.job_profile_id
LEFT JOIN v2_l3_star.dim_location_d dl ON aw.location_id = dl.location_id
LEFT JOIN v2_l3_star.dim_supervisory_org_d dso ON aw.sup_org_id = dso.department_id
LEFT JOIN v2_l3_star.dim_worker_status_d dws ON aw.employee_id = dws.employee_id
    AND aw.snapshot_dt = dws.effective_date::DATE
WHERE aw.rn = 1;


-- ============================================================
-- 3. DIMENSION DEDUPLICATION
-- Run after initial dimension load to remove duplicate SCD2 rows
-- (caused by multiple load/fix iterations creating overlapping windows)
-- ============================================================

-- Dedup company (keep latest dim_sk per company_id)
DELETE FROM v2_l3_star.dim_company_d WHERE dim_sk NOT IN (
    SELECT MAX(dim_sk) FROM v2_l3_star.dim_company_d GROUP BY company_id);

-- Dedup cost center
DELETE FROM v2_l3_star.dim_cost_center_d WHERE dim_sk NOT IN (
    SELECT MAX(dim_sk) FROM v2_l3_star.dim_cost_center_d GROUP BY cost_center_id);

-- Dedup grade profile
DELETE FROM v2_l3_star.dim_grade_profile_d WHERE dim_sk NOT IN (
    SELECT MAX(dim_sk) FROM v2_l3_star.dim_grade_profile_d GROUP BY grade_id);

-- Dedup job profile
DELETE FROM v2_l3_star.dim_job_profile_d WHERE dim_sk NOT IN (
    SELECT MAX(dim_sk) FROM v2_l3_star.dim_job_profile_d GROUP BY job_profile_id);

-- Dedup location
DELETE FROM v2_l3_star.dim_location_d WHERE dim_sk NOT IN (
    SELECT MAX(dim_sk) FROM v2_l3_star.dim_location_d GROUP BY location_id);

-- Dedup matrix org
DELETE FROM v2_l3_star.dim_matrix_org_d WHERE dim_sk NOT IN (
    SELECT MAX(dim_sk) FROM v2_l3_star.dim_matrix_org_d GROUP BY matrix_organization_id);

-- Dedup supervisory org
DELETE FROM v2_l3_star.dim_supervisory_org_d WHERE dim_sk NOT IN (
    SELECT MAX(dim_sk) FROM v2_l3_star.dim_supervisory_org_d GROUP BY department_id);

-- Dedup supervisory org layers
DELETE FROM v2_l3_star.dim_supervisory_org_layers_d WHERE dim_sk NOT IN (
    SELECT MAX(dim_sk) FROM v2_l3_star.dim_supervisory_org_layers_d GROUP BY department_id, parent_dept_id);
