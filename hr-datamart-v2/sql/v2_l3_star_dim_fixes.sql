-- ============================================================================
-- HR Datamart V2 - L3 Star Schema Dimension FIXES
-- Applied after initial load to resolve data issues
-- ============================================================================

-- ============================================================================
-- FIX 1: Populate owner_ein in L1 int6028_department_hierarchy
-- The generator left owner_ein empty; backfill from worker_job managers
-- ============================================================================

UPDATE v2_l1_workday.int6028_department_hierarchy
SET owner_ein = sub.mgr_id
FROM (
  SELECT supervisory_organization AS sup_org, manager_id AS mgr_id,
    ROW_NUMBER() OVER (PARTITION BY supervisory_organization ORDER BY effective_date DESC) AS rn
  FROM v2_l1_workday.int0095e_worker_job
  WHERE manager_id IS NOT NULL AND TRIM(manager_id) <> ''
    AND worker_status = 'Active'
) sub
WHERE sub.sup_org = v2_l1_workday.int6028_department_hierarchy.department_id
  AND sub.rn = 1;

-- Update dept_name_with_manager_name
UPDATE v2_l1_workday.int6028_department_hierarchy
SET dept_name_with_manager_name = department_name || ' (' || wp.preferred_full_name || ')'
FROM v2_l1_workday.int6031_worker_profile wp
WHERE wp.worker_id = v2_l1_workday.int6028_department_hierarchy.owner_ein
  AND v2_l1_workday.int6028_department_hierarchy.owner_ein IS NOT NULL
  AND TRIM(v2_l1_workday.int6028_department_hierarchy.owner_ein) <> '';

-- ============================================================
-- FIX 2: Update dim_supervisory_org_d owner_ein
-- ============================================================

UPDATE v2_l3_star.dim_supervisory_org_d
SET owner_ein = h.owner_ein
FROM v2_l1_workday.int6028_department_hierarchy h
WHERE h.department_id = v2_l3_star.dim_supervisory_org_d.department_id;

-- ============================================================
-- FIX 3: Populate sup_org level hierarchy columns
-- Build a permanent staging table (TEMP not supported across Data API calls)
-- ============================================================

DROP TABLE IF EXISTS v2_l3_star.tmp_hier_lookup;

CREATE TABLE v2_l3_star.tmp_hier_lookup AS
SELECT
  h.department_id AS org_id,
  h.department_level::INT AS org_level,
  -- Level 1 ancestor (walk to root)
  CASE
    WHEN h.department_level::INT = 1 THEN h.department_id
    WHEN h.department_level::INT = 2 THEN h.parent_dept_id
    WHEN h.department_level::INT = 3 THEN p1.parent_dept_id
    WHEN h.department_level::INT = 4 THEN p2.parent_dept_id
  END AS l1_id,
  -- Level 2
  CASE WHEN h.department_level::INT >= 2 THEN
    CASE
      WHEN h.department_level::INT = 2 THEN h.department_id
      WHEN h.department_level::INT = 3 THEN h.parent_dept_id
      WHEN h.department_level::INT = 4 THEN p1.parent_dept_id
    END
  END AS l2_id,
  -- Level 3
  CASE WHEN h.department_level::INT >= 3 THEN
    CASE
      WHEN h.department_level::INT = 3 THEN h.department_id
      WHEN h.department_level::INT = 4 THEN h.parent_dept_id
    END
  END AS l3_id,
  -- Level 4
  CASE WHEN h.department_level::INT = 4 THEN h.department_id END AS l4_id
FROM v2_l1_workday.int6028_department_hierarchy h
LEFT JOIN v2_l1_workday.int6028_department_hierarchy p1
  ON h.parent_dept_id = p1.department_id
LEFT JOIN v2_l1_workday.int6028_department_hierarchy p2
  ON p1.parent_dept_id = p2.department_id;

-- Update levels 1-4
UPDATE v2_l3_star.dim_supervisory_org_d d
SET sup_org_level_1_id = t.l1_id,
    sup_org_level_1_name = ref.department_name,
    sup_org_level_1_manager_id = ref.owner_ein,
    sup_org_level_1_wid = ref.department_wid
FROM v2_l3_star.tmp_hier_lookup t
JOIN v2_l1_workday.int6028_department_hierarchy ref ON ref.department_id = t.l1_id
WHERE d.department_id = t.org_id AND t.l1_id IS NOT NULL;

UPDATE v2_l3_star.dim_supervisory_org_d d
SET sup_org_level_2_id = t.l2_id,
    sup_org_level_2_name = ref.department_name,
    sup_org_level_2_manager_id = ref.owner_ein,
    sup_org_level_2_wid = ref.department_wid
FROM v2_l3_star.tmp_hier_lookup t
JOIN v2_l1_workday.int6028_department_hierarchy ref ON ref.department_id = t.l2_id
WHERE d.department_id = t.org_id AND t.l2_id IS NOT NULL;

UPDATE v2_l3_star.dim_supervisory_org_d d
SET sup_org_level_3_id = t.l3_id,
    sup_org_level_3_name = ref.department_name,
    sup_org_level_3_manager_id = ref.owner_ein,
    sup_org_level_3_wid = ref.department_wid
FROM v2_l3_star.tmp_hier_lookup t
JOIN v2_l1_workday.int6028_department_hierarchy ref ON ref.department_id = t.l3_id
WHERE d.department_id = t.org_id AND t.l3_id IS NOT NULL;

UPDATE v2_l3_star.dim_supervisory_org_d d
SET sup_org_level_4_id = t.l4_id,
    sup_org_level_4_name = ref.department_name,
    sup_org_level_4_manager_id = ref.owner_ein,
    sup_org_level_4_wid = ref.department_wid
FROM v2_l3_star.tmp_hier_lookup t
JOIN v2_l1_workday.int6028_department_hierarchy ref ON ref.department_id = t.l4_id
WHERE d.department_id = t.org_id AND t.l4_id IS NOT NULL;

DROP TABLE v2_l3_star.tmp_hier_lookup;

-- ============================================================
-- FIX 4: Reload dim_report_to_d (employee-centric manager chain)
-- Uses worker_profile.worker_id = worker_job.employee_id
-- ============================================================

TRUNCATE TABLE v2_l3_star.dim_report_to_d;

INSERT INTO v2_l3_star.dim_report_to_d (
  employee_id, manager_worker_id, manager_preferred_name,
  level_1_manager_id, level_1_manager_preferred_name,
  level_2_manager_id, level_2_manager_preferred_name,
  level_3_manager_id, level_3_manager_preferred_name,
  level_4_manager_id, level_4_manager_preferred_name,
  level_5_manager_id, level_5_manager_preferred_name,
  md5_hash, valid_from, valid_to, is_current
)
SELECT
  e.employee_id,
  e.manager_id,
  wp1.preferred_full_name,
  e.manager_id, wp1.preferred_full_name,
  m1.manager_id, wp2.preferred_full_name,
  m2.manager_id, wp3.preferred_full_name,
  m3.manager_id, wp4.preferred_full_name,
  m4.manager_id, wp5.preferred_full_name,
  MD5(
    COALESCE(e.employee_id,'') || '|' ||
    COALESCE(e.manager_id,'') || '|' ||
    COALESCE(m1.manager_id,'') || '|' ||
    COALESCE(m2.manager_id,'') || '|' ||
    COALESCE(m3.manager_id,'') || '|' ||
    COALESCE(m4.manager_id,'')
  ),
  CURRENT_DATE, '9999-12-31', 1
FROM (
  SELECT DISTINCT employee_id, manager_id
  FROM v2_l3_workday.l3_workday_worker_job_dly
  WHERE idp_obsolete_date IS NULL
    AND transaction_entry_date = idp_max_entry_ts
    AND sequence_number = idp_min_seq_num
) e
LEFT JOIN (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) m1 ON e.manager_id = m1.employee_id
LEFT JOIN (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) m2 ON m1.manager_id = m2.employee_id
LEFT JOIN (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) m3 ON m2.manager_id = m3.employee_id
LEFT JOIN (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) m4 ON m3.manager_id = m4.employee_id
LEFT JOIN v2_l1_workday.int6031_worker_profile wp1 ON wp1.worker_id = e.manager_id
LEFT JOIN v2_l1_workday.int6031_worker_profile wp2 ON wp2.worker_id = m1.manager_id
LEFT JOIN v2_l1_workday.int6031_worker_profile wp3 ON wp3.worker_id = m2.manager_id
LEFT JOIN v2_l1_workday.int6031_worker_profile wp4 ON wp4.worker_id = m3.manager_id
LEFT JOIN v2_l1_workday.int6031_worker_profile wp5 ON wp5.worker_id = m4.manager_id;

-- ============================================================
-- FIX 5: Reload dim_report_to_layers_d (normalized parent-child)
-- ============================================================

TRUNCATE TABLE v2_l3_star.dim_report_to_layers_d;

-- Level 1: Direct reports
INSERT INTO v2_l3_star.dim_report_to_layers_d (
  employee_id, parent_employee_id, employee_name, parent_employee_name,
  is_bottom, is_direct_report, is_indirect_report, is_top, levels_from_parent,
  md5_hash, valid_from, valid_to, is_current
)
SELECT
  e.employee_id, e.manager_id,
  ep.preferred_full_name, mp.preferred_full_name,
  0, 1, 0, 0, 1,
  MD5(COALESCE(e.employee_id,'') || '|' || COALESCE(e.manager_id,'') || '|1'),
  CURRENT_DATE, '9999-12-31', 1
FROM (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) e
LEFT JOIN v2_l1_workday.int6031_worker_profile ep ON ep.worker_id = e.employee_id
LEFT JOIN v2_l1_workday.int6031_worker_profile mp ON mp.worker_id = e.manager_id
WHERE e.manager_id IS NOT NULL AND TRIM(e.manager_id) <> '';

-- Level 2: Indirect reports (skip-level)
INSERT INTO v2_l3_star.dim_report_to_layers_d (
  employee_id, parent_employee_id, employee_name, parent_employee_name,
  is_bottom, is_direct_report, is_indirect_report, is_top, levels_from_parent,
  md5_hash, valid_from, valid_to, is_current
)
SELECT
  e.employee_id, m1.manager_id,
  ep.preferred_full_name, gp.preferred_full_name,
  0, 0, 1, 0, 2,
  MD5(COALESCE(e.employee_id,'') || '|' || COALESCE(m1.manager_id,'') || '|2'),
  CURRENT_DATE, '9999-12-31', 1
FROM (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) e
JOIN (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) m1 ON e.manager_id = m1.employee_id
LEFT JOIN v2_l1_workday.int6031_worker_profile ep ON ep.worker_id = e.employee_id
LEFT JOIN v2_l1_workday.int6031_worker_profile gp ON gp.worker_id = m1.manager_id
WHERE m1.manager_id IS NOT NULL AND TRIM(m1.manager_id) <> '';

-- Level 3: Indirect reports (2 skip-levels)
INSERT INTO v2_l3_star.dim_report_to_layers_d (
  employee_id, parent_employee_id, employee_name, parent_employee_name,
  is_bottom, is_direct_report, is_indirect_report, is_top, levels_from_parent,
  md5_hash, valid_from, valid_to, is_current
)
SELECT
  e.employee_id, m2.manager_id,
  ep.preferred_full_name, gp.preferred_full_name,
  0, 0, 1, 0, 3,
  MD5(COALESCE(e.employee_id,'') || '|' || COALESCE(m2.manager_id,'') || '|3'),
  CURRENT_DATE, '9999-12-31', 1
FROM (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) e
JOIN (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) m1 ON e.manager_id = m1.employee_id
JOIN (SELECT DISTINCT employee_id, manager_id FROM v2_l3_workday.l3_workday_worker_job_dly WHERE idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts AND sequence_number = idp_min_seq_num) m2 ON m1.manager_id = m2.employee_id
LEFT JOIN v2_l1_workday.int6031_worker_profile ep ON ep.worker_id = e.employee_id
LEFT JOIN v2_l1_workday.int6031_worker_profile gp ON gp.worker_id = m2.manager_id
WHERE m2.manager_id IS NOT NULL AND TRIM(m2.manager_id) <> '';

-- ============================================================
-- FIX 6: Reload dim_worker_status_d with IDP filter
-- Original load was missing IDP filter, resulting in 585k instead of 117k rows
-- ============================================================

TRUNCATE TABLE v2_l3_star.dim_worker_status_d;

INSERT INTO v2_l3_star.dim_worker_status_d (
  employee_id, effective_date, active_status_date, benefits_service_date,
  continuous_service_date, eligible_for_rehire, hire_reason, hire_rescinded,
  original_hire_date, primary_termination_category, retired,
  retirement_eligibility_date, seniority_date, termination_date,
  md5_hash, effective_date_from, effective_date_to, valid_from, valid_to, is_current
)
SELECT
  j.employee_id,
  j.transaction_effective_date,
  j.active_status_date,
  j.benefits_service_date,
  j.continuous_service_date,
  j.eligible_for_rehire,
  j.hire_reason,
  j.hire_rescinded,
  j.original_hire_date,
  j.primary_termination_category,
  j.retired,
  j.retirement_eligibility_date,
  j.seniority_date,
  j.termination_date,
  MD5(
    COALESCE(j.employee_id,'') || '|' ||
    COALESCE(j.active_status_date,'') || '|' ||
    COALESCE(j.benefits_service_date,'') || '|' ||
    COALESCE(j.continuous_service_date,'') || '|' ||
    COALESCE(j.eligible_for_rehire,'') || '|' ||
    COALESCE(j.hire_reason,'') || '|' ||
    COALESCE(j.primary_termination_category,'') || '|' ||
    COALESCE(j.retired,'') || '|' ||
    COALESCE(j.termination_date,'')
  ),
  j.transaction_effective_date,
  j.idp_calc_end_date,
  CURRENT_DATE,
  '9999-12-31',
  1
FROM v2_l3_workday.l3_workday_worker_job_dly j
WHERE j.idp_obsolete_date IS NULL
  AND j.transaction_entry_date = j.idp_max_entry_ts
  AND j.sequence_number = j.idp_min_seq_num;
