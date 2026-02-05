-- ============================================================================
-- L3 SOURCE DAILY TABLES - DML/LOAD PROCEDURES
-- HR Datamart - Workday Integration Layer
-- ============================================================================
-- Purpose: Load L3 source tables from L1 layer and INT270 rescinded tracking
-- Target Schema: l3_workday
-- Source Schema: l1_workday
-- ============================================================================

-- ============================================================================
-- LOAD PROCEDURE 1: l3_workday_worker_job_dly
-- Description: Full refresh load of worker job daily snapshot
-- Source: l1_workday.int0095e_worker_job + l1_workday.int270_rescinded
-- Logic:
--   1. Join INT270 rescinded records for idp_obsolete_date
--   2. Calculate idp_max_entry_ts as MAX transaction_entry_date per (employee_id, transaction_effective_date) excluding obsolete
--   3. Calculate idp_min_seq_num as MIN sequence_number at max timestamp, excluding obsolete
--   4. Calculate idp_calc_end_date as LEAD(effective_date)-1 partitioned by employee_id, defaulting to 9999-12-31
--   5. Compute idp_employee_status via CASE logic based on worker_status and termination flags
-- ============================================================================

BEGIN;

TRUNCATE TABLE l3_workday.l3_workday_worker_job_dly;

INSERT INTO l3_workday.l3_workday_worker_job_dly
WITH rescinded AS (
    -- Extract rescinded records for INT095E (worker job table code)
    SELECT workday_id, rescinded_moment
    FROM l1_workday.int270_rescinded
    WHERE idp_table = 'INT095E'
),
base AS (
    -- Join source data with rescinded tracking
    SELECT wj.*,
           r.rescinded_moment AS idp_obsolete_date
    FROM l1_workday.int0095e_worker_job wj
    LEFT JOIN rescinded r ON wj.transaction_wid = r.workday_id
),
with_max_ts AS (
    -- Calculate max transaction_entry_date per employee_id and transaction_effective_date
    -- Only consider non-obsolete rows (where idp_obsolete_date IS NULL)
    SELECT b.*,
           MAX(CASE WHEN idp_obsolete_date IS NULL THEN transaction_entry_date END)
               OVER (PARTITION BY employee_id, transaction_effective_date) AS idp_max_entry_ts
    FROM base b
),
with_min_seq AS (
    -- Calculate min sequence_number at the max transaction_entry_date
    -- Only for non-obsolete rows where transaction_entry_date equals idp_max_entry_ts
    SELECT wmt.*,
           MIN(CASE WHEN idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts THEN sequence_number END)
               OVER (PARTITION BY employee_id, transaction_effective_date) AS idp_min_seq_num
    FROM with_max_ts wmt
),
with_calc_end AS (
    -- Calculate end date as LEAD(effective_date)-1, defaulting to 9999-12-31
    SELECT wms.*,
           COALESCE(
               LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date) - INTERVAL '1 day',
               '9999-12-31'::DATE
           )::DATE AS idp_calc_end_date
    FROM with_min_seq wms
)
SELECT
    -- All source columns from int0095e_worker_job
    employee_id,
    transaction_wid,
    transaction_effective_date,
    transaction_entry_date,
    transaction_type,
    position_id,
    effective_date,
    worker_type,
    worker_sub_type,
    business_title,
    business_site_id,
    mailstop_floor,
    worker_status,
    active,
    active_status_date,
    hire_date,
    original_hire_date,
    hire_reason,
    employment_end_date,
    continuous_service_date,
    first_day_of_work,
    expected_retirement_date,
    retirement_eligibility_date,
    retired,
    seniority_date,
    severance_date,
    benefits_service_date,
    company_service_date,
    time_off_service_date,
    vesting_date,
    terminated,
    termination_date,
    pay_through_date,
    primary_termination_reason,
    primary_termination_category,
    termination_involuntary,
    secondary_termination_reason,
    local_termination_reason,
    not_eligible_for_hire,
    regrettable_termination,
    hire_rescinded,
    resignation_date,
    last_day_of_work,
    last_date_for_which_paid,
    expected_date_of_return,
    not_returning,
    return_unknown,
    probation_start_date,
    probation_end_date,
    academic_tenure_date,
    has_international_assignment,
    home_country,
    host_country,
    international_assignment_type,
    start_date_of_international_assignment,
    end_date_of_international_assignment,
    rehire,
    eligible_for_rehire,
    action,
    action_code,
    action_reason,
    action_reason_code,
    manager_id,
    soft_retirement_indicator,
    job_profile_id,
    sequence_number,
    planned_end_contract_date,
    job_entry_dt,
    stock_grants,
    time_type,
    supervisory_organization,
    location,
    job_title,
    french_job_title,
    shift_number,
    scheduled_weekly_hours,
    default_weekly_hours,
    scheduled_fte,
    work_model_start_date,
    work_model_type,
    worker_workday_id,
    -- IDP Columns
    idp_calc_end_date,
    idp_obsolete_date,
    idp_max_entry_ts,
    idp_min_seq_num,
    -- IDP_EMPLOYEE_STATUS: Derives employee status from multiple flags
    CASE
        WHEN worker_status = 'Active' THEN 'A'
        WHEN worker_status = 'On Leave' THEN 'L'
        WHEN worker_status = 'Terminated' THEN
            CASE
                WHEN CURRENT_DATE <= pay_through_date AND termination_date < pay_through_date THEN 'U'
                WHEN retired = true THEN 'R'
                WHEN primary_termination_reason = 'TER-DEA' THEN 'D'
                ELSE 'T'
            END
        ELSE ''
    END AS idp_employee_status,
    -- Audit Columns
    GETDATE() AS insert_datetime,
    GETDATE() AS update_datetime,
    '${ETL_BATCH_ID}' AS etl_batch_id
FROM with_calc_end;

COMMIT;

-- ============================================================================
-- LOAD PROCEDURE 2: l3_workday_worker_organization_dly
-- Description: Full refresh load of worker organization daily snapshot
-- Source: l1_workday.int0096_worker_organization + l1_workday.int270_rescinded
-- Logic:
--   1. Join INT270 rescinded records for idp_obsolete_date
--   2. Calculate idp_max_entry_ts as MAX transaction_entry_date per (employee_id, transaction_effective_date) excluding obsolete
--   3. Calculate idp_min_seq_num as MIN sequence_number at max timestamp, excluding obsolete
--   4. Calculate idp_calc_end_date as LEAD(transaction_effective_date)-1 partitioned by (employee_id, organization_type), defaulting to 9999-12-31
--   5. Set idp_employee_status to NULL (not applicable for this table as no worker_status field)
-- ============================================================================

BEGIN;

TRUNCATE TABLE l3_workday.l3_workday_worker_organization_dly;

INSERT INTO l3_workday.l3_workday_worker_organization_dly
WITH rescinded AS (
    -- Extract rescinded records for INT096 (worker organization table code)
    SELECT workday_id, rescinded_moment
    FROM l1_workday.int270_rescinded
    WHERE idp_table = 'INT096'
),
base AS (
    -- Join source data with rescinded tracking
    SELECT wo.*,
           r.rescinded_moment AS idp_obsolete_date
    FROM l1_workday.int0096_worker_organization wo
    LEFT JOIN rescinded r ON wo.transaction_wid = r.workday_id
),
with_max_ts AS (
    -- Calculate max transaction_entry_date per employee_id and transaction_effective_date
    -- Only consider non-obsolete rows (where idp_obsolete_date IS NULL)
    SELECT b.*,
           MAX(CASE WHEN idp_obsolete_date IS NULL THEN transaction_entry_date END)
               OVER (PARTITION BY employee_id, transaction_effective_date) AS idp_max_entry_ts
    FROM base b
),
with_min_seq AS (
    -- Calculate min sequence_number at the max transaction_entry_date
    -- Only for non-obsolete rows where transaction_entry_date equals idp_max_entry_ts
    SELECT wmt.*,
           MIN(CASE WHEN idp_obsolete_date IS NULL AND transaction_entry_date = idp_max_entry_ts THEN sequence_number END)
               OVER (PARTITION BY employee_id, transaction_effective_date) AS idp_min_seq_num
    FROM with_max_ts wmt
),
with_calc_end AS (
    -- Calculate end date as LEAD(transaction_effective_date)-1, defaulting to 9999-12-31
    -- Partitioned by employee_id and organization_type to handle multiple orgs per employee
    SELECT wms.*,
           COALESCE(
               LEAD(transaction_effective_date) OVER (PARTITION BY employee_id, organization_type ORDER BY transaction_effective_date) - INTERVAL '1 day',
               '9999-12-31'::DATE
           )::DATE AS idp_calc_end_date
    FROM with_min_seq wms
)
SELECT
    -- All source columns from int0096_worker_organization
    employee_id,
    transaction_wid,
    transaction_effective_date,
    transaction_entry_date,
    transaction_type,
    organization_id,
    organization_type,
    sequence_number,
    worker_workday_id,
    -- IDP Columns
    idp_calc_end_date,
    idp_obsolete_date,
    idp_max_entry_ts,
    idp_min_seq_num,
    -- IDP_EMPLOYEE_STATUS: Set to NULL for organization table (no status field in source)
    NULL::VARCHAR(5) AS idp_employee_status,
    -- Audit Columns
    GETDATE() AS insert_datetime,
    GETDATE() AS update_datetime,
    '${ETL_BATCH_ID}' AS etl_batch_id
FROM with_calc_end;

COMMIT;

-- ============================================================================
-- LOAD PROCEDURE 3: l3_workday_worker_comp_dly
-- Description: Full refresh load of worker compensation daily snapshot
-- Source: l1_workday.int0098_worker_compensation + l1_workday.int270_rescinded
-- Logic:
--   1. Join INT270 rescinded records for idp_obsolete_date ONLY
--   2. NO other IDP columns calculated (no idp_max_entry_ts, idp_min_seq_num, idp_calc_end_date, idp_employee_status)
-- ============================================================================

BEGIN;

TRUNCATE TABLE l3_workday.l3_workday_worker_comp_dly;

INSERT INTO l3_workday.l3_workday_worker_comp_dly
WITH rescinded AS (
    -- Extract rescinded records for INT098 (worker compensation table code)
    SELECT workday_id, rescinded_moment
    FROM l1_workday.int270_rescinded
    WHERE idp_table = 'INT098'
)
SELECT
    -- All source columns from int0098_worker_compensation
    wc.employee_id,
    wc.transaction_wid,
    wc.transaction_effective_date,
    wc.transaction_entry_moment,
    wc.transaction_type,
    wc.compensation_package_proposed,
    wc.compensation_grade_proposed,
    wc.comp_grade_profile_proposed,
    wc.compensation_step_proposed,
    wc.pay_range_minimum,
    wc.pay_range_midpoint,
    wc.pay_range_maximum,
    wc.base_pay_proposed_amount,
    wc.base_pay_proposed_currency,
    wc.base_pay_proposed_frequency,
    wc.benefits_annual_rate_abbr,
    wc.pay_rate_type,
    wc.compensation,
    wc.worker_workday_id,
    -- IDP Columns (ONLY idp_obsolete_date for this table)
    r.rescinded_moment AS idp_obsolete_date,
    -- Audit Columns
    GETDATE() AS insert_datetime,
    GETDATE() AS update_datetime,
    '${ETL_BATCH_ID}' AS etl_batch_id
FROM l1_workday.int0098_worker_compensation wc
LEFT JOIN rescinded r ON wc.transaction_wid = r.workday_id;

COMMIT;

-- ============================================================================
-- END OF DML
-- ============================================================================
