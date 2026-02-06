# HR Datamart Deployment — Lessons Learned

**Date:** February 5, 2026
**Environment:** DEV (Amazon Redshift, us-east-1)
**Outcome:** Fully deployed after resolving 15 distinct issues across SQL artifacts, Lambda, and dashboard

---

## Summary of All Fixes

During deployment of the HR Datamart pipeline to Amazon Redshift, 11 issues were discovered and resolved. These fell into four categories: Redshift SQL dialect incompatibilities, column/table name mismatches, schema reference errors, and DDL type mismatches. Every issue was resolved and the pipeline now runs end-to-end successfully.

---

## Fix #1 — CONCAT() Only Supports 2 Arguments on Redshift

**Files affected:** `l3_dim_load.sql`, `l3_fact_load.sql`
**Error:** `function concat() must have at most 2 arguments`

**Root cause:** The original SQL used `CONCAT(a, b, c, ...)` with 3+ arguments for hash_diff calculations. Redshift's `CONCAT()` function only accepts exactly 2 arguments, unlike SQL Server or PostgreSQL.

**Fix:** Replace all multi-argument `CONCAT()` calls with the `||` pipe operator.

```sql
-- BEFORE (broken on Redshift)
MD5(CONCAT(
    COALESCE(company_wid, ''),
    COALESCE(company_code, ''),
    COALESCE(company_name, '')
))

-- AFTER (Redshift-compatible)
MD5(COALESCE(company_wid::VARCHAR, '') || COALESCE(company_code::VARCHAR, '') || COALESCE(company_name::VARCHAR, ''))
```

**Lesson:** Always use `||` for string concatenation on Redshift. Reserve `CONCAT()` for only 2-argument cases, or avoid it entirely.

---

## Fix #2 — BOOLEAN Columns Cannot Receive VARCHAR Values

**Files affected:** `l3_dim_ddl.sql`, `l3_dim_load.sql`
**Error:** `column "active" is of type boolean but expression is of type character varying`

**Root cause:** The DML casts boolean source columns to VARCHAR using `::INT::VARCHAR` (e.g., `active::INT::VARCHAR`), but the DDL still declared those columns as `BOOLEAN`. Redshift enforces strict type checking and won't implicitly cast VARCHAR to BOOLEAN.

**Fix:** Changed 8 source-data boolean columns from `BOOLEAN` to `VARCHAR(256)` in the DDL:
- `dim_worker_job_d`: active, not_returning, has_international_assignment, soft_retirement_indicator
- `dim_worker_status_d`: not_eligible_for_hire, active, hire_rescinded, retired
- `dim_job_profile_d`: inactive_flag, public_job, work_shift_required, is_people_manager, is_manager, critical_job_flag, difficult_to_fill_flag, customer_facing, bonus_eligibility
- `dim_department_d`: active
- `dim_location_d`: inactive

**Important:** 14 columns were kept as `BOOLEAN` because they receive literal `TRUE`/`FALSE` values from SQL expressions (not source data):
- 4 date spine columns: is_weekend, is_month_end, is_quarter_end, is_year_end
- 9 is_current columns (one per dimension table)
- 1 is_current_job_row (dim_worker_job_d)

**Lesson:** When source data booleans are cast to VARCHAR in DML, the target DDL column type must match. Use VARCHAR(256) for any column that receives `::INT::VARCHAR` cast values. Keep BOOLEAN only for columns populated by SQL CASE expressions that produce literal TRUE/FALSE.

---

## Fix #3 — Boolean Source Columns Need ::INT::VARCHAR Cast

**Files affected:** `l3_dim_load.sql`
**Error:** `cannot cast type boolean to character varying`

**Root cause:** Redshift cannot directly cast BOOLEAN to VARCHAR. The intermediate cast to INTEGER is required: `column::INT::VARCHAR` converts `true` → `1` → `'1'` and `false` → `0` → `'0'`.

**Fix:** Added `::INT::VARCHAR` cast to every boolean source column reference in the DML, both in SELECT lists and in hash_diff MD5 calculations.

```sql
-- BEFORE (broken on Redshift)
active,
MD5(COALESCE(active::VARCHAR, ''))

-- AFTER (Redshift-compatible)
active::INT::VARCHAR,
MD5(COALESCE(active::INT::VARCHAR, ''))
```

**Lesson:** Redshift does not support `BOOLEAN::VARCHAR`. Always use the two-step cast: `BOOLEAN::INT::VARCHAR`.

---

## Fix #4 — transaction_entry_date vs transaction_entry_moment

**Files affected:** `l3_dim_load.sql` (Worker Compensation section)
**Error:** `column "transaction_entry_date" does not exist`

**Root cause:** The Worker Compensation view (`l3_workday_worker_comp_dly_vw`) uses `transaction_entry_moment` as its timestamp column, while the Worker Job and Worker Organization views use `transaction_entry_date`. The dim load SQL incorrectly used `transaction_entry_date` when querying the comp view.

**Fix:** Changed the comp view's `ORDER BY` clause:
```sql
-- BEFORE
ROW_NUMBER() OVER (... ORDER BY transaction_entry_date DESC) AS rn

-- AFTER (only for comp view)
ROW_NUMBER() OVER (... ORDER BY transaction_entry_moment DESC) AS rn
```

**Lesson:** Different Workday HRDP views use different column names for the entry timestamp. Always verify column names against each view's actual DDL/definition. The naming inconsistency (`transaction_entry_date` vs `transaction_entry_moment`) is a Workday design choice, not a bug.

---

## Fix #5 — Schema Reference Errors (l1 vs l3 Views)

**Files affected:** `l3_dim_load.sql`
**Error:** `relation "l1_workday.l1_workday_worker_job_dly_vw" does not exist`

**Root cause:** The dim load SQL referenced views in the `l1_workday` schema, but the views are actually created in the `l3_workday` schema with `l3_` prefixed names. The L3 Source DDL creates these views as part of the source layer.

**Fix:** Changed all view references from L1 to L3 schema:
```sql
-- BEFORE
FROM l1_workday.l1_workday_worker_job_dly_vw
FROM l1_workday.l1_workday_worker_comp_dly_vw
FROM l1_workday.l1_workday_worker_organization_dly_vw

-- AFTER
FROM l3_workday.l3_workday_worker_job_dly_vw
FROM l3_workday.l3_workday_worker_comp_dly_vw
FROM l3_workday.l3_workday_worker_organization_dly_vw
```

**Lesson:** Views belong to the schema where they were created (L3 Source), not the schema of their underlying tables (L1). Always verify object schemas against the DDL that creates them.

---

## Fix #6 — L1 Table Names Don't Match DDL

**Files affected:** `l3_dim_load.sql`
**Error:** `relation "l1_workday.int6028_supervisory_organization" does not exist`

**Root cause:** The dim load referenced L1 table names that didn't match the actual table names created by the L1 DDL:
- `int6028_supervisory_organization` → actual name is `int6028_department_hierarchy`
- `int6032_position` → actual name is `int6032_positions` (with 's')

**Fix:** Updated table name references to match L1 DDL.

**Lesson:** Table naming in the L1 DDL uses descriptive names that may differ from the Workday integration IDs. Always cross-reference table names against the actual DDL.

---

## Fix #7 — management_level_code and job_matrix Missing from dim_worker_job_d

**Files affected:** `l3_fact_load.sql`
**Error:** `column dwj.management_level_code does not exist`

**Root cause:** The fact load's `source_data` CTE referenced `dwj.management_level_code` and `dwj.job_matrix` from `dim_worker_job_d`, but these columns exist on `dim_job_profile_d` instead. The dim_worker_job_d table has a `job_profile_id` FK that links to dim_job_profile_d where these columns live.

**Fix:** Added a LEFT JOIN to `dim_job_profile_d` in the source_data CTE:
```sql
-- BEFORE
FROM l3_workday.dim_worker_job_d dwj
WHERE dwj.is_current = true

-- AFTER
FROM l3_workday.dim_worker_job_d dwj
LEFT JOIN l3_workday.dim_job_profile_d djp
    ON dwj.job_profile_id = djp.job_profile_id
    AND djp.is_current = true
WHERE dwj.is_current = true
```

Also changed 4 column references from `dwj.` to `djp.`:
- `dwj.management_level_code` → `djp.management_level_code` (SELECT + LAG)
- `dwj.job_matrix` → `djp.job_matrix` (SELECT + LAG)

**Lesson:** When building fact tables, verify which dimension each attribute belongs to. Job-profile-level attributes (management_level_code, job_matrix) live on dim_job_profile_d, not dim_worker_job_d, even though the worker's job record references them.

---

## Fix #8 — QA Tests Rewritten for Redshift Compatibility

**Files affected:** `qa_tests.sql`
**Error:** Multiple Redshift syntax errors in original 60+ test suite

**Root cause:** The original QA test suite used SQL patterns not supported by Redshift:
- VALUES in subqueries (`SELECT * FROM (VALUES ...)`)
- Procedural SQL blocks
- Complex string formatting functions

**Fix:** Rewrote the entire QA test suite as 43 standalone `INSERT...SELECT` statements, each independently executable via the Redshift Data API. Reduced from 60+ to 38 stored test results covering the essential categories: row counts (L1/L3/dim/fact), PK uniqueness, SCD2 validation, and data quality.

**Lesson:** Redshift does not support `VALUES` in subqueries, procedural SQL outside stored procedures, or many PostgreSQL-specific extensions. Write all QA tests as simple INSERT...SELECT statements that can execute independently.

---

## Fix #9 — Completion Report Rewritten for Redshift Compatibility

**Files affected:** `completion_report.sql`
**Error:** Same Redshift syntax errors as QA tests

**Root cause:** Original completion report used unsupported SQL patterns.

**Fix:** Rewrote as 8 standalone SELECT statements using UNION ALL for aggregation within each section.

---

## Fix #10 — L1 COPY Statements Need Redshift-Specific Options

**Files affected:** `l1_copy_statements.sql`
**Error:** Various COPY command failures

**Root cause:** Original COPY statements used generic syntax. Redshift requires specific options for pipe-delimited CSVs with headers.

**Fix:** Rewrote COPY statements with Redshift-specific options:
```sql
COPY l1_workday.table_name
FROM 's3://bucket/path/file.csv'
IAM_ROLE 'arn:aws:iam::...:role/RedshiftS3ReadRole'
DELIMITER '|'
IGNOREHEADER 1
ACCEPTINVCHARS
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
REGION 'us-east-1';
```

**Lesson:** Always use `IGNOREHEADER 1` for CSVs with headers, `ACCEPTINVCHARS` for data with unexpected characters, and explicit `DATEFORMAT`/`TIMEFORMAT` settings.

---

## Fix #11 — Deploy Script Needs Block Mode for SCD2 Transactions

**Files affected:** `deploy.sh`
**Error:** Temp tables not visible across API calls

**Root cause:** The Redshift Data API executes each `execute-statement` call in its own session. SCD2 merge logic uses temp tables that must persist across multiple statements within a transaction (CREATE TEMP TABLE → UPDATE → INSERT → DROP). Statement-by-statement execution loses the temp tables between calls.

**Fix:** Created two execution modes in the deploy script:
- **Block mode** (`run_sql_block`): Sends entire SQL file as one API call. Used for dim load and fact load where temp tables need session persistence.
- **Statement mode** (`run_sql_stmts`): Splits file on semicolons, executes each statement individually. Used for DDL files where each statement is independent.

**Lesson:** When using the Redshift Data API, any SQL that uses temp tables or transactions (BEGIN/COMMIT) must be sent as a single block, not split into individual statements.

---

### Issue 12: Cartesian Product in SCD2 As-Of JOINs (dim_worker_job_d, dim_worker_status_d)

**Date:** 2026-02-06

**Symptom:** `dim_worker_job_d` had 14,353 rows (expected ~2,751) with every `(employee_id, effective_date)` pair duplicated exactly 18 times. `fct_worker_movement_f` was inflated to 8,459,202 rows (expected ~2,751). Dashboard showed millions of movements for 429 employees.

**Root cause:** The as-of JOIN pattern used `<=` inequality to find the most recent source row as-of each effective date:

```sql
-- BROKEN: Multiple rows from each source match, creating cartesian product
LEFT JOIN tmp_worker_job wj
    ON ed.employee_id = wj.employee_id
    AND wj.transaction_effective_date <= ed.effective_date
    AND wj.rn = 1
```

With 5 LEFT JOINs using this pattern (worker_job, worker_comp, cost_centre, company, supervisory), the fanout multiplied to ~18x per row.

**Fix:** Two-step as-of join pattern — first compute the single best matching date per source in a separate temp table using correlated `MAX()` subqueries, then join with exact `=` equality:

```sql
-- Step 1: Find the single best matching date per source
CREATE TEMP TABLE tmp_as_of_keys AS
SELECT ed.employee_id, ed.effective_date,
    (SELECT MAX(wj2.transaction_effective_date)
     FROM tmp_worker_job wj2
     WHERE wj2.employee_id = ed.employee_id
       AND wj2.transaction_effective_date <= ed.effective_date
       AND wj2.rn = 1) AS wj_as_of_date
FROM tmp_effective_dates ed;

-- Step 2: Join with EXACT equality (no fanout)
CREATE TEMP TABLE tmp_assembled_rows AS
SELECT aok.*, wj.*
FROM tmp_as_of_keys aok
LEFT JOIN tmp_worker_job wj
    ON aok.employee_id = wj.employee_id
    AND wj.transaction_effective_date = aok.wj_as_of_date
    AND wj.rn = 1;
```

**Redshift limitation:** Correlated subqueries work inside `CREATE TEMP TABLE ... AS SELECT` but NOT inside `JOIN ON` clauses. The fix must use a separate temp table for the as-of key resolution.

**Additional fix:** `dim_grade_profile_d` had 5 duplicate rows per grade_id (50 rows instead of 10), causing a 2x fanout in fact table JOINs. Deduped by keeping lowest surrogate key per `(grade_id, valid_from, valid_to)`.

**Verification results after fix:**

| Table | Before | After |
|-------|--------|-------|
| dim_worker_job_d | 14,353 (18x dupes) | 2,751 (0 dupes) |
| dim_worker_status_d | 5,540 | 2,011 (0 dupes) |
| fct_worker_movement_f | 8,459,202 | 2,751 (0 dupes) |
| fct_worker_headcount_restat_f | 9,084 | 9,084 |
| Avg base pay | $177,994 | $175,344 (CSV: $175,741) |

**Lesson:** Never use `<=` inequality JOINs for as-of lookups when multiple rows can match. Always resolve the single best match in a prior step, then join on exact equality. This is a general SCD2 assembly anti-pattern, not Redshift-specific.

---

### Issue 13: dim_grade_profile_d Duplicate Rows Causing Fact Table Fanout

**Date:** 2026-02-06

**Symptom:** `fct_worker_movement_f` had exactly 2x the expected row count (5,395 vs 2,751).

**Root cause:** `dim_grade_profile_d` contained 5 duplicate rows per grade_id, all with `is_current = true` and identical `valid_from`/`valid_to` ranges. The fact table's `BETWEEN valid_from AND valid_to` join matched multiple rows.

**Fix:** Deduped the dimension by keeping the row with the lowest `grade_profile_sk` per `(grade_id, valid_from, valid_to, is_current)`. Root cause in the dim load needs investigation to prevent recurrence.

**Lesson:** Always verify reference dimension uniqueness constraints before loading fact tables. A single dimension with duplicate rows can silently inflate all downstream facts.

---

### Issue 14: Reference Dimension valid_from = CURRENT_DATE Breaks Historical Snapshot JOINs

**Date:** 2026-02-06

**Symptom:** All 9,084 rows in `fct_worker_headcount_restat_f` had NULL `company_sk`, `department_sk`, and `location_sk`. The Lambda headcount extraction returned empty arrays for `by_company`, `by_department`, and `by_location`.

**Root cause:** Seven reference dimensions (`dim_company_d`, `dim_department_d`, `dim_location_d`, `dim_cost_center_d`, `dim_grade_profile_d`, `dim_job_profile_d`, `dim_position_d`) were loaded with `valid_from = 2026-02-06` (the load date). Headcount snapshots span 2024-03-31 to 2026-01-31. The fact load's `BETWEEN valid_from AND valid_to` join never matched because all snapshot dates were earlier than the earliest valid_from.

**Fix:** Updated all seven reference dimensions: `SET valid_from = '2000-01-01' WHERE valid_from = '2026-02-06'`. Reloaded both fact tables.

**Lesson:** Reference dimensions that represent slowly changing data should use an early anchor date (e.g., `2000-01-01`) for their initial `valid_from`, not `CURRENT_DATE`. Using the load date as valid_from means historical fact snapshots will never match any dimension row.

---

### Issue 15: Lambda Queries Using Incompatible ID Formats (DPT vs CC) and NULL Surrogate Keys

**Date:** 2026-02-06

**Symptom:** Dashboard Headcount tab showed empty `by_company`, `by_department`, and `by_location` breakdowns. Org Health tab showed all departments with `department_size = 0`.

**Root cause (Headcount):** Lambda queries used surrogate key JOINs (`h.company_sk = c.company_sk`) but all surrogate keys were NULL in the fact table (see Issue 14). Even after fixing Issue 14, the department surrogate key remained NULL because the fact table's `sup_org_id` column was unpopulated.

**Root cause (Org Health):** The departments query joined `d.department_id = j.supervisory_organization`, but `department_id` uses the `DPT` prefix format (e.g., `DPT00013`) while `supervisory_organization` uses the `CC` prefix format (e.g., `CC00059`). These never match.

**Fix:** Rewrote all four Lambda queries:
- Headcount `by_company`: surrogate key JOIN → natural key JOIN (`h.company_id = c.company_id`)
- Headcount `by_department`: broken `department_sk` JOIN → direct `dim_worker_job_d` grouping by `supervisory_organization`
- Headcount `by_location`: surrogate key JOIN → natural key JOIN (`h.location_id = l.location_id`)
- Org Health `departments`: broken `department_id = supervisory_organization` JOIN → direct `dim_worker_job_d` grouping by `supervisory_organization`

**Lesson:** When surrogate keys may be NULL or unreliable (due to SCD2 date range issues, unmapped natural keys, etc.), Lambda/reporting queries should prefer natural key JOINs for robustness. Also verify that JOINs between tables use columns with compatible ID formats — different ID namespaces (DPT vs CC) will silently produce zero matches.

---

## Quick Reference: Redshift SQL Gotchas

| Pattern | Works on PostgreSQL/SQL Server | Redshift Replacement |
|---------|-------------------------------|---------------------|
| `CONCAT(a, b, c)` | Yes | `a \|\| b \|\| c` |
| `BOOLEAN::VARCHAR` | Yes (PostgreSQL) | `BOOLEAN::INT::VARCHAR` |
| `VALUES (1,'a'),(2,'b')` in subquery | Yes | `SELECT 1,'a' UNION ALL SELECT 2,'b'` |
| `WITH cte AS (...) INSERT INTO` | Yes (PostgreSQL) | `INSERT INTO ... WITH cte AS (...) SELECT` |
| `day(date_col)` | Yes (SQL Server) | `DATEPART(day, date_col)` |
| `INTERVAL '1 day' * n` | Yes (PostgreSQL) | `DATEADD(day, n, start_date)` |
| Temp tables across API calls | N/A | Must send as single block |
| `BOOLEAN` DDL + VARCHAR DML | Silent coercion | Match DDL type to DML output |
| `<= date` as-of JOIN | Works but creates cartesian product | Use separate temp table with `MAX()` + exact `=` JOIN |
| Correlated subquery in `JOIN ON` | Yes (PostgreSQL) | Not supported — use in `CREATE TEMP TABLE AS SELECT` instead |
| Ref dim `valid_from = CURRENT_DATE` | Works if facts are current | Use anchor date like `2000-01-01` for historical snapshots |
| Surrogate key JOINs in reports | Works if SKs populated | Prefer natural key JOINs for robustness in reporting/Lambda queries |
