# Task Timing Log

Automated task execution timing records for the WAR Lab HR Datamart project.

---

## Task 1: Add department_sk to Headcount Fact Table

**Start Time:** 2026-02-06T18:25:11Z
**End Time:** 2026-02-06T18:28:19Z
**Duration:** 3 minutes 8 seconds

### Scope
- Investigated root cause of NULL/0 department_sk values in `fct_worker_headcount_restat_f`
- Found CC-to-DPT ID format mismatch between `dim_worker_job_d.supervisory_organization` (CC00001) and `dim_department_d.department_id` (DPT00001)
- Fixed `l3_fact_load.sql` — added `REPLACE(aw.supervisory_organization, 'CC', 'DPT')` bridge in department JOIN
- Ran full headcount fact reload via Redshift (DELETE + INSERT for 24-month window)
- Validated: 9,084/9,084 rows now have populated department_sk (100 distinct departments)

### Files Changed
- `artifacts/dml/l3_fact_load/l3_fact_load.sql` — department JOIN fix

---

## Task 2: Turnover & Promotion Rate Trends on Dashboard

**Start Time:** 2026-02-06T18:28:39Z
**End Time:** 2026-02-06T18:37:43Z
**Duration:** 9 minutes 4 seconds

### Scope
- Added `rates_trend_query` (Query 4) to Lambda `extract_movements()` function
  - Calculates monthly turnover rate: employees transitioning A/L→T ÷ headcount × 100
  - Calculates monthly promotion rate: grade increases ÷ headcount × 100
  - JOINs `fct_worker_movement_f` with `fct_worker_headcount_restat_f` on LAST_DAY(month)
  - Returns 12 months of data
- Updated dashboard HTML (`index.html`):
  - Added `ratesTrend` data transformation in `transformLambdaData()`
  - Added demo data generation for `ratesTrend`
  - Added chart div in Movements & Attrition tab
  - Added `createRatesTrendChart()` function — ECharts dual-line chart with gradient area fill
  - Registered chart call in `renderMovementsTab()`
- Packaged and deployed Lambda to AWS (S3 → Lambda update-function-code)
- Uploaded updated dashboard HTML to S3
- Ran all 5 Lambda extractions (kpi_summary, headcount, movements, compensation, org_health)
- Invalidated CloudFront cache
- Validated: `movements.json` contains `rates_trend` array with 12 monthly entries

### Files Changed
- `artifacts/lambda/dashboard_extractor/lambda_function.py` — rates_trend query
- `artifacts/dashboard/index.html` — rates trend chart visualization

---

## Task 3: Movement Fact Metrics Expansion + dim_day Fix

**Start Time:** 2026-02-06T19:03:14Z
**End Time:** 2026-02-06T19:21:19Z
**Duration:** 18 minutes 5 seconds

### Scope
- Expanded `fct_worker_movement_f` from 15 to **28 metric columns** plus 4 new source attribute columns (`action`, `action_reason`, `primary_termination_category`, `primary_termination_reason`)
- Completely rewrote L3 fact load SQL with 3-CTE architecture:
  - `source_data` — joins dim_worker_job_d + dim_job_profile_d + dim_worker_status_d, uses LAG() for 13 prior-row columns
  - `with_current_fks` — resolves 8 current dimension FKs as-of effective_date
  - `with_prior_fks` — resolves 9 prior dimension FKs as-of prior_effective_date
  - Final SELECT computes all 28 metrics inline
- New metrics added: base_pay_change_count, company_change_count, cost_center_change_count, demotion_count, external_hire_count, grade_decrease_count, grade_increase_count, internal_hire_count, lateral_move_count, management_level_decrease_count, management_level_increase_count, matrix_organization_change_count, promotion_count_business_process, regrettable_termination_count (real logic), rehire_count, structured_termination_count, unstructured_termination_count, worker_model_change_count
- Extended `dim_day_d` date range from 2020-2030 to **2015-2030** (5,844 rows)
  - Expanded cross-join s4 from 4 to 6 rows (capacity: 10×10×10×6 = 6,000)
- Deployed all changes to Redshift via Data API
- QA validated: 2,751 movement rows, 500 employees, date range 2016-02-12 to 2028-11-10
  - Job change decomposition: PASS (promo + demo + lateral = job_changes)
  - All change-detection metrics populated (grade: 562, job: 1,285, location: 1,280, mgmt_level: 683, etc.)
  - Business-process metrics (hire, termination_count, promotion_bp) = 0 — expected for synthetic data lacking action/action_reason values
  - Termination status metrics working (involuntary: 69, voluntary: 77, regrettable: 77)

### Files Changed
- `artifacts/ddl/l3_star/l3_fact_ddl.sql` — expanded DDL with 28 metrics + 4 source attributes
- `artifacts/dml/l3_fact_load/l3_fact_load.sql` — complete rewrite of movement fact load
- `artifacts/dml/l3_dim_load/l3_dim_load.sql` — dim_day date range extended to 2015

---

*Log updated: 2026-02-06T19:21:19Z*
*Executed by: Claude AI Assistant*
