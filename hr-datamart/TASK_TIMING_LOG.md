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

*Log generated: 2026-02-06T18:37:43Z*
*Executed by: Claude AI Assistant*
