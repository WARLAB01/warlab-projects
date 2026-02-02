# Sample Queries

A collection of useful queries for common HR analytics scenarios.

---

## Table of Contents

1. [Headcount & Demographics](#headcount--demographics)
2. [Hiring & Onboarding](#hiring--onboarding)
3. [Turnover & Attrition](#turnover--attrition)
4. [Organizational Structure](#organizational-structure)
5. [Job Movement & Promotions](#job-movement--promotions)
6. [Compensation Analysis](#compensation-analysis)
7. [Tenure & Experience](#tenure--experience)
8. [Data Quality Checks](#data-quality-checks)

---

## Headcount & Demographics

### Active Headcount by Business Unit

```sql
SELECT
    business_unit,
    COUNT(*) as headcount
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY business_unit
ORDER BY headcount DESC;
```

### Headcount by Location and Country

```sql
SELECT
    country,
    location,
    COUNT(*) as headcount,
    SUM(CASE WHEN is_manager THEN 1 ELSE 0 END) as managers,
    ROUND(AVG(years_of_service), 1) as avg_tenure
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY country, location
ORDER BY country, headcount DESC;
```

### Gender Distribution by Department

```sql
SELECT
    department,
    gender,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY department), 1) as percentage
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY department, gender
ORDER BY department, gender;
```

### Headcount by Job Level

```sql
SELECT
    job_level,
    management_level,
    COUNT(*) as headcount,
    SUM(CASE WHEN is_manager THEN 1 ELSE 0 END) as with_direct_reports
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY job_level, management_level
ORDER BY job_level;
```

---

## Hiring & Onboarding

### New Hires - Last 30 Days

```sql
SELECT
    employee_id,
    first_name || ' ' || last_name as name,
    hire_date,
    business_title,
    department,
    location
FROM hr_workday.core_hr_employees
WHERE hire_date >= CURRENT_DATE - INTERVAL '30 days'
    AND worker_status = 'Active'
ORDER BY hire_date DESC;
```

### Monthly Hire Trend

```sql
SELECT
    DATE_TRUNC('month', hire_date) as hire_month,
    COUNT(*) as new_hires
FROM hr_workday.core_hr_employees
WHERE hire_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', hire_date)
ORDER BY hire_month;
```

### Hires by Source Department

```sql
SELECT
    business_unit,
    department,
    COUNT(*) as hires_ytd
FROM hr_workday.core_hr_employees
WHERE hire_date >= DATE_TRUNC('year', CURRENT_DATE)
    AND worker_status = 'Active'
GROUP BY business_unit, department
ORDER BY hires_ytd DESC
LIMIT 20;
```

---

## Turnover & Attrition

### Monthly Terminations

```sql
SELECT
    DATE_TRUNC('month', termination_date) as term_month,
    COUNT(*) as terminations
FROM hr_workday.core_hr_employees
WHERE termination_date IS NOT NULL
    AND termination_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', termination_date)
ORDER BY term_month;
```

### Turnover by Department

```sql
WITH headcount AS (
    SELECT
        department,
        COUNT(*) as total_employees
    FROM hr_workday.core_hr_employees
    WHERE worker_status = 'Active'
    GROUP BY department
),
terminations AS (
    SELECT
        department,
        COUNT(*) as term_count
    FROM hr_workday.core_hr_employees
    WHERE termination_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY department
)
SELECT
    h.department,
    h.total_employees,
    COALESCE(t.term_count, 0) as terminations,
    ROUND(100.0 * COALESCE(t.term_count, 0) / h.total_employees, 1) as turnover_rate
FROM headcount h
LEFT JOIN terminations t ON h.department = t.department
ORDER BY turnover_rate DESC;
```

### Terminations by Tenure Band

```sql
SELECT
    CASE
        WHEN years_of_service < 1 THEN '< 1 year'
        WHEN years_of_service < 2 THEN '1-2 years'
        WHEN years_of_service < 5 THEN '2-5 years'
        ELSE '5+ years'
    END as tenure_band,
    COUNT(*) as terminations
FROM hr_workday.core_hr_employees
WHERE termination_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 1
ORDER BY MIN(years_of_service);
```

---

## Organizational Structure

### Direct Reports per Manager

```sql
SELECT
    m.employee_id as manager_id,
    m.first_name || ' ' || m.last_name as manager_name,
    m.business_title as manager_title,
    m.department,
    COUNT(e.employee_id) as direct_reports
FROM hr_workday.core_hr_employees m
JOIN hr_workday.core_hr_employees e
    ON m.employee_id = e.manager_employee_id
WHERE m.worker_status = 'Active'
    AND e.worker_status = 'Active'
GROUP BY m.employee_id, m.first_name, m.last_name, m.business_title, m.department
ORDER BY direct_reports DESC
LIMIT 25;
```

### Org Chart - Two Levels

```sql
SELECT
    m.first_name || ' ' || m.last_name as manager,
    m.business_title as manager_title,
    e.first_name || ' ' || e.last_name as employee,
    e.business_title as employee_title
FROM hr_workday.core_hr_employees m
JOIN hr_workday.core_hr_employees e
    ON m.employee_id = e.manager_employee_id
WHERE m.department = 'Sales'  -- Change as needed
    AND m.worker_status = 'Active'
    AND e.worker_status = 'Active'
ORDER BY m.last_name, e.last_name;
```

### Span of Control Analysis

```sql
SELECT
    CASE
        WHEN direct_reports = 0 THEN 'Individual Contributor'
        WHEN direct_reports BETWEEN 1 AND 3 THEN '1-3 reports'
        WHEN direct_reports BETWEEN 4 AND 7 THEN '4-7 reports'
        WHEN direct_reports BETWEEN 8 AND 12 THEN '8-12 reports'
        ELSE '13+ reports'
    END as span_category,
    COUNT(*) as manager_count
FROM (
    SELECT
        m.employee_id,
        COUNT(e.employee_id) as direct_reports
    FROM hr_workday.core_hr_employees m
    LEFT JOIN hr_workday.core_hr_employees e
        ON m.employee_id = e.manager_employee_id AND e.worker_status = 'Active'
    WHERE m.worker_status = 'Active'
        AND m.is_manager = true
    GROUP BY m.employee_id
) spans
GROUP BY 1
ORDER BY MIN(direct_reports);
```

---

## Job Movement & Promotions

### Promotions This Year

```sql
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name as name,
    j.effective_date,
    j.prior_job_profile,
    j.new_job_profile,
    j.prior_job_level,
    j.new_job_level
FROM hr_workday.job_movement_transactions j
JOIN hr_workday.core_hr_employees e ON j.employee_id = e.employee_id
WHERE j.transaction_type = 'Promotion'
    AND j.effective_date >= DATE_TRUNC('year', CURRENT_DATE)
ORDER BY j.effective_date DESC;
```

### Promotion Rate by Department

```sql
WITH dept_headcount AS (
    SELECT department, COUNT(*) as headcount
    FROM hr_workday.core_hr_employees
    WHERE worker_status = 'Active'
    GROUP BY department
),
promotions AS (
    SELECT e.department, COUNT(*) as promo_count
    FROM hr_workday.job_movement_transactions j
    JOIN hr_workday.core_hr_employees e ON j.employee_id = e.employee_id
    WHERE j.transaction_type = 'Promotion'
        AND j.effective_date >= DATE_TRUNC('year', CURRENT_DATE)
    GROUP BY e.department
)
SELECT
    h.department,
    h.headcount,
    COALESCE(p.promo_count, 0) as promotions,
    ROUND(100.0 * COALESCE(p.promo_count, 0) / h.headcount, 1) as promotion_rate
FROM dept_headcount h
LEFT JOIN promotions p ON h.department = p.department
ORDER BY promotion_rate DESC;
```

### Internal Transfers

```sql
SELECT
    DATE_TRUNC('month', effective_date) as month,
    COUNT(*) as transfers
FROM hr_workday.worker_movement_transactions
WHERE movement_type IN ('Department Transfer', 'Division Transfer')
    AND effective_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', effective_date)
ORDER BY month;
```

---

## Compensation Analysis

> **Note**: Compensation queries require elevated access (Compensation Analyst role).

### Average Salary by Job Level

```sql
SELECT
    job_level,
    COUNT(*) as employee_count,
    ROUND(AVG(base_salary), 0) as avg_salary,
    ROUND(MIN(base_salary), 0) as min_salary,
    ROUND(MAX(base_salary), 0) as max_salary
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
    AND base_salary IS NOT NULL
GROUP BY job_level
ORDER BY job_level;
```

### Merit Increase Distribution

```sql
SELECT
    CASE
        WHEN base_change_percent < 2 THEN '< 2%'
        WHEN base_change_percent < 3 THEN '2-3%'
        WHEN base_change_percent < 4 THEN '3-4%'
        WHEN base_change_percent < 5 THEN '4-5%'
        ELSE '5%+'
    END as increase_band,
    COUNT(*) as employee_count
FROM hr_workday.compensation_change_transactions
WHERE transaction_type = 'Merit Increase'
    AND effective_date >= DATE_TRUNC('year', CURRENT_DATE)
GROUP BY 1
ORDER BY MIN(base_change_percent);
```

---

## Tenure & Experience

### Tenure Distribution

```sql
SELECT
    CASE
        WHEN years_of_service < 1 THEN '< 1 year'
        WHEN years_of_service < 3 THEN '1-3 years'
        WHEN years_of_service < 5 THEN '3-5 years'
        WHEN years_of_service < 10 THEN '5-10 years'
        WHEN years_of_service < 15 THEN '10-15 years'
        ELSE '15+ years'
    END as tenure_band,
    COUNT(*) as employee_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as percentage
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY 1
ORDER BY MIN(years_of_service);
```

### Average Tenure by Department

```sql
SELECT
    department,
    COUNT(*) as headcount,
    ROUND(AVG(years_of_service), 1) as avg_tenure,
    MAX(years_of_service) as max_tenure
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY department
ORDER BY avg_tenure DESC;
```

---

## Data Quality Checks

### Check for NULL Required Fields

```sql
SELECT
    SUM(CASE WHEN employee_id IS NULL THEN 1 ELSE 0 END) as null_employee_id,
    SUM(CASE WHEN hire_date IS NULL THEN 1 ELSE 0 END) as null_hire_date,
    SUM(CASE WHEN worker_status IS NULL THEN 1 ELSE 0 END) as null_status,
    SUM(CASE WHEN department IS NULL THEN 1 ELSE 0 END) as null_department,
    COUNT(*) as total_records
FROM hr_workday.core_hr_employees;
```

### Check for Duplicate Keys

```sql
SELECT employee_id, COUNT(*) as occurrences
FROM hr_workday.core_hr_employees
GROUP BY employee_id
HAVING COUNT(*) > 1;
```

### Check Data Freshness

```sql
SELECT
    'core_hr_employees' as table_name,
    MAX(loaded_at) as last_load,
    COUNT(*) as row_count
FROM hr_workday.core_hr_employees
UNION ALL
SELECT 'job_movement_transactions', MAX(loaded_at), COUNT(*)
FROM hr_workday.job_movement_transactions
UNION ALL
SELECT 'compensation_change_transactions', MAX(loaded_at), COUNT(*)
FROM hr_workday.compensation_change_transactions
UNION ALL
SELECT 'worker_movement_transactions', MAX(loaded_at), COUNT(*)
FROM hr_workday.worker_movement_transactions;
```

---

## Tips for Writing Queries

1. **Always filter by status**: Most reports should include `WHERE worker_status = 'Active'`

2. **Use date ranges for transactions**: Transaction tables can be large
   ```sql
   WHERE effective_date >= '2025-01-01'
   ```

3. **Join efficiently**: Use `employee_id` for all joins

4. **Test with LIMIT**: Always test queries with `LIMIT 100` first

5. **Use CTEs for complex queries**: Makes code readable and reusable
   ```sql
   WITH active_employees AS (
       SELECT * FROM hr_workday.core_hr_employees
       WHERE worker_status = 'Active'
   )
   SELECT ...
   ```

---

*Have a useful query to share? Submit to hr-analytics@company.com for inclusion!*
