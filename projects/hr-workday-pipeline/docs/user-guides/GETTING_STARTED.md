# Getting Started Guide
## HR Workday Data Pipeline

Welcome to the HR Workday Data Pipeline! This guide will help you get started with accessing and using HR data for analytics and reporting.

---

## 1. Overview

The HR Workday Data Pipeline provides consolidated HR data from Workday in a queryable format within Amazon Redshift. This enables HR analytics, workforce planning, and operational reporting.

### What Data is Available?

| Dataset | Description | Update Frequency |
|---------|-------------|------------------|
| Core HR Employees | Current state of all employees | Daily |
| Job Movement | Promotions, transfers, role changes | Daily |
| Compensation Changes | Salary adjustments, bonuses | Daily |
| Worker Movement | Location/org transfers | Daily |

### Who Should Use This Guide?

- HR Business Partners
- HR Analysts
- Workforce Planning Teams
- Compensation Analysts
- People Analytics Teams

---

## 2. Getting Access

### Step 1: Request Access

1. Submit an access request through the IT Service Portal
2. Select "HR Data Warehouse Access"
3. Choose your required access level:
   - **HR Analyst**: Standard reporting access (compensation masked)
   - **Compensation Analyst**: Full compensation visibility
   - **HR Leadership**: Full read access

### Step 2: Approval

Your request will be routed to:
1. Your manager (approval)
2. HR Data Owner (approval)
3. IT Security (provisioning)

Typical turnaround: 2-3 business days

### Step 3: Access Confirmation

Once approved, you'll receive:
- Confirmation email with access details
- Link to Redshift Query Editor v2
- This documentation link

---

## 3. Connecting to the Data

### Option 1: AWS Redshift Query Editor v2 (Recommended)

The easiest way to query HR data is through the browser-based Query Editor.

1. **Log in to AWS Console**
   - Go to https://console.aws.amazon.com
   - Sign in with your corporate SSO credentials

2. **Navigate to Query Editor**
   - Search for "Redshift" in the AWS console
   - Click "Query Editor v2" in the left menu

3. **Connect to the Workgroup**
   - Select workgroup: `hr-workday-wg`
   - Database: `hr_workday_db`
   - Schema: `hr_workday`

4. **Start Querying**
   - You're ready to write SQL queries!

### Option 2: SQL Client (Advanced)

For power users, connect via JDBC/ODBC:

**Connection Details:**
- Endpoint: `hr-workday-wg.{account}.{region}.redshift-serverless.amazonaws.com`
- Port: 5439
- Database: `hr_workday_db`
- Authentication: IAM or username/password

Supported clients: DBeaver, DataGrip, SQL Workbench/J, Tableau, Power BI

---

## 4. Understanding the Data Model

### Schema: `hr_workday`

```
hr_workday
├── core_hr_employees          (Current employee snapshot)
├── job_movement_transactions  (Job changes over time)
├── compensation_change_transactions  (Pay changes)
└── worker_movement_transactions      (Location/org changes)
```

### Key Relationships

All tables are linked by `employee_id`:

```
core_hr_employees.employee_id  ←──┬── job_movement_transactions.employee_id
                                  ├── compensation_change_transactions.employee_id
                                  └── worker_movement_transactions.employee_id
```

### Important Fields

| Field | Description | Found In |
|-------|-------------|----------|
| `employee_id` | Unique employee identifier (EMP-XXXXXX) | All tables |
| `effective_date` | When the change took effect | Transaction tables |
| `loaded_at` | When data was loaded to warehouse | All tables |
| `worker_status` | Active, Terminated, On Leave | core_hr_employees |

---

## 5. Your First Queries

### Query 1: Count of Active Employees by Business Unit

```sql
SELECT
    business_unit,
    COUNT(*) as employee_count
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY business_unit
ORDER BY employee_count DESC;
```

### Query 2: Recent Hires (Last 30 Days)

```sql
SELECT
    employee_id,
    first_name,
    last_name,
    hire_date,
    business_title,
    department
FROM hr_workday.core_hr_employees
WHERE hire_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY hire_date DESC;
```

### Query 3: Promotions This Year

```sql
SELECT
    e.first_name,
    e.last_name,
    j.prior_job_profile,
    j.new_job_profile,
    j.effective_date
FROM hr_workday.job_movement_transactions j
JOIN hr_workday.core_hr_employees e ON j.employee_id = e.employee_id
WHERE j.transaction_type = 'Promotion'
  AND j.effective_date >= DATE_TRUNC('year', CURRENT_DATE)
ORDER BY j.effective_date DESC;
```

### Query 4: Headcount by Location

```sql
SELECT
    location,
    country,
    COUNT(*) as headcount,
    SUM(CASE WHEN is_manager THEN 1 ELSE 0 END) as manager_count
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY location, country
ORDER BY headcount DESC;
```

### Query 5: Turnover Analysis

```sql
SELECT
    DATE_TRUNC('month', termination_date) as term_month,
    business_unit,
    COUNT(*) as terminations
FROM hr_workday.core_hr_employees
WHERE termination_date IS NOT NULL
  AND termination_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', termination_date), business_unit
ORDER BY term_month, business_unit;
```

---

## 6. Common Use Cases

### Workforce Demographics Report

```sql
SELECT
    business_unit,
    gender,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY business_unit), 1) as pct
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY business_unit, gender
ORDER BY business_unit, gender;
```

### Tenure Distribution

```sql
SELECT
    CASE
        WHEN years_of_service < 1 THEN '< 1 year'
        WHEN years_of_service < 3 THEN '1-3 years'
        WHEN years_of_service < 5 THEN '3-5 years'
        WHEN years_of_service < 10 THEN '5-10 years'
        ELSE '10+ years'
    END as tenure_band,
    COUNT(*) as employee_count
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active'
GROUP BY 1
ORDER BY MIN(years_of_service);
```

### Manager Span of Control

```sql
SELECT
    m.employee_id as manager_id,
    m.first_name || ' ' || m.last_name as manager_name,
    m.business_title,
    COUNT(e.employee_id) as direct_reports
FROM hr_workday.core_hr_employees m
JOIN hr_workday.core_hr_employees e ON m.employee_id = e.manager_employee_id
WHERE m.is_manager = true
  AND m.worker_status = 'Active'
  AND e.worker_status = 'Active'
GROUP BY m.employee_id, m.first_name, m.last_name, m.business_title
ORDER BY direct_reports DESC
LIMIT 20;
```

---

## 7. Data Refresh Schedule

| Process | Schedule | Completion Time |
|---------|----------|-----------------|
| Workday Extract | Daily 5:00 AM UTC | ~30 minutes |
| S3 Upload | Daily 5:30 AM UTC | ~10 minutes |
| Glue ETL Job | Daily 6:00 AM UTC | ~15 minutes |
| Data Available | Daily ~6:30 AM UTC | - |

**Note:** Data reflects the previous day's end-of-day state from Workday.

### Checking Data Freshness

```sql
SELECT
    'core_hr_employees' as table_name,
    MAX(loaded_at) as last_load,
    COUNT(*) as row_count
FROM hr_workday.core_hr_employees
UNION ALL
SELECT 'job_movement_transactions', MAX(loaded_at), COUNT(*)
FROM hr_workday.job_movement_transactions;
```

---

## 8. Best Practices

### Do's ✓

- **Filter by worker_status**: Most reports should filter for `worker_status = 'Active'`
- **Use date filters**: Always include date ranges for transaction queries
- **Join efficiently**: Use `employee_id` for all table joins
- **Test queries**: Run with `LIMIT 100` first to verify results

### Don'ts ✗

- **Don't SELECT ***: Always specify needed columns
- **Don't share compensation data**: Follow data classification guidelines
- **Don't export large datasets**: Use aggregations in the warehouse
- **Don't modify data**: This is a read-only analytics environment

### Query Performance Tips

1. **Use WHERE clauses early** to filter data
2. **Avoid functions on indexed columns** in WHERE clauses
3. **Use appropriate date functions** (DATE_TRUNC, EXTRACT)
4. **Limit result sets** when exploring data

---

## 9. Getting Help

### Self-Service Resources

- This documentation
- Sample queries in the `queries/` folder
- Data dictionary (DATA_DICTIONARY.md)

### Support Channels

| Issue Type | Contact | Response Time |
|------------|---------|---------------|
| Access requests | IT Service Portal | 2-3 days |
| Data questions | hr-analytics@company.com | 1 day |
| Technical issues | data-engineering@company.com | 4 hours |
| Data quality issues | hr-data-steward@company.com | 1 day |

### Office Hours

The Data Engineering team holds weekly office hours:
- **When**: Wednesdays, 2:00-3:00 PM ET
- **Where**: Teams channel "HR Data Support"
- **Topics**: Query help, new requirements, data questions

---

## 10. Next Steps

Now that you're set up:

1. ✅ Review the Data Dictionary to understand available fields
2. ✅ Try the sample queries above
3. ✅ Explore the pre-built reports (if available)
4. ✅ Join the HR Analytics Teams channel for updates
5. ✅ Attend office hours if you have questions

Happy querying!
