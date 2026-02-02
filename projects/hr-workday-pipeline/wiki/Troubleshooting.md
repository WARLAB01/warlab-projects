# Troubleshooting Guide

Solutions to common issues when using the HR Workday Data Pipeline.

---

## Access Issues

### "Access Denied" when connecting to Redshift

**Symptoms:**
- Cannot connect to Query Editor
- Permission denied errors on queries

**Solutions:**

1. **Verify access was approved**
   - Check your email for access confirmation
   - Contact IT Service Desk to verify status

2. **Refresh your session**
   - Log out of AWS Console completely
   - Clear browser cache
   - Log back in with SSO

3. **Check workgroup selection**
   - Ensure you're connecting to `hr-workday-wg`
   - Database should be `hr_workday_db`

4. **Verify role permissions**
   - Some data (compensation) requires elevated access
   - Request additional permissions if needed

---

### Cannot see compensation fields

**Symptoms:**
- `base_salary`, `total_compensation` columns show as "RESTRICTED"
- Query returns permission errors on specific columns

**Cause:** Compensation data requires Compensation Analyst role.

**Solution:**
- Submit elevated access request via IT Service Portal
- Provide business justification
- Use the analyst view: `hr_workday.employees_analyst_view`

---

## Query Issues

### Query running very slowly

**Symptoms:**
- Query takes more than 30 seconds
- Timeout errors

**Solutions:**

1. **Add WHERE clauses**
   ```sql
   -- Bad: Scans entire table
   SELECT * FROM hr_workday.core_hr_employees;

   -- Good: Filters early
   SELECT employee_id, first_name, last_name
   FROM hr_workday.core_hr_employees
   WHERE business_unit = 'Retail Banking'
     AND worker_status = 'Active';
   ```

2. **Avoid SELECT ***
   ```sql
   -- Bad
   SELECT * FROM hr_workday.job_movement_transactions;

   -- Good
   SELECT employee_id, effective_date, transaction_type
   FROM hr_workday.job_movement_transactions;
   ```

3. **Add date filters to transaction tables**
   ```sql
   WHERE effective_date >= '2025-01-01'
   ```

4. **Use LIMIT while testing**
   ```sql
   SELECT ... LIMIT 100;
   ```

---

### "Relation does not exist" error

**Symptoms:**
- `ERROR: relation "hr_workday.core_hr_employees" does not exist`

**Solutions:**

1. **Check schema prefix**
   - Always use `hr_workday.` prefix
   - Example: `hr_workday.core_hr_employees`

2. **Set search path**
   ```sql
   SET search_path TO hr_workday;
   SELECT * FROM core_hr_employees LIMIT 10;
   ```

3. **Verify table name spelling**
   - `core_hr_employees` (not `employees`)
   - `job_movement_transactions` (not `job_movements`)

---

### Unexpected NULL values

**Symptoms:**
- Joins returning fewer rows than expected
- Aggregations showing NULL

**Solutions:**

1. **Check for NULL join keys**
   ```sql
   SELECT COUNT(*) FROM hr_workday.job_movement_transactions
   WHERE employee_id IS NULL;
   ```

2. **Use LEFT JOIN instead of INNER JOIN**
   ```sql
   SELECT e.*, j.transaction_type
   FROM hr_workday.core_hr_employees e
   LEFT JOIN hr_workday.job_movement_transactions j
     ON e.employee_id = j.employee_id;
   ```

3. **Handle NULLs in aggregations**
   ```sql
   SELECT COALESCE(department, 'Unknown') as department,
          COUNT(*)
   FROM hr_workday.core_hr_employees
   GROUP BY COALESCE(department, 'Unknown');
   ```

---

## Data Issues

### Data hasn't refreshed today

**Symptoms:**
- `loaded_at` timestamp is more than 24 hours old
- Yesterday's changes not visible

**Diagnosis:**
```sql
SELECT MAX(loaded_at) as last_load
FROM hr_workday.core_hr_employees;
```

**Solutions:**

1. **Check if it's before 6:30 AM UTC**
   - Daily load completes around 6:30 AM UTC
   - Wait if job is still running

2. **Contact Data Engineering**
   - Email: data-engineering@company.com
   - Check #hr-data-alerts Slack channel
   - May be a source system or job failure

---

### Row counts don't match Workday

**Symptoms:**
- Headcount differs from Workday reports
- Missing or extra employees

**Common causes:**

1. **Timing difference**
   - Pipeline shows T-1 (yesterday's) data
   - Workday shows real-time

2. **Filter differences**
   - Verify same `worker_status` filter
   - Check worker types included

3. **Regional scope**
   - Pipeline includes all regions
   - Verify scope matches

**Diagnosis query:**
```sql
SELECT
    worker_status,
    worker_type,
    COUNT(*) as count
FROM hr_workday.core_hr_employees
GROUP BY worker_status, worker_type
ORDER BY worker_status, worker_type;
```

---

### Duplicate records appearing

**Symptoms:**
- Same employee appears multiple times
- Aggregations show inflated numbers

**Diagnosis:**
```sql
SELECT employee_id, COUNT(*) as occurrences
FROM hr_workday.core_hr_employees
GROUP BY employee_id
HAVING COUNT(*) > 1;
```

**Solution:**
- Report to hr-data-steward@company.com
- Include employee_id and screenshot
- Data team will investigate source

---

## Connection Issues

### Query Editor shows "Connection lost"

**Symptoms:**
- Disconnected message in Query Editor
- Queries won't run

**Cause:** Redshift Serverless scales down when idle.

**Solutions:**

1. **Wait and retry**
   - Workgroup auto-resumes in ~30 seconds
   - Try query again

2. **Refresh browser**
   - Press F5 or click refresh
   - Re-select workgroup if needed

3. **Check AWS status**
   - Visit status.aws.amazon.com
   - Look for Redshift service issues

---

### JDBC/ODBC connection fails

**Symptoms:**
- Cannot connect from Tableau/Power BI
- "Connection refused" errors

**Solutions:**

1. **Verify connection string**
   - Host: `hr-workday-wg.{account}.{region}.redshift-serverless.amazonaws.com`
   - Port: 5439
   - Database: `hr_workday_db`

2. **Check credentials**
   - Use IAM authentication if configured
   - Verify username/password if using database auth

3. **Network access**
   - Ensure you're on corporate network/VPN
   - Check firewall rules

4. **Driver version**
   - Use latest Redshift JDBC/ODBC driver
   - Download from AWS website

---

## Export Issues

### Cannot export large result set

**Symptoms:**
- Export fails or times out
- Browser crashes with large results

**Solutions:**

1. **Reduce result size**
   ```sql
   SELECT ... LIMIT 10000;
   ```

2. **Export in batches**
   ```sql
   -- Batch 1
   SELECT ... LIMIT 10000 OFFSET 0;
   -- Batch 2
   SELECT ... LIMIT 10000 OFFSET 10000;
   ```

3. **Use aggregations**
   - Summarize data before export
   - Don't export raw detail when possible

4. **Request data extract**
   - For large exports, contact hr-analytics@company.com
   - Data team can provide S3 extract

---

## Still Having Issues?

If your problem isn't covered here:

1. **Search Slack** - #hr-analytics channel
2. **Check FAQ** - [FAQ Page](FAQ)
3. **Contact support**:
   - Data questions: hr-analytics@company.com
   - Technical issues: data-engineering@company.com
4. **Attend office hours** - Wednesdays 2-3 PM ET

When reporting issues, please include:
- Exact error message
- Query you were running
- Time the issue occurred
- Screenshots if possible
