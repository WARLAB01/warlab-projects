# Frequently Asked Questions (FAQ)
## HR Workday Data Pipeline

---

## General Questions

### Q: What is the HR Workday Data Pipeline?

**A:** The HR Workday Data Pipeline is an automated data integration solution that extracts HR data from Workday, transforms it for analytics, and loads it into Amazon Redshift. This enables self-service reporting and analytics on workforce data.

---

### Q: How often is the data updated?

**A:** Data is refreshed daily. The ETL job runs at 6:00 AM UTC, and fresh data is typically available by 6:30 AM UTC. The data reflects the previous day's end-of-day state in Workday.

---

### Q: Why don't I see today's changes in the data?

**A:** The pipeline runs overnight, so changes made in Workday today will appear in the data warehouse tomorrow morning. If you need real-time data, please access Workday directly.

---

### Q: How do I get access to the data?

**A:** Submit an access request through the IT Service Portal under "HR Data Warehouse Access." Your request will require manager and HR Data Owner approval. See the Getting Started guide for details.

---

### Q: Who can I contact for help?

**A:**
- **Data questions**: hr-analytics@company.com
- **Technical issues**: data-engineering@company.com
- **Access requests**: IT Service Portal
- **Data quality concerns**: hr-data-steward@company.com

---

## Data Questions

### Q: What's the difference between `employee_id` and `worker_id`?

**A:**
- **employee_id** (EMP-XXXXXX): The primary identifier used throughout our systems. Use this for joins and analysis.
- **worker_id** (WRK-XXXXXX): The Workday-native identifier. Provided for reference but `employee_id` is preferred.

---

### Q: Why are there employees with NULL termination dates?

**A:** A NULL `termination_date` indicates the employee is currently active or has never been terminated. Only employees who have left the organization have a populated termination date.

---

### Q: What does `worker_status` mean?

**A:** The `worker_status` field indicates the employee's current employment state:
- **Active**: Currently employed
- **Terminated**: No longer employed
- **On Leave**: On approved leave of absence
- **Pre-Start**: Hired but not yet started

---

### Q: Why can't I see salary information?

**A:** Compensation data is classified as Highly Confidential. Access is restricted to:
- Compensation Analysts
- HR Leadership
- Specific approved roles

If you need compensation data for your work, request elevated access through the IT Service Portal with business justification.

---

### Q: How do I find an employee's manager?

**A:** Use the `manager_employee_id` field in `core_hr_employees`:

```sql
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name as employee_name,
    m.first_name || ' ' || m.last_name as manager_name
FROM hr_workday.core_hr_employees e
LEFT JOIN hr_workday.core_hr_employees m
    ON e.manager_employee_id = m.employee_id
WHERE e.employee_id = 'EMP-001234';
```

---

### Q: How do I track an employee's history?

**A:** Query the transaction tables to see an employee's complete history:

```sql
-- All job changes for an employee
SELECT effective_date, transaction_type, prior_job_profile, new_job_profile
FROM hr_workday.job_movement_transactions
WHERE employee_id = 'EMP-001234'
ORDER BY effective_date;

-- All compensation changes
SELECT effective_date, transaction_type, prior_base_salary, new_base_salary
FROM hr_workday.compensation_change_transactions
WHERE employee_id = 'EMP-001234'
ORDER BY effective_date;
```

---

### Q: Why do transaction counts seem high?

**A:** Transaction tables capture ALL changes, including:
- Annual merit increases (one per employee)
- Org restructures (can affect many employees at once)
- Job code standardization (mass updates)
- Corrections and adjustments

This means an employee may have multiple transactions in a single day.

---

### Q: What's the difference between `hire_date` and `original_hire_date`?

**A:**
- **hire_date**: The most recent hire date (relevant for rehires)
- **original_hire_date**: The very first hire date (used for total tenure calculations)

For employees who were never terminated, these dates are the same.

---

### Q: How do I calculate tenure correctly?

**A:** Use `years_of_service` for current tenure, or calculate from `original_hire_date`:

```sql
SELECT
    employee_id,
    original_hire_date,
    years_of_service,
    DATEDIFF('year', original_hire_date, CURRENT_DATE) as calculated_tenure
FROM hr_workday.core_hr_employees
WHERE worker_status = 'Active';
```

---

## Technical Questions

### Q: Why is my query running slowly?

**A:** Common causes and solutions:

1. **Missing WHERE clause**: Always filter data
   ```sql
   -- Bad
   SELECT * FROM hr_workday.core_hr_employees;

   -- Good
   SELECT employee_id, first_name, last_name
   FROM hr_workday.core_hr_employees
   WHERE business_unit = 'Retail Banking';
   ```

2. **SELECT ***: Specify only needed columns

3. **No date filter on transactions**: Always include date ranges
   ```sql
   WHERE effective_date >= '2025-01-01'
   ```

4. **Cross joins**: Ensure proper join conditions

---

### Q: How do I export data to Excel?

**A:** In Query Editor v2:
1. Run your query
2. Click "Export" in the results pane
3. Choose CSV or JSON format
4. Open in Excel

**Note:** Please follow data governance policies when exporting data.

---

### Q: Can I create my own tables or views?

**A:** No, the `hr_workday` schema is read-only for analysts. If you need custom views or aggregations:
1. Request them through hr-analytics@company.com
2. Provide your use case and proposed logic
3. Data Engineering will evaluate and potentially add to the pipeline

---

### Q: How do I connect Tableau/Power BI?

**A:** Use the Redshift JDBC/ODBC connector:
- **Driver**: Amazon Redshift JDBC/ODBC driver
- **Server**: `hr-workday-wg.{account}.{region}.redshift-serverless.amazonaws.com`
- **Port**: 5439
- **Database**: `hr_workday_db`
- **Authentication**: Use your AWS SSO credentials

Contact Data Engineering for detailed setup instructions for your specific tool.

---

### Q: What if I find data quality issues?

**A:** Please report issues to hr-data-steward@company.com with:
- Specific records affected (employee_id, etc.)
- Expected vs. actual values
- How you discovered the issue
- Business impact

The data quality team investigates all reported issues within 1 business day.

---

## Troubleshooting

### Q: I'm getting "Access Denied" errors

**A:** Possible causes:
1. **Access not provisioned**: Check if your access request was approved
2. **Expired session**: Log out and back in to AWS
3. **Wrong workgroup**: Ensure you're connecting to `hr-workday-wg`
4. **Restricted data**: You may not have access to certain fields (e.g., compensation)

---

### Q: The data hasn't updated today

**A:** Check the last load time:

```sql
SELECT MAX(loaded_at) as last_load
FROM hr_workday.core_hr_employees;
```

If data is stale:
1. Check if it's before 6:30 AM UTC (job may still be running)
2. Contact data-engineering@company.com if data is more than 24 hours old

---

### Q: My row counts don't match Workday

**A:** Common reasons for discrepancies:
1. **Timing**: Pipeline data is T-1 (yesterday's snapshot)
2. **Filters**: Ensure you're comparing apples to apples (same status filters)
3. **Contingent workers**: Pipeline may include/exclude different worker types
4. **Global scope**: Verify regional filters match

If discrepancies persist, contact hr-data-steward@company.com.

---

### Q: Query Editor shows "Connection lost"

**A:** Redshift Serverless may have scaled down due to inactivity. Solutions:
1. Wait 30 seconds and try again (workgroup is resuming)
2. Refresh the browser page
3. Check AWS status page for service issues

---

## Policy Questions

### Q: Can I share query results with my team?

**A:** It depends on the data:
- **Aggregated/anonymized data**: Generally yes
- **Individual employee data**: Follow data governance policies
- **Compensation data**: Restricted sharing - consult HR Data Owner
- **External sharing**: Requires explicit approval

When in doubt, contact hr-data-steward@company.com.

---

### Q: How long is data retained?

**A:**
- **Current data**: Always available in the warehouse
- **Historical transactions**: 7 years
- **Terminated employee records**: 7 years post-termination
- **Archived data**: Available upon request from S3 Glacier

---

### Q: Can I request custom data extracts?

**A:** Yes, for legitimate business needs:
1. Submit request to hr-analytics@company.com
2. Include: purpose, fields needed, date range, format
3. Requests are evaluated based on business need and data governance
4. Typical turnaround: 3-5 business days

---

## Still Have Questions?

If your question isn't answered here:

1. **Check the documentation**: Data Dictionary, Getting Started Guide, Architecture docs
2. **Search Slack**: #hr-analytics channel may have answers
3. **Attend office hours**: Wednesdays 2:00-3:00 PM ET
4. **Email the team**: hr-analytics@company.com

We're here to help you get value from HR data!
