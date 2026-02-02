# Operations Runbook
## HR Workday Data Pipeline

### Document Control

| Version | Date | Author | Status |
|---------|------|--------|--------|
| 1.0 | 2026-02-02 | Data Engineering Team | Current |

---

## 1. Overview

This runbook provides operational procedures for the HR Workday data pipeline, including routine monitoring, incident response, and maintenance tasks.

### 1.1 Pipeline Summary

| Component | Name | Schedule |
|-----------|------|----------|
| S3 Bucket | hr-workday-data-{account} | N/A |
| Glue ETL Job | hr-workday-load-to-redshift | Daily 6:00 AM UTC |
| Glue Crawler | hr-workday-s3-crawler | On-demand |
| Redshift Workgroup | hr-workday-wg | Always on |
| Glue Trigger | hr-workday-daily-load | Daily 6:00 AM UTC |

### 1.2 Key Contacts

| Role | Contact | Escalation |
|------|---------|------------|
| Primary On-Call | data-engineering@company.com | PagerDuty |
| Secondary On-Call | data-platform@company.com | Slack #data-alerts |
| HR Data Owner | hr-analytics@company.com | Email |
| AWS Support | Enterprise Support | Console |

---

## 2. Daily Operations

### 2.1 Morning Health Check (7:00 AM local)

**Purpose:** Verify overnight pipeline completed successfully.

**Steps:**

1. **Check Glue Job Status**
   ```bash
   aws glue get-job-runs \
       --job-name hr-workday-load-to-redshift \
       --max-results 1 \
       --query 'JobRuns[0].{Status:JobRunState,StartTime:StartedOn,Duration:ExecutionTime}'
   ```

   Expected: `Status: SUCCEEDED`

2. **Verify Data Freshness**
   ```sql
   -- Run in Redshift Query Editor
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

   Expected: `last_load` within last 24 hours

3. **Check CloudWatch Alarms**
   ```bash
   aws cloudwatch describe-alarms \
       --alarm-name-prefix "hr-workday" \
       --state-value ALARM
   ```

   Expected: No alarms in ALARM state

### 2.2 Monitoring Dashboard Checks

| Metric | Location | Expected Value |
|--------|----------|----------------|
| Job Success Rate | CloudWatch | 100% (7-day) |
| Average Duration | CloudWatch | < 30 minutes |
| Row Count Variance | Custom metric | < 5% |
| Redshift Queries | Redshift Console | < 30 sec avg |

---

## 3. Incident Response Procedures

### 3.1 Glue Job Failure

**Severity:** High
**SLA:** Resolve within 2 hours

**Symptoms:**
- Glue job status = FAILED or ERROR
- CloudWatch alarm triggered
- Data not refreshed (stale loaded_at)

**Diagnosis Steps:**

1. **Get Job Run Details**
   ```bash
   # Get the failed job run ID
   aws glue get-job-runs \
       --job-name hr-workday-load-to-redshift \
       --max-results 5

   # Get error details
   aws glue get-job-run \
       --job-name hr-workday-load-to-redshift \
       --run-id <run-id> \
       --query 'JobRun.ErrorMessage'
   ```

2. **Check CloudWatch Logs**
   ```bash
   # View recent logs
   aws logs filter-log-events \
       --log-group-name /aws-glue/jobs/error \
       --start-time $(date -d '1 hour ago' +%s000) \
       --filter-pattern "hr-workday"
   ```

3. **Common Error Patterns**

   | Error | Likely Cause | Resolution |
   |-------|--------------|------------|
   | "Access Denied" | IAM permission issue | Verify role permissions |
   | "Connection refused" | Redshift unavailable | Check workgroup status |
   | "S3 path not found" | Missing source files | Verify S3 upload completed |
   | "COPY failed" | Data format issue | Check source file format |
   | "Timeout" | Long-running query | Increase job timeout |

**Resolution Steps:**

1. **For IAM Issues:**
   ```bash
   # Verify role has required permissions
   aws iam simulate-principal-policy \
       --policy-source-arn <glue-role-arn> \
       --action-names s3:GetObject redshift-data:ExecuteStatement
   ```

2. **For Data Issues:**
   - Download and inspect source file
   - Check for encoding issues, malformed rows
   - Verify column count matches schema

3. **Re-run the Job:**
   ```bash
   aws glue start-job-run --job-name hr-workday-load-to-redshift
   ```

4. **Monitor Re-run:**
   ```bash
   # Get the new run ID from start-job-run output
   aws glue get-job-run \
       --job-name hr-workday-load-to-redshift \
       --run-id <new-run-id>
   ```

### 3.2 Data Quality Issues

**Severity:** Medium
**SLA:** Investigate within 4 hours

**Symptoms:**
- Row count significantly different from expected
- NULL values in required fields
- Duplicate primary keys

**Diagnosis Steps:**

1. **Check Row Counts**
   ```sql
   -- Compare current vs previous load
   SELECT
       table_name,
       current_count,
       LAG(current_count) OVER (ORDER BY load_date) as previous_count,
       current_count - LAG(current_count) OVER (ORDER BY load_date) as difference
   FROM (
       SELECT 'employees' as table_name, COUNT(*) as current_count,
              DATE(loaded_at) as load_date
       FROM hr_workday.core_hr_employees
       GROUP BY DATE(loaded_at)
   );
   ```

2. **Check for Duplicates**
   ```sql
   SELECT employee_id, COUNT(*)
   FROM hr_workday.core_hr_employees
   GROUP BY employee_id
   HAVING COUNT(*) > 1;
   ```

3. **Check for NULLs in Required Fields**
   ```sql
   SELECT
       SUM(CASE WHEN employee_id IS NULL THEN 1 ELSE 0 END) as null_employee_id,
       SUM(CASE WHEN hire_date IS NULL THEN 1 ELSE 0 END) as null_hire_date,
       SUM(CASE WHEN worker_status IS NULL THEN 1 ELSE 0 END) as null_status
   FROM hr_workday.core_hr_employees;
   ```

**Resolution:**
- If source data issue: Contact HR Operations
- If ETL issue: Review transformation logic
- If loading issue: Check COPY command errors

### 3.3 Redshift Unavailable

**Severity:** Critical
**SLA:** Resolve within 1 hour

**Symptoms:**
- Cannot connect to Redshift
- Queries timing out
- Workgroup status not "Available"

**Diagnosis:**

1. **Check Workgroup Status**
   ```bash
   aws redshift-serverless get-workgroup \
       --workgroup-name hr-workday-wg \
       --query 'workgroup.status'
   ```

2. **Check Namespace Status**
   ```bash
   aws redshift-serverless get-namespace \
       --namespace-name hr-workday-ns \
       --query 'namespace.status'
   ```

**Resolution:**

1. If status is "MODIFYING" - Wait for operation to complete
2. If status is "UNAVAILABLE":
   ```bash
   # Check for service issues
   aws health describe-events \
       --filter "services=redshift-serverless"
   ```
3. If persistent issue - Open AWS Support case

---

## 4. Maintenance Procedures

### 4.1 Manual Job Execution

**When:** Source data delayed, ad-hoc refresh needed

```bash
# Start job manually
aws glue start-job-run \
    --job-name hr-workday-load-to-redshift

# With custom parameters (if needed)
aws glue start-job-run \
    --job-name hr-workday-load-to-redshift \
    --arguments '{"--S3_PREFIX":"raw/hr_data/2026/02/01"}'
```

### 4.2 Pause/Resume Pipeline

**When:** Planned maintenance, source system downtime

**Pause:**
```bash
# Disable the trigger
aws glue stop-trigger --name hr-workday-daily-load
```

**Resume:**
```bash
# Re-enable the trigger
aws glue start-trigger --name hr-workday-daily-load
```

### 4.3 Update Glue Job Script

**When:** Bug fix, enhancement

1. **Update script in S3:**
   ```bash
   aws s3 cp updated_script.py \
       s3://hr-workday-data-{account}/glue_scripts/load_hr_data_copy_command.py
   ```

2. **Test with manual run:**
   ```bash
   aws glue start-job-run --job-name hr-workday-load-to-redshift
   ```

3. **Monitor for success**

### 4.4 Redshift Table Maintenance

**When:** Weekly or as needed

```sql
-- Analyze tables for query optimization
ANALYZE hr_workday.core_hr_employees;
ANALYZE hr_workday.job_movement_transactions;
ANALYZE hr_workday.compensation_change_transactions;
ANALYZE hr_workday.worker_movement_transactions;

-- Check table statistics
SELECT "table", size, tbl_rows, unsorted
FROM svv_table_info
WHERE "schema" = 'hr_workday';
```

### 4.5 S3 Cleanup

**When:** Monthly

```bash
# List old files (>90 days)
aws s3 ls s3://hr-workday-data-{account}/raw/hr_data/ --recursive \
    | awk '$1 < "'$(date -d '90 days ago' +%Y-%m-%d)'"'

# Archive to Glacier (if needed)
aws s3 cp s3://hr-workday-data-{account}/raw/hr_data/2025/ \
    s3://hr-workday-archive-{account}/2025/ --recursive \
    --storage-class GLACIER
```

---

## 5. Recovery Procedures

### 5.1 Restore from Previous Day's Data

**When:** Data corruption, bad source file

1. **Identify last good S3 file:**
   ```bash
   aws s3 ls s3://hr-workday-data-{account}/raw/hr_data/2026/02/ \
       --recursive
   ```

2. **Run job with specific date:**
   ```bash
   aws glue start-job-run \
       --job-name hr-workday-load-to-redshift \
       --arguments '{"--S3_PREFIX":"raw/hr_data/2026/02/01"}'
   ```

### 5.2 Restore Redshift from Snapshot

**When:** Major data loss, table corruption

1. **List available snapshots:**
   ```bash
   aws redshift-serverless list-snapshots \
       --namespace-name hr-workday-ns
   ```

2. **Restore to new namespace:**
   ```bash
   aws redshift-serverless restore-from-snapshot \
       --namespace-name hr-workday-ns-restored \
       --workgroup-name hr-workday-wg-restored \
       --snapshot-name <snapshot-name>
   ```

3. **Verify data, then swap if needed**

### 5.3 Full Pipeline Rebuild

**When:** Disaster recovery, major configuration change

1. Run deployment script: `./deploy.sh`
2. Upload latest data files
3. Execute Glue job
4. Verify all tables loaded

---

## 6. Monitoring & Alerting

### 6.1 CloudWatch Alarms

| Alarm Name | Metric | Threshold | Action |
|------------|--------|-----------|--------|
| hr-workday-job-failed | Glue job state | FAILED | SNS → PagerDuty |
| hr-workday-job-duration | Execution time | > 60 min | SNS → Email |
| hr-workday-redshift-storage | Storage used | > 80% | SNS → Email |

### 6.2 Setting Up Alerts

```bash
# Create SNS topic
aws sns create-topic --name hr-workday-alerts

# Subscribe email
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:{account}:hr-workday-alerts \
    --protocol email \
    --notification-endpoint alerts@company.com

# Create CloudWatch alarm for job failure
aws cloudwatch put-metric-alarm \
    --alarm-name hr-workday-job-failed \
    --metric-name glue.driver.aggregate.numFailedTasks \
    --namespace Glue \
    --statistic Sum \
    --period 300 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --evaluation-periods 1 \
    --alarm-actions arn:aws:sns:us-east-1:{account}:hr-workday-alerts
```

---

## 7. Appendix

### 7.1 Useful Commands Quick Reference

```bash
# Check job status
aws glue get-job-runs --job-name hr-workday-load-to-redshift --max-results 1

# Start manual job run
aws glue start-job-run --job-name hr-workday-load-to-redshift

# Check Redshift workgroup
aws redshift-serverless get-workgroup --workgroup-name hr-workday-wg

# Query Redshift via Data API
aws redshift-data execute-statement \
    --workgroup-name hr-workday-wg \
    --database hr_workday_db \
    --sql "SELECT COUNT(*) FROM hr_workday.core_hr_employees"

# List S3 files
aws s3 ls s3://hr-workday-data-{account}/raw/hr_data/ --recursive

# View Glue logs
aws logs tail /aws-glue/jobs/output --follow
```

### 7.2 Escalation Matrix

| Severity | Response Time | Escalation Path |
|----------|---------------|-----------------|
| Critical (P1) | 15 minutes | On-call → Manager → Director |
| High (P2) | 1 hour | On-call → Team Lead |
| Medium (P3) | 4 hours | Assigned engineer |
| Low (P4) | Next business day | Backlog |

### 7.3 Change Log

| Date | Change | Author |
|------|--------|--------|
| 2026-02-02 | Initial version | Data Engineering |
