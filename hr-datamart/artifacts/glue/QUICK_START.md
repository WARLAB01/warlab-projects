# HR Datamart Glue ETL - Quick Start Guide

Get the HR Datamart ETL pipeline running in 10 minutes.

## Prerequisites Checklist

Before starting, ensure you have:

- [ ] AWS CLI v2 installed and configured
- [ ] AWS credentials configured for warlab AWS account
- [ ] S3 bucket `warlab-hr-datamart-dev` exists with source CSV files
- [ ] Redshift cluster running and accessible
- [ ] Redshift schema `l1_workday` created
- [ ] Glue connection `warlab-redshift-connection` configured

To verify Glue connection:
```bash
aws glue get-connection --name warlab-redshift-connection --region us-east-1
```

## 5-Minute Deployment

### 1. Prepare S3 (1 minute)

```bash
# Create required S3 directories
aws s3api put-object --bucket warlab-hr-datamart-dev --key glue-scripts/
aws s3api put-object --bucket warlab-hr-datamart-dev --key glue-temp/
aws s3api put-object --bucket warlab-hr-datamart-dev --key spark-logs/
```

### 2. Create Redshift Schema (1 minute)

Connect to your Redshift cluster and run:

```sql
CREATE SCHEMA IF NOT EXISTS l1_workday;
GRANT CREATE ON SCHEMA l1_workday TO [glue_user];
```

### 3. Deploy Glue Jobs (3 minutes)

Navigate to this directory and run:

```bash
# Dry-run first to verify
./deploy_glue_jobs.sh --dry-run --region us-east-1

# Deploy for real
./deploy_glue_jobs.sh --region us-east-1
```

This creates:
- 12 Glue jobs (one per source table)
- 1 Glue workflow
- 2 workflow triggers (on-demand + scheduled daily at 6 AM UTC)

## First Test Run (5 minutes)

### Start the Workflow

```bash
./manage_workflow.sh start
```

### Monitor Execution

```bash
# Check workflow status
./manage_workflow.sh status

# Watch status continuously
watch -n 10 './manage_workflow.sh status'

# View logs for a specific job
./manage_workflow.sh logs warlab-hr-int6024-company
```

### Verify Data in Redshift

```bash
# Connect to Redshift
psql -h your-redshift-cluster.redshift.amazonaws.com -U admin -d dev

# Check if data loaded
SELECT COUNT(*) FROM l1_workday.int6024_company;
SELECT COUNT(*) FROM l1_workday.int0095e_worker_job;

# Check all tables loaded
SELECT schemaname, tablename, rows
FROM pg_stat_user_tables
WHERE schemaname = 'l1_workday'
ORDER BY rows DESC;
```

## Common Commands

### View Job Status
```bash
# Latest workflow run status
./manage_workflow.sh status

# List recent workflow runs
./manage_workflow.sh list-runs

# Specific job status
./manage_workflow.sh job-status warlab-hr-int6024-company
```

### Manage Jobs
```bash
# List all HR Datamart jobs
./manage_workflow.sh list-jobs

# View logs in real-time
./manage_workflow.sh logs warlab-hr-int6024-company

# Delete a single job
./manage_workflow.sh delete-job warlab-hr-int6024-company
```

### Manual Job Execution
```bash
# Start a single job directly
aws glue start-job-run \
    --job-name warlab-hr-int6024-company \
    --region us-east-1

# Monitor job
aws glue list-job-runs \
    --job-name warlab-hr-int6024-company \
    --region us-east-1 \
    --query 'JobRuns[0].[Id,JobRunState,StartedOn]'
```

## Troubleshooting

### Jobs Not Running

```bash
# Check if Glue connection exists
aws glue get-connection --name warlab-redshift-connection --region us-east-1

# Check if S3 path exists
aws s3 ls s3://warlab-hr-datamart-dev/workday/hrdp/int6024_company/

# Check Redshift connectivity
psql -h cluster-endpoint -U admin -d dev -c "SELECT 1"
```

### Job Timeouts

If jobs timeout with large datasets:

```bash
# Increase timeout (in minutes)
aws glue update-job \
    --name warlab-hr-int6024-company \
    --timeout 60 \
    --region us-east-1
```

### Out of Memory

If jobs fail with memory errors:

```bash
# Increase DPU allocation
aws glue update-job \
    --name warlab-hr-int6024-company \
    --max-capacity 20.0 \
    --region us-east-1
```

### View Full Logs

```bash
# See last 100 lines
aws logs tail /aws-glue/jobs/warlab-hr-int6024-company --max-items 100

# See logs from last 1 hour
aws logs tail /aws-glue/jobs/warlab-hr-int6024-company --since 1h
```

## Configuration Changes

### Change Job Capacity (DPUs)

```bash
# Single job
aws glue update-job \
    --name warlab-hr-int6024-company \
    --max-capacity 20.0 \
    --region us-east-1

# All jobs (update deploy script and re-run)
# Edit deploy_glue_jobs.sh line 45: MAX_CAPACITY="20.0"
```

### Change Schedule

Default: Daily at 6:00 AM UTC

```bash
# Update to 2:00 AM UTC
aws glue put-trigger \
    --name warlab-hr-l1-load-daily \
    --workflow-name warlab-hr-l1-load \
    --type "SCHEDULED" \
    --schedule "cron(0 2 * * ? *)" \
    --start-on-creation \
    --region us-east-1
```

### Add a New Source Table

1. Upload CSV file to S3: `s3://warlab-hr-datamart-dev/workday/hrdp/{table_name}/`
2. Create Redshift table in `l1_workday` schema
3. Add table name to `SOURCE_TABLES` array in `deploy_glue_jobs.sh`
4. Re-run deployment: `./deploy_glue_jobs.sh --region us-east-1`

## Performance Notes

### Expected Runtimes

- Small tables (< 100K rows): 2-3 minutes
- Medium tables (100K - 1M rows): 5-10 minutes
- Large tables (> 1M rows): 15-30 minutes
- All 12 jobs in parallel: ~30 minutes total

### Cost per Run

With default settings (10 DPUs per job):
- ~$26/day (~$790/month) for daily scheduled runs
- Adjust DPU allocation to balance cost vs speed

### Optimization Tips

1. Run jobs in parallel (default) rather than sequentially
2. Use S3 bucket in same region as Glue jobs
3. Increase DPU allocation only for large/slow jobs
4. Enable Glue job insights for performance metrics

## Next Steps

1. **Monitor First Week**: Check CloudWatch metrics and logs daily
2. **Set Up Alerts**: Create SNS alerts for job failures
3. **Plan Capacity**: Monitor Redshift storage and query performance
4. **Document Schema**: Create data dictionary for L1 staging tables
5. **Build L2 Jobs**: Start designing L2 transformation jobs

## Support

For issues:

1. Check CloudWatch logs: `/aws-glue/jobs/warlab-hr-*`
2. Review AWS Glue console
3. Check Redshift query logs:
   ```sql
   SELECT * FROM stl_load_errors ORDER BY starttime DESC;
   ```
4. Contact Data Engineering team

## File Reference

| File | Purpose |
|------|---------|
| `glue_s3_to_l1_etl.py` | Main ETL script (parameterized) |
| `deploy_glue_jobs.sh` | Automated deployment script |
| `manage_workflow.sh` | Workflow management utility |
| `glue_workflow_config.json` | Configuration and job definitions |
| `README.md` | Full documentation |
| `QUICK_START.md` | This file |

---

**Questions?** See README.md for detailed documentation.
