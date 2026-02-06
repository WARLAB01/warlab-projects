# HR Datamart AWS Glue ETL Pipeline

Production-grade AWS Glue ETL pipeline for loading HR Datamart source tables from S3 (pipe-delimited CSV) into Redshift L1 staging layer.

## Overview

This solution provides:
- **Parameterized ETL Script**: Single reusable script for all 12 source tables
- **Workflow Orchestration**: Glue workflow coordinating parallel execution of all jobs
- **Automated Deployment**: Bash script to create Glue jobs and workflow in AWS
- **Production Features**: Error handling, logging, monitoring, and data quality checks

## Architecture

### Data Flow

```
S3 (Pipe-delimited CSV)
    ↓
AWS Glue ETL Job
    ├─ Read CSV with delimiter='|' and header=true
    ├─ Apply ResolveChoice transform for type safety
    ├─ TRUNCATE Redshift target table (preaction)
    └─ Write to Redshift L1 table via Redshift connector
        ↓
Redshift L1 Staging Table (dev database, l1_workday schema)
```

### Components

1. **glue_s3_to_l1_etl.py** - Parameterized ETL script
2. **glue_workflow_config.json** - Workflow configuration and job definitions
3. **deploy_glue_jobs.sh** - Automated deployment helper
4. **README.md** - This documentation

## Source Tables (12 HR Datamart Tables)

All tables are loaded from `s3://warlab-hr-datamart-dev/workday/hrdp/{table_name}/`

| # | Source Table | Description |
|---|---|---|
| 1 | int0095e_worker_job | Worker job information from Workday |
| 2 | int0096_worker_organization | Worker organizational assignments |
| 3 | int0098_worker_compensation | Worker compensation details |
| 4 | int270_rescinded | Rescinded records tracking |
| 5 | int6020_grade_profile | Grade profile dimensions |
| 6 | int6021_job_profile | Job profile dimensions |
| 7 | int6022_job_classification | Job classification dimensions |
| 8 | int6023_location | Location dimensions |
| 9 | int6024_company | Company dimensions |
| 10 | int6025_cost_center | Cost center dimensions |
| 11 | int6028_department_hierarchy | Department hierarchy structures |
| 12 | int6032_positions | Positions dimensions |

## Prerequisites

### AWS Resources Required

1. **S3 Bucket**: `warlab-hr-datamart-dev`
   - Contains source CSV files in `workday/hrdp/{table_name}/` structure
   - Contains `glue-temp/` directory for Glue temporary files
   - Contains `glue-scripts/` directory for ETL scripts
   - Contains `spark-logs/` directory for Spark event logs

2. **Redshift Cluster**: Running and accessible
   - Database: `dev`
   - Schema: `l1_workday` (must be created)
   - Target tables with appropriate schema (L1 staging tables)

3. **Glue Connection**: `warlab-redshift-connection`
   - Type: Redshift
   - Endpoint: Your Redshift cluster endpoint
   - Port: 5439 (default)
   - Database: dev
   - SSL enabled (recommended)

4. **IAM Role**: GlueServiceRole with permissions for:
   - S3: GetObject, PutObject on warlab-hr-datamart-dev
   - Redshift: Connect and write data
   - CloudWatch Logs: Write job and spark logs
   - EC2: For VPC endpoint access if needed

### Software Prerequisites

- AWS CLI v2 (configured with appropriate credentials)
- bash 4.0+
- Read access to glue_s3_to_l1_etl.py file

## Installation & Deployment

### Step 1: Prepare AWS Infrastructure

```bash
# Create S3 directories
aws s3api put-object --bucket warlab-hr-datamart-dev --key glue-scripts/
aws s3api put-object --bucket warlab-hr-datamart-dev --key glue-temp/
aws s3api put-object --bucket warlab-hr-datamart-dev --key spark-logs/

# Create Redshift schema
# Connect to Redshift and execute:
# CREATE SCHEMA IF NOT EXISTS l1_workday;

# Verify Glue connection
aws glue get-connection --name warlab-redshift-connection
```

### Step 2: Deploy Glue Jobs

```bash
# Navigate to the artifacts directory
cd /path/to/warlab-projects/hr-datamart/artifacts/glue

# Run deployment script (dry-run first to verify)
./deploy_glue_jobs.sh --dry-run --region us-east-1 --profile default

# If dry-run looks good, deploy for real
./deploy_glue_jobs.sh --region us-east-1 --profile default
```

### Deployment Script Options

```
--dry-run    Show what would be deployed without making changes
--region     AWS region (default: us-east-1)
--profile    AWS CLI profile (default: default)
--help       Show help message
```

## Usage

### Running Individual Glue Jobs

```bash
# Start a single job
aws glue start-job-run \
    --job-name warlab-hr-int6024-company \
    --region us-east-1

# Monitor job run
aws glue get-job-run \
    --job-name warlab-hr-int6024-company \
    --run-id jr_1234567890123 \
    --region us-east-1

# View job logs in CloudWatch
aws logs tail /aws-glue/jobs/warlab-hr-int6024-company --follow
```

### Running the Complete Workflow

```bash
# Start workflow (on-demand trigger)
aws glue start-workflow-run \
    --name warlab-hr-l1-load \
    --region us-east-1

# Get workflow run details
aws glue get-workflow-run \
    --name warlab-hr-l1-load \
    --run-id wfr_1234567890123 \
    --region us-east-1

# List recent workflow runs
aws glue get-workflow-runs \
    --name warlab-hr-l1-load \
    --region us-east-1 \
    --max-results 10
```

### Scheduling Automatic Execution

The deployment creates a scheduled trigger that runs daily at 6:00 AM UTC:

```bash
# Update schedule (if needed)
aws glue put-trigger \
    --name warlab-hr-l1-load-daily \
    --workflow-name warlab-hr-l1-load \
    --type "SCHEDULED" \
    --schedule "cron(0 6 * * ? *)" \
    --start-on-creation
```

## Configuration

### Job Parameters

The ETL script accepts the following parameters:

```python
# Required parameters
--source_table      # Source table name (e.g., "int6024_company")
--s3_path          # S3 path to CSV file (e.g., "s3://bucket/path/")

# Optional parameters with defaults
--redshift_schema           # Default: "l1_workday"
--redshift_table           # Default: same as source_table
--redshift_connection      # Default: "warlab-redshift-connection"
--redshift_database        # Default: "dev"
--TempDir                  # Temporary directory for Glue operations
--enable-spark-ui          # Enable Spark UI (true/false)
--spark-event-logs-path    # Path for Spark event logs
--enable-job-insights      # Enable job insights monitoring
--enable-glue-datacatalog  # Enable Glue Data Catalog
```

### Modifying Job Configuration

Each job is configured in the deployment script with:

```bash
MAX_CAPACITY="10.0"           # DPUs (default: 10)
TIMEOUT_MINUTES="30"          # Job timeout (default: 30 minutes)
GLUE_VERSION="4.0"            # Glue runtime version
PYTHON_VERSION="3"            # Python version
```

To change these globally, edit `deploy_glue_jobs.sh`:

```bash
# Line ~45
MAX_CAPACITY="10.0"           # Change default DPUs
TIMEOUT_MINUTES="30"          # Change timeout
```

To change for individual jobs, create a modified deployment script or update jobs manually:

```bash
aws glue update-job \
    --name warlab-hr-int6024-company \
    --max-capacity 20.0 \
    --timeout 60
```

## Monitoring and Troubleshooting

### CloudWatch Logs

Each job writes logs to CloudWatch:

```bash
# View logs for a specific job
aws logs tail /aws-glue/jobs/warlab-hr-int6024-company --follow

# View logs for last 1 hour
aws logs tail /aws-glue/jobs/warlab-hr-int6024-company \
    --since 1h \
    --follow
```

### Glue Job Metrics

```bash
# Get job metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/Glue \
    --metric-name glue.driver.aggregate.numFailedTasks \
    --dimensions Name=JobName,Value=warlab-hr-int6024-company \
    --statistics Sum \
    --start-time 2026-02-01T00:00:00Z \
    --end-time 2026-02-06T00:00:00Z \
    --period 3600
```

### Common Issues and Solutions

#### Issue: Job fails with "Glue connection not found"

```bash
# Verify connection exists
aws glue get-connection --name warlab-redshift-connection

# If missing, create it manually in AWS Console
# Or update the connection name in the job parameters
```

#### Issue: Data not appearing in Redshift

```bash
# Check if table exists
psql -h <redshift-endpoint> -U <username> -d dev -c "SELECT COUNT(*) FROM l1_workday.int6024_company;"

# Check Redshift query logs
SELECT * FROM stl_load_errors ORDER BY starttime DESC;
SELECT * FROM stl_connection_log ORDER BY recordtime DESC;
```

#### Issue: Job timeout

```bash
# Increase timeout for large datasets
aws glue update-job \
    --name warlab-hr-int6024-company \
    --timeout 60  # Increase to 60 minutes
```

#### Issue: Out of memory errors

```bash
# Increase DPU allocation
aws glue update-job \
    --name warlab-hr-int6024-company \
    --max-capacity 20.0  # Increase from 10 to 20 DPUs
```

### Monitoring Script (Optional)

Create `monitor_workflow.sh` for automated monitoring:

```bash
#!/bin/bash
WORKFLOW_NAME="warlab-hr-l1-load"
REGION="us-east-1"

while true; do
    aws glue get-workflow-runs \
        --name $WORKFLOW_NAME \
        --region $REGION \
        --max-results 1 \
        --query 'Runs[0].[Name,Status,StartedOn,CompletedOn]' \
        --output table
    sleep 30
done
```

## Performance Tuning

### DPU Allocation

- **Small datasets (<1GB)**: 10 DPUs (default)
- **Medium datasets (1-10GB)**: 10-20 DPUs
- **Large datasets (>10GB)**: 20+ DPUs

Monitor and adjust based on CloudWatch metrics.

### Parallelism Configuration

The script uses `parallelism: 10` for Redshift writes. Adjust in glue_s3_to_l1_etl.py:

```python
# Line ~250
redshift_options = {
    "parallelism": 10,  # Increase for faster writes
}
```

### S3 Read Optimization

For large CSV files, ensure S3 bucket is in the same region as Glue jobs:

```bash
# Check bucket region
aws s3api get-bucket-location --bucket warlab-hr-datamart-dev
```

## Security Best Practices

1. **IAM Permissions**: Use least privilege
   ```bash
   # Restrict to specific tables/databases
   ```

2. **Redshift Connection**: Use SSL/TLS
   ```bash
   # Configure in Glue connection settings
   ```

3. **S3 Encryption**: Enable by default
   ```bash
   aws s3api put-bucket-encryption \
       --bucket warlab-hr-datamart-dev \
       --server-side-encryption-configuration '{...}'
   ```

4. **VPC Endpoints**: Use for private connectivity
   ```bash
   # Configure VPC endpoint for S3 and Redshift
   ```

5. **Audit Logging**: Enable CloudTrail
   ```bash
   # Monitor Glue API calls
   ```

## Maintenance and Updates

### Updating the ETL Script

1. Modify `glue_s3_to_l1_etl.py`
2. Upload to S3: `aws s3 cp glue_s3_to_l1_etl.py s3://warlab-hr-datamart-dev/glue-scripts/`
3. Glue jobs automatically reference the latest version

### Scaling to Additional Tables

To add a new source table:

1. Add entry to `SOURCE_TABLES` array in `deploy_glue_jobs.sh`
2. Re-run deployment script
3. New job will be created with same configuration

### Backup and Recovery

```bash
# Export job definitions
aws glue list-jobs --region us-east-1 > jobs_backup.json

# Export workflow definition
aws glue get-workflow --name warlab-hr-l1-load > workflow_backup.json
```

## Testing

### Unit Testing

Test the ETL script locally:

```bash
# Install Glue dependencies
pip install awsglue pyspark

# Test with sample data
python glue_s3_to_l1_etl.py \
    --source_table int6024_company \
    --s3_path s3://warlab-hr-datamart-dev/workday/hrdp/int6024_company/
```

### Integration Testing

Run a single job to verify end-to-end connectivity:

```bash
aws glue start-job-run \
    --job-name warlab-hr-int6024-company

# Monitor execution
aws logs tail /aws-glue/jobs/warlab-hr-int6024-company --follow
```

### Validation Queries

After successful load, verify data:

```sql
-- Check record count
SELECT COUNT(*) FROM l1_workday.int6024_company;

-- Check for NULL issues
SELECT COUNT(*) FROM l1_workday.int6024_company WHERE column_name IS NULL;

-- Compare with previous load
SELECT COUNT(*) FROM l1_workday.int6024_company
WHERE load_date = CURRENT_DATE;
```

## Cost Estimation

### AWS Glue Costs

- **DPU Rate**: ~$0.44 per DPU-hour
- **Job Run**: 10 DPUs × 0.5 hours = 5 DPU-hours × $0.44 = ~$2.20 per job run
- **Daily Cost**: 12 jobs × $2.20 = ~$26.40/day (~$790/month)

### S3 Costs

- **Data Transfer**: Minimal (same region)
- **Storage**: Depends on data volume and retention policy

### Redshift Costs

- **Existing cluster**: No additional cost
- **Data loading**: Included in cluster cost

## Support and Contact

For issues or questions:

1. Check CloudWatch logs: `/aws-glue/jobs/warlab-hr-*`
2. Review AWS Glue console for job status
3. Contact Data Engineering team

## License

Proprietary - Warlab Inc.

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-06 | Initial release with 12 HR Datamart tables |

---

**Last Updated**: 2026-02-06
**Maintained By**: Data Engineering Team
