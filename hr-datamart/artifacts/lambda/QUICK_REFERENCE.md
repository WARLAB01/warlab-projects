# People Analytics Dashboard Lambda Functions - Quick Reference

## Function Invocation Examples

### Dashboard Extractor Lambda

```bash
# KPI Summary
aws lambda invoke --function-name dashboard-extractor \
  --payload '{"extraction": "kpi_summary"}' response.json

# Headcount Analytics
aws lambda invoke --function-name dashboard-extractor \
  --payload '{"extraction": "headcount"}' response.json

# Employee Movements
aws lambda invoke --function-name dashboard-extractor \
  --payload '{"extraction": "movements"}' response.json

# Compensation Analysis
aws lambda invoke --function-name dashboard-extractor \
  --payload '{"extraction": "compensation"}' response.json

# Organizational Health
aws lambda invoke --function-name dashboard-extractor \
  --payload '{"extraction": "org_health"}' response.json
```

### CloudWatch Metrics Publisher

```bash
# No parameters needed - runs on schedule
aws lambda invoke --function-name cloudwatch-publisher response.json
```

## Output Locations

| Extraction Type | S3 Location | CloudWatch Metrics |
|---|---|---|
| kpi_summary | `s3://warlab-hr-dashboard/data/kpi_summary.json` | Yes (5 metrics) |
| headcount | `s3://warlab-hr-dashboard/data/headcount.json` | No |
| movements | `s3://warlab-hr-dashboard/data/movements.json` | No |
| compensation | `s3://warlab-hr-dashboard/data/compensation.json` | No |
| org_health | `s3://warlab-hr-dashboard/data/org_health.json` | No |

## CloudWatch Metrics Published

All metrics in namespace: `WarLabHRDashboard`

| Metric Name | Unit | Description |
|---|---|---|
| ActiveHeadcount | Count | Total active employees |
| TotalMovements | Count | Total movement records |
| AvgBasePay | None | Average base compensation |
| ActiveCompanies | Count | Number of active companies |
| ActiveDepartments | Count | Number of active departments |

## Configuration

**Redshift**:
```python
CLUSTER_ID = 'warlab-hr-datamart'
DATABASE = 'dev'
DB_USER = 'admin'
SCHEMA = 'l3_workday'
```

**AWS**:
```python
S3_BUCKET = 'warlab-hr-dashboard'
CLOUDWATCH_NAMESPACE = 'WarLabHRDashboard'
QUERY_TIMEOUT_SECONDS = 300
```

## Lambda Function Details

| Property | Extractor | Publisher |
|---|---|---|
| File | `dashboard_extractor/lambda_function.py` | `cloudwatch_publisher/lambda_function.py` |
| Handler | `lambda_function.lambda_handler` | `lambda_function.lambda_handler` |
| Runtime | Python 3.11+ | Python 3.11+ |
| Memory | 1024 MB | 512 MB |
| Timeout | 300 sec | 300 sec |
| Trigger | Manual / Step Functions | CloudWatch Events |

## CloudWatch Events Schedule Examples

### Hourly (Recommended)
```json
{
  "Name": "WarLabHRMetricsPublisher",
  "ScheduleExpression": "rate(1 hour)",
  "Targets": [{"Arn": "arn:aws:lambda:..."}]
}
```

### Daily at 8 AM UTC
```json
{
  "ScheduleExpression": "cron(0 8 * * ? *)"
}
```

### Business Hours (9 AM - 5 PM UTC, Mon-Fri)
```json
{
  "ScheduleExpression": "cron(0 9-17 ? * MON-FRI *)"
}
```

## IAM Permissions Required

### Dashboard Extractor Role
```json
{
  "Effect": "Allow",
  "Action": [
    "redshift-data:ExecuteStatement",
    "redshift-data:DescribeStatement",
    "redshift-data:GetStatementResult",
    "s3:PutObject",
    "cloudwatch:PutMetricData"
  ]
}
```

### CloudWatch Publisher Role
```json
{
  "Effect": "Allow",
  "Action": [
    "redshift-data:ExecuteStatement",
    "redshift-data:DescribeStatement",
    "redshift-data:GetStatementResult",
    "cloudwatch:PutMetricData"
  ]
}
```

## Monitoring Commands

```bash
# View recent logs
aws logs tail /aws/lambda/dashboard-extractor --follow

# Get metrics
aws cloudwatch list-metrics --namespace WarLabHRDashboard

# Get specific metric
aws cloudwatch get-metric-statistics \
  --namespace WarLabHRDashboard \
  --metric-name ActiveHeadcount \
  --start-time 2024-01-15T00:00:00Z \
  --end-time 2024-01-15T23:59:59Z \
  --period 3600 \
  --statistics Average

# List S3 output
aws s3 ls s3://warlab-hr-dashboard/data/

# View S3 output
aws s3 cp s3://warlab-hr-dashboard/data/kpi_summary.json - | jq .
```

## Troubleshooting Quick Guide

| Issue | Cause | Fix |
|---|---|---|
| 504 Timeout | Query > 300 sec | Increase timeout or optimize query |
| S3 Access Denied | Missing IAM permission | Add S3 permission to role |
| Redshift Connection Error | Wrong cluster/db/user | Verify config (see IMPLEMENTATION_GUIDE.md) |
| Metrics not visible | Not published yet | Wait 1-2 minutes, check logs |
| Empty results | Query returns no rows | Verify data exists in Redshift |
| Query failed | Syntax or column error | Check CloudWatch Logs for error |

## Key Classes & Methods

### Dashboard Extractor

```python
# Query execution
executor = RedshiftQueryExecutor(CLUSTER_ID, DATABASE, DB_USER)
query_id = executor.execute_query(sql)
executor.wait_for_completion(query_id)
results = executor.fetch_results(query_id)

# Data extraction
extractor = DashboardDataExtractor(executor)
kpi_data = extractor.extract_kpi_summary()
headcount_data = extractor.extract_headcount()
# ... etc

# Publishing
S3DataPublisher.publish(bucket, key, data)
CloudWatchMetricsPublisher.publish_kpi_metrics(metrics, namespace)
```

### CloudWatch Publisher

```python
# Query execution (same as above)
executor = RedshiftQueryExecutor(CLUSTER_ID, DATABASE, DB_USER)

# Metrics extraction
extractor = KPIMetricsExtractor(executor)
metrics = extractor.extract_kpi_metrics()

# Publishing to CloudWatch
CloudWatchMetricsPublisher.publish_metrics(metrics, namespace)
```

## Performance Notes

- **Query Timeout**: Default 300 sec, configurable
- **Poll Interval**: 1 sec (configurable)
- **S3 Batch**: Single put per extraction
- **CloudWatch Batch**: Up to 20 metrics per call
- **Memory**: 1024 MB extractor sufficient for ~10k rows
- **Cost**: Primarily Redshift query cost

## SQL Queries by Extraction Type

### KPI Summary (5 queries)
- Total headcount from `fct_worker_headcount_restat_f`
- Total movements from `fct_worker_movement_f`
- Avg pay from `dim_worker_job_d`
- Company count from `dim_company_d`
- Department count from `dim_department_d`

### Headcount (4 queries)
- By company (join to `dim_company_d`)
- By department (join to `dim_department_d`)
- By location (join to `dim_location_d`)
- Trend by snapshot_date

### Movements (3 queries)
- By type (job, location, compensation)
- Terminations (regrettable vs total)
- Monthly trend (last 12 months)

### Compensation (2 queries)
- By grade (join to `dim_grade_profile_d`)
- By job family (join to `dim_job_profile_d`)

### Org Health (4 queries)
- Departments with sizes
- Manager span of control
- Location distribution
- Worker type distribution

## Files & Locations

```
/sessions/hopeful-upbeat-ramanujan/warlab-projects/hr-datamart/artifacts/lambda/
├── dashboard_extractor/lambda_function.py      # Main extractor
├── cloudwatch_publisher/lambda_function.py     # Metrics publisher
├── config.py                                   # Shared config
├── test_functions.py                           # Test cases
├── README.md                                   # Deployment guide
├── IMPLEMENTATION_GUIDE.md                     # Deep dive
├── QUICK_REFERENCE.md                          # This file
└── DELIVERABLES.md                             # Inventory
```

## Common Tasks

### Deploy Dashboard Extractor
```bash
cd /path/to/lambda/dashboard_extractor
zip -r ../dashboard_extractor.zip lambda_function.py

aws lambda create-function \
  --function-name dashboard-extractor \
  --runtime python3.11 \
  --role arn:aws:iam::ACCOUNT:role/lambda-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://dashboard_extractor.zip \
  --timeout 300 \
  --memory-size 1024
```

### Deploy CloudWatch Publisher
```bash
cd /path/to/lambda/cloudwatch_publisher
zip -r ../cloudwatch_publisher.zip lambda_function.py

aws lambda create-function \
  --function-name cloudwatch-publisher \
  --runtime python3.11 \
  --role arn:aws:iam::ACCOUNT:role/lambda-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://cloudwatch_publisher.zip \
  --timeout 300 \
  --memory-size 512
```

### Test Extraction
```bash
aws lambda invoke \
  --function-name dashboard-extractor \
  --payload '{"extraction": "kpi_summary"}' \
  response.json && cat response.json | jq .
```

### View Results
```bash
aws s3 cp s3://warlab-hr-dashboard/data/kpi_summary.json - | jq .

# Pretty print with colors
aws s3 cp s3://warlab-hr-dashboard/data/kpi_summary.json - | jq '.' --color-output
```

## Response Format Examples

### KPI Summary Response
```json
{
  "extraction_type": "kpi_summary",
  "timestamp": "2024-01-15T10:30:00.123456",
  "metrics": {
    "total_headcount": 5000,
    "total_movements": 1200,
    "avg_base_pay": 85000.50,
    "active_companies": 15,
    "active_departments": 120
  }
}
```

### Lambda Success Response
```json
{
  "statusCode": 200,
  "body": {
    "message": "kpi_summary extraction completed successfully",
    "extraction_type": "kpi_summary",
    "timestamp": "2024-01-15T10:30:00.123456",
    "s3_location": "s3://warlab-hr-dashboard/data/kpi_summary.json"
  }
}
```

### Error Response
```json
{
  "statusCode": 500,
  "body": {
    "error": "Internal error: [error details here]"
  }
}
```

## Documentation Matrix

| Question | Reference |
|---|---|
| How do I deploy? | README.md → Deployment section |
| How does it work? | IMPLEMENTATION_GUIDE.md → Architecture |
| What queries are used? | IMPLEMENTATION_GUIDE.md → SQL Queries |
| How do I test? | README.md → Testing section |
| What are the metrics? | QUICK_REFERENCE.md (this file) |
| How do I troubleshoot? | README.md → Troubleshooting |
| What's the cost? | IMPLEMENTATION_GUIDE.md → Cost estimation |
| How do I extend it? | IMPLEMENTATION_GUIDE.md → Extending |
| What's included? | DELIVERABLES.md |
| What's the code doing? | Inline comments in source files |

---

**For complete documentation, see the other guides. This is a quick reference only.**
