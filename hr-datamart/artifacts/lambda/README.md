# People Analytics Dashboard Lambda Functions

This directory contains AWS Lambda functions for the People Analytics Dashboard extraction layer and metrics publishing.

## Functions

### 1. Dashboard Extractor Lambda
**Location**: `dashboard_extractor/lambda_function.py`

Parameterized Lambda function that extracts various analytics datasets from Redshift and publishes them to S3.

**Invocation Examples**:
```json
{"extraction": "kpi_summary"}
{"extraction": "headcount"}
{"extraction": "movements"}
{"extraction": "compensation"}
{"extraction": "org_health"}
```

**Extraction Types**:
- **kpi_summary**: Key performance indicators (headcount, movements, avg pay, companies, departments)
- **headcount**: Headcount analytics by company, department, location, and trend
- **movements**: Employee movement analytics including terminations and monthly trends
- **compensation**: Compensation data by grade and job family
- **org_health**: Organizational structure including departments, manager span of control, locations, worker types

**Features**:
- Uses Redshift Data API for query execution
- Automatic polling for query completion
- Results formatted as JSON and published to S3
- CloudWatch custom metrics published for KPI summary
- Comprehensive error handling and logging

**Output**:
- JSON files written to `s3://warlab-hr-dashboard/data/{extraction_name}.json`
- CloudWatch metrics published to namespace "WarLabHRDashboard" (KPI summary only)

**IAM Permissions Required**:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "redshift-data:ExecuteStatement",
                "redshift-data:DescribeStatement",
                "redshift-data:GetStatementResult"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "redshift:DescribeClusters"
            ],
            "Resource": "arn:aws:redshift:*:*:cluster/warlab-hr-datamart"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::warlab-hr-dashboard/data/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
```

---

### 2. CloudWatch Metrics Publisher Lambda
**Location**: `cloudwatch_publisher/lambda_function.py`

Standalone Lambda that queries KPI metrics from Redshift and publishes them to CloudWatch. Designed to run on a schedule (e.g., hourly or daily).

**Invocation**:
No event parameters required. Can be invoked manually or via CloudWatch Events.

**Metrics Published** (to "WarLabHRDashboard" namespace):
- **ActiveHeadcount**: Total active employees
- **TotalMovements**: Total employee movements recorded
- **AvgBasePay**: Average base compensation
- **ActiveCompanies**: Number of active companies
- **ActiveDepartments**: Number of active departments

**Features**:
- Queries same metrics as dashboard_extractor KPI summary
- Publishes to CloudWatch for dashboarding and alarming
- Handles metric batching (20 metrics per API call)
- Comprehensive error handling and logging

**Suggested Schedule**:
CloudWatch Events rule triggering every hour:
```json
{
    "Name": "WarLabHRMetricsPublisher",
    "ScheduleExpression": "rate(1 hour)",
    "State": "ENABLED",
    "Targets": [
        {
            "Arn": "arn:aws:lambda:region:account:function:cloudwatch-publisher",
            "RoleArn": "arn:aws:iam::account:role/service-role/lambda-cloudwatch-events-role"
        }
    ]
}
```

**IAM Permissions Required**:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "redshift-data:ExecuteStatement",
                "redshift-data:DescribeStatement",
                "redshift-data:GetStatementResult"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "redshift:DescribeClusters"
            ],
            "Resource": "arn:aws:redshift:*:*:cluster/warlab-hr-datamart"
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
```

---

## Deployment

### Prerequisites
- AWS Lambda runtime: Python 3.11 or later
- Redshift cluster: `warlab-hr-datamart`
- S3 bucket: `warlab-hr-dashboard`
- IAM role with permissions listed above

### Packaging
Both functions use only boto3 (no external dependencies), so they can be deployed directly.

For dashboard_extractor:
```bash
cd dashboard_extractor
zip -r ../dashboard_extractor.zip lambda_function.py
```

For cloudwatch_publisher:
```bash
cd cloudwatch_publisher
zip -r ../cloudwatch_publisher.zip lambda_function.py
```

### AWS CLI Deployment

**Dashboard Extractor**:
```bash
aws lambda create-function \
    --function-name dashboard-extractor \
    --runtime python3.11 \
    --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://dashboard_extractor.zip \
    --timeout 300 \
    --memory-size 1024
```

**CloudWatch Publisher**:
```bash
aws lambda create-function \
    --function-name cloudwatch-publisher \
    --runtime python3.11 \
    --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://cloudwatch_publisher.zip \
    --timeout 300 \
    --memory-size 512
```

### Testing

**Dashboard Extractor** - Test via AWS Console or CLI:
```bash
aws lambda invoke \
    --function-name dashboard-extractor \
    --payload '{"extraction": "kpi_summary"}' \
    response.json

cat response.json
```

**CloudWatch Publisher** - Test via AWS Console or CLI:
```bash
aws lambda invoke \
    --function-name cloudwatch-publisher \
    response.json

cat response.json
```

---

## SQL Queries Reference

### KPI Summary Queries
- **Total Active Headcount**: Distinct employee count from `fct_worker_headcount_restat_f` at max snapshot date
- **Total Movements**: Count from `fct_worker_movement_f`
- **Average Base Pay**: Average of `base_pay` from `dim_worker_job_d` where is_current_job_row = true
- **Active Companies**: Count from `dim_company_d` where is_current = true
- **Active Departments**: Count from `dim_department_d` where is_current = true

### Headcount Queries
- By company: Join `fct_worker_headcount_restat_f` to `dim_company_d`
- By department: Join to `dim_department_d`
- By location: Join to `dim_location_d`
- Trend: Group by snapshot_date

### Movement Queries
- By type: Counts of job_change, location_change, compensation_change
- Terminations: Regrettable vs total from `is_termination` flag
- Trend: Monthly counts and movement types

### Compensation Queries
- By grade: Join `dim_worker_job_d` to `dim_grade_profile_d`, calculate avg/min/max pay
- By job family: Join to `dim_job_profile_d`, include median pay calculation

### Org Health Queries
- Departments: Count and employee size
- Manager span of control: Direct reports per manager from `dim_worker_job_d`
- Locations: Distribution of workers by location
- Worker types: Count by worker type

---

## Monitoring and Troubleshooting

### CloudWatch Logs
Both functions log detailed information to CloudWatch Logs:
- Query execution IDs
- Query status polls
- Result retrieval
- S3 publishing (dashboard_extractor)
- CloudWatch metrics publishing

### Common Issues

**Query Timeout**:
- Increase Lambda timeout (default: 300 seconds)
- Check Redshift cluster availability
- Verify query complexity

**S3 Upload Failures** (dashboard_extractor):
- Check S3 bucket exists: `warlab-hr-dashboard`
- Verify IAM role has `s3:PutObject` permission

**Redshift Connection Issues**:
- Verify cluster ID: `warlab-hr-datamart`
- Verify database: `dev`
- Verify DB user: `admin`
- Check Redshift cluster is not paused

**CloudWatch Metrics Not Appearing**:
- Verify metrics are being published (check logs)
- Check CloudWatch namespace: `WarLabHRDashboard`
- Metrics appear within 1-2 minutes of publishing

---

## Performance Notes

### Query Timeouts
- Default timeout: 300 seconds (5 minutes)
- Adjustable via `QUERY_TIMEOUT_SECONDS` constant
- Polling interval: 1 second (configurable)

### Large Result Sets
- Results are paginated using NextToken
- Memory usage depends on result size
- For very large datasets, increase Lambda memory allocation

### Concurrent Executions
- Both functions can run concurrently
- No locking mechanism (safe for parallel execution)
- Monitor Redshift query queue during heavy load

---

## Version History

- **1.0.0** (Initial Release):
  - Dashboard Extractor with 5 extraction types
  - CloudWatch Metrics Publisher
  - Redshift Data API integration
  - S3 publishing
  - Comprehensive error handling
