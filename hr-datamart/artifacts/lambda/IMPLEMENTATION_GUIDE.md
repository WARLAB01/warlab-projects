# People Analytics Dashboard Lambda Functions - Implementation Guide

## Overview

This implementation provides two AWS Lambda functions for the People Analytics Dashboard:

1. **Dashboard Extractor** (`dashboard_extractor/lambda_function.py`) - Parameterized extraction layer
2. **CloudWatch Metrics Publisher** (`cloudwatch_publisher/lambda_function.py`) - Metrics publishing layer

Both functions use the Redshift Data API for querying and are designed for production deployment with comprehensive error handling, logging, and monitoring.

## File Structure

```
lambda/
├── README.md                                    # Deployment and usage guide
├── IMPLEMENTATION_GUIDE.md                      # This file
├── config.py                                    # Shared configuration
├── test_functions.py                            # Test cases and examples
├── dashboard_extractor/
│   └── lambda_function.py                       # Main extractor function
└── cloudwatch_publisher/
    └── lambda_function.py                       # Metrics publisher function
```

## Architecture Overview

### Data Flow

```
                    ┌─────────────────────────────────┐
                    │   Redshift Data API             │
                    │  (warlab-hr-datamart cluster)  │
                    └────────────┬────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
         ┌──────────▼──────────┐   ┌──────────▼──────────┐
         │ Dashboard Extractor │   │ CloudWatch Metrics  │
         │    Lambda (5 types) │   │  Publisher Lambda   │
         └──────────┬──────────┘   └──────────┬──────────┘
                    │                         │
         ┌──────────▼──────────┐   ┌──────────▼──────────┐
         │  S3 JSON Output     │   │  CloudWatch Metrics │
         │  (warlab-hr-       │   │  (WarLabHRDashboard│
         │   dashboard bucket) │   │       namespace)    │
         └─────────────────────┘   └────────────────────┘
```

## Function Specifications

### 1. Dashboard Extractor Lambda

**Purpose**: Extract various HR analytics datasets from Redshift and publish to S3

**Handler**: `lambda_function.lambda_handler`

**Event Parameter**:
```json
{"extraction": "extraction_type"}
```

**Supported Extraction Types**:

| Type | Purpose | Output | Rows |
|------|---------|--------|------|
| `kpi_summary` | Key performance indicators | Single metrics object | 1 |
| `headcount` | Headcount by dimensions | Company, dept, location, trend | Variable |
| `movements` | Employee movements | By type, terminations, trends | Variable |
| `compensation` | Pay data | By grade, by job family | Variable |
| `org_health` | Org structure | Departments, managers, locations | Variable |

**Output Location**: `s3://warlab-hr-dashboard/data/{extraction_name}.json`

**Key Classes**:
- `RedshiftQueryExecutor` - Manages Redshift Data API calls
- `DashboardDataExtractor` - Orchestrates extraction logic
- `S3DataPublisher` - Handles S3 publishing
- `CloudWatchMetricsPublisher` - Publishes KPI metrics

**SQL Query Details**:

All queries target the `l3_workday` schema on the `warlab-hr-datamart` cluster.

#### KPI Summary Queries:
- Total active headcount: `COUNT(DISTINCT employee_id)` from `fct_worker_headcount_restat_f` at max snapshot
- Total movements: `COUNT(*)` from `fct_worker_movement_f`
- Average base pay: `AVG(base_pay)` from `dim_worker_job_d` where current
- Active companies: `COUNT(DISTINCT company_id)` from `dim_company_d` where current
- Active departments: `COUNT(DISTINCT department_id)` from `dim_department_d` where current

#### Headcount Queries:
- By company: `fct_worker_headcount_restat_f` joined to `dim_company_d`
- By department: `fct_worker_headcount_restat_f` joined to `dim_department_d`
- By location: `fct_worker_headcount_restat_f` joined to `dim_location_d`
- Trend: `GROUP BY snapshot_date`

#### Movement Queries:
- By type: Separate counts for job_change, location_change, compensation_change
- Terminations: Split by `is_regrettable_termination` flag
- Trend: `DATE_TRUNC('month')` grouping with last 12 months

#### Compensation Queries:
- By grade: `dim_worker_job_d` joined to `dim_grade_profile_d`
- By job family: `dim_worker_job_d` joined to `dim_job_profile_d`
- Includes percentile calculations for medians

#### Org Health Queries:
- Departments: Count and employee size per department
- Manager span: Direct reports per manager from `dim_worker_job_d`
- Locations: Worker distribution by `dim_location_d`
- Worker types: Counts by `dim_worker_type_d`

**CloudWatch Metrics Published** (KPI summary only):
- `ActiveHeadcount` (Count)
- `TotalMovements` (Count)
- `AvgBasePay` (None)
- `ActiveCompanies` (Count)
- `ActiveDepartments` (Count)

### 2. CloudWatch Metrics Publisher Lambda

**Purpose**: Query KPI metrics from Redshift and publish to CloudWatch

**Handler**: `lambda_function.lambda_handler`

**Event Parameter**: None required (can be invoked with empty `{}`)

**Typical Schedule**: CloudWatch Events rule running hourly or on business hours

**Key Classes**:
- `RedshiftQueryExecutor` - Manages Redshift Data API calls
- `KPIMetricsExtractor` - Extracts metrics from Redshift
- `CloudWatchMetricsPublisher` - Publishes to CloudWatch

**Metrics Published** (to `WarLabHRDashboard` namespace):
- `ActiveHeadcount` - Total active employees
- `TotalMovements` - Total movements recorded
- `AvgBasePay` - Average base pay
- `ActiveCompanies` - Number of companies
- `ActiveDepartments` - Number of departments

**Features**:
- Individual error handling per metric (one failure doesn't stop others)
- Batch publishing (20 metrics per API call)
- Timestamped metrics for accurate historical tracking

## Configuration

All configuration is centralized in `config.py`:

```python
CLUSTER_ID = 'warlab-hr-datamart'
DATABASE = 'dev'
DB_USER = 'admin'
SCHEMA = 'l3_workday'
S3_BUCKET = 'warlab-hr-dashboard'
CLOUDWATCH_NAMESPACE = 'WarLabHRDashboard'
QUERY_TIMEOUT_SECONDS = 300
POLL_INTERVAL_SECONDS = 1
```

## Deployment Checklist

### Prerequisites
- [ ] AWS Account with appropriate IAM role
- [ ] Redshift cluster `warlab-hr-datamart` exists and is accessible
- [ ] S3 bucket `warlab-hr-dashboard` exists
- [ ] IAM role has required permissions (see README.md)
- [ ] Python 3.11+ runtime available

### Pre-Deployment
- [ ] Review IAM permission policies
- [ ] Verify Redshift connection parameters
- [ ] Test Redshift queries in Redshift console
- [ ] Create S3 bucket if not exists

### Deployment Steps

1. **Package Dashboard Extractor**:
   ```bash
   cd dashboard_extractor
   zip -r ../dashboard_extractor.zip lambda_function.py
   ```

2. **Package CloudWatch Publisher**:
   ```bash
   cd cloudwatch_publisher
   zip -r ../cloudwatch_publisher.zip lambda_function.py
   ```

3. **Deploy via AWS CLI**:
   ```bash
   # Dashboard Extractor
   aws lambda create-function \
       --function-name dashboard-extractor \
       --runtime python3.11 \
       --role arn:aws:iam::ACCOUNT:role/lambda-execution-role \
       --handler lambda_function.lambda_handler \
       --zip-file fileb://dashboard_extractor.zip \
       --timeout 300 \
       --memory-size 1024

   # CloudWatch Publisher
   aws lambda create-function \
       --function-name cloudwatch-publisher \
       --runtime python3.11 \
       --role arn:aws:iam::ACCOUNT:role/lambda-execution-role \
       --handler lambda_function.lambda_handler \
       --zip-file fileb://cloudwatch_publisher.zip \
       --timeout 300 \
       --memory-size 512
   ```

4. **Create CloudWatch Events Schedule**:
   ```bash
   # Create EventBridge rule for hourly execution
   aws events put-rule \
       --name WarLabHRMetricsPublisher \
       --schedule-expression "rate(1 hour)"

   # Add Lambda as target
   aws events put-targets \
       --rule WarLabHRMetricsPublisher \
       --targets "Id"="1","Arn"="arn:aws:lambda:region:account:function:cloudwatch-publisher","RoleArn"="arn:aws:iam::account:role/service-role/lambda-events-role"
   ```

5. **Post-Deployment**:
   - [ ] Test each extraction type
   - [ ] Verify S3 output
   - [ ] Check CloudWatch metrics appear
   - [ ] Monitor CloudWatch Logs for errors
   - [ ] Set up CloudWatch alarms if needed

## Testing

### Test Suite
Run `python test_functions.py` to see all test cases and examples.

### Manual Testing via AWS Console

1. **Dashboard Extractor - KPI Summary**:
   - Navigate to Lambda console
   - Select `dashboard-extractor` function
   - Create test event:
     ```json
     {"extraction": "kpi_summary"}
     ```
   - Click Test
   - Review CloudWatch Logs

2. **All Extraction Types**:
   - Repeat above with events:
     - `{"extraction": "headcount"}`
     - `{"extraction": "movements"}`
     - `{"extraction": "compensation"}`
     - `{"extraction": "org_health"}`

3. **CloudWatch Publisher**:
   - Navigate to Lambda console
   - Select `cloudwatch-publisher` function
   - Create test event: `{}`
   - Click Test
   - Navigate to CloudWatch > Metrics > WarLabHRDashboard
   - Verify 5 metrics appear

### Manual Testing via AWS CLI

```bash
# Test KPI summary
aws lambda invoke \
    --function-name dashboard-extractor \
    --payload '{"extraction": "kpi_summary"}' \
    response.json

# Test all extractions
for extraction in kpi_summary headcount movements compensation org_health; do
    aws lambda invoke \
        --function-name dashboard-extractor \
        --payload "{\"extraction\": \"$extraction\"}" \
        response_$extraction.json
done

# Test CloudWatch publisher
aws lambda invoke \
    --function-name cloudwatch-publisher \
    response.json
```

### Verify Output

```bash
# Check S3 output
aws s3 ls s3://warlab-hr-dashboard/data/

# Download specific extraction
aws s3 cp s3://warlab-hr-dashboard/data/kpi_summary.json - | jq .

# Check CloudWatch metrics
aws cloudwatch list-metrics --namespace WarLabHRDashboard
```

## Monitoring

### CloudWatch Logs
- **Log Group**: `/aws/lambda/dashboard-extractor`
- **Log Group**: `/aws/lambda/cloudwatch-publisher`
- **Log Level**: INFO (all queries, status updates, errors)

### CloudWatch Metrics
- **Namespace**: `WarLabHRDashboard`
- **Metrics**: 5 KPI metrics (see above)
- **Retention**: Default (15 months)

### CloudWatch Alarms (Recommended)

```bash
# Alarm for Lambda errors
aws cloudwatch put-metric-alarm \
    --alarm-name dashboard-extractor-errors \
    --alarm-description "Alert if dashboard extractor has errors" \
    --metric-name Errors \
    --namespace AWS/Lambda \
    --statistic Sum \
    --period 300 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --evaluation-periods 1

# Alarm for Lambda duration
aws cloudwatch put-metric-alarm \
    --alarm-name dashboard-extractor-duration \
    --alarm-description "Alert if dashboard extractor takes too long" \
    --metric-name Duration \
    --namespace AWS/Lambda \
    --statistic Maximum \
    --period 300 \
    --threshold 250000 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1

# Alarm for active headcount anomaly
aws cloudwatch put-metric-alarm \
    --alarm-name headcount-anomaly-detector \
    --alarm-description "Alert on unusual headcount changes" \
    --comparison-operator LessThanLowerOrGreaterThanUpperThreshold \
    --evaluation-periods 2 \
    --metrics '[{"Id": "m1", "ReturnData": true, "MetricStat": {"Metric": {"Namespace": "WarLabHRDashboard", "MetricName": "ActiveHeadcount"}, "Period": 3600, "Stat": "Average"}}]' \
    --threshold-metric-id e1
```

## Troubleshooting

### Common Issues

**Issue**: Query Timeout (504 error)
- **Cause**: Redshift query running > 300 seconds
- **Solution**:
  - Increase Lambda timeout to 600 seconds
  - Check Redshift cluster performance
  - Optimize query (add indexes, etc.)
  - Check if cluster is paused

**Issue**: S3 Access Denied
- **Cause**: IAM role missing S3 permissions
- **Solution**:
  - Verify IAM policy includes `s3:PutObject` on `warlab-hr-dashboard` bucket
  - Check bucket policy if using bucket policies
  - Verify bucket exists

**Issue**: Redshift Connection Error
- **Cause**: Invalid cluster, database, or user
- **Solution**:
  - Verify CLUSTER_ID: `warlab-hr-datamart`
  - Verify DATABASE: `dev`
  - Verify DB_USER: `admin`
  - Test connection from Redshift console

**Issue**: CloudWatch Metrics Not Appearing
- **Cause**: Metrics published but not visible yet
- **Solution**:
  - Wait 1-2 minutes for metrics to appear
  - Check CloudWatch namespace: `WarLabHRDashboard`
  - Verify metrics are being published (check logs)
  - Confirm IAM role has `cloudwatch:PutMetricData`

**Issue**: Incomplete Extractions
- **Cause**: Partial failure or malformed data
- **Solution**:
  - Check CloudWatch Logs for specific error
  - Verify all required tables exist in Redshift
  - Verify table columns match query expectations
  - Re-run extraction

### Debug Commands

```bash
# View recent logs
aws logs tail /aws/lambda/dashboard-extractor --follow

# Get specific log events
aws logs filter-log-events \
    --log-group-name /aws/lambda/dashboard-extractor \
    --start-time $(date -d '1 hour ago' +%s)000

# Check function configuration
aws lambda get-function-configuration \
    --function-name dashboard-extractor

# Check recent invocations
aws lambda list-function-event-invoke-configs \
    --function-name dashboard-extractor

# Get dead-letter queue configuration
aws lambda get-function-concurrency \
    --function-name dashboard-extractor
```

## Performance Tuning

### Lambda Memory Allocation
- **Dashboard Extractor**: 1024 MB (recommended for large extractions)
- **CloudWatch Publisher**: 512 MB (sufficient for 5 metrics)

### Lambda Timeout
- **Dashboard Extractor**: 300 seconds (5 minutes) - increase to 600 if needed
- **CloudWatch Publisher**: 300 seconds (5 minutes)

### Query Optimization
- Indexes on foreign keys in dimension tables
- Partition pruning on `snapshot_date` in fact tables
- Use LIMIT clauses where possible
- Pre-aggregate in separate queries for large result sets

### Concurrent Execution
- Set reserved concurrency to prevent runaway execution
- Monitor queue depth during peak hours
- Scale Redshift cluster during high load periods

## Cost Estimation

### Monthly Costs (Example)

**Dashboard Extractor** (5 types, hourly):
- 5 extractions × 24 hours = 120 invocations/day
- 120 × 30 = 3,600 invocations/month
- Per invocation: ~0.00001667 (0.001 GB-second × 0.0000166667 $/GB-second)
- Monthly: $0.06 (compute) + Redshift query costs

**CloudWatch Publisher** (hourly):
- 1 invocation × 24 hours = 24 invocations/day
- 24 × 30 = 720 invocations/month
- Per invocation: ~0.00000834
- Monthly: $0.01 (compute) + Redshift query costs

**S3 Costs**:
- 5 extractions × 30 days = 150 writes/month
- Assuming ~1 MB per file: $0.02/month storage

**Redshift Costs**:
- Primary cost driver
- Depends on cluster size and query complexity
- Estimated: $X-XX/month (based on your cluster size)

**Total Estimated Monthly**: $XX-XXX (depending on Redshift)

## Security Considerations

1. **IAM Principle of Least Privilege**:
   - Lambda role should only access required resources
   - Use resource-specific ARNs in bucket policies
   - Don't grant S3 access to all buckets

2. **Redshift Security**:
   - DB user should be read-only account
   - Consider row-level security if sensitive data
   - Encrypt connections (SSL/TLS)
   - Audit query logging in Redshift

3. **S3 Security**:
   - Enable versioning on S3 bucket
   - Enable server-side encryption
   - Restrict bucket access to required IAM roles
   - Enable MFA delete for production

4. **Secrets Management**:
   - Current implementation uses IAM role (no secrets needed)
   - If using passwords, store in AWS Secrets Manager
   - Rotate credentials regularly

5. **VPC Considerations**:
   - If Redshift is in VPC, Lambda must be in same VPC
   - Configure security groups appropriately
   - Use VPC endpoints for S3 if required

## Version History

- **1.0.0** (2024-01-15)
  - Initial release
  - 5 extraction types (KPI, headcount, movements, compensation, org_health)
  - Redshift Data API integration
  - S3 publishing
  - CloudWatch metrics
  - Comprehensive error handling
  - Production-ready code

## Support & Maintenance

### Ongoing Maintenance Tasks
- Monitor CloudWatch Logs for errors weekly
- Check CloudWatch Metrics for anomalies weekly
- Update Lambda functions when new HR data sources added
- Performance tune Redshift queries as data grows
- Review IAM permissions quarterly

### Extending the Functions

To add a new extraction type:

1. Add method to `DashboardDataExtractor` class
2. Update `extraction_methods` dictionary in `lambda_handler`
3. Add test case to `test_functions.py`
4. Update documentation
5. Deploy updated function

To add a new CloudWatch metric:

1. Add metric extraction to `KPIMetricsExtractor.extract_kpi_metrics()`
2. Add metric definition to `CLOUDWATCH_METRICS` in `config.py`
3. Add unit to metric_units dictionary in `CloudWatchMetricsPublisher`
4. Update documentation
5. Deploy updated function

## Contact & Resources

- AWS Lambda Documentation: https://docs.aws.amazon.com/lambda/
- AWS Redshift Data API: https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html
- AWS CloudWatch: https://docs.aws.amazon.com/cloudwatch/
- boto3 Documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
