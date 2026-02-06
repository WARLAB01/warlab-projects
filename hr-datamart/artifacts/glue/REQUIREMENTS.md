# AWS Glue ETL - Requirements and Prerequisites

Complete list of requirements for deploying and running the HR Datamart Glue ETL pipeline.

## AWS Account Requirements

### AWS Services Required

- **AWS Glue**: For ETL job execution
- **Amazon S3**: For source data and temporary storage
- **Amazon Redshift**: For data warehouse target
- **CloudWatch**: For logging and monitoring
- **AWS IAM**: For permissions and access control
- **AWS Secrets Manager** (optional): For credentials management

### AWS Service Quotas

Verify these quotas are sufficient:

```
AWS Glue:
  - Glue jobs per account: Default 10 (request increase to 20+)
  - Concurrent job runs: Default 100
  - Triggers per workflow: No limit

Amazon S3:
  - Buckets per account: Default 100
  - Object size limit: 5TB max
  - Request rate: Sufficient for daily ETL loads

Amazon Redshift:
  - Cluster nodes: At least 1 (4+ recommended for production)
  - Database size: Sufficient for staging tables
  - Connection slots: At least 10 available
```

## AWS Glue Configuration

### Glue Connection: warlab-redshift-connection

Must be configured before deployment:

```
Connection Name: warlab-redshift-connection
Type: Redshift
Host: [Your Redshift cluster endpoint]
Port: 5439 (default)
Database: dev
Username: [Redshift admin user]
Password: [Secure password]
SSL: Enabled (recommended)
```

**To Create:**
1. AWS Glue Console → Connections
2. Create connection → Redshift
3. Fill in cluster details
4. Test connection
5. Create connection

### Glue Service Role

IAM role with name matching `GlueServiceRole-*` or custom name.

**Required Permissions:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::warlab-hr-datamart-dev",
        "arn:aws:s3:::warlab-hr-datamart-dev/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement",
        "redshift-data:GetStatementResult",
        "redshift:DescribeClusters",
        "redshift:DescribeClusterSnapshots",
        "redshift:DescribeTableRestoreStatus"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:CreateNetworkInterfacePermission"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
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

## AWS Infrastructure Setup

### S3 Bucket Structure

```
warlab-hr-datamart-dev/
├── workday/hrdp/
│   ├── int0095e_worker_job/              # Source CSV files
│   ├── int0096_worker_organization/
│   ├── int0098_worker_compensation/
│   ├── int270_rescinded/
│   ├── int6020_grade_profile/
│   ├── int6021_job_profile/
│   ├── int6022_job_classification/
│   ├── int6023_location/
│   ├── int6024_company/
│   ├── int6025_cost_center/
│   ├── int6028_department_hierarchy/
│   └── int6032_positions/
├── glue-scripts/                         # ETL script location
│   └── glue_s3_to_l1_etl.py
├── glue-temp/                            # Temporary Glue working directory
│   └── [Glue temporary files]
└── spark-logs/                           # Spark event logs
    └── [Spark logs]
```

**S3 Bucket Configuration:**

```bash
# Enable versioning (recommended)
aws s3api put-bucket-versioning \
    --bucket warlab-hr-datamart-dev \
    --versioning-configuration Status=Enabled

# Enable server-side encryption
aws s3api put-bucket-encryption \
    --bucket warlab-hr-datamart-dev \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            }
        }]
    }'

# Block public access (security)
aws s3api put-public-access-block \
    --bucket warlab-hr-datamart-dev \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

### Redshift Setup

**Create L1 Staging Schema:**

```sql
-- Connect as admin user
psql -h your-cluster.redshift.amazonaws.com -U admin -d dev

-- Create schema
CREATE SCHEMA IF NOT EXISTS l1_workday;

-- Grant permissions to Glue service user
GRANT CREATE ON SCHEMA l1_workday TO [glue_user];
GRANT USAGE ON SCHEMA l1_workday TO [glue_user];

-- Create tables (example for int6024_company)
CREATE TABLE l1_workday.int6024_company (
    -- Define columns based on your CSV structure
    -- Example columns:
    -- company_id VARCHAR(50),
    -- company_name VARCHAR(255),
    -- country_code VARCHAR(10),
    -- -- ... other columns
    DISTKEY (company_id),
    SORTKEY (company_id)
);

-- Verify schema and tables
SELECT * FROM information_schema.tables
WHERE table_schema = 'l1_workday';
```

## Local Machine Requirements

### Software Installed

- **AWS CLI v2**: For AWS API calls
  ```bash
  # Verify installation
  aws --version
  ```

- **bash 4.0+**: For shell scripts
  ```bash
  # Verify version
  bash --version
  ```

- **jq** (optional): For JSON parsing in scripts
  ```bash
  # Install on macOS
  brew install jq

  # Install on Linux
  apt-get install jq
  ```

- **psql** (optional): For Redshift connectivity testing
  ```bash
  # Install on macOS
  brew install postgresql

  # Install on Linux
  apt-get install postgresql-client
  ```

### AWS CLI Configuration

```bash
# Configure AWS credentials
aws configure --profile warlab

# Enter:
# AWS Access Key ID: [your-access-key]
# AWS Secret Access Key: [your-secret-key]
# Default region: us-east-1
# Default output format: json

# Verify configuration
aws sts get-caller-identity --profile warlab
```

**Expected Output:**
```json
{
    "UserId": "AIDAI...",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/your-user"
}
```

### SSH Access (if needed)

For bastion/jump host access to Redshift:

```bash
# Add SSH key to .ssh
chmod 600 ~/.ssh/glue-deployment-key.pem

# Test SSH connection
ssh -i ~/.ssh/glue-deployment-key.pem ec2-user@bastion.example.com
```

## Data Requirements

### Source CSV Files

All 12 source files must exist in S3 with these specifications:

**Format:**
- **Delimiter**: Pipe character (`|`)
- **Header**: First row contains column names
- **Encoding**: UTF-8
- **Line Endings**: Unix (LF) preferred

**Example Format:**
```csv
worker_id|first_name|last_name|email|hire_date|department
EMP001|John|Doe|john.doe@company.com|2020-01-15|Engineering
EMP002|Jane|Smith|jane.smith@company.com|2020-02-20|Sales
```

**File Size Expectations:**
- Small: < 100MB (load in 2-3 minutes)
- Medium: 100MB - 1GB (load in 5-10 minutes)
- Large: > 1GB (load in 15-30 minutes)

### Redshift Table Schema

Target tables must be created in `l1_workday` schema before Glue jobs run.

**Minimum Requirements:**
- Tables can have any column structure
- Column names should match CSV headers
- Data types can be all VARCHAR for flexibility (Glue handles type conversion)

**Recommended Approach:**
```sql
-- Simple approach: all columns as VARCHAR
CREATE TABLE l1_workday.int6024_company (
    -- Define one column per CSV field
    -- Example based on likely structure:
    company_code VARCHAR(50),
    company_name VARCHAR(500),
    country_code VARCHAR(10),
    currency_code VARCHAR(10),
    -- ... additional fields from CSV
    DISTKEY (company_code),
    SORTKEY (company_code)
);
```

## Network Requirements

### Security Group Configuration

If Glue jobs are in a VPC:

**Inbound Rules for Glue Cluster:**
- Port 5439 (Redshift): From Glue security group

**Outbound Rules for Glue Cluster:**
- Port 443 (HTTPS): To AWS services (S3, Redshift)
- Port 5439 (Redshift): To Redshift cluster

**Redshift Cluster Security Group:**
- Port 5439: From Glue security group

### DNS/Network Connectivity

- Glue jobs can reach S3 (same AWS region is fastest)
- Glue jobs can reach Redshift cluster
- Glue jobs can reach AWS services (CloudWatch, CloudTrail)

**Test Connectivity:**
```bash
# Test S3 access
aws s3 ls s3://warlab-hr-datamart-dev/workday/hrdp/ --region us-east-1

# Test Redshift (from Glue context)
# This is tested during first job run
```

## Deployment Environment

### Recommended AWS Region

- **Primary**: us-east-1
- **Alternatives**: us-west-2, eu-west-1

All resources should be in the same region:
- Glue jobs: us-east-1
- S3 bucket: us-east-1
- Redshift cluster: us-east-1

## Testing Requirements

Before production deployment:

1. **AWS CLI Access Test**
   ```bash
   aws glue list-jobs --region us-east-1 --profile warlab
   ```

2. **S3 Access Test**
   ```bash
   aws s3 ls s3://warlab-hr-datamart-dev/workday/hrdp/ --region us-east-1 --profile warlab
   ```

3. **Redshift Connectivity Test**
   ```bash
   psql -h cluster-endpoint.redshift.amazonaws.com -U admin -d dev \
       -c "SELECT 1"
   ```

4. **Glue Connection Test**
   ```bash
   aws glue get-connection --name warlab-redshift-connection \
       --region us-east-1 --profile warlab
   ```

## Scalability Limits

### Concurrent Executions

- **Max jobs per workflow**: No limit
- **Default concurrent job runs per account**: 100
- **Recommended**: Start with 12 jobs running in parallel

### Data Volume Limits

| Metric | Limit | Notes |
|--------|-------|-------|
| Max S3 object size | 5TB | Per CSV file |
| Max Redshift table size | PB+ | Depends on cluster |
| Glue job runtime | Hours | 48 hour max |
| Glue DPU max | 100 DPUs | For single job |

### Cost Implications

- **10 DPUs per job × 12 jobs**: ~$52/month if each runs 1 hour
- **20 DPUs per job × 12 jobs**: ~$105/month if each runs 1 hour
- **Redshift**: Cluster cost (separate from Glue)
- **S3**: Data transfer cost (minimal if same region)

## Compliance and Security Requirements

- [ ] Data encryption in transit (SSL/TLS)
- [ ] Data encryption at rest (S3, Redshift)
- [ ] IAM role least privilege principle
- [ ] CloudTrail enabled for audit logging
- [ ] VPC endpoints for private connectivity (optional)
- [ ] Data retention policies defined
- [ ] Access control lists in place

## Monitoring and Alerting Setup

Required before production:

1. **CloudWatch Dashboards**
   ```bash
   # Create dashboard for Glue metrics
   ```

2. **SNS Topics for Failures**
   ```bash
   aws sns create-topic --name warlab-glue-failures
   ```

3. **CloudWatch Alarms**
   ```bash
   # Job failure alarm
   # Job timeout alarm
   # Redshift connection failure alarm
   ```

## Documentation and Handoff

Required documentation:

- [ ] Data dictionary for all 12 source tables
- [ ] ETL process diagram
- [ ] Runbook for common issues
- [ ] Escalation contact list
- [ ] Backup and recovery procedures
- [ ] Change management process

---

**Checklist Summary:**

Before running deployment script:
- [ ] AWS account configured with CLI
- [ ] S3 bucket created and structured
- [ ] Redshift cluster running with l1_workday schema
- [ ] Glue connection configured
- [ ] IAM role with proper permissions
- [ ] All 12 source CSV files in S3
- [ ] Target tables created in Redshift

After deployment:
- [ ] Dry-run successful
- [ ] First test workflow run successful
- [ ] Data verified in Redshift
- [ ] CloudWatch logs accessible
- [ ] Alerts configured

**Last Updated:** 2026-02-06
