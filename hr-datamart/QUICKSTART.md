# HR Datamart - Quick Start Deployment Guide

This guide walks you through deploying the HR Datamart project end-to-end: from generating synthetic HR data to loading a fully operational Redshift data warehouse with a star schema analytics layer. The entire process can be automated with `deploy.sh` or executed step-by-step for learning and troubleshooting.

---

## Prerequisites

Before starting, ensure you have:

- **AWS Account** with permissions to create:
  - Redshift clusters (or access to an existing cluster)
  - S3 buckets
  - IAM roles and policies
  - (Optional) Glue jobs for automated pipelines

- **AWS CLI** installed and configured with credentials
  - Installation: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
  - Verify: `aws --version`

- **Python 3.9+** (for data generation)
  - Verify: `python3 --version`
  - Required package: `pg8000` (for Redshift connectivity)

- **PostgreSQL Client (psql)** for Redshift interaction
  - macOS: `brew install postgresql`
  - Ubuntu/Debian: `apt-get install postgresql-client`
  - Windows: Install PostgreSQL and add to PATH
  - Verify: `psql --version`

- **Git** for repository access
  - Verify: `git --version`

---

## Step 0: Clone the Repository

Clone the WARLAB projects repository and navigate to the HR Datamart directory:

```bash
git clone https://github.com/WARLAB01/warlab-projects.git
cd warlab-projects/hr-datamart
```

Verify the directory structure:

```bash
ls -la
# Expected output:
# drwxr-xr-x  artifacts/       # SQL, Python, and data generation scripts
# -rw-r--r--  deploy.sh        # Automated deployment script
# -rw-r--r--  QUICKSTART.md    # This file
```

---

## Step 1: AWS CLI Setup

### 1.1 Install AWS CLI

If not already installed, install via pip:

```bash
pip install awscli
```

Or download the installer: https://aws.amazon.com/cli/

### 1.2 Configure AWS Credentials

Run the AWS CLI configuration command:

```bash
aws configure
```

You will be prompted for:

```
AWS Access Key ID [None]: <YOUR_ACCESS_KEY>
AWS Secret Access Key [None]: <YOUR_SECRET_KEY>
Default region name [None]: us-east-1
Default output format [None]: json
```

**Note:** If you don't have access keys, generate them in the AWS IAM Console:
1. Navigate to https://console.aws.amazon.com/iam/
2. Select Users → Your User → Security Credentials
3. Click "Create access key" and save the credentials securely

### 1.3 Test AWS Credentials

Verify your AWS credentials are configured correctly:

```bash
aws sts get-caller-identity
```

Expected output:
```json
{
    "UserId": "AIDASAMPLEUSERID",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/your-username"
}
```

---

## Step 2: Set Environment Variables

Set all required environment variables in your shell. These are used by both the deployment script and manual steps.

```bash
# S3 Configuration
export S3_BUCKET="hr-datamart-dev"

# Redshift Configuration
export REDSHIFT_HOST="your-cluster.abc123.us-east-1.redshift.amazonaws.com"
export REDSHIFT_PORT="5439"
export REDSHIFT_DB="dev"
export REDSHIFT_USER="admin"
export REDSHIFT_PASSWORD="YourSecurePassword123!"

# IAM Role for Redshift → S3 Access (for COPY commands)
export REDSHIFT_IAM_ROLE_ARN="arn:aws:iam::123456789012:role/RedshiftS3Role"

# Data Loading Parameters
export DATA_DATE="2026-02-05"
export ETL_BATCH_ID="BATCH_$(date +%Y%m%d)_001"
```

### Example Environment Setup Script

Create a file `~/.hr-datamart.env` for convenience:

```bash
#!/bin/bash
export S3_BUCKET="hr-datamart-dev"
export REDSHIFT_HOST="your-cluster.abc123.us-east-1.redshift.amazonaws.com"
export REDSHIFT_PORT="5439"
export REDSHIFT_DB="dev"
export REDSHIFT_USER="admin"
export REDSHIFT_PASSWORD="YourSecurePassword123!"
export REDSHIFT_IAM_ROLE_ARN="arn:aws:iam::123456789012:role/RedshiftS3Role"
export DATA_DATE="2026-02-05"
export ETL_BATCH_ID="BATCH_$(date +%Y%m%d)_001"
```

Then source it before running deployment:

```bash
source ~/.hr-datamart.env
```

### Variable Reference

| Variable | Required | Example | Description |
|----------|----------|---------|-------------|
| `S3_BUCKET` | Yes | `hr-datamart-dev` | S3 bucket for CSV inbound files |
| `REDSHIFT_HOST` | Yes | `redshift.abc123.us-east-1.redshift.amazonaws.com` | Redshift cluster endpoint |
| `REDSHIFT_PORT` | No | `5439` | Redshift port (default: 5439) |
| `REDSHIFT_DB` | Yes | `dev` | Target database name |
| `REDSHIFT_USER` | Yes | `admin` | Redshift user (must have CREATE SCHEMA permission) |
| `REDSHIFT_PASSWORD` | Yes | `SecurePassword123!` | Redshift password |
| `REDSHIFT_IAM_ROLE_ARN` | Yes | `arn:aws:iam::123456789012:role/RedshiftS3Role` | IAM role ARN with S3 read access |
| `DATA_DATE` | No | `2026-02-05` | Data date for partition (default: today) |
| `ETL_BATCH_ID` | No | `BATCH_20260205_001` | Batch identifier (auto-generated if not set) |

---

## Step 3: Create AWS Resources (if needed)

If you don't have existing S3 and Redshift resources, create them using AWS CLI.

### 3.1 Create S3 Bucket

```bash
aws s3 mb s3://${S3_BUCKET} --region us-east-1
```

Verify:
```bash
aws s3 ls s3://${S3_BUCKET}
```

### 3.2 Create IAM Role for Redshift S3 Access

Create an IAM policy document (`redshift-s3-policy.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${S3_BUCKET}",
        "arn:aws:s3:::${S3_BUCKET}/*"
      ]
    }
  ]
}
```

Create the role:

```bash
# Create trust policy (allows Redshift to assume the role)
cat > trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create the role
aws iam create-role \
  --role-name RedshiftS3Role \
  --assume-role-policy-document file://trust-policy.json

# Create the policy
aws iam put-role-policy \
  --role-name RedshiftS3Role \
  --policy-name S3AccessPolicy \
  --policy-document file://redshift-s3-policy.json

# Verify the role ARN
aws iam get-role --role-name RedshiftS3Role --query 'Role.Arn' --output text
```

Output: `arn:aws:iam::123456789012:role/RedshiftS3Role` (update `REDSHIFT_IAM_ROLE_ARN` with this value)

### 3.3 Create Redshift Cluster (Optional)

If you don't have a Redshift cluster, create one:

```bash
aws redshift create-cluster \
  --cluster-identifier hr-datamart-dev \
  --node-type dc2.large \
  --master-username admin \
  --master-user-password 'YourSecurePassword123!' \
  --number-of-nodes 2 \
  --db-name dev \
  --publicly-accessible
```

**Note:** This takes 10-15 minutes. Check status with:

```bash
aws redshift describe-clusters \
  --cluster-identifier hr-datamart-dev \
  --query 'Clusters[0].ClusterStatus'
```

Wait for status to be `"available"`.

### 3.4 Verify psql Installation

Ensure PostgreSQL client is installed:

```bash
# macOS
brew install postgresql

# Ubuntu/Debian
sudo apt-get install postgresql-client

# Verify
psql --version
```

---

## Step 4: Generate Synthetic Test Data

Generate realistic HR feed data using the Python data generator:

```bash
cd artifacts/data_gen
python3 generate_all_feeds.py
```

The generator creates CSV files in the `output/csv/` directory with the naming pattern:

```
workday.hrdp.dly_<FEED_NAME>.full.<TIMESTAMP>.csv
```

Example output files:
```
workday.hrdp.dly_grade_profile.full.20260205060000.csv
workday.hrdp.dly_job_profile.full.20260205060000.csv
workday.hrdp.dly_worker_job.full.20260205060000.csv
...
```

**Note:** The generator uses an `OUTPUT_DIR` environment variable. The `deploy.sh` script automatically sets this to `./output/csv/`, but you can override it:

```bash
OUTPUT_DIR="/custom/path" python3 generate_all_feeds.py
```

---

## Step 5: Upload Data to S3

### 5.1 Manual Upload (Step-by-Step)

Map each CSV file to its corresponding S3 location using the following mapping table:

| Generator CSV | S3 Feed Path | L1 Table |
|---|---|---|
| `dly_grade_profile` | `int6020_grade_profile` | `l1_workday.int6020_grade_profile` |
| `dly_job_profile` | `int6021_job_profile` | `l1_workday.int6021_job_profile` |
| `dly_job_classification` | `int6022_job_classification` | `l1_workday.int6022_job_classification` |
| `dly_location` | `int1000_location` | `l1_workday.int1000_location` |
| `dly_company` | `int1010_company` | `l1_workday.int1010_company` |
| `dly_cost_center` | `int1020_cost_center` | `l1_workday.int1020_cost_center` |
| `dly_department_hierarchy` | `int1030_department_hierarchy` | `l1_workday.int1030_department_hierarchy` |
| `dly_positions` | `int9000_positions` | `l1_workday.int9000_positions` |
| `dly_worker_job` | `int6060_worker_job` | `l1_workday.int6060_worker_job` |
| `dly_worker_organization` | `int6080_worker_organization` | `l1_workday.int6080_worker_organization` |
| `dly_worker_compensation` | `int6070_worker_compensation` | `l1_workday.int6070_worker_compensation` |
| `dly_rescinded` | `int270_rescinded` | `l1_workday.int270_rescinded` |

Upload each CSV with the proper S3 path structure:

```bash
# Example: Upload grade_profile
aws s3 cp artifacts/data_gen/output/csv/workday.hrdp.dly_grade_profile.full.20260205060000.csv \
  s3://${S3_BUCKET}/workday/hrdp/int6020_grade_profile/dt=${DATA_DATE}/

# Repeat for each CSV file with proper mapping
```

### 5.2 Automated Upload

The `deploy.sh` script handles all uploads automatically. See Step 14 for details.

---

## Step 6: Connect to Redshift & Create Schemas

### 6.1 Test Connection

Verify you can connect to Redshift:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -c "SELECT 1;"
```

Expected output: `?column? = 1`

### 6.2 Create Schemas

Create the L1 (staging) and L3 (analytics) schemas:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  << 'EOF'
CREATE SCHEMA IF NOT EXISTS l1_workday;
CREATE SCHEMA IF NOT EXISTS l3_workday;
\dt
EOF
```

Expected output shows two schemas created.

---

## Step 7: Create L1 Tables (Staging Layer)

Deploy the L1 schema DDL which creates all staging tables:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f artifacts/ddl/l1/l1_schema_ddl.sql
```

Verify table creation:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -c "SELECT table_name FROM information_schema.tables WHERE table_schema='l1_workday' ORDER BY table_name;"
```

Expected output: ~12 staging tables (int6020_grade_profile, int6021_job_profile, etc.)

---

## Step 8: Load L1 Data (COPY from S3)

The L1 COPY statements use environment variable placeholders for S3 bucket, IAM role, and data date.

### 8.1 Prepare SQL with Variable Substitution

Use `envsubst` to substitute environment variables in the SQL template:

```bash
# Create a modified copy of the COPY statements with variables substituted
envsubst < artifacts/dml/l1_copy/l1_copy_statements.sql > /tmp/l1_copy_substituted.sql

# Verify substitution worked
grep "s3://" /tmp/l1_copy_substituted.sql | head -1
# Should show actual bucket name, not ${S3_BUCKET}
```

### 8.2 Execute COPY Commands

Load all L1 tables from S3:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f /tmp/l1_copy_substituted.sql
```

Monitor the output for COPY command results.

### 8.3 Verify L1 Load

Count rows in a few L1 tables:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  << 'EOF'
SELECT 'int6020_grade_profile' AS table_name, COUNT(*) AS row_count FROM l1_workday.int6020_grade_profile
UNION ALL
SELECT 'int6021_job_profile', COUNT(*) FROM l1_workday.int6021_job_profile
UNION ALL
SELECT 'int1000_location', COUNT(*) FROM l1_workday.int1000_location
ORDER BY table_name;
EOF
```

Expected output: Multiple tables with row counts > 0

---

## Step 9: Create L3 Tables (Analytics Layer)

Deploy the L3 star schema in the correct order: Source tables → Dimensions → Facts

### 9.1 Deploy L3 Source DDL

Create source tables (mapping layer between L1 and dimensions):

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f artifacts/ddl/l3_source/l3_source_ddl.sql
```

### 9.2 Deploy L3 Dimension DDL

Create dimension tables (slowly changing dimension Type 2):

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f artifacts/ddl/l3_star/l3_dim_ddl.sql
```

### 9.3 Deploy L3 Fact DDL

Create fact tables:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f artifacts/ddl/l3_star/l3_fact_ddl.sql
```

### 9.4 Verify L3 Tables

List all L3 tables created:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -c "SELECT table_name FROM information_schema.tables WHERE table_schema='l3_workday' ORDER BY table_name;"
```

Expected output: Source tables, Dimensions (dim_*), and Facts (fct_*)

---

## Step 10: Load L3 Source Tables

Transform and load L1 staging data into L3 source tables:

```bash
envsubst < artifacts/dml/l3_source_load/l3_source_load.sql > /tmp/l3_source_load_substituted.sql

PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f /tmp/l3_source_load_substituted.sql
```

---

## Step 11: Load L3 Dimensions

Load dimension tables with SCD2 logic. **Dimensions must load before facts**:

```bash
envsubst < artifacts/dml/l3_dim_load/l3_dim_load.sql > /tmp/l3_dim_load_substituted.sql

PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f /tmp/l3_dim_load_substituted.sql
```

Verify dimension row counts:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  << 'EOF'
SELECT table_name, COUNT(*) AS row_count
FROM (
  SELECT 'dim_worker_d' AS table_name, COUNT(*) FROM l3_workday.dim_worker_d
  UNION ALL
  SELECT 'dim_job_d', COUNT(*) FROM l3_workday.dim_job_d
  UNION ALL
  SELECT 'dim_organization_d', COUNT(*) FROM l3_workday.dim_organization_d
) t
ORDER BY table_name;
EOF
```

---

## Step 12: Load L3 Facts

Load fact tables (only after dimensions are complete):

```bash
envsubst < artifacts/dml/l3_fact_load/l3_fact_load.sql > /tmp/l3_fact_load_substituted.sql

PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f /tmp/l3_fact_load_substituted.sql
```

Verify fact row counts:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  << 'EOF'
SELECT table_name, COUNT(*) AS row_count
FROM (
  SELECT 'fct_worker_movement_f' AS table_name, COUNT(*) FROM l3_workday.fct_worker_movement_f
  UNION ALL
  SELECT 'fct_worker_compensation_f', COUNT(*) FROM l3_workday.fct_worker_compensation_f
  UNION ALL
  SELECT 'fct_worker_status_f', COUNT(*) FROM l3_workday.fct_worker_status_f
) t
ORDER BY table_name;
EOF
```

---

## Step 13: Run QA Tests

Validate data quality and completeness:

```bash
envsubst < artifacts/qa/qa_tests.sql > /tmp/qa_tests_substituted.sql

PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f /tmp/qa_tests_substituted.sql
```

Review test results:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  << 'EOF'
SELECT test_category, status, COUNT(*) AS test_count
FROM l3_workday.qa_results
GROUP BY test_category, status
ORDER BY test_category, status;
EOF
```

Expected output: All tests should show `status = 'PASS'`

---

## Step 14: Generate Completion Report

Generate a deployment completion report:

```bash
envsubst < artifacts/qa/completion_report.sql > /tmp/completion_report_substituted.sql

PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -f /tmp/completion_report_substituted.sql
```

View the report:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  << 'EOF'
SELECT * FROM l3_workday.deployment_summary;
EOF
```

---

## Automated Option: Use deploy.sh

Rather than executing all steps manually, use the automated deployment script which handles all steps above:

### 15.1 Make the Script Executable

```bash
chmod +x deploy.sh
```

### 15.2 Run the Deployment

Basic usage (uses environment variables from Step 2):

```bash
./deploy.sh
```

With custom data date:

```bash
./deploy.sh --data-date 2026-01-15
```

Skip specific steps:

```bash
# Skip data generation (use existing CSVs)
./deploy.sh --skip-data-gen

# Skip S3 upload (data already in S3)
./deploy.sh --skip-s3-upload

# Only deploy DDL (no data loading)
./deploy.sh --skip-load --skip-qa

# Deploy only DDL and load, skip QA
./deploy.sh --skip-qa
```

### 15.3 View Script Help

```bash
./deploy.sh --help
```

Output shows all available options and examples.

### 15.4 Monitor Deployment

The script creates a detailed log file in the `logs/` directory:

```bash
tail -f logs/deploy_*.log
```

Log shows:
- Step progression and timing
- SQL execution results
- Data load counts
- QA test results
- Any errors or warnings

### 15.5 Deployment Summary

After completion, the script prints a summary:

```
===============================================================================
                    DEPLOYMENT SUMMARY
===============================================================================

Total Steps:        15
Completed Steps:    15
Skipped Steps:      0
Failed Steps:       0

Deployment Date:    2026-02-05
ETL Batch ID:       1707119280
S3 Bucket:          hr-datamart-dev
Redshift Host:      redshift.abc123.us-east-1.redshift.amazonaws.com
Redshift Database:  dev
Log File:           logs/deploy_20260205_143022.log

✓ Deployment completed successfully!
```

---

## Troubleshooting

### Issue: psql command not found

**Solution:**
```bash
# macOS
brew install postgresql

# Ubuntu/Debian
sudo apt-get update && sudo apt-get install postgresql-client

# Windows
Download PostgreSQL installer and add to PATH
```

### Issue: Cannot connect to Redshift

**Diagnosis:**
```bash
# Test connection with verbose output
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  -v ON_ERROR_STOP=1
```

**Solution:**
- Verify cluster is in "available" status: `aws redshift describe-clusters`
- Confirm security group allows inbound traffic on port 5439
- Check credentials in environment variables: `env | grep REDSHIFT`
- Verify you're using the correct cluster endpoint (not the `JDBC` endpoint)

### Issue: COPY command fails with "Insufficient privileges"

**Solution:**
Ensure the Redshift user has been granted access to the S3 role:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  << 'EOF'
GRANT USAGE ON SCHEMA l1_workday TO PUBLIC;
GRANT ALL ON ALL TABLES IN SCHEMA l1_workday TO PUBLIC;
EOF
```

Or ensure the role is attached to the cluster:
```bash
aws redshift modify-cluster-iam-roles \
  --cluster-identifier hr-datamart-dev \
  --add-iam-roles arn:aws:iam::123456789012:role/RedshiftS3Role
```

### Issue: S3 files not found (NoSuchKey)

**Solution:**
Verify files were uploaded correctly:

```bash
aws s3 ls s3://${S3_BUCKET}/workday/hrdp/ --recursive

# Expected output:
# s3://hr-datamart-dev/workday/hrdp/int6020_grade_profile/dt=2026-02-05/workday.hrdp.dly_grade_profile.full.20260205060000.csv
# s3://hr-datamart-dev/workday/hrdp/int6021_job_profile/dt=2026-02-05/workday.hrdp.dly_job_profile.full.20260205060000.csv
```

### Issue: "envsubst: command not found"

**Solution:**
Install gettext which provides `envsubst`:

```bash
# macOS
brew install gettext

# Ubuntu/Debian
sudo apt-get install gettext

# Or use sed as a fallback (in deploy.sh this is handled automatically)
sed "s|\${S3_BUCKET}|${S3_BUCKET}|g; s|\${REDSHIFT_IAM_ROLE_ARN}|${REDSHIFT_IAM_ROLE_ARN}|g; s|\${DATA_DATE}|${DATA_DATE}|g" \
  artifacts/dml/l1_copy/l1_copy_statements.sql > /tmp/l1_copy_substituted.sql
```

### Issue: QA Tests Show FAIL Status

**Diagnosis:**
Check which tests failed:

```bash
PGPASSWORD=${REDSHIFT_PASSWORD} psql \
  -h ${REDSHIFT_HOST} \
  -p ${REDSHIFT_PORT} \
  -d ${REDSHIFT_DB} \
  -U ${REDSHIFT_USER} \
  << 'EOF'
SELECT test_name, test_category, status, error_message
FROM l3_workday.qa_results
WHERE status = 'FAIL'
ORDER BY test_category, test_name;
EOF
```

**Solution:**
- Review error messages for specific issues
- Check row counts in L3 tables: verify facts and dimensions are not empty
- Validate L1 load completed successfully
- Check for NULL values in key columns
- Review data type mismatches in dimension tables

### Issue: Deployment Script Hangs

**Solution:**
The script may be waiting for SQL execution or S3 operations. Check the log file:

```bash
tail -100 logs/deploy_*.log
```

If stuck:
1. Interrupt script: `Ctrl+C`
2. Check Redshift cluster status
3. Review S3 access logs
4. Test connectivity: `aws s3 ls s3://${S3_BUCKET}/`
5. Restart deployment with `--skip-data-gen --skip-s3-upload` to skip early steps

### Issue: Out of Memory During Data Generation

**Solution:**
The `generate_all_feeds.py` script loads all data in memory before writing. If limited:

```bash
# Edit generate_all_feeds.py to reduce record counts
# Lines 16-40 define the number of generated records
# Reduce these values for lower memory usage, e.g.:
#   EMPLOYEES = 500  (instead of 5000)
#   POSITIONS = 200  (instead of 1000)

# Or use the deploy.sh with --skip-data-gen and manually create smaller test data
```

---

## Next Steps

After successful deployment:

1. **Explore the Data Model:**
   - Review the star schema in `artifacts/docs/README.md`
   - Query dimension and fact tables to understand the structure

2. **Run Analytical Queries:**
   ```bash
   PGPASSWORD=${REDSHIFT_PASSWORD} psql \
     -h ${REDSHIFT_HOST} \
     -p ${REDSHIFT_PORT} \
     -d ${REDSHIFT_DB} \
     -U ${REDSHIFT_USER} \
     << 'EOF'
   -- Example: Worker movement by organization
   SELECT
     org.organization_name,
     COUNT(DISTINCT wm.worker_key) AS workers_moved
   FROM l3_workday.fct_worker_movement_f wm
   JOIN l3_workday.dim_organization_d org ON wm.organization_key = org.organization_key
   GROUP BY org.organization_name
   ORDER BY workers_moved DESC;
   EOF
   ```

3. **Set Up BI Tool Connection:**
   - Connect Tableau, Looker, or QuickSight to Redshift L3 schema
   - Create dashboards using dimension and fact tables

4. **Automate Future Loads:**
   - Deploy Glue jobs from `artifacts/glue/` for automated daily ETL
   - Schedule using EventBridge or Airflow

5. **Review Data Modeling Standards:**
   - Naming conventions: `artifacts/docs/README.md` Section 2.4
   - Slowly Changing Dimension Type 2 pattern used in dimensions
   - Star schema optimization for BI query performance

---

## Support & Additional Resources

- **HR Datamart Architecture:** See `artifacts/docs/README.md`
- **AWS Redshift Docs:** https://docs.aws.amazon.com/redshift/
- **Data Model Diagram:** See project wiki or design documentation
- **Git Repository:** https://github.com/WARLAB01/warlab-projects

---

## Deployment Checklist

Use this checklist to track your progress:

- [ ] Clone repository (Step 0)
- [ ] Install/configure AWS CLI (Step 1)
- [ ] Set environment variables (Step 2)
- [ ] Create AWS resources - S3, IAM, Redshift (Step 3)
- [ ] Generate test data (Step 4)
- [ ] Upload CSVs to S3 (Step 5)
- [ ] Create schemas in Redshift (Step 6)
- [ ] Create L1 staging tables (Step 7)
- [ ] Load L1 data from S3 (Step 8)
- [ ] Create L3 source tables (Step 9.1)
- [ ] Create L3 dimensions (Step 9.2)
- [ ] Create L3 facts (Step 9.3)
- [ ] Load L3 source tables (Step 10)
- [ ] Load L3 dimensions (Step 11)
- [ ] Load L3 facts (Step 12)
- [ ] Run QA tests (Step 13)
- [ ] Generate completion report (Step 14)
- [ ] Verify all data in Redshift
- [ ] Begin analytics and BI tool integration

---

**Congratulations!** You now have a fully operational HR Datamart with a star schema ready for analytics and BI tool integration.
