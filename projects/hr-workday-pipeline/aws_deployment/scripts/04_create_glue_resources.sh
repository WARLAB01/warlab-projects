#!/bin/bash
# =============================================================================
# Script: 04_create_glue_resources.sh
# Purpose: Create Glue database, crawlers, and ETL jobs
# =============================================================================

set -e

# Load configurations
source /tmp/hr_bucket_config.sh 2>/dev/null || true
source /tmp/hr_redshift_config.sh 2>/dev/null || true

# Configuration
GLUE_DATABASE="hr_workday_catalog"
GLUE_ROLE_ARN="${GLUE_IAM_ROLE:-}"
REDSHIFT_IAM_ROLE="${REDSHIFT_IAM_ROLE:-}"
S3_BUCKET="${HR_DATA_BUCKET:-}"
S3_PREFIX="${HR_DATA_S3_PREFIX:-raw/hr_data}"
REDSHIFT_WORKGROUP="${REDSHIFT_WORKGROUP:-hr-workday-wg}"
REDSHIFT_DATABASE="${REDSHIFT_DATABASE:-hr_workday_db}"
REGION="${AWS_REGION:-us-east-1}"
SCRIPT_BUCKET="${S3_BUCKET}"

echo "=============================================="
echo "Creating AWS Glue Resources"
echo "=============================================="
echo "Glue Database: ${GLUE_DATABASE}"
echo "S3 Bucket: ${S3_BUCKET}"
echo "Redshift Workgroup: ${REDSHIFT_WORKGROUP}"
echo ""

# Validate required parameters
if [ -z "${S3_BUCKET}" ]; then
    echo "ERROR: S3_BUCKET not set. Run 01_create_s3_bucket.sh first."
    exit 1
fi

if [ -z "${GLUE_ROLE_ARN}" ]; then
    echo "WARNING: GLUE_IAM_ROLE not set. Getting from CloudFormation..."
    GLUE_ROLE_ARN=$(aws cloudformation describe-stacks \
        --stack-name hr-workday-iam-roles \
        --query 'Stacks[0].Outputs[?OutputKey==`GlueServiceRoleArn`].OutputValue' \
        --output text \
        --region "${REGION}" 2>/dev/null || echo "")

    if [ -z "${GLUE_ROLE_ARN}" ]; then
        echo "ERROR: Could not find Glue IAM role. Deploy IAM CloudFormation stack first."
        exit 1
    fi
fi

# =============================================================================
# Step 1: Create Glue Database
# =============================================================================
echo "[1/5] Creating Glue Data Catalog database..."

aws glue create-database \
    --database-input "{
        \"Name\": \"${GLUE_DATABASE}\",
        \"Description\": \"HR Workday data catalog for S3 source files\"
    }" \
    --region "${REGION}" 2>/dev/null || echo "Database may already exist, continuing..."

echo "    ✓ Glue database created/verified"

# =============================================================================
# Step 2: Upload Glue job scripts to S3
# =============================================================================
echo "[2/5] Uploading Glue job scripts to S3..."

SCRIPT_DIR="$(dirname "$0")/../glue_jobs"

# Upload the COPY-based loader (recommended for performance)
aws s3 cp "${SCRIPT_DIR}/load_hr_data_copy_command.py" \
    "s3://${SCRIPT_BUCKET}/glue_scripts/load_hr_data_copy_command.py"

# Upload the Spark-based loader (alternative)
aws s3 cp "${SCRIPT_DIR}/load_hr_data_to_redshift.py" \
    "s3://${SCRIPT_BUCKET}/glue_scripts/load_hr_data_to_redshift.py"

echo "    ✓ Glue scripts uploaded to s3://${SCRIPT_BUCKET}/glue_scripts/"

# =============================================================================
# Step 3: Create Glue Crawlers for S3 data
# =============================================================================
echo "[3/5] Creating Glue crawlers..."

# Get crawler role
CRAWLER_ROLE_ARN=$(aws cloudformation describe-stacks \
    --stack-name hr-workday-iam-roles \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueCrawlerRoleArn`].OutputValue' \
    --output text \
    --region "${REGION}" 2>/dev/null || echo "${GLUE_ROLE_ARN}")

# Create crawler for all HR data
aws glue create-crawler \
    --name "hr-workday-s3-crawler" \
    --role "${CRAWLER_ROLE_ARN}" \
    --database-name "${GLUE_DATABASE}" \
    --targets "{
        \"S3Targets\": [
            {\"Path\": \"s3://${S3_BUCKET}/${S3_PREFIX}/core_hr_employees/\"},
            {\"Path\": \"s3://${S3_BUCKET}/${S3_PREFIX}/job_movement_transactions/\"},
            {\"Path\": \"s3://${S3_BUCKET}/${S3_PREFIX}/compensation_change_transactions/\"},
            {\"Path\": \"s3://${S3_BUCKET}/${S3_PREFIX}/worker_movement_transactions/\"}
        ]
    }" \
    --schema-change-policy "{
        \"UpdateBehavior\": \"UPDATE_IN_DATABASE\",
        \"DeleteBehavior\": \"LOG\"
    }" \
    --recrawl-policy "{\"RecrawlBehavior\": \"CRAWL_EVERYTHING\"}" \
    --region "${REGION}" 2>/dev/null || echo "Crawler may already exist, continuing..."

echo "    ✓ Glue crawler created"

# =============================================================================
# Step 4: Create Glue ETL Job (COPY-based - Recommended)
# =============================================================================
echo "[4/5] Creating Glue ETL job (COPY command)..."

aws glue create-job \
    --name "hr-workday-load-to-redshift" \
    --role "${GLUE_ROLE_ARN}" \
    --command "{
        \"Name\": \"pythonshell\",
        \"ScriptLocation\": \"s3://${SCRIPT_BUCKET}/glue_scripts/load_hr_data_copy_command.py\",
        \"PythonVersion\": \"3.9\"
    }" \
    --default-arguments "{
        \"--S3_BUCKET\": \"${S3_BUCKET}\",
        \"--S3_PREFIX\": \"${S3_PREFIX}\",
        \"--REDSHIFT_WORKGROUP\": \"${REDSHIFT_WORKGROUP}\",
        \"--REDSHIFT_DATABASE\": \"${REDSHIFT_DATABASE}\",
        \"--REDSHIFT_IAM_ROLE\": \"${REDSHIFT_IAM_ROLE}\",
        \"--extra-py-files\": \"\",
        \"--TempDir\": \"s3://${S3_BUCKET}/temp/glue/\"
    }" \
    --max-capacity 0.0625 \
    --glue-version "3.0" \
    --timeout 60 \
    --description "Load HR Workday data from S3 to Redshift using COPY command" \
    --region "${REGION}" 2>/dev/null || echo "Job may already exist, updating..."

# Update job if it exists
aws glue update-job \
    --job-name "hr-workday-load-to-redshift" \
    --job-update "{
        \"Role\": \"${GLUE_ROLE_ARN}\",
        \"Command\": {
            \"Name\": \"pythonshell\",
            \"ScriptLocation\": \"s3://${SCRIPT_BUCKET}/glue_scripts/load_hr_data_copy_command.py\",
            \"PythonVersion\": \"3.9\"
        },
        \"DefaultArguments\": {
            \"--S3_BUCKET\": \"${S3_BUCKET}\",
            \"--S3_PREFIX\": \"${S3_PREFIX}\",
            \"--REDSHIFT_WORKGROUP\": \"${REDSHIFT_WORKGROUP}\",
            \"--REDSHIFT_DATABASE\": \"${REDSHIFT_DATABASE}\",
            \"--REDSHIFT_IAM_ROLE\": \"${REDSHIFT_IAM_ROLE}\",
            \"--TempDir\": \"s3://${S3_BUCKET}/temp/glue/\"
        },
        \"MaxCapacity\": 0.0625,
        \"Timeout\": 60,
        \"GlueVersion\": \"3.0\"
    }" \
    --region "${REGION}" 2>/dev/null || true

echo "    ✓ Glue ETL job created"

# =============================================================================
# Step 5: Create Glue Trigger (Optional - for scheduling)
# =============================================================================
echo "[5/5] Creating Glue trigger (daily schedule)..."

aws glue create-trigger \
    --name "hr-workday-daily-load" \
    --type "SCHEDULED" \
    --schedule "cron(0 6 * * ? *)" \
    --start-on-creation \
    --actions "[{
        \"JobName\": \"hr-workday-load-to-redshift\"
    }]" \
    --description "Daily load of HR Workday data at 6 AM UTC" \
    --region "${REGION}" 2>/dev/null || echo "Trigger may already exist, continuing..."

echo "    ✓ Glue trigger created (runs daily at 6 AM UTC)"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=============================================="
echo "✓ Glue Resources Created Successfully!"
echo "=============================================="
echo ""
echo "Resources:"
echo "  Database:  ${GLUE_DATABASE}"
echo "  Crawler:   hr-workday-s3-crawler"
echo "  ETL Job:   hr-workday-load-to-redshift"
echo "  Trigger:   hr-workday-daily-load (6 AM UTC)"
echo ""
echo "To run the crawler manually:"
echo "  aws glue start-crawler --name hr-workday-s3-crawler"
echo ""
echo "To run the ETL job manually:"
echo "  aws glue start-job-run --job-name hr-workday-load-to-redshift"
echo ""
