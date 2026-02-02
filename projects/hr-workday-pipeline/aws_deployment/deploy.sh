#!/bin/bash
# =============================================================================
# Master Deployment Script for HR Workday Data Pipeline
# =============================================================================
#
# This script orchestrates the complete deployment of:
#   1. S3 bucket for HR data
#   2. IAM roles for Glue and Redshift
#   3. Redshift Serverless namespace and workgroup
#   4. Redshift tables
#   5. Glue database, crawlers, and ETL jobs
#   6. Initial data load
#
# Prerequisites:
#   - AWS CLI configured with appropriate credentials
#   - jq installed for JSON parsing
#   - CSV data files in the specified DATA_DIR
#
# Usage:
#   ./deploy.sh [DATA_DIR]
#
# =============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${1:-${SCRIPT_DIR}/..}"  # Directory containing CSV files
REGION="${AWS_REGION:-us-east-1}"
ENVIRONMENT="${ENVIRONMENT:-dev}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║           HR WORKDAY DATA PIPELINE - AWS DEPLOYMENT                  ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# =============================================================================
# Pre-flight checks
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PRE-FLIGHT CHECKS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check AWS CLI
echo -n "  Checking AWS CLI... "
if command -v aws &> /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗ AWS CLI not found${NC}"
    exit 1
fi

# Check AWS credentials
echo -n "  Checking AWS credentials... "
if aws sts get-caller-identity &> /dev/null; then
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    echo -e "${GREEN}✓${NC} (Account: ${ACCOUNT_ID})"
else
    echo -e "${RED}✗ AWS credentials not configured${NC}"
    exit 1
fi

# Check jq
echo -n "  Checking jq... "
if command -v jq &> /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠ jq not found (some features may not work)${NC}"
fi

# Check data files
echo -n "  Checking data files... "
if [ -f "${DATA_DIR}/core_hr_employees.csv" ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗ CSV files not found in ${DATA_DIR}${NC}"
    echo ""
    echo "  Expected files:"
    echo "    - core_hr_employees.csv"
    echo "    - job_movement_transactions.csv"
    echo "    - compensation_change_transactions.csv"
    echo "    - worker_movement_transactions.csv"
    echo ""
    echo "  Run the Python data generator first or specify the correct DATA_DIR"
    exit 1
fi

echo ""

# =============================================================================
# Confirm deployment
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "DEPLOYMENT CONFIGURATION"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  AWS Account:     ${ACCOUNT_ID}"
echo "  Region:          ${REGION}"
echo "  Environment:     ${ENVIRONMENT}"
echo "  Data Directory:  ${DATA_DIR}"
echo ""

read -p "  Proceed with deployment? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 0
fi

echo ""

# =============================================================================
# Step 1: Create S3 Bucket
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 1/6: Creating S3 Bucket"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bash "${SCRIPT_DIR}/scripts/01_create_s3_bucket.sh"
source /tmp/hr_bucket_config.sh

echo ""

# =============================================================================
# Step 2: Deploy IAM Roles
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 2/6: Deploying IAM Roles"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

aws cloudformation deploy \
    --template-file "${SCRIPT_DIR}/cloudformation/iam-roles.yaml" \
    --stack-name "hr-workday-iam-roles" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        S3BucketName="${HR_DATA_BUCKET}" \
        Environment="${ENVIRONMENT}" \
    --region "${REGION}"

# Get role ARNs
export GLUE_IAM_ROLE=$(aws cloudformation describe-stacks \
    --stack-name hr-workday-iam-roles \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueServiceRoleArn`].OutputValue' \
    --output text \
    --region "${REGION}")

export REDSHIFT_IAM_ROLE=$(aws cloudformation describe-stacks \
    --stack-name hr-workday-iam-roles \
    --query 'Stacks[0].Outputs[?OutputKey==`RedshiftServerlessRoleArn`].OutputValue' \
    --output text \
    --region "${REGION}")

echo "  ✓ IAM roles deployed"
echo "    Glue Role: ${GLUE_IAM_ROLE}"
echo "    Redshift Role: ${REDSHIFT_IAM_ROLE}"

echo ""

# =============================================================================
# Step 3: Upload Data to S3
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 3/6: Uploading Data to S3"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bash "${SCRIPT_DIR}/scripts/02_upload_data.sh" "${DATA_DIR}"
source /tmp/hr_bucket_config.sh

echo ""

# =============================================================================
# Step 4: Create Redshift Serverless
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 4/6: Creating Redshift Serverless"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bash "${SCRIPT_DIR}/scripts/03_create_redshift_serverless.sh"
source /tmp/hr_redshift_config.sh

echo ""

# =============================================================================
# Step 5: Create Redshift Tables
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 5/6: Creating Redshift Tables"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bash "${SCRIPT_DIR}/scripts/05_create_redshift_tables.sh"

echo ""

# =============================================================================
# Step 6: Create Glue Resources
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 6/6: Creating Glue Resources"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bash "${SCRIPT_DIR}/scripts/04_create_glue_resources.sh"

echo ""

# =============================================================================
# Run Initial Data Load
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Running Initial Data Load"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

read -p "  Run initial Glue job to load data? (y/N) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "  Starting Glue job..."
    JOB_RUN_ID=$(aws glue start-job-run \
        --job-name "hr-workday-load-to-redshift" \
        --region "${REGION}" \
        --query 'JobRunId' \
        --output text)

    echo "  Job Run ID: ${JOB_RUN_ID}"
    echo "  Monitoring job progress..."

    while true; do
        STATUS=$(aws glue get-job-run \
            --job-name "hr-workday-load-to-redshift" \
            --run-id "${JOB_RUN_ID}" \
            --region "${REGION}" \
            --query 'JobRun.JobRunState' \
            --output text)

        echo "    Status: ${STATUS}"

        if [ "${STATUS}" == "SUCCEEDED" ]; then
            echo -e "  ${GREEN}✓ Data load completed successfully!${NC}"
            break
        elif [ "${STATUS}" == "FAILED" ] || [ "${STATUS}" == "ERROR" ] || [ "${STATUS}" == "TIMEOUT" ]; then
            echo -e "  ${RED}✗ Data load failed. Check Glue console for details.${NC}"
            break
        fi

        sleep 10
    done
else
    echo "  Skipping initial data load."
    echo "  Run manually with: aws glue start-job-run --job-name hr-workday-load-to-redshift"
fi

echo ""

# =============================================================================
# Deployment Summary
# =============================================================================
echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                    DEPLOYMENT COMPLETE!                              ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "  RESOURCES CREATED:"
echo "  ─────────────────────────────────────────────────────────────────────"
echo "  S3 Bucket:           ${HR_DATA_BUCKET}"
echo "  IAM Roles:           hr-workday-glue-role-${ENVIRONMENT}"
echo "                       hr-workday-redshift-role-${ENVIRONMENT}"
echo "  Redshift Workgroup:  ${REDSHIFT_WORKGROUP}"
echo "  Redshift Database:   ${REDSHIFT_DATABASE}"
echo "  Glue Database:       hr_workday_catalog"
echo "  Glue ETL Job:        hr-workday-load-to-redshift"
echo ""
echo "  NEXT STEPS:"
echo "  ─────────────────────────────────────────────────────────────────────"
echo "  1. Connect to Redshift using Query Editor v2 in AWS Console"
echo "  2. Verify data in hr_workday schema tables"
echo "  3. The Glue job runs daily at 6 AM UTC (or run manually)"
echo ""
echo "  USEFUL COMMANDS:"
echo "  ─────────────────────────────────────────────────────────────────────"
echo "  # Run Glue ETL job"
echo "  aws glue start-job-run --job-name hr-workday-load-to-redshift"
echo ""
echo "  # Run Glue crawler to update catalog"
echo "  aws glue start-crawler --name hr-workday-s3-crawler"
echo ""
echo "  # Query Redshift (via Data API)"
echo "  aws redshift-data execute-statement \\"
echo "    --workgroup-name ${REDSHIFT_WORKGROUP} \\"
echo "    --database ${REDSHIFT_DATABASE} \\"
echo "    --sql 'SELECT COUNT(*) FROM hr_workday.core_hr_employees;'"
echo ""
