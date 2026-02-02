#!/bin/bash
# =============================================================================
# Script: 02_upload_data.sh
# Purpose: Upload HR CSV files to S3
# =============================================================================

set -e

# Load configuration
if [ -f /tmp/hr_bucket_config.sh ]; then
    source /tmp/hr_bucket_config.sh
fi

BUCKET_NAME="${HR_DATA_BUCKET:-hr-workday-data-$(aws sts get-caller-identity --query Account --output text)}"
DATA_DIR="${1:-.}"  # Directory containing CSV files (default: current directory)
S3_PREFIX="raw/hr_data/$(date +%Y/%m/%d)"

echo "=============================================="
echo "Uploading HR Data to S3"
echo "=============================================="
echo "Bucket: ${BUCKET_NAME}"
echo "Source: ${DATA_DIR}"
echo "S3 Path: s3://${BUCKET_NAME}/${S3_PREFIX}/"
echo ""

# Check if files exist
if [ ! -f "${DATA_DIR}/core_hr_employees.csv" ]; then
    echo "ERROR: CSV files not found in ${DATA_DIR}"
    echo "Expected files:"
    echo "  - core_hr_employees.csv"
    echo "  - job_movement_transactions.csv"
    echo "  - compensation_change_transactions.csv"
    echo "  - worker_movement_transactions.csv"
    exit 1
fi

# Upload files
echo "[1/4] Uploading core_hr_employees.csv..."
aws s3 cp "${DATA_DIR}/core_hr_employees.csv" \
    "s3://${BUCKET_NAME}/${S3_PREFIX}/core_hr_employees/core_hr_employees.csv"

echo "[2/4] Uploading job_movement_transactions.csv..."
aws s3 cp "${DATA_DIR}/job_movement_transactions.csv" \
    "s3://${BUCKET_NAME}/${S3_PREFIX}/job_movement_transactions/job_movement_transactions.csv"

echo "[3/4] Uploading compensation_change_transactions.csv..."
aws s3 cp "${DATA_DIR}/compensation_change_transactions.csv" \
    "s3://${BUCKET_NAME}/${S3_PREFIX}/compensation_change_transactions/compensation_change_transactions.csv"

echo "[4/4] Uploading worker_movement_transactions.csv..."
aws s3 cp "${DATA_DIR}/worker_movement_transactions.csv" \
    "s3://${BUCKET_NAME}/${S3_PREFIX}/worker_movement_transactions/worker_movement_transactions.csv"

echo ""
echo "✓ All files uploaded successfully!"
echo ""
echo "S3 Locations:"
echo "  s3://${BUCKET_NAME}/${S3_PREFIX}/core_hr_employees/"
echo "  s3://${BUCKET_NAME}/${S3_PREFIX}/job_movement_transactions/"
echo "  s3://${BUCKET_NAME}/${S3_PREFIX}/compensation_change_transactions/"
echo "  s3://${BUCKET_NAME}/${S3_PREFIX}/worker_movement_transactions/"
echo ""

# Save paths for other scripts
echo "export HR_DATA_S3_PREFIX=${S3_PREFIX}" >> /tmp/hr_bucket_config.sh
