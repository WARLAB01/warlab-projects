#!/bin/bash
# =============================================================================
# Script: 01_create_s3_bucket.sh
# Purpose: Create S3 bucket and upload HR data files
# =============================================================================

set -e

# Configuration - MODIFY THESE VALUES
BUCKET_NAME="hr-workday-data-${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
REGION="${AWS_REGION:-us-east-1}"
DATA_PREFIX="raw/hr_data"

echo "=============================================="
echo "Creating S3 Bucket for HR Workday Data"
echo "=============================================="
echo "Bucket: ${BUCKET_NAME}"
echo "Region: ${REGION}"
echo ""

# Create the bucket
echo "[1/4] Creating S3 bucket..."
if [ "$REGION" == "us-east-1" ]; then
    aws s3api create-bucket \
        --bucket "${BUCKET_NAME}" \
        --region "${REGION}"
else
    aws s3api create-bucket \
        --bucket "${BUCKET_NAME}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
fi

# Enable versioning
echo "[2/4] Enabling versioning..."
aws s3api put-bucket-versioning \
    --bucket "${BUCKET_NAME}" \
    --versioning-configuration Status=Enabled

# Enable server-side encryption
echo "[3/4] Enabling server-side encryption..."
aws s3api put-bucket-encryption \
    --bucket "${BUCKET_NAME}" \
    --server-side-encryption-configuration '{
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                },
                "BucketKeyEnabled": true
            }
        ]
    }'

# Block public access
echo "[4/4] Blocking public access..."
aws s3api put-public-access-block \
    --bucket "${BUCKET_NAME}" \
    --public-access-block-configuration '{
        "BlockPublicAcls": true,
        "IgnorePublicAcls": true,
        "BlockPublicPolicy": true,
        "RestrictPublicBuckets": true
    }'

echo ""
echo "✓ S3 bucket created successfully!"
echo ""
echo "Bucket ARN: arn:aws:s3:::${BUCKET_NAME}"
echo ""

# Export for other scripts
echo "export HR_DATA_BUCKET=${BUCKET_NAME}" > /tmp/hr_bucket_config.sh
echo "export HR_DATA_REGION=${REGION}" >> /tmp/hr_bucket_config.sh

echo "Configuration saved to /tmp/hr_bucket_config.sh"
echo "Run: source /tmp/hr_bucket_config.sh"
