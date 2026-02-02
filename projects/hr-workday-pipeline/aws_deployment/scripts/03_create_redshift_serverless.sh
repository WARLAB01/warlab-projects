#!/bin/bash
# =============================================================================
# Script: 03_create_redshift_serverless.sh
# Purpose: Create Redshift Serverless namespace and workgroup
# =============================================================================

set -e

# Configuration
NAMESPACE_NAME="hr-workday-ns"
WORKGROUP_NAME="hr-workday-wg"
DATABASE_NAME="hr_workday_db"
ADMIN_USER="admin"
BASE_CAPACITY=8  # RPUs (8-512, in increments of 8)
REGION="${AWS_REGION:-us-east-1}"

# Get IAM role ARN (from CloudFormation output or manual input)
REDSHIFT_ROLE_ARN="${REDSHIFT_IAM_ROLE:-}"

echo "=============================================="
echo "Creating Redshift Serverless Infrastructure"
echo "=============================================="
echo "Namespace: ${NAMESPACE_NAME}"
echo "Workgroup: ${WORKGROUP_NAME}"
echo "Database: ${DATABASE_NAME}"
echo "Region: ${REGION}"
echo ""

# Generate a secure password
ADMIN_PASSWORD=$(openssl rand -base64 16 | tr -dc 'a-zA-Z0-9' | head -c 16)
ADMIN_PASSWORD="${ADMIN_PASSWORD}Aa1!"  # Ensure complexity requirements

# Store password in Secrets Manager
echo "[1/4] Storing credentials in Secrets Manager..."
aws secretsmanager create-secret \
    --name "hr-workday-redshift-credentials" \
    --description "Redshift Serverless admin credentials for HR Workday" \
    --secret-string "{\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASSWORD}\",\"database\":\"${DATABASE_NAME}\"}" \
    --region "${REGION}" || echo "Secret may already exist, updating..."

aws secretsmanager update-secret \
    --secret-id "hr-workday-redshift-credentials" \
    --secret-string "{\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASSWORD}\",\"database\":\"${DATABASE_NAME}\"}" \
    --region "${REGION}" 2>/dev/null || true

# Create namespace
echo "[2/4] Creating Redshift Serverless namespace..."
if [ -n "${REDSHIFT_ROLE_ARN}" ]; then
    aws redshift-serverless create-namespace \
        --namespace-name "${NAMESPACE_NAME}" \
        --admin-username "${ADMIN_USER}" \
        --admin-user-password "${ADMIN_PASSWORD}" \
        --db-name "${DATABASE_NAME}" \
        --iam-roles "${REDSHIFT_ROLE_ARN}" \
        --region "${REGION}"
else
    aws redshift-serverless create-namespace \
        --namespace-name "${NAMESPACE_NAME}" \
        --admin-username "${ADMIN_USER}" \
        --admin-user-password "${ADMIN_PASSWORD}" \
        --db-name "${DATABASE_NAME}" \
        --region "${REGION}"
fi

# Wait for namespace to be available
echo "    Waiting for namespace to be available..."
aws redshift-serverless wait namespace-available \
    --namespace-name "${NAMESPACE_NAME}" \
    --region "${REGION}"

# Get default VPC and subnets
echo "[3/4] Getting VPC configuration..."
DEFAULT_VPC=$(aws ec2 describe-vpcs \
    --filters "Name=isDefault,Values=true" \
    --query 'Vpcs[0].VpcId' \
    --output text \
    --region "${REGION}")

SUBNET_IDS=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC}" \
    --query 'Subnets[*].SubnetId' \
    --output text \
    --region "${REGION}" | tr '\t' ',')

# Get default security group
SECURITY_GROUP=$(aws ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC}" "Name=group-name,Values=default" \
    --query 'SecurityGroups[0].GroupId' \
    --output text \
    --region "${REGION}")

# Create workgroup
echo "[4/4] Creating Redshift Serverless workgroup..."
aws redshift-serverless create-workgroup \
    --workgroup-name "${WORKGROUP_NAME}" \
    --namespace-name "${NAMESPACE_NAME}" \
    --base-capacity ${BASE_CAPACITY} \
    --publicly-accessible \
    --subnet-ids ${SUBNET_IDS//,/ } \
    --security-group-ids "${SECURITY_GROUP}" \
    --region "${REGION}"

# Wait for workgroup to be available
echo "    Waiting for workgroup to be available (this may take a few minutes)..."
aws redshift-serverless wait workgroup-available \
    --workgroup-name "${WORKGROUP_NAME}" \
    --region "${REGION}"

# Get endpoint
ENDPOINT=$(aws redshift-serverless get-workgroup \
    --workgroup-name "${WORKGROUP_NAME}" \
    --query 'workgroup.endpoint.address' \
    --output text \
    --region "${REGION}")

echo ""
echo "=============================================="
echo "✓ Redshift Serverless Created Successfully!"
echo "=============================================="
echo ""
echo "Connection Details:"
echo "  Endpoint: ${ENDPOINT}"
echo "  Port: 5439"
echo "  Database: ${DATABASE_NAME}"
echo "  Username: ${ADMIN_USER}"
echo "  Password: (stored in Secrets Manager: hr-workday-redshift-credentials)"
echo ""
echo "Workgroup: ${WORKGROUP_NAME}"
echo "Namespace: ${NAMESPACE_NAME}"
echo ""

# Save configuration
cat > /tmp/hr_redshift_config.sh << EOF
export REDSHIFT_WORKGROUP=${WORKGROUP_NAME}
export REDSHIFT_NAMESPACE=${NAMESPACE_NAME}
export REDSHIFT_DATABASE=${DATABASE_NAME}
export REDSHIFT_ENDPOINT=${ENDPOINT}
export REDSHIFT_SECRET_ARN=hr-workday-redshift-credentials
EOF

echo "Configuration saved to /tmp/hr_redshift_config.sh"
echo "Run: source /tmp/hr_redshift_config.sh"
