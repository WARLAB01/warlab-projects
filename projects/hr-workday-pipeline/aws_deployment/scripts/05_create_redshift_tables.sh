#!/bin/bash
# =============================================================================
# Script: 05_create_redshift_tables.sh
# Purpose: Execute DDL to create Redshift tables
# =============================================================================

set -e

# Load configuration
source /tmp/hr_redshift_config.sh 2>/dev/null || true

REDSHIFT_WORKGROUP="${REDSHIFT_WORKGROUP:-hr-workday-wg}"
REDSHIFT_DATABASE="${REDSHIFT_DATABASE:-hr_workday_db}"
REGION="${AWS_REGION:-us-east-1}"
SQL_FILE="$(dirname "$0")/../sql/01_create_schema.sql"

echo "=============================================="
echo "Creating Redshift Tables"
echo "=============================================="
echo "Workgroup: ${REDSHIFT_WORKGROUP}"
echo "Database: ${REDSHIFT_DATABASE}"
echo "SQL File: ${SQL_FILE}"
echo ""

# Check if SQL file exists
if [ ! -f "${SQL_FILE}" ]; then
    echo "ERROR: SQL file not found: ${SQL_FILE}"
    exit 1
fi

# Function to execute SQL and wait for completion
execute_sql() {
    local sql="$1"
    local description="$2"

    echo "  Executing: ${description}..."

    # Execute statement
    response=$(aws redshift-data execute-statement \
        --workgroup-name "${REDSHIFT_WORKGROUP}" \
        --database "${REDSHIFT_DATABASE}" \
        --sql "${sql}" \
        --region "${REGION}" \
        --output json)

    statement_id=$(echo "${response}" | jq -r '.Id')

    # Wait for completion
    while true; do
        status_response=$(aws redshift-data describe-statement \
            --id "${statement_id}" \
            --region "${REGION}" \
            --output json)

        status=$(echo "${status_response}" | jq -r '.Status')

        if [ "${status}" == "FINISHED" ]; then
            echo "    ✓ Completed"
            return 0
        elif [ "${status}" == "FAILED" ]; then
            error=$(echo "${status_response}" | jq -r '.Error')
            echo "    ✗ Failed: ${error}"
            return 1
        elif [ "${status}" == "ABORTED" ]; then
            echo "    ✗ Aborted"
            return 1
        fi

        sleep 2
    done
}

# =============================================================================
# Execute DDL Statements
# =============================================================================

echo "[1/5] Creating schema..."
execute_sql "CREATE SCHEMA IF NOT EXISTS hr_workday;" "Create schema"

echo "[2/5] Creating core_hr_employees table..."
execute_sql "$(cat << 'EOF'
DROP TABLE IF EXISTS hr_workday.core_hr_employees CASCADE;

CREATE TABLE hr_workday.core_hr_employees (
    employee_id VARCHAR(20) NOT NULL,
    worker_id VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    preferred_name VARCHAR(100),
    legal_full_name VARCHAR(200),
    email_work VARCHAR(200),
    gender VARCHAR(10),
    original_hire_date DATE,
    hire_date DATE,
    termination_date DATE,
    worker_status VARCHAR(50),
    worker_type VARCHAR(50),
    business_title VARCHAR(200),
    job_profile VARCHAR(200),
    job_family VARCHAR(100),
    job_level INTEGER,
    management_level VARCHAR(50),
    supervisory_organization VARCHAR(200),
    manager_employee_id VARCHAR(20),
    business_unit VARCHAR(100),
    division VARCHAR(100),
    department VARCHAR(100),
    team VARCHAR(200),
    cost_center VARCHAR(50),
    location VARCHAR(100),
    country VARCHAR(50),
    region VARCHAR(50),
    pay_rate_type VARCHAR(50),
    fte DECIMAL(5,2),
    base_salary DECIMAL(15,2),
    bonus_target_percent DECIMAL(5,4),
    bonus_target_amount DECIMAL(15,2),
    annual_equity_grant DECIMAL(15,2),
    total_compensation DECIMAL(15,2),
    currency VARCHAR(10),
    car_allowance DECIMAL(15,2),
    phone_allowance DECIMAL(15,2),
    executive_perquisite DECIMAL(15,2),
    last_performance_rating VARCHAR(50),
    years_of_service INTEGER,
    time_in_position INTEGER,
    is_manager BOOLEAN,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    source_file VARCHAR(500),
    PRIMARY KEY (employee_id)
)
DISTSTYLE KEY
DISTKEY (employee_id)
SORTKEY (employee_id, hire_date);
EOF
)" "Create core_hr_employees table"

echo "[3/5] Creating job_movement_transactions table..."
execute_sql "$(cat << 'EOF'
DROP TABLE IF EXISTS hr_workday.job_movement_transactions CASCADE;

CREATE TABLE hr_workday.job_movement_transactions (
    transaction_id VARCHAR(20) NOT NULL,
    employee_id VARCHAR(20) NOT NULL,
    worker_id VARCHAR(20),
    effective_date DATE NOT NULL,
    transaction_type VARCHAR(50),
    transaction_status VARCHAR(50),
    reason_code VARCHAR(100),
    prior_job_profile VARCHAR(200),
    new_job_profile VARCHAR(200),
    prior_job_level INTEGER,
    new_job_level INTEGER,
    prior_business_unit VARCHAR(100),
    new_business_unit VARCHAR(100),
    prior_division VARCHAR(100),
    new_division VARCHAR(100),
    prior_department VARCHAR(100),
    new_department VARCHAR(100),
    prior_manager_id VARCHAR(20),
    new_manager_id VARCHAR(20),
    prior_location VARCHAR(100),
    new_location VARCHAR(100),
    prior_worker_type VARCHAR(50),
    new_worker_type VARCHAR(50),
    initiated_by VARCHAR(100),
    initiated_date DATE,
    completed_date DATE,
    comments VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    source_file VARCHAR(500),
    PRIMARY KEY (transaction_id)
)
DISTSTYLE KEY
DISTKEY (employee_id)
SORTKEY (employee_id, effective_date);
EOF
)" "Create job_movement_transactions table"

echo "[4/5] Creating compensation_change_transactions table..."
execute_sql "$(cat << 'EOF'
DROP TABLE IF EXISTS hr_workday.compensation_change_transactions CASCADE;

CREATE TABLE hr_workday.compensation_change_transactions (
    transaction_id VARCHAR(20) NOT NULL,
    employee_id VARCHAR(20) NOT NULL,
    worker_id VARCHAR(20),
    effective_date DATE NOT NULL,
    transaction_type VARCHAR(50),
    transaction_status VARCHAR(50),
    reason_code VARCHAR(100),
    prior_base_salary DECIMAL(15,2),
    new_base_salary DECIMAL(15,2),
    base_change_amount DECIMAL(15,2),
    base_change_percent DECIMAL(8,2),
    prior_bonus_target_percent DECIMAL(5,4),
    new_bonus_target_percent DECIMAL(5,4),
    prior_bonus_target_amount DECIMAL(15,2),
    new_bonus_target_amount DECIMAL(15,2),
    prior_annual_equity DECIMAL(15,2),
    new_annual_equity DECIMAL(15,2),
    allowance_type VARCHAR(100),
    allowance_amount DECIMAL(15,2),
    currency VARCHAR(10),
    performance_rating VARCHAR(50),
    compa_ratio_prior DECIMAL(8,4),
    compa_ratio_new DECIMAL(8,4),
    initiated_by VARCHAR(100),
    approved_by VARCHAR(100),
    initiated_date DATE,
    completed_date DATE,
    comments VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    source_file VARCHAR(500),
    PRIMARY KEY (transaction_id)
)
DISTSTYLE KEY
DISTKEY (employee_id)
SORTKEY (employee_id, effective_date);
EOF
)" "Create compensation_change_transactions table"

echo "[5/5] Creating worker_movement_transactions table..."
execute_sql "$(cat << 'EOF'
DROP TABLE IF EXISTS hr_workday.worker_movement_transactions CASCADE;

CREATE TABLE hr_workday.worker_movement_transactions (
    transaction_id VARCHAR(20) NOT NULL,
    employee_id VARCHAR(20) NOT NULL,
    worker_id VARCHAR(20),
    effective_date DATE NOT NULL,
    movement_type VARCHAR(50),
    movement_status VARCHAR(50),
    reason_code VARCHAR(100),
    prior_location VARCHAR(100),
    new_location VARCHAR(100),
    prior_country VARCHAR(50),
    new_country VARCHAR(50),
    prior_region VARCHAR(50),
    new_region VARCHAR(50),
    prior_business_unit VARCHAR(100),
    new_business_unit VARCHAR(100),
    prior_division VARCHAR(100),
    new_division VARCHAR(100),
    prior_department VARCHAR(100),
    new_department VARCHAR(100),
    prior_team VARCHAR(200),
    new_team VARCHAR(200),
    prior_cost_center VARCHAR(50),
    new_cost_center VARCHAR(50),
    prior_manager_id VARCHAR(20),
    new_manager_id VARCHAR(20),
    prior_supervisory_org VARCHAR(200),
    new_supervisory_org VARCHAR(200),
    relocation_package VARCHAR(100),
    remote_work_arrangement VARCHAR(50),
    initiated_by VARCHAR(100),
    approved_by VARCHAR(100),
    initiated_date DATE,
    completed_date DATE,
    comments VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    source_file VARCHAR(500),
    PRIMARY KEY (transaction_id)
)
DISTSTYLE KEY
DISTKEY (employee_id)
SORTKEY (employee_id, effective_date);
EOF
)" "Create worker_movement_transactions table"

echo ""
echo "=============================================="
echo "✓ All Redshift Tables Created Successfully!"
echo "=============================================="
echo ""
echo "Tables created in schema 'hr_workday':"
echo "  - core_hr_employees"
echo "  - job_movement_transactions"
echo "  - compensation_change_transactions"
echo "  - worker_movement_transactions"
echo ""
