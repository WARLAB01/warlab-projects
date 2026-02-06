#!/bin/bash

################################################################################
# AWS Glue Job Deployment Script
################################################################################
#
# Purpose:
#   Automates the deployment of HR Datamart ETL jobs to AWS Glue
#   - Uploads the parameterized ETL script to S3
#   - Creates 12 Glue jobs with appropriate configurations
#   - Creates a Glue workflow to orchestrate all jobs
#   - Configures job triggers and monitoring
#
# Usage:
#   ./deploy_glue_jobs.sh [--dry-run] [--region us-east-1] [--profile default]
#
# Prerequisites:
#   - AWS CLI configured with appropriate credentials
#   - Permissions to create/modify Glue jobs and workflows
#   - S3 bucket warlab-hr-datamart-dev exists and is accessible
#   - Glue connection 'warlab-redshift-connection' already exists
#
# Author: Data Engineering Team
# Version: 1.0
# Created: 2026-02-06
#
################################################################################

set -euo pipefail

# ============================================================================
# CONFIGURATION
# ============================================================================

# AWS Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-default}"
DRY_RUN="${DRY_RUN:-false}"

# S3 Configuration
S3_BUCKET="warlab-hr-datamart-dev"
GLUE_SCRIPTS_PATH="glue-scripts"
ETL_SCRIPT_S3_PATH="s3://${S3_BUCKET}/${GLUE_SCRIPTS_PATH}/glue_s3_to_l1_etl.py"
LOCAL_ETL_SCRIPT_PATH="glue_s3_to_l1_etl.py"

# Glue Configuration
GLUE_ROLE="arn:aws:iam::$(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE}):role/GlueServiceRole"
GLUE_VERSION="4.0"
PYTHON_VERSION="3"
MAX_CAPACITY="10.0"
TIMEOUT_MINUTES="30"

# Workflow Configuration
WORKFLOW_NAME="warlab-hr-l1-load"
WORKFLOW_DESCRIPTION="Orchestrated ETL pipeline for loading HR Datamart source tables from S3 to Redshift L1 staging layer"

# Redshift Configuration
REDSHIFT_CONNECTION="warlab-redshift-connection"
REDSHIFT_DATABASE="dev"
REDSHIFT_SCHEMA="l1_workday"
REDSHIFT_TEMP_DIR="s3://${S3_BUCKET}/glue-temp/"

# Define all 12 source tables
declare -a SOURCE_TABLES=(
    "int0095e_worker_job"
    "int0096_worker_organization"
    "int0098_worker_compensation"
    "int270_rescinded"
    "int6020_grade_profile"
    "int6021_job_profile"
    "int6022_job_classification"
    "int6023_location"
    "int6024_company"
    "int6025_cost_center"
    "int6028_department_hierarchy"
    "int6032_positions"
)

# ============================================================================
# LOGGING AND UTILITIES
# ============================================================================

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${BLUE}================================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================================================${NC}"
}

# AWS CLI wrapper function
aws_cmd() {
    if [ "${DRY_RUN}" = "true" ]; then
        log_warning "[DRY RUN] Would execute: aws --region ${AWS_REGION} --profile ${AWS_PROFILE} $@"
        return 0
    else
        aws --region "${AWS_REGION}" --profile "${AWS_PROFILE}" "$@"
    fi
}

# ============================================================================
# VALIDATION AND PREREQUISITES
# ============================================================================

validate_prerequisites() {
    log_section "Validating Prerequisites"

    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed or not in PATH"
        exit 1
    fi
    log_success "AWS CLI is installed"

    # Check AWS credentials
    if ! aws_cmd sts get-caller-identity > /dev/null 2>&1; then
        log_error "AWS credentials are not valid or not configured for profile: ${AWS_PROFILE}"
        exit 1
    fi
    log_success "AWS credentials are valid"

    # Check if local ETL script exists
    if [ ! -f "${LOCAL_ETL_SCRIPT_PATH}" ]; then
        log_error "Local ETL script not found: ${LOCAL_ETL_SCRIPT_PATH}"
        log_info "This script must be run from the directory containing glue_s3_to_l1_etl.py"
        exit 1
    fi
    log_success "ETL script found: ${LOCAL_ETL_SCRIPT_PATH}"

    # Check if S3 bucket exists
    if ! aws_cmd s3 ls "s3://${S3_BUCKET}" --recursive --max-items 1 > /dev/null 2>&1; then
        log_error "S3 bucket is not accessible: ${S3_BUCKET}"
        exit 1
    fi
    log_success "S3 bucket is accessible: ${S3_BUCKET}"

    # Check if Glue connection exists
    if ! aws_cmd glue get-connection --name "${REDSHIFT_CONNECTION}" > /dev/null 2>&1; then
        log_warning "Glue connection does not exist: ${REDSHIFT_CONNECTION}"
        log_info "This connection will need to be created manually in AWS Glue console"
    else
        log_success "Glue connection exists: ${REDSHIFT_CONNECTION}"
    fi
}

# ============================================================================
# SCRIPT UPLOAD
# ============================================================================

upload_etl_script() {
    log_section "Uploading ETL Script to S3"

    log_info "Uploading ${LOCAL_ETL_SCRIPT_PATH} to ${ETL_SCRIPT_S3_PATH}"

    if [ "${DRY_RUN}" = "true" ]; then
        log_warning "[DRY RUN] Would upload script to ${ETL_SCRIPT_S3_PATH}"
    else
        aws_cmd s3 cp "${LOCAL_ETL_SCRIPT_PATH}" "${ETL_SCRIPT_S3_PATH}"
        log_success "Script uploaded successfully"

        # Verify upload
        if aws_cmd s3 ls "${ETL_SCRIPT_S3_PATH}" > /dev/null 2>&1; then
            log_success "Script upload verified"
        else
            log_error "Script upload verification failed"
            exit 1
        fi
    fi
}

# ============================================================================
# GLUE JOB CREATION
# ============================================================================

create_glue_job() {
    local job_name=$1
    local source_table=$2
    local s3_path=$3

    log_info "Creating Glue job: ${job_name}"

    # Build the job arguments
    local job_args="{
        \"--source_table\": \"${source_table}\",
        \"--s3_path\": \"${s3_path}\",
        \"--redshift_schema\": \"${REDSHIFT_SCHEMA}\",
        \"--redshift_table\": \"${source_table}\",
        \"--redshift_connection\": \"${REDSHIFT_CONNECTION}\",
        \"--redshift_database\": \"${REDSHIFT_DATABASE}\",
        \"--TempDir\": \"${REDSHIFT_TEMP_DIR}\",
        \"--enable-spark-ui\": \"true\",
        \"--spark-event-logs-path\": \"s3://${S3_BUCKET}/spark-logs/\",
        \"--enable-job-insights\": \"true\",
        \"--enable-glue-datacatalog\": \"true\"
    }"

    if [ "${DRY_RUN}" = "true" ]; then
        log_warning "[DRY RUN] Would create job with:"
        echo "  Name: ${job_name}"
        echo "  Source Table: ${source_table}"
        echo "  S3 Path: ${s3_path}"
    else
        # Check if job already exists
        if aws_cmd glue get-job --name "${job_name}" > /dev/null 2>&1; then
            log_warning "Job already exists: ${job_name} - Updating..."
            aws_cmd glue update-job \
                --name "${job_name}" \
                --role "${GLUE_ROLE}" \
                --command "Name=glueetl,ScriptLocation=${ETL_SCRIPT_S3_PATH},PythonVersion=${PYTHON_VERSION}" \
                --default-arguments "${job_args}" \
                --glue-version "${GLUE_VERSION}" \
                --max-capacity "${MAX_CAPACITY}" \
                --timeout "${TIMEOUT_MINUTES}" \
                --description "Load ${source_table} from S3 to Redshift L1 staging"
            log_success "Job updated: ${job_name}"
        else
            aws_cmd glue create-job \
                --name "${job_name}" \
                --role "${GLUE_ROLE}" \
                --command "Name=glueetl,ScriptLocation=${ETL_SCRIPT_S3_PATH},PythonVersion=${PYTHON_VERSION}" \
                --default-arguments "${job_args}" \
                --glue-version "${GLUE_VERSION}" \
                --max-capacity "${MAX_CAPACITY}" \
                --timeout "${TIMEOUT_MINUTES}" \
                --description "Load ${source_table} from S3 to Redshift L1 staging"
            log_success "Job created: ${job_name}"
        fi
    fi
}

create_all_glue_jobs() {
    log_section "Creating Glue Jobs (12 jobs for HR Datamart)"

    for source_table in "${SOURCE_TABLES[@]}"; do
        job_name="warlab-hr-${source_table}"
        s3_path="s3://${S3_BUCKET}/workday/hrdp/${source_table}/"
        create_glue_job "${job_name}" "${source_table}" "${s3_path}"
    done

    log_success "All Glue jobs created/updated"
}

# ============================================================================
# GLUE WORKFLOW CREATION
# ============================================================================

create_glue_workflow() {
    log_section "Creating Glue Workflow"

    log_info "Creating workflow: ${WORKFLOW_NAME}"

    if [ "${DRY_RUN}" = "true" ]; then
        log_warning "[DRY RUN] Would create workflow: ${WORKFLOW_NAME}"
    else
        # Check if workflow already exists
        if aws_cmd glue get-workflow --name "${WORKFLOW_NAME}" > /dev/null 2>&1; then
            log_warning "Workflow already exists: ${WORKFLOW_NAME}"
            log_info "Deleting existing workflow to recreate with updated configuration..."

            # Delete triggers first (required before deleting workflow)
            local triggers=$(aws_cmd glue list-triggers --filter "Name=WORKFLOW_NAME,Values=${WORKFLOW_NAME}" --query 'TriggerNames' --output text)
            for trigger in ${triggers}; do
                log_info "Deleting trigger: ${trigger}"
                aws_cmd glue delete-trigger --name "${trigger}"
            done

            # Delete the workflow
            aws_cmd glue delete-workflow --name "${WORKFLOW_NAME}"
            log_info "Workflow deleted"
        fi

        # Create new workflow
        aws_cmd glue create-workflow \
            --name "${WORKFLOW_NAME}" \
            --description "${WORKFLOW_DESCRIPTION}"
        log_success "Workflow created: ${WORKFLOW_NAME}"
    fi
}

create_workflow_triggers() {
    log_section "Creating Workflow Triggers"

    # Create on-demand trigger (StartingTrigger)
    local trigger_name="${WORKFLOW_NAME}-start"
    log_info "Creating on-demand trigger: ${trigger_name}"

    if [ "${DRY_RUN}" = "false" ]; then
        aws_cmd glue create-trigger \
            --name "${trigger_name}" \
            --workflow-name "${WORKFLOW_NAME}" \
            --type "ON_DEMAND" \
            --description "Manual trigger for HR Datamart L1 load workflow"
        log_success "On-demand trigger created: ${trigger_name}"
    else
        log_warning "[DRY RUN] Would create on-demand trigger: ${trigger_name}"
    fi

    # Create scheduled trigger (daily at 6 AM UTC)
    local schedule_trigger_name="${WORKFLOW_NAME}-daily"
    log_info "Creating scheduled trigger: ${schedule_trigger_name}"

    if [ "${DRY_RUN}" = "false" ]; then
        aws_cmd glue create-trigger \
            --name "${schedule_trigger_name}" \
            --workflow-name "${WORKFLOW_NAME}" \
            --type "SCHEDULED" \
            --schedule "cron(0 6 * * ? *)" \
            --start-on-creation \
            --description "Daily trigger at 6 AM UTC for HR Datamart L1 load"
        log_success "Scheduled trigger created: ${schedule_trigger_name}"
    else
        log_warning "[DRY RUN] Would create scheduled trigger: ${schedule_trigger_name}"
    fi
}

create_job_actions() {
    log_section "Adding Job Actions to Workflow"

    # Add all job actions to the workflow
    # Jobs run in parallel, triggered by the initial trigger
    for source_table in "${SOURCE_TABLES[@]}"; do
        job_name="warlab-hr-${source_table}"
        log_info "Adding job action: ${job_name}"

        if [ "${DRY_RUN}" = "true" ]; then
            log_warning "[DRY RUN] Would add job action: ${job_name}"
        else
            aws_cmd glue put-workflow-run_properties \
                --name "${WORKFLOW_NAME}" \
                --run-properties '{}' > /dev/null 2>&1 || true

            # Get or create a starting trigger
            local trigger_name="${WORKFLOW_NAME}-start"
            if ! aws_cmd glue get-trigger --name "${trigger_name}" > /dev/null 2>&1; then
                aws_cmd glue create-trigger \
                    --name "${trigger_name}" \
                    --workflow-name "${WORKFLOW_NAME}" \
                    --type "ON_DEMAND"
            fi

            # Add the job action
            aws_cmd glue put-trigger \
                --name "${trigger_name}" \
                --workflow-name "${WORKFLOW_NAME}" \
                --type "ON_DEMAND" \
                --actions "Name=${job_name}" || log_warning "Could not add job action (may need manual configuration)"
        fi
    done

    log_success "Job actions added to workflow"
}

# ============================================================================
# VERIFICATION AND REPORTING
# ============================================================================

verify_deployment() {
    log_section "Verifying Deployment"

    if [ "${DRY_RUN}" = "true" ]; then
        log_warning "DRY RUN MODE: Skipping actual verification"
        return 0
    fi

    local job_count=0
    for source_table in "${SOURCE_TABLES[@]}"; do
        job_name="warlab-hr-${source_table}"
        if aws_cmd glue get-job --name "${job_name}" > /dev/null 2>&1; then
            ((job_count++))
        fi
    done

    log_info "Created/Updated ${job_count} of ${#SOURCE_TABLES[@]} Glue jobs"

    if aws_cmd glue get-workflow --name "${WORKFLOW_NAME}" > /dev/null 2>&1; then
        log_success "Workflow created: ${WORKFLOW_NAME}"
    else
        log_warning "Workflow may not have been created successfully"
    fi
}

# ============================================================================
# SUMMARY AND NEXT STEPS
# ============================================================================

print_summary() {
    log_section "Deployment Summary"

    echo ""
    echo "ETL Script Location:"
    echo "  ${ETL_SCRIPT_S3_PATH}"
    echo ""
    echo "Glue Jobs Created:"
    for source_table in "${SOURCE_TABLES[@]}"; do
        echo "  - warlab-hr-${source_table}"
    done
    echo ""
    echo "Workflow Details:"
    echo "  Name: ${WORKFLOW_NAME}"
    echo "  Region: ${AWS_REGION}"
    echo "  Description: ${WORKFLOW_DESCRIPTION}"
    echo ""
    echo "Next Steps:"
    echo "  1. Verify Glue connection exists: ${REDSHIFT_CONNECTION}"
    echo "  2. Configure CloudWatch alarms for job failures"
    echo "  3. Run workflow on-demand to test: aws glue start-workflow-run --name ${WORKFLOW_NAME}"
    echo "  4. Monitor workflow run in AWS Glue console"
    echo "  5. Schedule daily runs if testing is successful"
    echo ""
    echo "Useful Commands:"
    echo "  # List all jobs"
    echo "  aws glue list-jobs --region ${AWS_REGION}"
    echo ""
    echo "  # Get job details"
    echo "  aws glue get-job --name warlab-hr-int6024-company --region ${AWS_REGION}"
    echo ""
    echo "  # Trigger workflow manually"
    echo "  aws glue start-workflow-run --name ${WORKFLOW_NAME} --region ${AWS_REGION}"
    echo ""
    echo "  # Get workflow run status"
    echo "  aws glue get-workflow-runs --name ${WORKFLOW_NAME} --region ${AWS_REGION}"
    echo ""
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    log_section "AWS Glue Job Deployment"
    echo "Start Time: $(date)"
    echo "Configuration:"
    echo "  AWS Region: ${AWS_REGION}"
    echo "  AWS Profile: ${AWS_PROFILE}"
    echo "  S3 Bucket: ${S3_BUCKET}"
    echo "  Dry Run: ${DRY_RUN}"
    echo ""

    # Execute deployment steps
    validate_prerequisites
    upload_etl_script
    create_all_glue_jobs
    create_glue_workflow
    create_workflow_triggers
    create_job_actions
    verify_deployment
    print_summary

    log_section "Deployment Complete"
    echo "End Time: $(date)"
}

# ============================================================================
# ARGUMENT PARSING
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="true"
            log_warning "DRY RUN MODE ENABLED - No actual changes will be made"
            shift
            ;;
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --profile)
            AWS_PROFILE="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [--dry-run] [--region REGION] [--profile PROFILE]"
            echo ""
            echo "Options:"
            echo "  --dry-run       Show what would be deployed without making changes"
            echo "  --region        AWS region (default: us-east-1)"
            echo "  --profile       AWS CLI profile (default: default)"
            echo "  --help          Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Execute main function
main
