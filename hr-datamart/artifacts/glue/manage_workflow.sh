#!/bin/bash

################################################################################
# AWS Glue Workflow Management Script
################################################################################
#
# Purpose:
#   Provides utilities for managing the HR Datamart Glue workflow
#   - Start/stop workflow runs
#   - Monitor job status
#   - View CloudWatch logs
#   - List and manage triggers
#   - Troubleshooting utilities
#
# Usage:
#   ./manage_workflow.sh [command] [options]
#
# Commands:
#   start           Start a workflow run
#   status          Get status of latest workflow run
#   logs            Tail CloudWatch logs for a job
#   list-jobs       List all HR Datamart Glue jobs
#   list-runs       List recent workflow runs
#   job-status      Get status of a specific job
#   delete-job      Delete a Glue job
#   delete-workflow Delete the entire workflow and jobs
#   help            Show this help message
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

AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-default}"
WORKFLOW_NAME="warlab-hr-l1-load"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# ============================================================================
# LOGGING AND UTILITIES
# ============================================================================

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
    echo -e "${CYAN}================================================================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}================================================================================${NC}"
}

# ============================================================================
# WORKFLOW MANAGEMENT
# ============================================================================

cmd_start_workflow() {
    log_section "Starting Workflow Run"

    log_info "Starting workflow: ${WORKFLOW_NAME}"

    local run_response=$(aws glue start-workflow-run \
        --name "${WORKFLOW_NAME}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --output json)

    local run_id=$(echo "${run_response}" | jq -r '.RunId')

    if [ -z "${run_id}" ] || [ "${run_id}" = "null" ]; then
        log_error "Failed to start workflow run"
        exit 1
    fi

    log_success "Workflow run started"
    echo "Run ID: ${run_id}"
    echo ""
    echo "Monitor workflow with:"
    echo "  ./manage_workflow.sh status"
    echo "  ./manage_workflow.sh list-runs"
}

cmd_workflow_status() {
    log_section "Workflow Status"

    log_info "Fetching latest workflow run..."

    local runs=$(aws glue get-workflow-runs \
        --name "${WORKFLOW_NAME}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --output json)

    local run_count=$(echo "${runs}" | jq '.Runs | length')

    if [ "${run_count}" -eq 0 ]; then
        log_warning "No workflow runs found"
        return
    fi

    # Get the latest run
    local latest_run=$(echo "${runs}" | jq '.Runs[0]')
    local run_id=$(echo "${latest_run}" | jq -r '.Id')
    local status=$(echo "${latest_run}" | jq -r '.Status')
    local started=$(echo "${latest_run}" | jq -r '.StartedOn')
    local completed=$(echo "${latest_run}" | jq -r '.CompletedOn // "In Progress"')

    echo "Latest Workflow Run:"
    echo "  Run ID: ${run_id}"
    echo "  Status: ${status}"
    echo "  Started: ${started}"
    echo "  Completed: ${completed}"
    echo ""

    # Get detailed job runs from the latest workflow run
    log_info "Fetching job runs in this workflow..."

    local job_runs=$(aws glue get-workflow-run-properties \
        --name "${WORKFLOW_NAME}" \
        --run-id "${run_id}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --output json 2>/dev/null || echo '{}')

    log_info "Job Status Summary:"
    echo ""
    printf "%-50s %-15s %-15s\n" "Job Name" "Status" "Duration"
    printf "%-50s %-15s %-15s\n" "$(printf '%.0s-' {1..50})" "$(printf '%.0s-' {1..15})" "$(printf '%.0s-' {1..15})"

    # Try to get individual job run status
    local job_names=$(aws glue list-jobs --region "${AWS_REGION}" --profile "${AWS_PROFILE}" --output json | jq -r '.JobNames[]' | grep "warlab-hr-" || true)

    if [ -z "${job_names}" ]; then
        log_warning "No HR Datamart jobs found"
        return
    fi

    for job_name in ${job_names}; do
        local job_runs=$(aws glue list-job-runs \
            --job-name "${job_name}" \
            --region "${AWS_REGION}" \
            --profile "${AWS_PROFILE}" \
            --output json)

        if [ $(echo "${job_runs}" | jq '.JobRuns | length') -gt 0 ]; then
            local latest_job_run=$(echo "${job_runs}" | jq '.JobRuns[0]')
            local job_status=$(echo "${latest_job_run}" | jq -r '.JobRunState')
            local job_started=$(echo "${latest_job_run}" | jq -r '.StartedOn')
            local job_duration=$(echo "${latest_job_run}" | jq -r '.ExecutionTime // "0"')

            # Color code status
            case "${job_status}" in
                SUCCEEDED)
                    status_colored="${GREEN}${job_status}${NC}"
                    ;;
                RUNNING)
                    status_colored="${YELLOW}${job_status}${NC}"
                    ;;
                FAILED)
                    status_colored="${RED}${job_status}${NC}"
                    ;;
                *)
                    status_colored="${job_status}"
                    ;;
            esac

            printf "%-50s ${status_colored} %-15s\n" "${job_name:0:50}" "${job_duration}s"
        fi
    done
}

cmd_list_runs() {
    log_section "Recent Workflow Runs"

    local runs=$(aws glue get-workflow-runs \
        --name "${WORKFLOW_NAME}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --max-results 10 \
        --output json)

    local run_count=$(echo "${runs}" | jq '.Runs | length')

    if [ "${run_count}" -eq 0 ]; then
        log_warning "No workflow runs found"
        return
    fi

    log_info "Latest 10 workflow runs:"
    echo ""
    printf "%-40s %-15s %-20s %-20s\n" "Run ID" "Status" "Started" "Completed"
    printf "%-40s %-15s %-20s %-20s\n" "$(printf '%.0s-' {1..40})" "$(printf '%.0s-' {1..15})" "$(printf '%.0s-' {1..20})" "$(printf '%.0s-' {1..20})"

    echo "${runs}" | jq -r '.Runs[] | "\(.Id) \(.Status) \(.StartedOn) \(.CompletedOn // "In Progress")"' | \
    while read -r run_id status started completed; do
        # Truncate long values
        run_id_short="${run_id:0:35}"
        started_short="${started:0:19}"
        completed_short="${completed:0:19}"

        printf "%-40s %-15s %-20s %-20s\n" "${run_id_short}" "${status}" "${started_short}" "${completed_short}"
    done
}

# ============================================================================
# JOB MANAGEMENT
# ============================================================================

cmd_list_jobs() {
    log_section "HR Datamart Glue Jobs"

    local jobs=$(aws glue list-jobs \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --output json)

    local hr_jobs=$(echo "${jobs}" | jq '.JobNames[]' | grep "warlab-hr-" | sort)

    if [ -z "${hr_jobs}" ]; then
        log_warning "No HR Datamart jobs found"
        return
    fi

    log_info "Found HR Datamart jobs:"
    echo ""
    local count=0
    echo "${hr_jobs}" | while read -r job_name; do
        ((count++))
        echo "  ${count}. ${job_name}"
    done
}

cmd_job_status() {
    local job_name=$1

    if [ -z "${job_name}" ]; then
        log_error "Job name is required"
        echo "Usage: ./manage_workflow.sh job-status <job-name>"
        exit 1
    fi

    log_section "Job Status: ${job_name}"

    # Get job details
    local job=$(aws glue get-job \
        --name "${job_name}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --output json)

    echo "Job Details:"
    echo "  Name: $(echo "${job}" | jq -r '.Job.Name')"
    echo "  Description: $(echo "${job}" | jq -r '.Job.Description')"
    echo "  Role: $(echo "${job}" | jq -r '.Job.Role')"
    echo "  Glue Version: $(echo "${job}" | jq -r '.Job.GlueVersion')"
    echo "  Max Capacity: $(echo "${job}" | jq -r '.Job.MaxCapacity')"
    echo "  Timeout: $(echo "${job}" | jq -r '.Job.Timeout')"
    echo ""

    # Get latest job run
    local job_runs=$(aws glue list-job-runs \
        --job-name "${job_name}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --max-results 5 \
        --output json)

    local run_count=$(echo "${job_runs}" | jq '.JobRuns | length')

    if [ "${run_count}" -eq 0 ]; then
        log_warning "No job runs found"
        return
    fi

    echo "Recent Job Runs:"
    echo ""
    printf "%-40s %-15s %-15s %-20s\n" "Run ID" "Status" "Duration (s)" "Started"
    printf "%-40s %-15s %-15s %-20s\n" "$(printf '%.0s-' {1..40})" "$(printf '%.0s-' {1..15})" "$(printf '%.0s-' {1..15})" "$(printf '%.0s-' {1..20})"

    echo "${job_runs}" | jq -r '.JobRuns[] | "\(.Id) \(.JobRunState) \(.ExecutionTime // 0) \(.StartedOn)"' | \
    while read -r run_id status duration started; do
        run_id_short="${run_id:0:35}"
        started_short="${started:0:19}"
        printf "%-40s %-15s %-15s %-20s\n" "${run_id_short}" "${status}" "${duration}" "${started_short}"
    done
}

# ============================================================================
# LOGS
# ============================================================================

cmd_logs() {
    local job_name=$1

    if [ -z "${job_name}" ]; then
        log_error "Job name is required"
        echo "Usage: ./manage_workflow.sh logs <job-name>"
        echo ""
        echo "Example job names:"
        echo "  warlab-hr-int6024-company"
        echo "  warlab-hr-int0095e-worker-job"
        exit 1
    fi

    log_section "CloudWatch Logs for: ${job_name}"

    local log_group="/aws-glue/jobs/${job_name}"

    log_info "Tailing logs from: ${log_group}"
    echo "Press Ctrl+C to stop"
    echo ""

    aws logs tail "${log_group}" \
        --follow \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" || log_warning "Could not fetch logs. Job may not have run yet."
}

# ============================================================================
# DELETION/CLEANUP
# ============================================================================

cmd_delete_job() {
    local job_name=$1

    if [ -z "${job_name}" ]; then
        log_error "Job name is required"
        echo "Usage: ./manage_workflow.sh delete-job <job-name>"
        exit 1
    fi

    log_section "Deleting Job: ${job_name}"

    read -p "Are you sure you want to delete ${job_name}? (yes/no): " confirmation
    if [ "${confirmation}" != "yes" ]; then
        log_warning "Deletion cancelled"
        return
    fi

    aws glue delete-job \
        --name "${job_name}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}"

    log_success "Job deleted: ${job_name}"
}

cmd_delete_workflow() {
    log_section "Deleting Workflow: ${WORKFLOW_NAME}"

    read -p "Are you sure you want to delete the entire workflow and all associated jobs? (yes/no): " confirmation
    if [ "${confirmation}" != "yes" ]; then
        log_warning "Deletion cancelled"
        return
    fi

    log_info "Deleting all HR Datamart jobs..."

    local jobs=$(aws glue list-jobs \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --output json)

    local hr_jobs=$(echo "${jobs}" | jq -r '.JobNames[]' | grep "warlab-hr-" || true)

    if [ -n "${hr_jobs}" ]; then
        echo "${hr_jobs}" | while read -r job_name; do
            log_info "Deleting job: ${job_name}"
            aws glue delete-job \
                --name "${job_name}" \
                --region "${AWS_REGION}" \
                --profile "${AWS_PROFILE}" || log_warning "Could not delete ${job_name}"
        done
    fi

    # Delete triggers
    log_info "Deleting workflow triggers..."
    local triggers=$(aws glue list-triggers \
        --filter "Name=WORKFLOW_NAME,Values=${WORKFLOW_NAME}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --output json)

    local trigger_names=$(echo "${triggers}" | jq -r '.TriggerNames[]' || true)

    if [ -n "${trigger_names}" ]; then
        echo "${trigger_names}" | while read -r trigger_name; do
            log_info "Deleting trigger: ${trigger_name}"
            aws glue delete-trigger \
                --name "${trigger_name}" \
                --region "${AWS_REGION}" \
                --profile "${AWS_PROFILE}" || log_warning "Could not delete ${trigger_name}"
        done
    fi

    # Delete workflow
    log_info "Deleting workflow: ${WORKFLOW_NAME}"
    aws glue delete-workflow \
        --name "${WORKFLOW_NAME}" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}"

    log_success "Workflow and all associated resources deleted"
}

# ============================================================================
# HELP
# ============================================================================

show_help() {
    cat << 'EOF'
AWS Glue Workflow Management Script

Usage: ./manage_workflow.sh [command] [options]

Commands:

  start
    Start a new workflow run
    Example: ./manage_workflow.sh start

  status
    Get status of the latest workflow run
    Example: ./manage_workflow.sh status

  list-runs
    List recent workflow runs (up to 10)
    Example: ./manage_workflow.sh list-runs

  list-jobs
    List all HR Datamart Glue jobs
    Example: ./manage_workflow.sh list-jobs

  job-status <job-name>
    Get status and details of a specific job
    Example: ./manage_workflow.sh job-status warlab-hr-int6024-company

  logs <job-name>
    Tail CloudWatch logs for a job in real-time
    Example: ./manage_workflow.sh logs warlab-hr-int6024-company

  delete-job <job-name>
    Delete a single Glue job
    Example: ./manage_workflow.sh delete-job warlab-hr-int6024-company

  delete-workflow
    Delete entire workflow and all associated jobs
    Example: ./manage_workflow.sh delete-workflow

  help
    Show this help message
    Example: ./manage_workflow.sh help

Options:

  AWS_REGION     AWS region (default: us-east-1)
  AWS_PROFILE    AWS CLI profile (default: default)

Examples:

  # Set region for command
  AWS_REGION=us-west-2 ./manage_workflow.sh status

  # Set both region and profile
  AWS_REGION=us-east-1 AWS_PROFILE=prod ./manage_workflow.sh start

  # Monitor workflow status continuously
  watch -n 10 './manage_workflow.sh status'

  # Tail logs for a job in real-time
  ./manage_workflow.sh logs warlab-hr-int6024-company

EOF
}

# ============================================================================
# MAIN
# ============================================================================

main() {
    local command="${1:-help}"

    case "${command}" in
        start)
            cmd_start_workflow
            ;;
        status)
            cmd_workflow_status
            ;;
        list-runs)
            cmd_list_runs
            ;;
        list-jobs)
            cmd_list_jobs
            ;;
        job-status)
            cmd_job_status "$2"
            ;;
        logs)
            cmd_logs "$2"
            ;;
        delete-job)
            cmd_delete_job "$2"
            ;;
        delete-workflow)
            cmd_delete_workflow
            ;;
        help)
            show_help
            ;;
        *)
            log_error "Unknown command: ${command}"
            echo "Use './manage_workflow.sh help' for usage information"
            exit 1
            ;;
    esac
}

main "$@"
