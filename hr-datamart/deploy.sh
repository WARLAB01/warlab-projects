#!/usr/bin/env bash

################################################################################
# HR DATAMART DEPLOYMENT SCRIPT
################################################################################
#
# PURPOSE:
#   Automates the complete HR Datamart deployment pipeline from synthetic data
#   generation through L1 staging load and L3 dimensional/fact table population.
#   Handles data mapping, S3 uploads, Redshift DDL/DML, and QA validation.
#
# USAGE:
#   ./deploy.sh [OPTIONS]
#
# OPTIONS:
#   --skip-data-gen       Skip synthetic data generation (use existing CSVs)
#   --skip-s3-upload      Skip S3 upload (assumes data already in S3)
#   --skip-l1             Skip L1 schema creation and data load
#   --skip-l3             Skip L3 schema creation and data load
#   --skip-qa             Skip QA tests and completion report
#   --dry-run             Print all commands without executing them
#   --data-date DATE      Override default data date (format: YYYY-MM-DD)
#   --csv-dir DIR         Directory containing CSV files (default: ./data/csv)
#   -h, --help            Display this help message
#
# PREREQUISITES:
#   - AWS CLI v2+ installed and configured
#   - PostgreSQL psql client installed
#   - Python 3.6+ installed
#   - Data generator script: generate_all_feeds.py
#   - SQL scripts in ./sql/ directory:
#       * l1_schema_ddl.sql
#       * l1_copy_statements.sql
#       * l3_source_ddl.sql, l3_dim_ddl.sql, l3_fact_ddl.sql
#       * l3_source_load.sql, l3_dim_load.sql, l3_fact_load.sql
#       * qa_tests.sql
#       * completion_report.sql
#
# ENVIRONMENT VARIABLES REQUIRED:
#   - S3_BUCKET           S3 bucket name for staging data
#   - REDSHIFT_HOST       Redshift cluster endpoint
#   - REDSHIFT_PORT       Redshift port (default: 5439)
#   - REDSHIFT_DB         Redshift database (default: dev)
#   - REDSHIFT_USER       Redshift user (default: admin)
#   - REDSHIFT_IAM_ROLE_ARN    IAM role ARN for S3 access
#   - PGPASSWORD          Password for Redshift user
#
# AUTHOR:
#   HR Datamart Team
#
# VERSION:
#   1.0.0
#
################################################################################

set -euo pipefail

################################################################################
# CONFIGURATION & DEFAULTS
################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
START_TIME="$(date +%s)"

# Options
SKIP_DATA_GEN="${SKIP_DATA_GEN:-false}"
SKIP_S3_UPLOAD="${SKIP_S3_UPLOAD:-false}"
SKIP_L1="${SKIP_L1:-false}"
SKIP_L3="${SKIP_L3:-false}"
SKIP_QA="${SKIP_QA:-false}"
DRY_RUN="${DRY_RUN:-false}"
DATA_DATE="${DATA_DATE:-2026-02-05}"
CSV_DIR="${CSV_DIR:-${SCRIPT_DIR}/data/csv}"

# Directories
SQL_DIR="${SCRIPT_DIR}/sql"
TEMP_DIR=""
GENERATOR_SCRIPT="${SCRIPT_DIR}/generate_all_feeds.py"

# Redshift defaults
REDSHIFT_PORT="${REDSHIFT_PORT:-5439}"
REDSHIFT_DB="${REDSHIFT_DB:-dev}"
REDSHIFT_USER="${REDSHIFT_USER:-admin}"

# Feed name mapping: CSV feed name → S3 feed identifier
declare -A FEED_MAP=(
  ["dly_grade_profile"]="int6020_grade_profile"
  ["dly_job_profile"]="int6021_job_profile"
  ["dly_job_classification"]="int6022_job_classification"
  ["dly_location"]="int6023_location"
  ["dly_company"]="int6024_company"
  ["dly_cost_center"]="int6025_cost_center"
  ["dly_department_hierarchy"]="int6028_department_hierarchy"
  ["dly_positions"]="int6032_positions"
  ["dly_worker_job"]="int0095e_worker_job"
  ["dly_worker_organization"]="int0096_worker_organization"
  ["dly_worker_compensation"]="int0098_worker_compensation"
  ["dly_rescinded"]="int270_rescinded"
)

# Tracking
STEP_COUNT=0
QA_RESULTS_FILE=""
COMPLETION_RESULTS_FILE=""

################################################################################
# LOGGING & OUTPUT FUNCTIONS
################################################################################

log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

log_step() {
  local step_num="$1"
  local step_name="$2"
  STEP_COUNT=$((STEP_COUNT + 1))
  echo ""
  echo "================================================================================"
  log "STEP $step_num: $step_name"
  echo "================================================================================"
}

log_info() {
  log "INFO: $*"
}

log_warn() {
  log "WARN: $*" >&2
}

log_error() {
  log "ERROR: $*" >&2
}

display_help() {
  head -n 60 "$0" | tail -n 58 | grep -E "^#" | sed 's/^# *//'
  exit 0
}

display_usage() {
  cat << EOF
Usage: $SCRIPT_NAME [OPTIONS]

OPTIONS:
  --skip-data-gen       Skip synthetic data generation (use existing CSVs)
  --skip-s3-upload      Skip S3 upload (assumes data already in S3)
  --skip-l1             Skip L1 schema creation and data load
  --skip-l3             Skip L3 schema creation and data load
  --skip-qa             Skip QA tests and completion report
  --dry-run             Print all commands without executing them
  --data-date DATE      Override default data date (format: YYYY-MM-DD)
  --csv-dir DIR         Directory containing CSV files (default: ./data/csv)
  -h, --help            Display full help message

EXAMPLES:
  # Full deployment
  ./deploy.sh

  # Skip data generation, use existing CSVs
  ./deploy.sh --skip-data-gen

  # Dry run to see what would happen
  ./deploy.sh --dry-run

  # Custom data date
  ./deploy.sh --data-date 2026-02-10

EOF
}

################################################################################
# CLEANUP & ERROR HANDLING
################################################################################

cleanup() {
  local exit_code=$?
  if [[ -n "$TEMP_DIR" ]] && [[ -d "$TEMP_DIR" ]]; then
    log "Cleaning up temporary directory: $TEMP_DIR"
    rm -rf "$TEMP_DIR"
  fi
  return $exit_code
}

trap cleanup EXIT

error_exit() {
  local step="$1"
  local message="$2"
  log_error "DEPLOYMENT FAILED at $step"
  log_error "Message: $message"
  echo ""
  log "=== TROUBLESHOOTING ==="
  case "$step" in
    "prerequisites")
      log "Ensure aws, psql, and python3 are installed and on your PATH."
      log "Run: which aws psql python3"
      ;;
    "data-generation")
      log "Check that generate_all_feeds.py exists and is executable."
      log "Check CSV_DIR: $CSV_DIR"
      ;;
    "s3-upload")
      log "Verify S3_BUCKET is set and credentials are valid:"
      log "  aws s3 ls s3://\$S3_BUCKET/"
      ;;
    "l1-ddl"|"l1-copy"|"l3-ddl"|"l3-load"|"qa")
      log "Check Redshift connectivity:"
      log "  PGPASSWORD=\$PGPASSWORD psql -h \$REDSHIFT_HOST -p \$REDSHIFT_PORT -d \$REDSHIFT_DB -U \$REDSHIFT_USER -c 'SELECT 1'"
      log "Review SQL scripts in: $SQL_DIR"
      ;;
  esac
  exit 1
}

################################################################################
# EXECUTION HELPERS
################################################################################

execute_cmd() {
  local cmd="$1"
  local description="${2:-}"
  if [[ -n "$description" ]]; then
    log "$description"
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] $cmd"
    return 0
  else
    eval "$cmd"
  fi
}

run_sql_file() {
  local file="$1"
  local description="$2"
  local schema="${3:-}"

  if [[ ! -f "$file" ]]; then
    error_exit "sql-file" "SQL file not found: $file"
  fi

  log "Executing SQL file: $description"
  log "File: $file"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -d $REDSHIFT_DB -U $REDSHIFT_USER -v ON_ERROR_STOP=1 -f $file"
    return 0
  fi

  PGPASSWORD="$PGPASSWORD" psql \
    -h "$REDSHIFT_HOST" \
    -p "$REDSHIFT_PORT" \
    -d "$REDSHIFT_DB" \
    -U "$REDSHIFT_USER" \
    -v ON_ERROR_STOP=1 \
    -f "$file" || error_exit "sql-execution" "Failed to execute: $description"
}

run_sql_string() {
  local sql="$1"
  local description="$2"

  log "Executing SQL: $description"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] psql SQL command: $description"
    echo "$sql" | head -c 200
    echo "..."
    return 0
  fi

  echo "$sql" | PGPASSWORD="$PGPASSWORD" psql \
    -h "$REDSHIFT_HOST" \
    -p "$REDSHIFT_PORT" \
    -d "$REDSHIFT_DB" \
    -U "$REDSHIFT_USER" \
    -v ON_ERROR_STOP=1 \
    -q || error_exit "sql-execution" "Failed to execute: $description"
}

################################################################################
# VALIDATION
################################################################################

validate_prereqs() {
  log_info "Checking required tools..."

  # Check commands
  local missing_tools=()
  for tool in aws psql python3; do
    if ! command -v "$tool" &> /dev/null; then
      missing_tools+=("$tool")
    fi
  done

  if [[ ${#missing_tools[@]} -gt 0 ]]; then
    error_exit "prerequisites" "Missing required tools: ${missing_tools[*]}"
  fi

  log_info "All required tools found"

  # Check environment variables
  log_info "Validating required environment variables..."
  local missing_vars=()

  for var in S3_BUCKET REDSHIFT_HOST REDSHIFT_IAM_ROLE_ARN PGPASSWORD; do
    if [[ -z "${!var:-}" ]]; then
      missing_vars+=("$var")
    fi
  done

  if [[ ${#missing_vars[@]} -gt 0 ]]; then
    error_exit "prerequisites" "Missing required environment variables: ${missing_vars[*]}"
  fi

  log_info "All required environment variables are set"

  # Check SQL directory
  if [[ ! -d "$SQL_DIR" ]]; then
    error_exit "prerequisites" "SQL directory not found: $SQL_DIR"
  fi

  log_info "SQL directory found: $SQL_DIR"

  # Check data generator if not skipping data gen
  if [[ "$SKIP_DATA_GEN" != "true" ]]; then
    if [[ ! -f "$GENERATOR_SCRIPT" ]]; then
      error_exit "prerequisites" "Data generator script not found: $GENERATOR_SCRIPT"
    fi
    log_info "Data generator script found: $GENERATOR_SCRIPT"
  fi

  log_info "Prerequisites validation complete"
}

################################################################################
# DATA GENERATION
################################################################################

generate_data() {
  log_info "Creating temporary output directory..."
  TEMP_DIR=$(mktemp -d)
  log_info "Temporary directory: $TEMP_DIR"

  # Create a modified copy of the generator with OUTPUT_DIR set to temp directory
  log_info "Creating modified data generator with OUTPUT_DIR=$TEMP_DIR..."
  local temp_generator="$TEMP_DIR/generate_all_feeds.py"
  sed "s|OUTPUT_DIR = .*|OUTPUT_DIR = '$TEMP_DIR'|g" "$GENERATOR_SCRIPT" > "$temp_generator"

  if [[ "$DRY_RUN" != "true" ]]; then
    # Run the data generator
    log_info "Executing data generator..."
    python3 "$temp_generator" || error_exit "data-generation" "Data generator failed"
  else
    echo "[DRY RUN] python3 $temp_generator"
  fi

  # Copy CSV files to the configured CSV directory
  log_info "Copying generated CSV files to: $CSV_DIR"
  mkdir -p "$CSV_DIR"

  if [[ "$DRY_RUN" != "true" ]]; then
    cp "$TEMP_DIR"/*.csv "$CSV_DIR/" || error_exit "data-generation" "Failed to copy CSV files"
    log_info "Generated $(ls "$CSV_DIR"/*.csv 2>/dev/null | wc -l) CSV files"
  else
    echo "[DRY RUN] cp $TEMP_DIR/*.csv $CSV_DIR/"
  fi
}

################################################################################
# S3 UPLOAD
################################################################################

upload_to_s3() {
  if [[ ! -d "$CSV_DIR" ]]; then
    error_exit "s3-upload" "CSV directory not found: $CSV_DIR"
  fi

  local csv_count=0
  local upload_count=0

  # Find all CSV files
  while IFS= read -r csv_file; do
    csv_count=$((csv_count + 1))
    local basename=$(basename "$csv_file")

    # Extract feed name from filename
    # Example: workday.hrdp.dly_grade_profile.full.20260205060000.csv → dly_grade_profile
    local feed_name
    feed_name=$(echo "$basename" | sed 's/workday\.hrdp\.\(.*\)\.full\..*/\1/')

    # Look up S3 feed identifier
    if [[ -z "${FEED_MAP[$feed_name]:-}" ]]; then
      log_warn "Unknown feed name: $feed_name (file: $basename). Skipping."
      continue
    fi

    local s3_feed="${FEED_MAP[$feed_name]}"
    local s3_path="s3://${S3_BUCKET}/workday/hrdp/${s3_feed}/dt=${DATA_DATE}/"

    log_info "Uploading: $basename → $s3_path"

    if [[ "$DRY_RUN" != "true" ]]; then
      aws s3 cp "$csv_file" "$s3_path" || error_exit "s3-upload" "Failed to upload $basename to $s3_path"
      upload_count=$((upload_count + 1))
    else
      echo "[DRY RUN] aws s3 cp $csv_file $s3_path"
      upload_count=$((upload_count + 1))
    fi
  done < <(find "$CSV_DIR" -maxdepth 1 -name "*.csv" -type f)

  if [[ $csv_count -eq 0 ]]; then
    log_warn "No CSV files found in: $CSV_DIR"
  else
    log_info "S3 upload complete: $upload_count/$csv_count files uploaded"
  fi
}

################################################################################
# REDSHIFT SCHEMA & LOADING
################################################################################

create_schemas() {
  log_info "Creating L1 schema if not exists..."
  run_sql_string "CREATE SCHEMA IF NOT EXISTS l1_stage;" "Create L1 schema"

  log_info "Creating L3 schema if not exists..."
  run_sql_string "CREATE SCHEMA IF NOT EXISTS l3_analytics;" "Create L3 schema"
}

run_l1_ddl() {
  local ddl_file="$SQL_DIR/l1_schema_ddl.sql"
  if [[ ! -f "$ddl_file" ]]; then
    log_warn "L1 DDL file not found: $ddl_file. Skipping."
    return 0
  fi
  run_sql_file "$ddl_file" "L1 Schema DDL"
}

run_l1_copy() {
  local copy_file="$SQL_DIR/l1_copy_statements.sql"
  if [[ ! -f "$copy_file" ]]; then
    log_warn "L1 COPY file not found: $copy_file. Skipping."
    return 0
  fi

  log_info "Processing L1 COPY statements with variable substitution..."

  # Read, substitute variables, and execute
  local sql_content
  sql_content=$(cat "$copy_file")

  # Replace placeholders
  sql_content="${sql_content//\$\{S3_BUCKET\}/$S3_BUCKET}"
  sql_content="${sql_content//\$\{REDSHIFT_IAM_ROLE_ARN\}/$REDSHIFT_IAM_ROLE_ARN}"
  sql_content="${sql_content//dt=YYYY-MM-DD/dt=$DATA_DATE}"

  run_sql_string "$sql_content" "L1 COPY FROM S3"
}

run_l3_ddl() {
  local source_ddl="$SQL_DIR/l3_source_ddl.sql"
  local dim_ddl="$SQL_DIR/l3_dim_ddl.sql"
  local fact_ddl="$SQL_DIR/l3_fact_ddl.sql"

  if [[ -f "$source_ddl" ]]; then
    run_sql_file "$source_ddl" "L3 Source DDL"
  fi

  if [[ -f "$dim_ddl" ]]; then
    run_sql_file "$dim_ddl" "L3 Dimension DDL"
  fi

  if [[ -f "$fact_ddl" ]]; then
    run_sql_file "$fact_ddl" "L3 Fact DDL"
  fi
}

run_l3_source_load() {
  local source_load="$SQL_DIR/l3_source_load.sql"
  if [[ ! -f "$source_load" ]]; then
    log_warn "L3 source load file not found: $source_load. Skipping."
    return 0
  fi
  run_sql_file "$source_load" "L3 Source Load"
}

run_l3_dim_load() {
  local dim_load="$SQL_DIR/l3_dim_load.sql"
  if [[ ! -f "$dim_load" ]]; then
    log_warn "L3 dimension load file not found: $dim_load. Skipping."
    return 0
  fi
  run_sql_file "$dim_load" "L3 Dimension Load (SCD2)"
}

run_l3_fact_load() {
  local fact_load="$SQL_DIR/l3_fact_load.sql"
  if [[ ! -f "$fact_load" ]]; then
    log_warn "L3 fact load file not found: $fact_load. Skipping."
    return 0
  fi
  run_sql_file "$fact_load" "L3 Fact Load"
}

################################################################################
# QA & REPORTING
################################################################################

run_qa() {
  local qa_file="$SQL_DIR/qa_tests.sql"
  if [[ ! -f "$qa_file" ]]; then
    log_warn "QA tests file not found: $qa_file. Skipping."
    return 0
  fi

  log_info "Running QA validation tests..."

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Running QA tests from $qa_file"
    return 0
  fi

  # Execute QA tests and capture output
  QA_RESULTS_FILE=$(mktemp)
  PGPASSWORD="$PGPASSWORD" psql \
    -h "$REDSHIFT_HOST" \
    -p "$REDSHIFT_PORT" \
    -d "$REDSHIFT_DB" \
    -U "$REDSHIFT_USER" \
    -v ON_ERROR_STOP=1 \
    -f "$qa_file" | tee "$QA_RESULTS_FILE" || error_exit "qa" "QA tests failed"

  log_info "QA validation complete"
}

run_completion_report() {
  local report_file="$SQL_DIR/completion_report.sql"
  if [[ ! -f "$report_file" ]]; then
    log_warn "Completion report file not found: $report_file. Skipping."
    return 0
  fi

  log_info "Generating completion report..."

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Generating completion report from $report_file"
    return 0
  fi

  # Execute report and capture output
  COMPLETION_RESULTS_FILE=$(mktemp)
  PGPASSWORD="$PGPASSWORD" psql \
    -h "$REDSHIFT_HOST" \
    -p "$REDSHIFT_PORT" \
    -d "$REDSHIFT_DB" \
    -U "$REDSHIFT_USER" \
    -v ON_ERROR_STOP=1 \
    -f "$report_file" | tee "$COMPLETION_RESULTS_FILE" || error_exit "report" "Report generation failed"

  log_info "Completion report generated"
}

################################################################################
# SUMMARY & REPORTING
################################################################################

display_summary() {
  local end_time="$(date +%s)"
  local duration=$((end_time - START_TIME))
  local minutes=$((duration / 60))
  local seconds=$((duration % 60))

  echo ""
  echo "================================================================================"
  log "DEPLOYMENT SUMMARY"
  echo "================================================================================"
  echo ""
  log "Start Time:        $(date -d @"$START_TIME" +'%Y-%m-%d %H:%M:%S')"
  log "End Time:          $(date -d @"$end_time" +'%Y-%m-%d %H:%M:%S')"
  log "Total Duration:    ${minutes}m ${seconds}s"
  echo ""
  log "Data Date:         $DATA_DATE"
  log "S3 Bucket:         $S3_BUCKET"
  log "Redshift Host:     $REDSHIFT_HOST"
  log "Redshift DB:       $REDSHIFT_DB"
  echo ""
  log "Steps Completed:   $STEP_COUNT"
  log "Dry Run Mode:      $DRY_RUN"
  echo ""

  if [[ -f "$QA_RESULTS_FILE" ]]; then
    log "QA Results:"
    tail -n 20 "$QA_RESULTS_FILE" | sed 's/^/  /'
  fi

  echo ""
  log "DEPLOYMENT STATUS: SUCCESS"
  echo "================================================================================"
}

################################################################################
# ARGUMENT PARSING
################################################################################

parse_arguments() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-data-gen)
        SKIP_DATA_GEN="true"
        log_info "Skipping data generation"
        shift
        ;;
      --skip-s3-upload)
        SKIP_S3_UPLOAD="true"
        log_info "Skipping S3 upload"
        shift
        ;;
      --skip-l1)
        SKIP_L1="true"
        log_info "Skipping L1 schema and loading"
        shift
        ;;
      --skip-l3)
        SKIP_L3="true"
        log_info "Skipping L3 schema and loading"
        shift
        ;;
      --skip-qa)
        SKIP_QA="true"
        log_info "Skipping QA validation"
        shift
        ;;
      --dry-run)
        DRY_RUN="true"
        log_info "DRY RUN MODE: Commands will be printed but not executed"
        shift
        ;;
      --data-date)
        if [[ -z "${2:-}" ]]; then
          log_error "Missing value for --data-date"
          display_usage
          exit 1
        fi
        DATA_DATE="$2"
        log_info "Data date set to: $DATA_DATE"
        shift 2
        ;;
      --csv-dir)
        if [[ -z "${2:-}" ]]; then
          log_error "Missing value for --csv-dir"
          display_usage
          exit 1
        fi
        CSV_DIR="$2"
        log_info "CSV directory set to: $CSV_DIR"
        shift 2
        ;;
      -h|--help)
        display_help
        ;;
      *)
        log_error "Unknown option: $1"
        display_usage
        exit 1
        ;;
    esac
  done
}

################################################################################
# MAIN EXECUTION
################################################################################

main() {
  log "=========================================="
  log "HR DATAMART DEPLOYMENT PIPELINE"
  log "=========================================="
  log "Start Time: $(date +'%Y-%m-%d %H:%M:%S')"
  echo ""

  # Step 0: Validate prerequisites
  log_step 0 "VALIDATING PREREQUISITES"
  validate_prereqs || exit 1

  # Step 1: Generate synthetic data
  if [[ "$SKIP_DATA_GEN" != "true" ]]; then
    log_step 1 "GENERATING SYNTHETIC DATA"
    generate_data || exit 1
  else
    log_step 1 "SKIPPING DATA GENERATION (using existing CSVs)"
    log_info "CSV directory: $CSV_DIR"
  fi

  # Step 2: Upload data to S3
  if [[ "$SKIP_S3_UPLOAD" != "true" ]]; then
    log_step 2 "UPLOADING DATA TO S3"
    upload_to_s3 || exit 1
  else
    log_step 2 "SKIPPING S3 UPLOAD"
    log_info "Assuming data already in S3"
  fi

  # Step 3-4: Create L1 schema and load data
  if [[ "$SKIP_L1" != "true" ]]; then
    log_step 3 "CREATING L1 SCHEMA AND TABLES"
    create_schemas || exit 1
    run_l1_ddl || exit 1

    log_step 4 "LOADING L1 DATA FROM S3"
    run_l1_copy || exit 1
  else
    log_step 3 "SKIPPING L1 SCHEMA AND LOAD"
  fi

  # Step 5-8: Create and load L3 schemas
  if [[ "$SKIP_L3" != "true" ]]; then
    log_step 5 "CREATING L3 SCHEMA AND TABLES"
    run_l3_ddl || exit 1

    log_step 6 "LOADING L3 SOURCE TABLES"
    run_l3_source_load || exit 1

    log_step 7 "LOADING L3 DIMENSIONS (SCD2)"
    run_l3_dim_load || exit 1

    log_step 8 "LOADING L3 FACT TABLES"
    run_l3_fact_load || exit 1
  else
    log_step 5 "SKIPPING L3 SCHEMA AND LOAD"
  fi

  # Step 9-10: QA validation and reporting
  if [[ "$SKIP_QA" != "true" ]]; then
    log_step 9 "RUNNING QA VALIDATION TESTS"
    run_qa || exit 1

    log_step 10 "GENERATING COMPLETION REPORT"
    run_completion_report || exit 1
  else
    log_step 9 "SKIPPING QA VALIDATION"
  fi

  # Final summary
  display_summary
}

################################################################################
# ENTRY POINT
################################################################################

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  parse_arguments "$@"
  main
fi
