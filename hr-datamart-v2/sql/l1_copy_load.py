#!/usr/bin/env python3
"""
L1 COPY Load: S3 → Redshift L1 Staging via COPY commands.
===========================================================

Uses Redshift COPY command (via Data API) to load CSV data from S3
into L1 staging tables. This is the reliable path since Glue job
execution is not available in this environment.

Follows the spec:
  - COPY from S3 using IAM role
  - TIMEFORMAT 'auto', DATEFORMAT 'auto'
  - Truncate-and-reload for idempotent loads
  - Stamps ingest_timestamp, source_file_name via post-load UPDATE

V1 Lessons Applied:
  - Boolean columns as VARCHAR (handled by all-VARCHAR L1 DDL)
  - Standalone SQL statements for Data API (no multi-statement)
  - Column name sanitization (hyphens/slashes → underscores)

Author: WARLab Data Engineering
Version: 2.0
"""
import subprocess
import json
import time
import sys
from datetime import datetime

# Configuration
CLUSTER_ID = "warlab-hr-datamart"
DATABASE = "dev"
DB_USER = "admin"
IAM_ROLE = "arn:aws:iam::155659077496:role/RedshiftS3ReadRole"
S3_BUCKET = "warlab-hr-datamart-dev"
V2_PREFIX = "v2/workday/hrdp"
L1_SCHEMA = "v2_l1_workday"

# All 14 feeds
FEEDS = [
    "int0095e_worker_job",
    "int0096_worker_organization",
    "int0098_worker_compensation",
    "int270_rescinded",
    "int6020_grade_profile",
    "int6021_job_profile",
    "int6022_job_classification",
    "int6023_location",
    "int6024_company",
    "int6025_cost_center",
    "int6027_matrix_organization",
    "int6028_department_hierarchy",
    "int6031_worker_profile",
    "int6032_positions",
]


def run_sql(sql, description="", timeout_secs=120):
    """Execute a single SQL statement via Redshift Data API and wait for completion."""
    cmd = [
        "aws", "redshift-data", "execute-statement",
        "--cluster-identifier", CLUSTER_ID,
        "--database", DATABASE,
        "--db-user", DB_USER,
        "--sql", sql
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  EXECUTE ERROR: {result.stderr}", file=sys.stderr)
        return False, result.stderr

    response = json.loads(result.stdout)
    stmt_id = response["Id"]

    for _ in range(timeout_secs):
        time.sleep(1)
        check = subprocess.run(
            ["aws", "redshift-data", "describe-statement", "--id", stmt_id],
            capture_output=True, text=True
        )
        status_data = json.loads(check.stdout)
        status = status_data.get("Status", "")

        if status == "FINISHED":
            rows = status_data.get("ResultRows", 0)
            return True, f"OK ({rows} rows affected)"
        elif status in ("FAILED", "ABORTED"):
            error = status_data.get("Error", "Unknown error")
            return False, error

    return False, "TIMEOUT"


def get_csv_columns(feed_name):
    """Get CSV header columns by downloading first line from S3."""
    s3_path = f"s3://{S3_BUCKET}/{V2_PREFIX}/{feed_name}/"

    # List files
    result = subprocess.run(
        ["aws", "s3", "ls", s3_path],
        capture_output=True, text=True
    )
    lines = result.stdout.strip().split('\n')
    csv_file = None
    for line in lines:
        parts = line.strip().split()
        if parts and parts[-1].endswith('.csv'):
            csv_file = parts[-1]
            break

    if not csv_file:
        return None, None

    # Download to get header
    full_path = f"{s3_path}{csv_file}"
    subprocess.run(
        ["aws", "s3", "cp", full_path, "/tmp/copy_header_check.csv", "--quiet"],
        capture_output=True, text=True
    )

    with open("/tmp/copy_header_check.csv", 'r') as f:
        header_line = f.readline().strip()

    raw_columns = header_line.split(',')
    # Sanitize to match L1 table column names
    sanitized = []
    seen = {}
    for col in raw_columns:
        name = col.strip().lower().replace('-', '_').replace('/', '_').replace(' ', '_')
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        sanitized.append(name)

    return sanitized, csv_file


def load_feed(feed_name, batch_id):
    """Load a single feed: TRUNCATE → COPY → UPDATE metadata."""
    table = f"{L1_SCHEMA}.{feed_name}"
    s3_path = f"s3://{S3_BUCKET}/{V2_PREFIX}/{feed_name}/"

    print(f"\n{'='*60}")
    print(f"Loading: {feed_name}")
    print(f"  S3: {s3_path}")
    print(f"  Table: {table}")

    # Get column list for explicit column mapping in COPY
    columns, csv_file = get_csv_columns(feed_name)
    if not columns:
        print(f"  ERROR: Could not read CSV headers")
        return False

    print(f"  Columns: {len(columns)}")

    # Step 1: TRUNCATE
    print(f"  [1/3] Truncating {table}...")
    ok, msg = run_sql(f"TRUNCATE TABLE {table};", "truncate")
    if not ok:
        print(f"  TRUNCATE FAILED: {msg}")
        return False

    # Step 2: COPY from S3
    # Explicit column list ensures CSV columns map to sanitized table columns
    col_list = ", ".join(columns)
    copy_sql = f"""COPY {table} ({col_list})
FROM '{s3_path}'
IAM_ROLE '{IAM_ROLE}'
CSV
IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
BLANKSASNULL
EMPTYASNULL
TRIMBLANKS
TRUNCATECOLUMNS
REGION 'us-east-1';"""

    print(f"  [2/3] COPY from S3...")
    ok, msg = run_sql(copy_sql, "copy", timeout_secs=300)
    if not ok:
        print(f"  COPY FAILED: {msg}")
        return False
    print(f"  COPY: {msg}")

    # Step 3: UPDATE warehouse metadata columns
    update_sql = f"""UPDATE {table}
SET ingest_timestamp = GETDATE(),
    source_file_name = '{csv_file}',
    etl_batch_id = '{batch_id}',
    insert_datetime = GETDATE(),
    update_datetime = GETDATE()
WHERE source_file_name IS NULL;"""

    print(f"  [3/3] Updating metadata columns...")
    ok, msg = run_sql(update_sql, "update_meta", timeout_secs=120)
    if not ok:
        print(f"  UPDATE FAILED: {msg}")
        return False
    print(f"  UPDATE: {msg}")

    return True


def verify_loads():
    """Verify row counts in all L1 tables."""
    print(f"\n{'='*60}")
    print("VERIFICATION: Row counts")
    print(f"{'='*60}")

    total = 0
    for feed in sorted(FEEDS):
        table = f"{L1_SCHEMA}.{feed}"
        ok, msg = run_sql(f"SELECT COUNT(*) FROM {table};", "count")
        if ok:
            # Get the actual count
            # Need to fetch result
            pass

    # Use a single query to get all counts
    union_parts = []
    for feed in sorted(FEEDS):
        table = f"{L1_SCHEMA}.{feed}"
        union_parts.append(f"SELECT '{feed}' as feed, COUNT(*) as cnt FROM {table}")

    count_sql = " UNION ALL ".join(union_parts) + " ORDER BY feed;"

    cmd = [
        "aws", "redshift-data", "execute-statement",
        "--cluster-identifier", CLUSTER_ID,
        "--database", DATABASE,
        "--db-user", DB_USER,
        "--sql", count_sql
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    response = json.loads(result.stdout)
    stmt_id = response["Id"]

    for _ in range(60):
        time.sleep(1)
        check = subprocess.run(
            ["aws", "redshift-data", "describe-statement", "--id", stmt_id],
            capture_output=True, text=True
        )
        sd = json.loads(check.stdout)
        if sd["Status"] == "FINISHED":
            res = subprocess.run(
                ["aws", "redshift-data", "get-statement-result", "--id", stmt_id],
                capture_output=True, text=True
            )
            data = json.loads(res.stdout)
            total = 0
            for row in data.get("Records", []):
                feed_name = row[0].get("stringValue", "")
                count = int(row[1].get("longValue", row[1].get("stringValue", 0)))
                total += count
                status = "✓" if count > 0 else "✗ EMPTY"
                print(f"  {feed_name:45s} {count:>10,}  {status}")
            print(f"  {'TOTAL':45s} {total:>10,}")
            return total > 0
        elif sd["Status"] in ("FAILED", "ABORTED"):
            print(f"  Verification query failed: {sd.get('Error')}")
            return False

    return False


def main():
    start_time = datetime.now()
    batch_id = f"v2_copy_{start_time.strftime('%Y%m%d_%H%M%S')}"

    print("=" * 60)
    print("V2 HR Datamart - L1 COPY Load")
    print(f"Batch ID: {batch_id}")
    print(f"Started: {start_time}")
    print("=" * 60)

    success_count = 0
    fail_count = 0

    for feed in FEEDS:
        if load_feed(feed, batch_id):
            success_count += 1
        else:
            fail_count += 1

    print(f"\n{'='*60}")
    print(f"Load Summary: {success_count} succeeded, {fail_count} failed")
    print(f"Duration: {(datetime.now() - start_time).total_seconds():.1f}s")
    print(f"{'='*60}")

    if success_count == len(FEEDS):
        verify_loads()

    return fail_count == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
