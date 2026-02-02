"""
AWS Glue ETL Job: Load HR Workday Data to Redshift using COPY Command
======================================================================
This job uses Redshift's native COPY command for optimal performance.
Much faster than JDBC-based loading for large datasets.

Job Parameters:
    --S3_BUCKET: S3 bucket name containing HR data
    --S3_PREFIX: S3 prefix path to data files
    --REDSHIFT_WORKGROUP: Redshift Serverless workgroup name
    --REDSHIFT_DATABASE: Redshift database name
    --REDSHIFT_IAM_ROLE: IAM role ARN for Redshift to access S3
"""

import sys
import boto3
import time
import json
from awsglue.utils import getResolvedOptions

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET',
    'S3_PREFIX',
    'REDSHIFT_WORKGROUP',
    'REDSHIFT_DATABASE',
    'REDSHIFT_IAM_ROLE'
])

# Configuration
S3_BUCKET = args['S3_BUCKET']
S3_PREFIX = args['S3_PREFIX']
REDSHIFT_WORKGROUP = args['REDSHIFT_WORKGROUP']
REDSHIFT_DATABASE = args['REDSHIFT_DATABASE']
REDSHIFT_IAM_ROLE = args['REDSHIFT_IAM_ROLE']
SCHEMA_NAME = 'hr_workday'

# Initialize clients
redshift_data = boto3.client('redshift-data')

def execute_sql(sql, description="SQL Statement"):
    """Execute SQL using Redshift Data API and wait for completion"""
    print(f"\n  Executing: {description}")

    response = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql
    )

    statement_id = response['Id']

    # Poll for completion
    while True:
        status_response = redshift_data.describe_statement(Id=statement_id)
        status = status_response['Status']

        if status == 'FINISHED':
            result_rows = status_response.get('ResultRows', 0)
            print(f"    ✓ Completed ({result_rows} rows affected)")
            return True, result_rows

        elif status == 'FAILED':
            error = status_response.get('Error', 'Unknown error')
            print(f"    ✗ Failed: {error}")
            return False, error

        elif status == 'ABORTED':
            return False, "Query aborted"

        time.sleep(1)

def load_table_with_copy(table_name, s3_path):
    """Load table using COPY command"""
    print(f"\n{'='*60}")
    print(f"Loading: {SCHEMA_NAME}.{table_name}")
    print(f"Source: {s3_path}")
    print(f"{'='*60}")

    # Step 1: Truncate table
    truncate_sql = f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name};"
    success, _ = execute_sql(truncate_sql, "Truncate table")
    if not success:
        raise Exception(f"Failed to truncate {table_name}")

    # Step 2: Build COPY command
    copy_sql = f"""
    COPY {SCHEMA_NAME}.{table_name}
    FROM '{s3_path}'
    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
    FORMAT AS CSV
    IGNOREHEADER 1
    DATEFORMAT 'auto'
    TIMEFORMAT 'auto'
    BLANKSASNULL
    EMPTYASNULL
    TRUNCATECOLUMNS
    REGION '{boto3.Session().region_name}';
    """

    success, result = execute_sql(copy_sql, f"COPY from S3 to {table_name}")
    if not success:
        raise Exception(f"COPY failed for {table_name}: {result}")

    # Step 3: Get row count
    count_sql = f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table_name};"
    response = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=count_sql
    )
    statement_id = response['Id']

    # Wait for count query
    while True:
        status_response = redshift_data.describe_statement(Id=statement_id)
        if status_response['Status'] == 'FINISHED':
            break
        elif status_response['Status'] in ['FAILED', 'ABORTED']:
            return 0
        time.sleep(1)

    # Get the result
    result_response = redshift_data.get_statement_result(Id=statement_id)
    row_count = int(result_response['Records'][0][0]['longValue'])

    print(f"    Records loaded: {row_count:,}")
    return row_count

def update_audit_columns(table_name, s3_path):
    """Update audit columns after COPY"""
    update_sql = f"""
    UPDATE {SCHEMA_NAME}.{table_name}
    SET loaded_at = GETDATE(),
        source_file = '{s3_path}'
    WHERE loaded_at IS NULL OR source_file IS NULL;
    """
    execute_sql(update_sql, "Update audit columns")

def main():
    """Main ETL process"""
    print("\n" + "="*70)
    print("HR WORKDAY DATA LOAD - REDSHIFT COPY COMMAND")
    print("="*70)
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"S3 Prefix: {S3_PREFIX}")
    print(f"Redshift Workgroup: {REDSHIFT_WORKGROUP}")
    print(f"Database: {REDSHIFT_DATABASE}")
    print(f"IAM Role: {REDSHIFT_IAM_ROLE}")
    print("="*70)

    # Table configurations
    tables = [
        ("core_hr_employees", f"s3://{S3_BUCKET}/{S3_PREFIX}/core_hr_employees/"),
        ("job_movement_transactions", f"s3://{S3_BUCKET}/{S3_PREFIX}/job_movement_transactions/"),
        ("compensation_change_transactions", f"s3://{S3_BUCKET}/{S3_PREFIX}/compensation_change_transactions/"),
        ("worker_movement_transactions", f"s3://{S3_BUCKET}/{S3_PREFIX}/worker_movement_transactions/")
    ]

    results = []
    total_records = 0

    for table_name, s3_path in tables:
        try:
            record_count = load_table_with_copy(table_name, s3_path)
            update_audit_columns(table_name, s3_path)
            total_records += record_count
            results.append({
                "table": table_name,
                "status": "SUCCESS",
                "records": record_count
            })
        except Exception as e:
            print(f"\nERROR: {str(e)}")
            results.append({
                "table": table_name,
                "status": "FAILED",
                "error": str(e)
            })

    # Print summary
    print("\n" + "="*70)
    print("LOAD SUMMARY")
    print("="*70)

    for r in results:
        if r["status"] == "SUCCESS":
            print(f"  ✓ {r['table']}: {r['records']:,} records")
        else:
            print(f"  ✗ {r['table']}: FAILED - {r.get('error', 'Unknown')}")

    print("-"*70)
    print(f"  Total Records Loaded: {total_records:,}")
    print("="*70)

    # Raise exception if any failures
    failures = [r for r in results if r["status"] == "FAILED"]
    if failures:
        raise Exception(f"{len(failures)} table(s) failed to load")

    print("\n✓ All tables loaded successfully!")

if __name__ == "__main__":
    main()
