"""
AWS Glue ETL Job: Load HR Workday Data to Redshift
===================================================
This job loads CSV files from S3 into Redshift Serverless tables.
Uses full-load strategy (truncate and reload).

Job Parameters:
    --S3_BUCKET: S3 bucket name containing HR data
    --S3_PREFIX: S3 prefix path to data files
    --REDSHIFT_WORKGROUP: Redshift Serverless workgroup name
    --REDSHIFT_DATABASE: Redshift database name
    --REDSHIFT_IAM_ROLE: IAM role ARN for Redshift to access S3
    --REDSHIFT_SECRET_ARN: Secrets Manager secret ARN for credentials
"""

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import current_timestamp, lit
import time

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET',
    'S3_PREFIX',
    'REDSHIFT_WORKGROUP',
    'REDSHIFT_DATABASE',
    'REDSHIFT_IAM_ROLE',
    'REDSHIFT_SECRET_ARN'
])

job.init(args['JOB_NAME'], args)

# Configuration
S3_BUCKET = args['S3_BUCKET']
S3_PREFIX = args['S3_PREFIX']
REDSHIFT_WORKGROUP = args['REDSHIFT_WORKGROUP']
REDSHIFT_DATABASE = args['REDSHIFT_DATABASE']
REDSHIFT_IAM_ROLE = args['REDSHIFT_IAM_ROLE']
REDSHIFT_SECRET_ARN = args['REDSHIFT_SECRET_ARN']
SCHEMA_NAME = 'hr_workday'

# Initialize Redshift Data API client
redshift_data = boto3.client('redshift-data')
secrets_manager = boto3.client('secretsmanager')

def get_redshift_credentials():
    """Get Redshift credentials from Secrets Manager"""
    response = secrets_manager.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
    import json
    secret = json.loads(response['SecretString'])
    return secret['username'], secret['password']

def execute_redshift_sql(sql, description=""):
    """Execute SQL on Redshift Serverless using Data API"""
    print(f"Executing: {description}")
    print(f"SQL: {sql[:200]}...")

    response = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql
    )

    statement_id = response['Id']

    # Wait for completion
    while True:
        status_response = redshift_data.describe_statement(Id=statement_id)
        status = status_response['Status']

        if status == 'FINISHED':
            print(f"  ✓ Completed: {description}")
            return True
        elif status == 'FAILED':
            error = status_response.get('Error', 'Unknown error')
            print(f"  ✗ Failed: {error}")
            raise Exception(f"SQL execution failed: {error}")
        elif status == 'ABORTED':
            raise Exception("SQL execution was aborted")

        time.sleep(1)

def load_table(table_name, s3_path, column_mapping):
    """Load a single table from S3 to Redshift"""
    print(f"\n{'='*60}")
    print(f"Loading table: {SCHEMA_NAME}.{table_name}")
    print(f"Source: {s3_path}")
    print(f"{'='*60}")

    # Step 1: Read from S3
    print(f"[1/4] Reading CSV from S3...")
    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .option("quote", '"') \
                   .option("escape", '"') \
                   .csv(s3_path)

    record_count = df.count()
    print(f"      Records read: {record_count:,}")

    # Step 2: Add audit columns
    print(f"[2/4] Adding audit columns...")
    df = df.withColumn("loaded_at", current_timestamp())
    df = df.withColumn("source_file", lit(s3_path))

    # Step 3: Truncate target table
    print(f"[3/4] Truncating target table...")
    truncate_sql = f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name};"
    execute_redshift_sql(truncate_sql, f"Truncate {table_name}")

    # Step 4: Write to Redshift using COPY
    print(f"[4/4] Loading data to Redshift...")

    # Convert to DynamicFrame for Glue write
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, table_name)

    # Write to Redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="",  # Not using catalog connection
        connection_options={
            "url": f"jdbc:redshift://{REDSHIFT_WORKGROUP}.{boto3.Session().region_name}.redshift-serverless.amazonaws.com:5439/{REDSHIFT_DATABASE}",
            "dbtable": f"{SCHEMA_NAME}.{table_name}",
            "redshiftTmpDir": f"s3://{S3_BUCKET}/temp/glue/",
            "aws_iam_role": REDSHIFT_IAM_ROLE
        },
        redshift_tmp_dir=f"s3://{S3_BUCKET}/temp/glue/"
    )

    print(f"      ✓ Loaded {record_count:,} records to {SCHEMA_NAME}.{table_name}")
    return record_count

def main():
    """Main ETL process"""
    print("\n" + "="*70)
    print("HR WORKDAY DATA LOAD - GLUE ETL JOB")
    print("="*70)
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"S3 Prefix: {S3_PREFIX}")
    print(f"Redshift Workgroup: {REDSHIFT_WORKGROUP}")
    print(f"Database: {REDSHIFT_DATABASE}")
    print("="*70 + "\n")

    # Table configurations
    tables = [
        {
            "name": "core_hr_employees",
            "s3_path": f"s3://{S3_BUCKET}/{S3_PREFIX}/core_hr_employees/",
            "columns": []  # Will infer from CSV
        },
        {
            "name": "job_movement_transactions",
            "s3_path": f"s3://{S3_BUCKET}/{S3_PREFIX}/job_movement_transactions/",
            "columns": []
        },
        {
            "name": "compensation_change_transactions",
            "s3_path": f"s3://{S3_BUCKET}/{S3_PREFIX}/compensation_change_transactions/",
            "columns": []
        },
        {
            "name": "worker_movement_transactions",
            "s3_path": f"s3://{S3_BUCKET}/{S3_PREFIX}/worker_movement_transactions/",
            "columns": []
        }
    ]

    # Load each table
    total_records = 0
    results = []

    for table in tables:
        try:
            count = load_table(table["name"], table["s3_path"], table["columns"])
            total_records += count
            results.append({"table": table["name"], "status": "SUCCESS", "records": count})
        except Exception as e:
            print(f"ERROR loading {table['name']}: {str(e)}")
            results.append({"table": table["name"], "status": "FAILED", "error": str(e)})

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
    print("="*70 + "\n")

    # Check for failures
    failures = [r for r in results if r["status"] == "FAILED"]
    if failures:
        raise Exception(f"{len(failures)} table(s) failed to load")

if __name__ == "__main__":
    main()
    job.commit()
