#!/usr/bin/env python3
"""
AWS Glue Python Shell Job: S3 to L1 Staging Layer Load

Purpose:
    Loads CSV files from S3 into Redshift L1 (staging) schema using COPY commands.
    Processes all 12 feeds in dependency order with error handling and logging.

Author: Data Engineering Team
Version: 1.0
Created: 2024-01-31
"""

import sys
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import traceback

import boto3
import pg8000.native

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Attempt to import Glue utilities if running in Glue environment
try:
    from awsglue.utils import getResolvedOptions
    RUNNING_IN_GLUE = True
except ImportError:
    RUNNING_IN_GLUE = False
    logger.warning("Not running in Glue environment; using direct argument parsing")


# ============================================================================
# FEED INVENTORY & CONFIGURATION
# ============================================================================

FEED_INVENTORY = {
    "INT010": {
        "feed_name": "Employee Master",
        "csv_file": "int_010_employee.csv",
        "l1_table": "stg_employees",
        "expected_rows": 2500,
        "dependencies": [],
        "pii_flag": True,
        "order": 1
    },
    "INT020": {
        "feed_name": "Job Assignment",
        "csv_file": "int_020_job.csv",
        "l1_table": "stg_jobs",
        "expected_rows": 3200,
        "dependencies": ["INT010"],
        "pii_flag": False,
        "order": 2
    },
    "INT030": {
        "feed_name": "Organization",
        "csv_file": "int_030_organization.csv",
        "l1_table": "stg_organizations",
        "expected_rows": 150,
        "dependencies": [],
        "pii_flag": False,
        "order": 1
    },
    "INT040": {
        "feed_name": "Worker Status",
        "csv_file": "int_040_worker_status.csv",
        "l1_table": "stg_worker_status",
        "expected_rows": 12000,
        "dependencies": ["INT010"],
        "pii_flag": True,
        "order": 2
    },
    "INT050": {
        "feed_name": "Job Classification",
        "csv_file": "int_050_job_classification.csv",
        "l1_table": "stg_job_classification",
        "expected_rows": 85,
        "dependencies": [],
        "pii_flag": False,
        "order": 1
    },
    "INT060": {
        "feed_name": "Compensation",
        "csv_file": "int_060_compensation.csv",
        "l1_table": "stg_compensation",
        "expected_rows": 2800,
        "dependencies": ["INT010"],
        "pii_flag": True,
        "order": 2
    },
    "INT070": {
        "feed_name": "Benefits",
        "csv_file": "int_070_benefits.csv",
        "l1_table": "stg_benefits",
        "expected_rows": 5200,
        "dependencies": ["INT010"],
        "pii_flag": True,
        "order": 2
    },
    "INT080": {
        "feed_name": "Salary History",
        "csv_file": "int_080_salary_history.csv",
        "l1_table": "stg_salary_history",
        "expected_rows": 8500,
        "dependencies": ["INT010"],
        "pii_flag": True,
        "order": 2
    },
    "INT090": {
        "feed_name": "Hire Events",
        "csv_file": "int_090_hire_events.csv",
        "l1_table": "stg_hire_events",
        "expected_rows": 180,
        "dependencies": ["INT010"],
        "pii_flag": True,
        "order": 2
    },
    "INT100": {
        "feed_name": "Transfer Events",
        "csv_file": "int_100_transfer_events.csv",
        "l1_table": "stg_transfer_events",
        "expected_rows": 320,
        "dependencies": ["INT010", "INT020", "INT030"],
        "pii_flag": False,
        "order": 3
    },
    "INT110": {
        "feed_name": "Promotion Events",
        "csv_file": "int_110_promotion_events.csv",
        "l1_table": "stg_promotion_events",
        "expected_rows": 240,
        "dependencies": ["INT010", "INT020", "INT050"],
        "pii_flag": False,
        "order": 3
    },
    "INT270": {
        "feed_name": "Termination/Rescind",
        "csv_file": "int_270_termination.csv",
        "l1_table": "stg_termination",
        "expected_rows": 95,
        "dependencies": ["INT010"],
        "pii_flag": True,
        "order": 2
    }
}

# Load order respecting dependencies
LOAD_ORDER = sorted(FEED_INVENTORY.items(), key=lambda x: x[1]["order"])


# ============================================================================
# GLUE JOB CLASS
# ============================================================================

class S3ToL1Loader:
    """
    Handles loading CSV files from S3 into Redshift L1 staging layer.

    Responsibilities:
    - Parse job arguments
    - Connect to Redshift
    - Validate S3 files
    - Execute COPY commands in dependency order
    - Record timing and row counts
    - Handle errors and rollbacks
    """

    def __init__(self, args: Dict[str, str]):
        """
        Initialize loader with job arguments.

        Args:
            args: Dictionary of job parameters from Glue
                - s3_bucket: S3 bucket name
                - s3_prefix: S3 prefix for inbound files (e.g., workday/inbound)
                - redshift_host: Redshift cluster endpoint
                - redshift_port: Redshift port (default: 5439)
                - redshift_db: Redshift database name
                - redshift_schema: L1 schema name (e.g., l1_workday)
                - redshift_iam_role: IAM role for COPY command
                - data_date: Business date (YYYY-MM-DD)
                - etl_batch_id: Unique batch identifier
        """
        self.args = args
        self.s3_bucket = args.get("s3_bucket")
        self.s3_prefix = args.get("s3_prefix")
        self.redshift_host = args.get("redshift_host")
        self.redshift_port = int(args.get("redshift_port", "5439"))
        self.redshift_db = args.get("redshift_db")
        self.redshift_schema = args.get("redshift_schema")
        self.redshift_iam_role = args.get("redshift_iam_role")
        self.data_date = args.get("data_date")
        self.etl_batch_id = args.get("etl_batch_id")
        self.dry_run = args.get("dry_run", "false").lower() == "true"

        self.conn = None
        self.cursor = None
        self.s3_client = None
        self.load_results = []
        self.job_start_time = datetime.now()

        self._validate_arguments()
        self._initialize_clients()

    def _validate_arguments(self) -> None:
        """Validate that all required arguments are provided."""
        required = [
            "s3_bucket", "s3_prefix", "redshift_host", "redshift_db",
            "redshift_schema", "redshift_iam_role", "data_date", "etl_batch_id"
        ]
        missing = [arg for arg in required if not self.args.get(arg)]

        if missing:
            raise ValueError(f"Missing required arguments: {', '.join(missing)}")

        logger.info(f"Arguments validated. Dry run: {self.dry_run}")

    def _initialize_clients(self) -> None:
        """Initialize AWS and Redshift clients."""
        try:
            self.s3_client = boto3.client("s3")
            logger.info("S3 client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def connect_to_redshift(self) -> None:
        """
        Establish connection to Redshift cluster.

        Uses pg8000 (pure Python) to avoid dependency on psycopg2 libraries
        that may not be pre-installed in Glue.
        """
        try:
            logger.info(f"Connecting to Redshift: {self.redshift_host}:{self.redshift_port}/{self.redshift_db}")

            self.conn = pg8000.native.Connection(
                host=self.redshift_host,
                port=self.redshift_port,
                database=self.redshift_db,
                user="glue_user",  # Must be created in Redshift first
                password=self._get_redshift_password()
            )

            self.cursor = self.conn.cursor()
            logger.info("Connected to Redshift successfully")

        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {e}")
            raise

    def _get_redshift_password(self) -> str:
        """
        Retrieve Redshift password from AWS Secrets Manager.

        In production, use:
            secrets_client = boto3.client("secretsmanager")
            secret = secrets_client.get_secret_value(SecretId="redshift/glue_user")
            return json.loads(secret["SecretString"])["password"]

        For now, use environment variable as fallback.
        """
        import os
        password = os.environ.get("REDSHIFT_PASSWORD")
        if not password:
            raise ValueError("REDSHIFT_PASSWORD environment variable not set")
        return password

    def validate_s3_files(self) -> Dict[str, bool]:
        """
        Validate that all expected feed files exist in S3.

        Returns:
            Dictionary mapping feed code to existence (True/False)
        """
        results = {}
        logger.info(f"Validating S3 files in s3://{self.s3_bucket}/{self.s3_prefix}")

        for feed_code, feed_info in FEED_INVENTORY.items():
            csv_file = feed_info["csv_file"]
            s3_key = f"{self.s3_prefix}/{feed_code}/{csv_file}"

            try:
                self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                results[feed_code] = True
                logger.info(f"✓ {feed_code}: {csv_file} exists")
            except self.s3_client.exceptions.NoSuchKey:
                results[feed_code] = False
                logger.warning(f"✗ {feed_code}: {csv_file} not found in S3")
            except Exception as e:
                results[feed_code] = False
                logger.error(f"✗ {feed_code}: Error checking S3 - {e}")

        missing_feeds = [code for code, exists in results.items() if not exists]
        if missing_feeds:
            logger.warning(f"Missing feeds: {', '.join(missing_feeds)}")

        return results

    def create_l1_schema(self) -> None:
        """Create L1 schema if it doesn't exist."""
        try:
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {self.redshift_schema};"

            if self.dry_run:
                logger.info(f"[DRY RUN] Would execute: {create_schema_sql}")
            else:
                self.cursor.execute(create_schema_sql)
                self.conn.commit()
                logger.info(f"L1 schema '{self.redshift_schema}' created or already exists")
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise

    def create_l1_table(self, feed_code: str, feed_info: Dict) -> None:
        """
        Create L1 staging table if it doesn't exist.

        Uses minimal schema: all VARCHAR columns for maximum flexibility.
        Type conversion happens in L3 transformation layer.

        Args:
            feed_code: Feed identifier (INT010, INT020, etc.)
            feed_info: Feed metadata dictionary
        """
        table_name = feed_info["l1_table"]

        # Get CSV header to determine column names
        csv_file = feed_info["csv_file"]
        s3_key = f"{self.s3_prefix}/{feed_code}/{csv_file}"

        try:
            # Download CSV header (first line only)
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            first_line = response["Body"].read().decode("utf-8").split("\n")[0]
            columns = first_line.split(",")

            # Build CREATE TABLE statement with all VARCHAR columns
            column_defs = ", ".join([f"{col.strip()} VARCHAR(4096)" for col in columns])
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.redshift_schema}.{table_name} (
                {column_defs},
                etl_load_ts TIMESTAMP DEFAULT GETDATE(),
                etl_batch_id VARCHAR(50)
            );
            """

            if self.dry_run:
                logger.info(f"[DRY RUN] Would create table: {self.redshift_schema}.{table_name}")
            else:
                self.cursor.execute(create_table_sql)
                self.conn.commit()
                logger.info(f"Table created: {self.redshift_schema}.{table_name}")

        except Exception as e:
            logger.error(f"Failed to create table {self.redshift_schema}.{table_name}: {e}")
            raise

    def truncate_l1_table(self, table_name: str) -> None:
        """Truncate L1 table before reload."""
        try:
            truncate_sql = f"TRUNCATE TABLE {self.redshift_schema}.{table_name};"

            if self.dry_run:
                logger.info(f"[DRY RUN] Would truncate: {self.redshift_schema}.{table_name}")
            else:
                self.cursor.execute(truncate_sql)
                self.conn.commit()
                logger.info(f"Truncated: {self.redshift_schema}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to truncate table {table_name}: {e}")
            raise

    def load_feed(self, feed_code: str, feed_info: Dict) -> Dict[str, any]:
        """
        Load single feed from S3 to L1 using COPY command.

        Args:
            feed_code: Feed identifier
            feed_info: Feed metadata

        Returns:
            Dictionary with load metrics
        """
        table_name = feed_info["l1_table"]
        csv_file = feed_info["csv_file"]
        s3_path = f"s3://{self.s3_bucket}/{self.s3_prefix}/{feed_code}/{csv_file}"

        load_start = time.time()
        result = {
            "feed_code": feed_code,
            "feed_name": feed_info["feed_name"],
            "table_name": table_name,
            "status": "PENDING",
            "rows_loaded": 0,
            "error": None,
            "duration_seconds": 0
        }

        try:
            logger.info(f"Loading {feed_code} ({feed_info['feed_name']}) from {s3_path}")

            # COPY command with error handling
            copy_sql = f"""
            COPY {self.redshift_schema}.{table_name}
            FROM '{s3_path}'
            IAM_ROLE '{self.redshift_iam_role}'
            FORMAT CSV
            DELIMITER ','
            IGNOREHEADER 1
            MAXERROR 100
            COMPUPDATE OFF
            STATUPDATE OFF;
            """

            if self.dry_run:
                logger.info(f"[DRY RUN] Would execute COPY for {feed_code}")
                result["status"] = "DRY_RUN"
                result["rows_loaded"] = feed_info["expected_rows"]
            else:
                # Execute COPY
                self.cursor.execute(copy_sql)
                self.conn.commit()

                # Get row count
                count_sql = f"SELECT COUNT(*) FROM {self.redshift_schema}.{table_name};"
                count_result = self.cursor.execute(count_sql)
                rows_loaded = count_result[0][0] if count_result else 0

                result["status"] = "SUCCESS"
                result["rows_loaded"] = rows_loaded

                logger.info(f"✓ Loaded {rows_loaded} rows into {table_name}")

            result["duration_seconds"] = round(time.time() - load_start, 2)
            return result

        except Exception as e:
            result["status"] = "FAILED"
            result["error"] = str(e)
            result["duration_seconds"] = round(time.time() - load_start, 2)
            logger.error(f"✗ Failed to load {feed_code}: {e}")
            logger.error(traceback.format_exc())
            return result

    def execute_load(self) -> Dict[str, any]:
        """
        Execute complete load process:
        1. Validate S3 files
        2. Connect to Redshift
        3. Create schema and tables
        4. Load feeds in dependency order
        5. Generate execution report

        Returns:
            Job execution report
        """
        job_report = {
            "execution_date": datetime.now().isoformat(),
            "data_date": self.data_date,
            "etl_batch_id": self.etl_batch_id,
            "dry_run": self.dry_run,
            "status": "RUNNING",
            "s3_bucket": self.s3_bucket,
            "s3_prefix": self.s3_prefix,
            "redshift_schema": self.redshift_schema,
            "feeds": []
        }

        try:
            # Step 1: Validate S3 files
            logger.info("="*80)
            logger.info("STEP 1: Validating S3 files")
            logger.info("="*80)
            s3_validation = self.validate_s3_files()

            # Step 2: Connect to Redshift
            logger.info("="*80)
            logger.info("STEP 2: Connecting to Redshift")
            logger.info("="*80)
            self.connect_to_redshift()

            # Step 3: Create schema and tables
            logger.info("="*80)
            logger.info("STEP 3: Creating L1 schema and tables")
            logger.info("="*80)
            self.create_l1_schema()

            for feed_code, feed_info in FEED_INVENTORY.items():
                self.create_l1_table(feed_code, feed_info)

            # Step 4: Load feeds in dependency order
            logger.info("="*80)
            logger.info("STEP 4: Loading feeds in dependency order")
            logger.info("="*80)

            for feed_code, feed_info in LOAD_ORDER:
                # Check if S3 file exists
                if not s3_validation.get(feed_code, False):
                    logger.warning(f"Skipping {feed_code}: S3 file not found")
                    continue

                # Truncate table before load (idempotent)
                self.truncate_l1_table(feed_info["l1_table"])

                # Load feed
                load_result = self.load_feed(feed_code, feed_info)
                job_report["feeds"].append(load_result)
                self.load_results.append(load_result)

            # Step 5: Generate execution summary
            logger.info("="*80)
            logger.info("STEP 5: Load execution summary")
            logger.info("="*80)

            successful = sum(1 for r in self.load_results if r["status"] in ["SUCCESS", "DRY_RUN"])
            failed = sum(1 for r in self.load_results if r["status"] == "FAILED")
            total_rows = sum(r["rows_loaded"] for r in self.load_results)
            total_duration = round(time.time() - self.job_start_time.timestamp(), 2)

            logger.info(f"Successful loads: {successful}/{len(self.load_results)}")
            logger.info(f"Failed loads: {failed}/{len(self.load_results)}")
            logger.info(f"Total rows loaded: {total_rows:,}")
            logger.info(f"Total duration: {total_duration} seconds")

            job_report["status"] = "SUCCESS" if failed == 0 else "PARTIAL_FAILURE"
            job_report["summary"] = {
                "successful": successful,
                "failed": failed,
                "total_feeds": len(self.load_results),
                "total_rows_loaded": total_rows,
                "total_duration_seconds": total_duration
            }

            return job_report

        except Exception as e:
            logger.error(f"Job failed with error: {e}")
            logger.error(traceback.format_exc())
            job_report["status"] = "FAILED"
            job_report["error"] = str(e)
            return job_report

        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Close database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main entry point for Glue job."""

    try:
        # Parse job arguments
        if RUNNING_IN_GLUE:
            args = getResolvedOptions(
                sys.argv,
                [
                    "s3_bucket",
                    "s3_prefix",
                    "redshift_host",
                    "redshift_db",
                    "redshift_schema",
                    "redshift_iam_role",
                    "data_date",
                    "etl_batch_id"
                ]
            )
            # Optional arguments
            args["redshift_port"] = getResolvedOptions(sys.argv, ["redshift_port"]).get("redshift_port", "5439")
            args["dry_run"] = getResolvedOptions(sys.argv, ["dry_run"]).get("dry_run", "false")
        else:
            # For local testing
            args = {
                "s3_bucket": "hr-datamart-lake",
                "s3_prefix": "workday/inbound",
                "redshift_host": "localhost",
                "redshift_port": "5439",
                "redshift_db": "dev",
                "redshift_schema": "l1_workday",
                "redshift_iam_role": "arn:aws:iam::ACCOUNT:role/glue-redshift-role",
                "data_date": "2024-01-31",
                "etl_batch_id": "batch_001",
                "dry_run": "false"
            }

        logger.info("="*80)
        logger.info("AWS GLUE JOB: S3 → L1 (Staging Layer)")
        logger.info("="*80)
        logger.info(f"Job started: {datetime.now().isoformat()}")
        logger.info(f"Arguments: {json.dumps(args, indent=2)}")

        # Create loader and execute
        loader = S3ToL1Loader(args)
        report = loader.execute_load()

        # Output execution report
        logger.info("="*80)
        logger.info("EXECUTION REPORT")
        logger.info("="*80)
        logger.info(json.dumps(report, indent=2))

        # Return report for Glue job bookmarks/status tracking
        print(json.dumps(report, indent=2))

        # Exit with appropriate code
        sys.exit(0 if report["status"] == "SUCCESS" else 1)

    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
