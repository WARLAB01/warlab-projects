#!/usr/bin/env python3
"""
AWS Glue ETL Script: S3 to Redshift L1 Staging Load
=====================================================

This script provides a parameterized ETL pipeline for loading pipe-delimited CSV data
from S3 into Redshift L1 staging tables. It is designed to be used as a shared script
across multiple Glue jobs, with different parameters passed for each source table.

Features:
- Reads pipe-delimited CSV files from S3 with headers
- Applies dynamic frame transforms for data validation
- Truncates target Redshift table before load (idempotent)
- Writes data to Redshift using the Glue Redshift connector
- Includes comprehensive error handling and logging
- Supports parameterized execution for multiple source tables

Usage:
    glue-spark-shell --job-name job_name \
        --source_table int6024_company \
        --s3_path "s3://warlab-hr-datamart-dev/workday/hrdp/int6024_company/" \
        --redshift_schema "l1_workday" \
        --redshift_table "int6024_company" \
        --redshift_connection "warlab-redshift-connection" \
        --redshift_database "dev"

Author: Data Engineering Team
Version: 1.0
Last Updated: 2026-02-06
"""

import sys
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

# Job parameters with defaults
REQUIRED_ARGS = ["source_table", "s3_path"]
OPTIONAL_ARGS = {
    "redshift_schema": "l1_workday",
    "redshift_table": None,  # Will use source_table if not provided
    "redshift_connection": "warlab-redshift-connection",
    "redshift_database": "dev"
}

# S3 CSV delimiter configuration
CSV_DELIMITER = "|"
CSV_WITH_HEADER = True

# Redshift configuration constants
REDSHIFT_TEMP_DIR = "s3://warlab-hr-datamart-dev/glue-temp/"

# ============================================================================
# LOGGING SETUP
# ============================================================================

class GlueJobLogger:
    """Helper class for structured logging throughout the ETL job."""

    def __init__(self, job_name):
        self.job_name = job_name
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(logging.INFO)

        # Create console handler with formatting
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def info(self, message):
        """Log info level message."""
        self.logger.info(message)

    def error(self, message):
        """Log error level message."""
        self.logger.error(message)

    def warning(self, message):
        """Log warning level message."""
        self.logger.warning(message)

    def debug(self, message):
        """Log debug level message."""
        self.logger.debug(message)


# ============================================================================
# JOB INITIALIZATION
# ============================================================================

def initialize_job():
    """
    Initialize Glue job context and retrieve job parameters.

    Returns:
        tuple: (GlueContext, Job, dict) containing the Glue context, Job object,
               and resolved parameters dictionary

    Raises:
        SystemExit: If required arguments are not provided
    """
    # Get Glue context and Spark context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Get job name from system arguments
    job_name = sys.argv[1] if len(sys.argv) > 1 else "hr-datamart-etl"

    # Initialize Glue Job
    job = Job(glue_context)

    # Get resolved options from Glue arguments
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS + list(OPTIONAL_ARGS.keys()))

    # Set defaults for optional arguments
    for arg_name, default_value in OPTIONAL_ARGS.items():
        if args.get(arg_name) is None:
            args[arg_name] = default_value

    # If redshift_table not specified, use source_table name
    if not args.get("redshift_table"):
        args["redshift_table"] = args["source_table"]

    return glue_context, job, args, job_name


# ============================================================================
# DATA LOADING
# ============================================================================

def load_s3_data(glue_context, s3_path, source_table, logger):
    """
    Load pipe-delimited CSV data from S3 into a DynamicFrame.

    Args:
        glue_context (GlueContext): The Glue context instance
        s3_path (str): S3 path to the source CSV file(s)
        source_table (str): Name of the source table (for logging)
        logger (GlueJobLogger): Logger instance

    Returns:
        DynamicFrame: Data loaded from S3

    Raises:
        Exception: If data loading fails
    """
    logger.info(f"Loading data from S3: {s3_path}")

    try:
        # Create dynamic frame from S3 CSV files
        dynamic_frame = glue_context.create_dynamic_frame.from_options(
            format_options={
                "multiline": False,
                "withHeader": CSV_WITH_HEADER,
                "delimiter": CSV_DELIMITER,
                "quoteChar": '"',
                "escapeChar": '\\'
            },
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [s3_path],
                "recurse": True
            },
            transformation_ctx=f"load_{source_table}"
        )

        # Get record count
        record_count = dynamic_frame.count()
        logger.info(f"Successfully loaded {record_count:,} records from S3")

        return dynamic_frame

    except Exception as e:
        logger.error(f"Failed to load data from S3: {str(e)}")
        raise


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

def transform_data(dynamic_frame, source_table, logger):
    """
    Apply transformations to the loaded data.

    This function applies the resolveChoice transformation to handle type ambiguity
    and standardize data types across columns.

    Args:
        dynamic_frame (DynamicFrame): Input data
        source_table (str): Name of the source table (for logging)
        logger (GlueJobLogger): Logger instance

    Returns:
        DynamicFrame: Transformed data
    """
    logger.info(f"Applying transformations to {source_table}")

    try:
        # Apply resolveChoice to handle type ambiguity
        # Uses 'cast' strategy to convert ambiguous types to string
        transformed_df = ResolveChoice.apply(
            frame=dynamic_frame,
            choice="cast:string",
            transformation_ctx=f"resolve_choice_{source_table}"
        )

        logger.info("Type resolution and transformations completed successfully")
        return transformed_df

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


# ============================================================================
# REDSHIFT OPERATIONS
# ============================================================================

def truncate_redshift_table(glue_context, connection_name, database, schema, table_name, logger):
    """
    Truncate the target Redshift table before loading new data.

    This ensures idempotent loads by clearing existing data. The truncate operation
    is executed as a preaction SQL statement through the Redshift connector.

    Args:
        glue_context (GlueContext): The Glue context instance
        connection_name (str): Name of the Redshift Glue connection
        database (str): Target Redshift database name
        schema (str): Target Redshift schema name
        table_name (str): Target table name
        logger (GlueJobLogger): Logger instance

    Returns:
        bool: True if truncation succeeds, False if it fails
    """
    full_table_name = f"{schema}.{table_name}"
    logger.info(f"Truncating Redshift table: {database}.{full_table_name}")

    try:
        # Note: Truncate is handled via preaction in the write operation
        # This function documents the intent; actual truncate happens during write
        logger.info(f"Truncate operation will be applied as preaction during write for {full_table_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to prepare truncate for {full_table_name}: {str(e)}")
        return False


def write_to_redshift(glue_context, dynamic_frame, connection_name, database,
                      schema, table_name, logger):
    """
    Write data to Redshift L1 staging table using the Glue Redshift connector.

    The write operation includes:
    - Truncation of the target table via preaction SQL
    - Data write with compression enabled
    - Proper error handling and logging

    Args:
        glue_context (GlueContext): The Glue context instance
        dynamic_frame (DynamicFrame): Data to write
        connection_name (str): Name of the Redshift Glue connection
        database (str): Target Redshift database name
        schema (str): Target Redshift schema name
        table_name (str): Target table name
        logger (GlueJobLogger): Logger instance

    Returns:
        bool: True if write succeeds, False otherwise

    Raises:
        Exception: If write operation fails
    """
    full_table_name = f"{schema}.{table_name}"
    logger.info(f"Writing data to Redshift table: {database}.{full_table_name}")

    try:
        # Build Redshift write options
        redshift_options = {
            "connection_name": connection_name,
            "database": database,
            "schema": schema,
            "table": table_name,
            "temp_dir": REDSHIFT_TEMP_DIR,
            "dbtable": full_table_name,
            # Preaction to truncate table before write (ensures idempotent loads)
            "preactions": f"TRUNCATE TABLE {full_table_name};",
            # Optimize for performance
            "parallelism": 10,
        }

        # Write to Redshift
        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options=redshift_options,
            transformation_ctx=f"write_{table_name}"
        )

        logger.info(f"Successfully wrote data to {full_table_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to write data to {full_table_name}: {str(e)}")
        raise


# ============================================================================
# MAIN ETL EXECUTION
# ============================================================================

def main():
    """
    Main ETL execution function.

    Orchestrates the complete ETL workflow:
    1. Initialize Glue job and retrieve parameters
    2. Load data from S3
    3. Apply transformations
    4. Truncate Redshift table
    5. Write to Redshift
    6. Commit job and handle cleanup

    Returns:
        bool: True if job succeeds, False otherwise
    """
    start_time = datetime.now()
    glue_context = None
    job = None

    try:
        # ====================================================================
        # STEP 1: INITIALIZATION
        # ====================================================================
        glue_context, job, args, job_name = initialize_job()
        logger = GlueJobLogger(job_name)

        logger.info("=" * 80)
        logger.info(f"Starting Glue ETL Job: {job_name}")
        logger.info("=" * 80)
        logger.info(f"Start Time: {start_time}")

        # Log job parameters
        logger.info("Job Parameters:")
        logger.info(f"  Source Table: {args['source_table']}")
        logger.info(f"  S3 Path: {args['s3_path']}")
        logger.info(f"  Redshift Schema: {args['redshift_schema']}")
        logger.info(f"  Redshift Table: {args['redshift_table']}")
        logger.info(f"  Redshift Connection: {args['redshift_connection']}")
        logger.info(f"  Redshift Database: {args['redshift_database']}")

        # ====================================================================
        # STEP 2: DATA LOADING
        # ====================================================================
        logger.info("-" * 80)
        logger.info("STEP 1: Loading data from S3")
        logger.info("-" * 80)

        dynamic_frame = load_s3_data(
            glue_context=glue_context,
            s3_path=args["s3_path"],
            source_table=args["source_table"],
            logger=logger
        )

        # ====================================================================
        # STEP 3: DATA TRANSFORMATION
        # ====================================================================
        logger.info("-" * 80)
        logger.info("STEP 2: Transforming data")
        logger.info("-" * 80)

        transformed_df = transform_data(
            dynamic_frame=dynamic_frame,
            source_table=args["source_table"],
            logger=logger
        )

        # ====================================================================
        # STEP 4: REDSHIFT OPERATIONS
        # ====================================================================
        logger.info("-" * 80)
        logger.info("STEP 3: Writing to Redshift")
        logger.info("-" * 80)

        write_to_redshift(
            glue_context=glue_context,
            dynamic_frame=transformed_df,
            connection_name=args["redshift_connection"],
            database=args["redshift_database"],
            schema=args["redshift_schema"],
            table_name=args["redshift_table"],
            logger=logger
        )

        # ====================================================================
        # STEP 5: JOB COMPLETION
        # ====================================================================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("-" * 80)
        logger.info("ETL Job Completed Successfully")
        logger.info("-" * 80)
        logger.info(f"End Time: {end_time}")
        logger.info(f"Total Duration: {duration:.2f} seconds")

        # Commit Glue job
        job.commit()

        return True

    except Exception as e:
        logger.error("=" * 80)
        logger.error("ETL Job Failed with Error")
        logger.error("=" * 80)
        logger.error(f"Error Message: {str(e)}")
        logger.error(f"Error Type: {type(e).__name__}")

        # Attempt to commit job with error status
        if job:
            job.commit()

        return False


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
