#!/usr/bin/env python3
"""
AWS Glue ETL Script: S3 to Redshift L1 Staging Load (V2)
=========================================================

V2 version of the HR Datamart S3-to-L1 ETL pipeline.
Key differences from V1:
  - CSV delimiter: comma (V1 used pipe)
  - Target schema: v2_l1_workday (isolated from V1's l1_workday)
  - S3 paths: s3://warlab-hr-datamart-dev/v2/workday/hrdp/<feed>/
  - Column name sanitization: hyphens/slashes → underscores
  - Handles duplicate column names (e.g. Indigenous/INDIGENOUS)

V1 Lessons Applied:
  - Boolean columns stored as VARCHAR(256) in L1 (type casting at L3)
  - All L1 columns are VARCHAR(1000) for safe staging
  - Truncate-and-reload pattern for idempotent loads
  - ResolveChoice cast:string for type ambiguity

Usage:
    Pass --source_table, --s3_path, --redshift_schema, --redshift_connection,
    --redshift_database as Glue job parameters.

Author: WARLab Data Engineering
Version: 2.0
Last Updated: 2026-02-13
"""

import sys
import re
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, current_timestamp, lit, input_file_name
from pyspark.sql.types import StringType

# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

REQUIRED_ARGS = ["source_table", "s3_path"]
OPTIONAL_ARGS = {
    "redshift_schema": "v2_l1_workday",
    "redshift_table": None,
    "redshift_connection": "warlab-redshift-connection",
    "redshift_database": "dev"
}

# V2 uses comma-delimited CSVs (V1 used pipe)
CSV_DELIMITER = ","
CSV_WITH_HEADER = True

REDSHIFT_TEMP_DIR = "s3://warlab-hr-datamart-dev/v2/glue-temp/"

# ============================================================================
# LOGGING
# ============================================================================

class GlueJobLogger:
    def __init__(self, job_name):
        self.job_name = job_name
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(handler)

    def info(self, msg): self.logger.info(msg)
    def error(self, msg): self.logger.error(msg)
    def warning(self, msg): self.logger.warning(msg)


# ============================================================================
# COLUMN NAME SANITIZATION
# ============================================================================

def sanitize_column_name(name):
    """
    Convert CSV column names to valid Redshift identifiers.
    - Lowercase
    - Replace hyphens, slashes, spaces with underscores
    - Handle duplicates by appending suffix
    """
    return name.lower().replace('-', '_').replace('/', '_').replace(' ', '_')


def sanitize_dataframe_columns(spark_df, logger):
    """
    Rename DataFrame columns to match Redshift L1 table column names.
    Handles duplicates (e.g. 'Indigenous' and 'INDIGENOUS' both → 'indigenous').
    """
    seen = {}
    new_names = []

    for col_name in spark_df.columns:
        sanitized = sanitize_column_name(col_name)
        if sanitized in seen:
            seen[sanitized] += 1
            sanitized = f"{sanitized}_{seen[sanitized]}"
        else:
            seen[sanitized] = 1
        new_names.append(sanitized)

    # Apply renames
    renamed_df = spark_df
    for old_name, new_name in zip(spark_df.columns, new_names):
        if old_name != new_name:
            renamed_df = renamed_df.withColumnRenamed(old_name, new_name)
            logger.info(f"  Column renamed: '{old_name}' → '{new_name}'")

    return renamed_df


# ============================================================================
# JOB INITIALIZATION
# ============================================================================

def initialize_job():
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job_name = sys.argv[1] if len(sys.argv) > 1 else "v2-hr-datamart-etl"
    job = Job(glue_context)

    args = getResolvedOptions(sys.argv, REQUIRED_ARGS + list(OPTIONAL_ARGS.keys()))

    for arg_name, default_value in OPTIONAL_ARGS.items():
        if args.get(arg_name) is None:
            args[arg_name] = default_value

    if not args.get("redshift_table"):
        args["redshift_table"] = args["source_table"]

    return glue_context, job, args, job_name


# ============================================================================
# DATA LOADING
# ============================================================================

def load_s3_data(glue_context, s3_path, source_table, logger):
    """Load comma-delimited CSV data from S3."""
    logger.info(f"Loading data from S3: {s3_path}")

    try:
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

        record_count = dynamic_frame.count()
        logger.info(f"Loaded {record_count:,} records from S3")
        return dynamic_frame

    except Exception as e:
        logger.error(f"Failed to load data from S3: {str(e)}")
        raise


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

def transform_data(glue_context, dynamic_frame, source_table, logger):
    """
    Transform data for L1 load:
    1. Resolve type ambiguity (cast all to string)
    2. Sanitize column names for Redshift compatibility
    3. Add warehouse metadata columns
    """
    logger.info(f"Transforming {source_table}")

    try:
        # Step 1: Resolve type ambiguity - cast all to string (V1 lesson)
        resolved_df = ResolveChoice.apply(
            frame=dynamic_frame,
            choice="cast:string",
            transformation_ctx=f"resolve_choice_{source_table}"
        )

        # Convert to Spark DataFrame for column operations
        spark_df = resolved_df.toDF()

        # Step 2: Sanitize column names
        logger.info("Sanitizing column names...")
        spark_df = sanitize_dataframe_columns(spark_df, logger)

        # Step 3: Add warehouse metadata columns
        spark_df = spark_df \
            .withColumn("ingest_timestamp", current_timestamp()) \
            .withColumn("source_file_name", lit(source_table)) \
            .withColumn("etl_batch_id", lit(f"v2_glue_{datetime.now().strftime('%Y%m%d_%H%M%S')}")) \
            .withColumn("insert_datetime", current_timestamp()) \
            .withColumn("update_datetime", current_timestamp())

        # Convert back to DynamicFrame
        result = DynamicFrame.fromDF(spark_df, glue_context, f"transformed_{source_table}")

        logger.info(f"Transformation complete. Columns: {len(spark_df.columns)}")
        return result

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


# ============================================================================
# REDSHIFT WRITE
# ============================================================================

def write_to_redshift(glue_context, dynamic_frame, connection_name, database,
                      schema, table_name, logger):
    """Write to Redshift L1 with truncate-and-reload (idempotent)."""
    full_table_name = f"{schema}.{table_name}"
    logger.info(f"Writing to Redshift: {database}.{full_table_name}")

    try:
        redshift_options = {
            "connection_name": connection_name,
            "database": database,
            "schema": schema,
            "table": table_name,
            "temp_dir": REDSHIFT_TEMP_DIR,
            "dbtable": full_table_name,
            "preactions": f"TRUNCATE TABLE {full_table_name};",
            "parallelism": 10,
        }

        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options=redshift_options,
            transformation_ctx=f"write_{table_name}"
        )

        logger.info(f"Successfully wrote data to {full_table_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to write to {full_table_name}: {str(e)}")
        raise


# ============================================================================
# MAIN
# ============================================================================

def main():
    start_time = datetime.now()

    try:
        glue_context, job, args, job_name = initialize_job()
        logger = GlueJobLogger(job_name)

        logger.info("=" * 80)
        logger.info(f"V2 Glue ETL Job: {job_name}")
        logger.info("=" * 80)
        logger.info(f"Source: {args['source_table']}")
        logger.info(f"S3 Path: {args['s3_path']}")
        logger.info(f"Target: {args['redshift_database']}.{args['redshift_schema']}.{args['redshift_table']}")

        # Load
        dynamic_frame = load_s3_data(
            glue_context, args["s3_path"], args["source_table"], logger
        )

        # Transform
        transformed = transform_data(
            glue_context, dynamic_frame, args["source_table"], logger
        )

        # Write
        write_to_redshift(
            glue_context, transformed, args["redshift_connection"],
            args["redshift_database"], args["redshift_schema"],
            args["redshift_table"], logger
        )

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Job completed in {duration:.2f}s")
        job.commit()
        return True

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        if job:
            job.commit()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
