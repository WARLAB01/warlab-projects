#!/usr/bin/env python3
"""
AWS Glue Python Shell Job: L1 to L3 Transformation Layer Load

Purpose:
    Transforms raw L1 staging data into L3 conformed star schema (dimensions and facts).
    Executes SQL transformations in dependency order with comprehensive error handling.

Flow:
    1. Load L3 source tables (denormalized staging layer)
    2. Load L3 dimension tables (SCD2 dimensions with surrogate keys)
    3. Load L3 fact tables (grain-specific fact tables)

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

# Attempt to import Glue utilities
try:
    from awsglue.utils import getResolvedOptions
    RUNNING_IN_GLUE = True
except ImportError:
    RUNNING_IN_GLUE = False
    logger.warning("Not running in Glue environment; using direct argument parsing")


# ============================================================================
# SQL TRANSFORMATION BLOCKS
# ============================================================================

SQL_BLOCKS = {
    "1_create_l3_schema": """
        CREATE SCHEMA IF NOT EXISTS {schema};
    """,

    "2_create_l3_day_dimension": """
        CREATE TABLE IF NOT EXISTS {schema}.dim_day_d (
            day_key INT NOT NULL PRIMARY KEY,
            calendar_date DATE NOT NULL UNIQUE,
            year_of_date INT,
            month_of_year INT,
            day_of_month INT,
            day_of_week INT,
            day_of_week_name VARCHAR(10),
            week_of_year INT,
            month_name VARCHAR(10),
            quarter_of_year INT,
            is_weekend BOOLEAN,
            is_holiday BOOLEAN,
            etl_load_ts TIMESTAMP DEFAULT GETDATE()
        )
        DISTSTYLE ALL
        SORTKEY (calendar_date);
    """,

    "3_populate_l3_day_dimension": """
        INSERT INTO {schema}.dim_day_d (
            day_key, calendar_date, year_of_date, month_of_year, day_of_month,
            day_of_week, day_of_week_name, week_of_year, month_name, quarter_of_year,
            is_weekend, is_holiday, etl_load_ts
        )
        SELECT DISTINCT
            CAST(REPLACE(CAST(calendar_date AS VARCHAR), '-', '') AS INT) AS day_key,
            calendar_date,
            EXTRACT(YEAR FROM calendar_date) AS year_of_date,
            EXTRACT(MONTH FROM calendar_date) AS month_of_year,
            EXTRACT(DAY FROM calendar_date) AS day_of_month,
            EXTRACT(DOW FROM calendar_date) AS day_of_week,
            TO_CHAR(calendar_date, 'Day') AS day_of_week_name,
            EXTRACT(WEEK FROM calendar_date) AS week_of_year,
            TO_CHAR(calendar_date, 'Month') AS month_name,
            EXTRACT(QUARTER FROM calendar_date) AS quarter_of_year,
            CASE WHEN EXTRACT(DOW FROM calendar_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
            FALSE AS is_holiday,
            GETDATE() AS etl_load_ts
        FROM (
            SELECT DISTINCT
                TO_DATE(CAST(effective_date AS VARCHAR), 'YYYY-MM-DD') AS calendar_date
            FROM {l1_schema}.stg_employees
            WHERE effective_date IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                TO_DATE(CAST(start_date AS VARCHAR), 'YYYY-MM-DD') AS calendar_date
            FROM {l1_schema}.stg_jobs
            WHERE start_date IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                TO_DATE(CAST(effective_date AS VARCHAR), 'YYYY-MM-DD') AS calendar_date
            FROM {l1_schema}.stg_worker_status
            WHERE effective_date IS NOT NULL
        )
        WHERE calendar_date NOT IN (SELECT DISTINCT calendar_date FROM {schema}.dim_day_d);
    """,

    "4_create_src_employees": """
        CREATE TABLE IF NOT EXISTS {schema}._src_employees AS
        SELECT
            e.employee_id,
            e.first_name,
            e.last_name,
            e.email,
            e.hire_date,
            e.termination_date,
            o.org_id,
            o.org_name AS organization_name,
            e.effective_date,
            LEAD(e.effective_date) OVER (
                PARTITION BY e.employee_id
                ORDER BY e.effective_date
            ) - INTERVAL 1 DAY AS idp_calc_end_date,
            MD5(CONCAT(
                COALESCE(e.first_name, ''),
                COALESCE(e.last_name, ''),
                COALESCE(e.email, ''),
                COALESCE(o.org_name, '')
            )) AS hash_diff,
            GETDATE() AS etl_load_ts,
            '{batch_id}' AS etl_batch_id
        FROM {l1_schema}.stg_employees e
        LEFT JOIN {l1_schema}.stg_organizations o
            ON e.org_id = o.org_id;
    """,

    "5_create_dim_worker_d": """
        CREATE TABLE IF NOT EXISTS {schema}.dim_worker_d (
            worker_key BIGINT IDENTITY(1, 1) PRIMARY KEY,
            worker_business_key VARCHAR(50) NOT NULL,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            organization_name VARCHAR(255),
            hire_date DATE,
            valid_from DATE NOT NULL,
            valid_to DATE,
            is_current BOOLEAN NOT NULL,
            hash_diff VARCHAR(32),
            etl_load_ts TIMESTAMP NOT NULL,
            etl_batch_id VARCHAR(50)
        )
        DISTSTYLE AUTO
        SORTKEY (worker_business_key, valid_from);
    """,

    "6_load_dim_worker_d": """
        INSERT INTO {schema}.dim_worker_d (
            worker_business_key, first_name, last_name, email, organization_name,
            hire_date, valid_from, valid_to, is_current, hash_diff,
            etl_load_ts, etl_batch_id
        )
        SELECT
            e.employee_id AS worker_business_key,
            e.first_name,
            e.last_name,
            e.email,
            e.organization_name,
            TO_DATE(e.hire_date, 'YYYY-MM-DD') AS hire_date,
            TO_DATE(CAST(e.effective_date AS VARCHAR), 'YYYY-MM-DD') AS valid_from,
            CASE
                WHEN e.idp_calc_end_date IS NOT NULL
                    THEN TO_DATE(CAST(e.idp_calc_end_date AS VARCHAR), 'YYYY-MM-DD')
                ELSE NULL
            END AS valid_to,
            CASE WHEN e.idp_calc_end_date IS NULL THEN TRUE ELSE FALSE END AS is_current,
            e.hash_diff,
            e.etl_load_ts,
            e.etl_batch_id
        FROM {schema}._src_employees e
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.dim_worker_d d
            WHERE d.worker_business_key = e.employee_id
            AND d.hash_diff = e.hash_diff
        );
    """,

    "7_create_src_jobs": """
        CREATE TABLE IF NOT EXISTS {schema}._src_jobs AS
        SELECT
            j.job_id,
            j.employee_id,
            j.job_title,
            j.department,
            j.start_date,
            j.end_date,
            jc.job_class_code,
            MD5(CONCAT(
                COALESCE(j.job_title, ''),
                COALESCE(j.department, ''),
                COALESCE(jc.job_class_code, '')
            )) AS hash_diff,
            GETDATE() AS etl_load_ts,
            '{batch_id}' AS etl_batch_id
        FROM {l1_schema}.stg_jobs j
        LEFT JOIN {l1_schema}.stg_job_classification jc
            ON j.job_class_id = jc.job_class_id;
    """,

    "8_create_dim_job_d": """
        CREATE TABLE IF NOT EXISTS {schema}.dim_job_d (
            job_key BIGINT IDENTITY(1, 1) PRIMARY KEY,
            job_business_key VARCHAR(50) NOT NULL,
            job_title VARCHAR(255),
            job_class_code VARCHAR(50),
            department VARCHAR(255),
            valid_from DATE NOT NULL,
            valid_to DATE,
            is_current BOOLEAN NOT NULL,
            hash_diff VARCHAR(32),
            etl_load_ts TIMESTAMP NOT NULL,
            etl_batch_id VARCHAR(50)
        )
        DISTSTYLE AUTO
        SORTKEY (job_business_key, valid_from);
    """,

    "9_load_dim_job_d": """
        INSERT INTO {schema}.dim_job_d (
            job_business_key, job_title, job_class_code, department,
            valid_from, valid_to, is_current, hash_diff,
            etl_load_ts, etl_batch_id
        )
        SELECT
            j.job_id AS job_business_key,
            j.job_title,
            j.job_class_code,
            j.department,
            TO_DATE(j.start_date, 'YYYY-MM-DD') AS valid_from,
            CASE
                WHEN j.end_date IS NOT NULL
                    THEN TO_DATE(j.end_date, 'YYYY-MM-DD')
                ELSE NULL
            END AS valid_to,
            CASE WHEN j.end_date IS NULL THEN TRUE ELSE FALSE END AS is_current,
            j.hash_diff,
            j.etl_load_ts,
            j.etl_batch_id
        FROM {schema}._src_jobs j
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.dim_job_d d
            WHERE d.job_business_key = j.job_id
            AND d.hash_diff = j.hash_diff
        );
    """,

    "10_create_dim_organization_d": """
        CREATE TABLE IF NOT EXISTS {schema}.dim_organization_d (
            organization_key BIGINT IDENTITY(1, 1) PRIMARY KEY,
            organization_business_key VARCHAR(50) NOT NULL,
            organization_name VARCHAR(255) NOT NULL,
            parent_organization_key BIGINT,
            organizational_level INT,
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            etl_load_ts TIMESTAMP NOT NULL,
            etl_batch_id VARCHAR(50)
        )
        DISTSTYLE AUTO
        SORTKEY (organization_business_key);
    """,

    "11_load_dim_organization_d": """
        INSERT INTO {schema}.dim_organization_d (
            organization_business_key, organization_name, organizational_level,
            is_current, etl_load_ts, etl_batch_id
        )
        SELECT
            o.org_id,
            o.org_name,
            1 AS organizational_level,
            TRUE,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_organizations o
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.dim_organization_d d
            WHERE d.organization_business_key = o.org_id
        );
    """,

    "12_create_dim_worker_status_d": """
        CREATE TABLE IF NOT EXISTS {schema}.dim_worker_status_d (
            status_key BIGINT IDENTITY(1, 1) PRIMARY KEY,
            status_business_key VARCHAR(50) NOT NULL,
            status_code VARCHAR(50) NOT NULL,
            status_name VARCHAR(255),
            status_category VARCHAR(50),
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            etl_load_ts TIMESTAMP NOT NULL,
            etl_batch_id VARCHAR(50)
        )
        DISTSTYLE AUTO
        SORTKEY (status_code);
    """,

    "13_load_dim_worker_status_d": """
        INSERT INTO {schema}.dim_worker_status_d (
            status_business_key, status_code, status_name, status_category,
            is_current, etl_load_ts, etl_batch_id
        )
        SELECT DISTINCT
            s.status_id,
            s.status_code,
            s.status_name,
            CASE
                WHEN s.status_code IN ('ACTIVE') THEN 'Active'
                WHEN s.status_code IN ('LEAVE_OF_ABSENCE', 'UNPAID_LEAVE') THEN 'On Leave'
                WHEN s.status_code IN ('TERM', 'TERMINATED') THEN 'Terminated'
                ELSE 'Inactive'
            END AS status_category,
            TRUE,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_worker_status s
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.dim_worker_status_d d
            WHERE d.status_business_key = s.status_id
        );
    """,

    "14_create_dim_job_classification_d": """
        CREATE TABLE IF NOT EXISTS {schema}.dim_job_classification_d (
            job_class_key BIGINT IDENTITY(1, 1) PRIMARY KEY,
            job_class_business_key VARCHAR(50) NOT NULL,
            job_class_code VARCHAR(50) NOT NULL,
            job_class_title VARCHAR(255),
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            etl_load_ts TIMESTAMP NOT NULL,
            etl_batch_id VARCHAR(50)
        )
        DISTSTYLE AUTO
        SORTKEY (job_class_code);
    """,

    "15_load_dim_job_classification_d": """
        INSERT INTO {schema}.dim_job_classification_d (
            job_class_business_key, job_class_code, job_class_title,
            is_current, etl_load_ts, etl_batch_id
        )
        SELECT
            j.job_class_id,
            j.job_class_code,
            j.job_class_title,
            TRUE,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_job_classification j
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.dim_job_classification_d d
            WHERE d.job_class_business_key = j.job_class_id
        );
    """,

    "16_create_fct_worker_movement_f": """
        CREATE TABLE IF NOT EXISTS {schema}.fct_worker_movement_f (
            movement_fact_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
            worker_key BIGINT NOT NULL,
            from_job_key BIGINT,
            to_job_key BIGINT,
            from_organization_key BIGINT,
            to_organization_key BIGINT,
            movement_date_key INT,
            movement_type VARCHAR(50) NOT NULL,
            movement_reason VARCHAR(255),
            movement_source_id VARCHAR(100),
            etl_load_ts TIMESTAMP NOT NULL,
            etl_batch_id VARCHAR(50)
        )
        DISTSTYLE AUTO
        SORTKEY (worker_key, movement_date_key);
    """,

    "17_load_fct_worker_movement_f": """
        INSERT INTO {schema}.fct_worker_movement_f (
            worker_key, from_job_key, to_job_key, from_organization_key,
            to_organization_key, movement_date_key, movement_type, movement_reason,
            movement_source_id, etl_load_ts, etl_batch_id
        )
        -- Load Hire Events (INT090)
        SELECT
            d.worker_key,
            NULL::BIGINT,
            j.job_key,
            NULL::BIGINT,
            o.organization_key,
            CAST(REPLACE(h.hire_date, '-', '') AS INT) AS movement_date_key,
            'Hire' AS movement_type,
            h.hire_type AS movement_reason,
            h.hire_event_id AS movement_source_id,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_hire_events h
        LEFT JOIN {schema}.dim_worker_d d
            ON d.worker_business_key = h.employee_id AND d.is_current = TRUE
        LEFT JOIN {schema}.dim_job_d j
            ON j.job_business_key = h.job_id AND j.is_current = TRUE
        LEFT JOIN {schema}.dim_organization_d o
            ON o.organization_business_key = h.org_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.fct_worker_movement_f f
            WHERE f.movement_source_id = h.hire_event_id
        )

        UNION ALL

        -- Load Transfer Events (INT100)
        SELECT
            d.worker_key,
            j1.job_key,
            j2.job_key,
            o1.organization_key,
            o2.organization_key,
            CAST(REPLACE(t.transfer_date, '-', '') AS INT) AS movement_date_key,
            'Transfer' AS movement_type,
            'Org Transfer' AS movement_reason,
            t.transfer_id AS movement_source_id,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_transfer_events t
        LEFT JOIN {schema}.dim_worker_d d
            ON d.worker_business_key = t.employee_id AND d.is_current = TRUE
        LEFT JOIN {schema}.dim_job_d j1
            ON j1.job_business_key = t.from_job_id AND j1.is_current = TRUE
        LEFT JOIN {schema}.dim_job_d j2
            ON j2.job_business_key = t.to_job_id AND j2.is_current = TRUE
        LEFT JOIN {schema}.dim_organization_d o1
            ON o1.organization_business_key = t.from_org_id
        LEFT JOIN {schema}.dim_organization_d o2
            ON o2.organization_business_key = t.to_org_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.fct_worker_movement_f f
            WHERE f.movement_source_id = t.transfer_id
        )

        UNION ALL

        -- Load Promotion Events (INT110)
        SELECT
            d.worker_key,
            j1.job_key,
            j2.job_key,
            o.organization_key,
            o.organization_key,
            CAST(REPLACE(p.promo_date, '-', '') AS INT) AS movement_date_key,
            'Promotion' AS movement_type,
            'Promotion' AS movement_reason,
            p.promo_id AS movement_source_id,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_promotion_events p
        LEFT JOIN {schema}.dim_worker_d d
            ON d.worker_business_key = p.employee_id AND d.is_current = TRUE
        LEFT JOIN {schema}.dim_job_d j1
            ON j1.job_business_key = p.from_job_id AND j1.is_current = TRUE
        LEFT JOIN {schema}.dim_job_d j2
            ON j2.job_business_key = p.to_job_id AND j2.is_current = TRUE
        LEFT JOIN {schema}.dim_organization_d o
            ON o.organization_business_key = p.org_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.fct_worker_movement_f f
            WHERE f.movement_source_id = p.promo_id
        );
    """,

    "18_create_fct_worker_compensation_f": """
        CREATE TABLE IF NOT EXISTS {schema}.fct_worker_compensation_f (
            compensation_fact_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
            worker_key BIGINT NOT NULL,
            job_key BIGINT,
            effective_date_key INT,
            compensation_type VARCHAR(50),
            compensation_amount DECIMAL(18, 2),
            compensation_currency VARCHAR(3),
            compensation_source_id VARCHAR(100),
            etl_load_ts TIMESTAMP NOT NULL,
            etl_batch_id VARCHAR(50)
        )
        DISTSTYLE AUTO
        SORTKEY (worker_key, effective_date_key);
    """,

    "19_load_fct_worker_compensation_f": """
        INSERT INTO {schema}.fct_worker_compensation_f (
            worker_key, job_key, effective_date_key, compensation_type,
            compensation_amount, compensation_currency, compensation_source_id,
            etl_load_ts, etl_batch_id
        )
        -- Load Salary History (INT080)
        SELECT
            d.worker_key,
            j.job_key,
            CAST(REPLACE(s.effective_date, '-', '') AS INT) AS effective_date_key,
            'Salary' AS compensation_type,
            CAST(s.salary_amount AS DECIMAL(18, 2)) AS compensation_amount,
            'USD' AS compensation_currency,
            s.salary_id AS compensation_source_id,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_salary_history s
        LEFT JOIN {schema}.dim_worker_d d
            ON d.worker_business_key = s.employee_id
        LEFT JOIN {schema}.dim_job_d j
            ON j.job_business_key = s.job_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.fct_worker_compensation_f f
            WHERE f.compensation_source_id = s.salary_id
        )

        UNION ALL

        -- Load Compensation Records (INT060)
        SELECT
            d.worker_key,
            j.job_key,
            CAST(REPLACE(c.effective_date, '-', '') AS INT) AS effective_date_key,
            c.comp_type AS compensation_type,
            CAST(c.amount AS DECIMAL(18, 2)) AS compensation_amount,
            'USD' AS compensation_currency,
            c.comp_id AS compensation_source_id,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_compensation c
        LEFT JOIN {schema}.dim_worker_d d
            ON d.worker_business_key = c.employee_id
        LEFT JOIN {schema}.dim_job_d j
            ON j.job_business_key = c.job_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.fct_worker_compensation_f f
            WHERE f.compensation_source_id = c.comp_id
        );
    """,

    "20_create_fct_worker_status_f": """
        CREATE TABLE IF NOT EXISTS {schema}.fct_worker_status_f (
            status_fact_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
            worker_key BIGINT NOT NULL,
            status_key BIGINT NOT NULL,
            effective_date_key INT,
            organization_key BIGINT,
            status_source_id VARCHAR(100),
            etl_load_ts TIMESTAMP NOT NULL,
            etl_batch_id VARCHAR(50)
        )
        DISTSTYLE AUTO
        SORTKEY (worker_key, effective_date_key);
    """,

    "21_load_fct_worker_status_f": """
        INSERT INTO {schema}.fct_worker_status_f (
            worker_key, status_key, effective_date_key, organization_key,
            status_source_id, etl_load_ts, etl_batch_id
        )
        SELECT
            d.worker_key,
            s.status_key,
            CAST(REPLACE(ws.effective_date, '-', '') AS INT) AS effective_date_key,
            o.organization_key,
            ws.status_id AS status_source_id,
            GETDATE(),
            '{batch_id}'
        FROM {l1_schema}.stg_worker_status ws
        LEFT JOIN {schema}.dim_worker_d d
            ON d.worker_business_key = ws.employee_id
        LEFT JOIN {schema}.dim_worker_status_d s
            ON s.status_business_key = ws.status_id
        LEFT JOIN {schema}.dim_organization_d o
            ON o.organization_business_key = ws.org_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.fct_worker_status_f f
            WHERE f.status_source_id = ws.status_id
                AND f.effective_date_key = CAST(REPLACE(ws.effective_date, '-', '') AS INT)
        );
    """
}


# ============================================================================
# L1 TO L3 TRANSFORMER CLASS
# ============================================================================

class L1ToL3Transformer:
    """
    Orchestrates transformation from L1 staging to L3 analytics layer.

    Execution flow:
    1. Connect to Redshift
    2. Execute SQL blocks in sequence:
       - Create L3 schema and tables
       - Populate dimension tables (SCD2)
       - Populate fact tables with FK resolution
    3. Validate row counts
    4. Generate execution report
    """

    def __init__(self, args: Dict[str, str]):
        """
        Initialize transformer with job parameters.

        Args:
            args: Job parameters dictionary
                - redshift_host: Redshift endpoint
                - redshift_port: Port (default 5439)
                - redshift_db: Database name
                - redshift_schema: L3 schema name
                - redshift_iam_role: IAM role for COPY/UNLOAD
                - data_date: Business date (YYYY-MM-DD)
                - etl_batch_id: Batch identifier
        """
        self.args = args
        self.redshift_host = args.get("redshift_host")
        self.redshift_port = int(args.get("redshift_port", "5439"))
        self.redshift_db = args.get("redshift_db")
        self.redshift_schema = args.get("redshift_schema")
        self.l1_schema = args.get("l1_schema", "l1_workday")
        self.redshift_iam_role = args.get("redshift_iam_role")
        self.data_date = args.get("data_date")
        self.etl_batch_id = args.get("etl_batch_id")
        self.dry_run = args.get("dry_run", "false").lower() == "true"

        self.conn = None
        self.cursor = None
        self.execution_results = []
        self.job_start_time = datetime.now()

        self._validate_arguments()

    def _validate_arguments(self) -> None:
        """Validate required arguments."""
        required = [
            "redshift_host", "redshift_db", "redshift_schema",
            "redshift_iam_role", "data_date", "etl_batch_id"
        ]
        missing = [arg for arg in required if not self.args.get(arg)]

        if missing:
            raise ValueError(f"Missing required arguments: {', '.join(missing)}")

        logger.info(f"Arguments validated. Dry run: {self.dry_run}")

    def connect_to_redshift(self) -> None:
        """Establish Redshift connection."""
        try:
            logger.info(f"Connecting to Redshift: {self.redshift_host}:{self.redshift_port}/{self.redshift_db}")

            self.conn = pg8000.native.Connection(
                host=self.redshift_host,
                port=self.redshift_port,
                database=self.redshift_db,
                user="glue_user",
                password=self._get_redshift_password()
            )

            self.cursor = self.conn.cursor()
            logger.info("Connected to Redshift successfully")

        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {e}")
            raise

    def _get_redshift_password(self) -> str:
        """Retrieve Redshift password from environment."""
        import os
        password = os.environ.get("REDSHIFT_PASSWORD")
        if not password:
            raise ValueError("REDSHIFT_PASSWORD environment variable not set")
        return password

    def execute_sql_block(self, block_name: str, sql_template: str) -> Dict[str, any]:
        """
        Execute single SQL block with error handling.

        Args:
            block_name: Name of SQL block for logging
            sql_template: SQL string with placeholders

        Returns:
            Execution result dictionary
        """
        result = {
            "block_name": block_name,
            "status": "PENDING",
            "error": None,
            "rows_affected": 0,
            "duration_seconds": 0
        }

        try:
            # Format SQL with schema and batch parameters
            sql = sql_template.format(
                schema=self.redshift_schema,
                l1_schema=self.l1_schema,
                batch_id=self.etl_batch_id
            )

            block_start = time.time()

            if self.dry_run:
                logger.info(f"[DRY RUN] {block_name}")
                logger.debug(f"SQL:\n{sql}")
                result["status"] = "DRY_RUN"
            else:
                logger.info(f"Executing: {block_name}")

                # Execute SQL
                self.cursor.execute(sql)
                self.conn.commit()

                result["status"] = "SUCCESS"
                logger.info(f"✓ {block_name} completed successfully")

            result["duration_seconds"] = round(time.time() - block_start, 2)
            return result

        except Exception as e:
            result["status"] = "FAILED"
            result["error"] = str(e)
            result["duration_seconds"] = round(time.time() - block_start, 2)
            logger.error(f"✗ {block_name} failed: {e}")
            logger.error(traceback.format_exc())
            return result

    def validate_table_row_counts(self) -> Dict[str, int]:
        """
        Query L3 tables to validate row counts.

        Returns:
            Dictionary mapping table names to row counts
        """
        row_counts = {}
        tables = [
            "dim_day_d",
            "dim_worker_d",
            "dim_job_d",
            "dim_organization_d",
            "dim_worker_status_d",
            "dim_job_classification_d",
            "fct_worker_movement_f",
            "fct_worker_compensation_f",
            "fct_worker_status_f"
        ]

        try:
            for table_name in tables:
                query = f"SELECT COUNT(*) FROM {self.redshift_schema}.{table_name};"

                if not self.dry_run:
                    result = self.cursor.execute(query)
                    count = result[0][0] if result else 0
                    row_counts[table_name] = count
                    logger.info(f"{table_name}: {count:,} rows")
                else:
                    row_counts[table_name] = 0
                    logger.info(f"[DRY RUN] {table_name}: skipped row count")

        except Exception as e:
            logger.warning(f"Failed to validate row counts: {e}")

        return row_counts

    def execute_transformation(self) -> Dict[str, any]:
        """
        Execute complete L1 to L3 transformation.

        Returns:
            Job execution report
        """
        job_report = {
            "execution_date": datetime.now().isoformat(),
            "data_date": self.data_date,
            "etl_batch_id": self.etl_batch_id,
            "dry_run": self.dry_run,
            "status": "RUNNING",
            "redshift_schema": self.redshift_schema,
            "l1_schema": self.l1_schema,
            "sql_blocks": []
        }

        try:
            # Step 1: Connect to Redshift
            logger.info("="*80)
            logger.info("STEP 1: Connecting to Redshift")
            logger.info("="*80)
            self.connect_to_redshift()

            # Step 2: Execute SQL blocks in sequence
            logger.info("="*80)
            logger.info("STEP 2: Executing L3 transformation SQL blocks")
            logger.info("="*80)

            for block_name, sql_template in SQL_BLOCKS.items():
                result = self.execute_sql_block(block_name, sql_template)
                job_report["sql_blocks"].append(result)
                self.execution_results.append(result)

            # Step 3: Validate results
            logger.info("="*80)
            logger.info("STEP 3: Validating L3 tables")
            logger.info("="*80)

            row_counts = self.validate_table_row_counts()
            job_report["row_counts"] = row_counts

            # Step 4: Generate execution summary
            logger.info("="*80)
            logger.info("STEP 4: Execution summary")
            logger.info("="*80)

            successful = sum(1 for r in self.execution_results if r["status"] in ["SUCCESS", "DRY_RUN"])
            failed = sum(1 for r in self.execution_results if r["status"] == "FAILED")
            total_duration = round(time.time() - self.job_start_time.timestamp(), 2)

            logger.info(f"Successful blocks: {successful}/{len(self.execution_results)}")
            logger.info(f"Failed blocks: {failed}/{len(self.execution_results)}")
            logger.info(f"Total duration: {total_duration} seconds")

            job_report["status"] = "SUCCESS" if failed == 0 else "PARTIAL_FAILURE"
            job_report["summary"] = {
                "successful_blocks": successful,
                "failed_blocks": failed,
                "total_blocks": len(self.execution_results),
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
            args["l1_schema"] = getResolvedOptions(sys.argv, ["l1_schema"]).get("l1_schema", "l1_workday")
            args["dry_run"] = getResolvedOptions(sys.argv, ["dry_run"]).get("dry_run", "false")
        else:
            # For local testing
            args = {
                "redshift_host": "localhost",
                "redshift_port": "5439",
                "redshift_db": "dev",
                "redshift_schema": "l3_workday",
                "l1_schema": "l1_workday",
                "redshift_iam_role": "arn:aws:iam::ACCOUNT:role/glue-redshift-role",
                "data_date": "2024-01-31",
                "etl_batch_id": "batch_001",
                "dry_run": "false"
            }

        logger.info("="*80)
        logger.info("AWS GLUE JOB: L1 → L3 (Transformation Layer)")
        logger.info("="*80)
        logger.info(f"Job started: {datetime.now().isoformat()}")
        logger.info(f"Arguments: {json.dumps(args, indent=2)}")

        # Create transformer and execute
        transformer = L1ToL3Transformer(args)
        report = transformer.execute_transformation()

        # Output execution report
        logger.info("="*80)
        logger.info("EXECUTION REPORT")
        logger.info("="*80)
        logger.info(json.dumps(report, indent=2))

        # Return report
        print(json.dumps(report, indent=2))

        # Exit with appropriate code
        sys.exit(0 if report["status"] == "SUCCESS" else 1)

    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
