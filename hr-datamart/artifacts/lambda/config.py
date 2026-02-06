"""
Shared configuration for People Analytics Dashboard Lambda functions.

This module provides centralized configuration that can be imported by both
dashboard_extractor and cloudwatch_publisher Lambda functions.

To use in Lambda functions:
    from config import CLUSTER_ID, DATABASE, DB_USER, SCHEMA, etc.
"""

# Redshift Configuration
CLUSTER_ID = 'warlab-hr-datamart'
DATABASE = 'dev'
DB_USER = 'admin'
SCHEMA = 'l3_workday'

# S3 Configuration (Dashboard Extractor)
S3_BUCKET = 'warlab-hr-dashboard'
S3_DATA_PREFIX = 'data'

# CloudWatch Configuration
CLOUDWATCH_NAMESPACE = 'WarLabHRDashboard'

# Query Execution Configuration
QUERY_TIMEOUT_SECONDS = 300
POLL_INTERVAL_SECONDS = 1

# CloudWatch Batch Configuration
CLOUDWATCH_BATCH_SIZE = 20

# Logging Configuration
LOG_LEVEL = 'INFO'

# Extraction Types Supported by Dashboard Extractor
SUPPORTED_EXTRACTIONS = [
    'kpi_summary',
    'headcount',
    'movements',
    'compensation',
    'org_health'
]

# CloudWatch Metrics to Publish
CLOUDWATCH_METRICS = {
    'ActiveHeadcount': {
        'unit': 'Count',
        'description': 'Total active employees at latest snapshot'
    },
    'TotalMovements': {
        'unit': 'Count',
        'description': 'Total employee movements recorded'
    },
    'AvgBasePay': {
        'unit': 'None',
        'description': 'Average base compensation for current jobs'
    },
    'ActiveCompanies': {
        'unit': 'Count',
        'description': 'Number of active companies'
    },
    'ActiveDepartments': {
        'unit': 'Count',
        'description': 'Number of active departments'
    }
}

# SQL Query Templates
SQL_QUERIES = {
    'kpi_summary': {
        'total_headcount': f"""
            SELECT COUNT(DISTINCT employee_id) as total_headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
        """,
        'total_movements': f"""
            SELECT COUNT(*) as total_movements
            FROM {SCHEMA}.fct_worker_movement_f
        """,
        'avg_base_pay': f"""
            SELECT AVG(CAST(base_pay AS DECIMAL(15,2))) as avg_base_pay
            FROM {SCHEMA}.dim_worker_job_d
            WHERE is_current_job_row = true
        """,
        'active_companies': f"""
            SELECT COUNT(DISTINCT company_id) as active_companies
            FROM {SCHEMA}.dim_company_d
            WHERE is_current = true
        """,
        'active_departments': f"""
            SELECT COUNT(DISTINCT department_id) as active_departments
            FROM {SCHEMA}.dim_department_d
            WHERE is_current = true
        """
    },
    'headcount': {
        'by_company': f"""
            SELECT
                c.company_id,
                c.company_name,
                COUNT(DISTINCT h.employee_id) as headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f h
            JOIN {SCHEMA}.dim_company_d c ON h.company_id = c.company_id
            WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
                AND c.is_current = true
            GROUP BY c.company_id, c.company_name
            ORDER BY headcount DESC
        """,
        'by_department': f"""
            SELECT
                d.department_id,
                d.department_name,
                COUNT(DISTINCT h.employee_id) as headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f h
            JOIN {SCHEMA}.dim_department_d d ON h.department_id = d.department_id
            WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
                AND d.is_current = true
            GROUP BY d.department_id, d.department_name
            ORDER BY headcount DESC
        """,
        'by_location': f"""
            SELECT
                l.location_id,
                l.location_name,
                l.city,
                l.country_code,
                COUNT(DISTINCT h.employee_id) as headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f h
            JOIN {SCHEMA}.dim_location_d l ON h.location_id = l.location_id
            WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
                AND l.is_current = true
            GROUP BY l.location_id, l.location_name, l.city, l.country_code
            ORDER BY headcount DESC
        """,
        'trend': f"""
            SELECT
                snapshot_date,
                COUNT(DISTINCT employee_id) as headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f
            GROUP BY snapshot_date
            ORDER BY snapshot_date
        """
    }
}
