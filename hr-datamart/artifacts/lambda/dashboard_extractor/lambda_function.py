"""
People Analytics Dashboard Extraction Layer Lambda Function

This Lambda function extracts data from Redshift for the People Analytics Dashboard.
It supports multiple extraction types via event parameter:
- kpi_summary: Key performance indicators
- headcount: Headcount analytics
- movements: Employee movement analytics
- compensation: Compensation analytics
- org_health: Organizational health metrics

The function uses Redshift Data API for query execution and publishes results to S3.
CloudWatch metrics are also published for KPI values.
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
redshift_data_client = boto3.client('redshift-data')
s3_client = boto3.client('s3')
cloudwatch_client = boto3.client('cloudwatch')

# Configuration constants
CLUSTER_ID = 'warlab-hr-datamart'
DATABASE = 'dev'
DB_USER = 'admin'
SCHEMA = 'l3_workday'
S3_BUCKET = 'warlab-hr-dashboard'
CLOUDWATCH_NAMESPACE = 'WarLabHRDashboard'
QUERY_TIMEOUT_SECONDS = 300
POLL_INTERVAL_SECONDS = 1
MAX_POLL_ATTEMPTS = QUERY_TIMEOUT_SECONDS // POLL_INTERVAL_SECONDS


class RedshiftQueryExecutor:
    """Handles Redshift Data API query execution and result retrieval."""

    def __init__(self, cluster_id: str, database: str, db_user: str):
        """
        Initialize the query executor.

        Args:
            cluster_id: Redshift cluster identifier
            database: Database name
            db_user: Database user for authentication
        """
        self.cluster_id = cluster_id
        self.database = database
        self.db_user = db_user

    def execute_query(self, sql_query: str) -> str:
        """
        Execute a query against Redshift using the Data API.

        Args:
            sql_query: SQL query string to execute

        Returns:
            Query execution ID

        Raises:
            Exception: If query execution fails
        """
        try:
            logger.info(f"Executing query on cluster {self.cluster_id}, database {self.database}")
            response = redshift_data_client.execute_statement(
                ClusterIdentifier=self.cluster_id,
                Database=self.database,
                DbUser=self.db_user,
                Sql=sql_query
            )
            query_id = response['Id']
            logger.info(f"Query submitted with ID: {query_id}")
            return query_id
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            raise

    def wait_for_completion(self, query_id: str, timeout_seconds: int = QUERY_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """
        Poll for query completion.

        Args:
            query_id: Query execution ID
            timeout_seconds: Maximum wait time in seconds

        Returns:
            Query status response

        Raises:
            TimeoutError: If query doesn't complete within timeout
        """
        start_time = time.time()
        poll_count = 0

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(f"Query {query_id} did not complete within {timeout_seconds} seconds")

            try:
                response = redshift_data_client.describe_statement(Id=query_id)
                status = response['Status']
                logger.info(f"Query {query_id} status: {status} (poll #{poll_count})")

                if status == 'FINISHED':
                    logger.info(f"Query {query_id} completed successfully")
                    return response
                elif status == 'FAILED':
                    error_msg = response.get('Error', 'Unknown error')
                    raise Exception(f"Query {query_id} failed: {error_msg}")
                elif status == 'ABORTED':
                    raise Exception(f"Query {query_id} was aborted")

                time.sleep(POLL_INTERVAL_SECONDS)
                poll_count += 1

            except redshift_data_client.exceptions.ClientError as e:
                logger.error(f"Error checking query status: {str(e)}")
                raise

    def fetch_results(self, query_id: str) -> List[Dict[str, Any]]:
        """
        Fetch results from a completed query.

        Args:
            query_id: Query execution ID

        Returns:
            List of result rows as dictionaries

        Raises:
            Exception: If result retrieval fails
        """
        try:
            results = []
            next_token = None

            while True:
                params = {'Id': query_id}
                if next_token:
                    params['NextToken'] = next_token

                response = redshift_data_client.get_statement_result(**params)

                # Extract column names from first response
                if not results:
                    column_names = [col['name'] for col in response.get('ColumnMetadata', [])]
                    if not column_names:
                        logger.warning(f"No column metadata found for query {query_id}")
                        return []

                # Convert rows to dictionaries
                for row in response.get('Records', []):
                    row_dict = {}
                    for col_idx, col_name in enumerate(column_names):
                        # Each record value is a dict with type and value
                        if col_idx < len(row):
                            cell_value = row[col_idx]
                            # Handle different data types
                            if isinstance(cell_value, dict):
                                # Get the actual value from the type dict
                                if 'stringValue' in cell_value:
                                    row_dict[col_name] = cell_value['stringValue']
                                elif 'longValue' in cell_value:
                                    row_dict[col_name] = cell_value['longValue']
                                elif 'doubleValue' in cell_value:
                                    row_dict[col_name] = cell_value['doubleValue']
                                elif 'booleanValue' in cell_value:
                                    row_dict[col_name] = cell_value['booleanValue']
                                elif 'isNull' in cell_value and cell_value['isNull']:
                                    row_dict[col_name] = None
                                else:
                                    row_dict[col_name] = cell_value
                            else:
                                row_dict[col_name] = cell_value
                        else:
                            row_dict[col_name] = None
                    results.append(row_dict)

                next_token = response.get('NextToken')
                if not next_token:
                    break

            logger.info(f"Retrieved {len(results)} rows from query {query_id}")
            return results

        except Exception as e:
            logger.error(f"Failed to fetch results for query {query_id}: {str(e)}")
            raise


class DashboardDataExtractor:
    """Handles extraction of specific dashboard datasets."""

    def __init__(self, executor: RedshiftQueryExecutor):
        """
        Initialize the extractor.

        Args:
            executor: RedshiftQueryExecutor instance
        """
        self.executor = executor
        self.kpi_metrics = {}

    def _execute_and_fetch(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a query and fetch results.

        Args:
            query: SQL query to execute

        Returns:
            List of result rows as dictionaries
        """
        query_id = self.executor.execute_query(query)
        self.executor.wait_for_completion(query_id)
        return self.executor.fetch_results(query_id)

    def extract_kpi_summary(self) -> Dict[str, Any]:
        """
        Extract KPI summary metrics.

        Returns:
            Dictionary with KPI values
        """
        logger.info("Extracting KPI summary")

        # Query 1: Total active headcount (from latest snapshot)
        headcount_query = f"""
        SELECT COUNT(DISTINCT employee_id) as total_headcount
        FROM {SCHEMA}.fct_worker_headcount_restat_f
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
        """
        headcount_result = self._execute_and_fetch(headcount_query)
        total_headcount = headcount_result[0]['total_headcount'] if headcount_result else 0

        # Query 2: Total movements (all movement records)
        movements_query = f"""
        SELECT COUNT(*) as total_movements
        FROM {SCHEMA}.fct_worker_movement_f
        """
        movements_result = self._execute_and_fetch(movements_query)
        total_movements = movements_result[0]['total_movements'] if movements_result else 0

        # Query 3: Average base pay (active workers only)
        compensation_query = f"""
        SELECT ROUND(AVG(base_pay_proposed_amount), 2) as avg_base_pay
        FROM {SCHEMA}.dim_worker_job_d
        WHERE is_current_job_row = true
            AND active = '1'
            AND base_pay_proposed_amount IS NOT NULL
            AND base_pay_proposed_amount > 0
        """
        compensation_result = self._execute_and_fetch(compensation_query)
        avg_base_pay = float(compensation_result[0]['avg_base_pay']) if compensation_result and compensation_result[0]['avg_base_pay'] else 0.0

        # Query 4: Active companies
        companies_query = f"""
        SELECT COUNT(DISTINCT company_id) as active_companies
        FROM {SCHEMA}.dim_company_d
        WHERE is_current = true
        """
        companies_result = self._execute_and_fetch(companies_query)
        active_companies = companies_result[0]['active_companies'] if companies_result else 0

        # Query 5: Active departments
        departments_query = f"""
        SELECT COUNT(DISTINCT department_id) as active_departments
        FROM {SCHEMA}.dim_department_d
        WHERE is_current = true
            AND active = '1'
        """
        departments_result = self._execute_and_fetch(departments_query)
        active_departments = departments_result[0]['active_departments'] if departments_result else 0

        kpi_data = {
            'extraction_type': 'kpi_summary',
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': {
                'total_headcount': int(total_headcount),
                'total_movements': int(total_movements),
                'avg_base_pay': round(avg_base_pay, 2),
                'active_companies': int(active_companies),
                'active_departments': int(active_departments)
            }
        }

        # Store for CloudWatch publishing
        self.kpi_metrics = kpi_data['metrics']

        logger.info(f"KPI summary extracted: {kpi_data}")
        return kpi_data

    def extract_headcount(self) -> Dict[str, Any]:
        """
        Extract headcount analytics.

        Returns:
            Dictionary with headcount data grouped by company, department, location, and trend
        """
        logger.info("Extracting headcount data")

        # Query 1: Headcount by company (using surrogate key join)
        company_query = f"""
        SELECT
            c.company_id,
            c.company_name,
            COUNT(DISTINCT h.employee_id) as headcount
        FROM {SCHEMA}.fct_worker_headcount_restat_f h
        JOIN {SCHEMA}.dim_company_d c ON h.company_sk = c.company_sk
        WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
            AND c.is_current = true
        GROUP BY c.company_id, c.company_name
        ORDER BY headcount DESC
        """
        company_data = self._execute_and_fetch(company_query)

        # Query 2: Headcount by department (using surrogate key join)
        department_query = f"""
        SELECT
            d.department_id,
            d.department_name,
            COUNT(DISTINCT h.employee_id) as headcount
        FROM {SCHEMA}.fct_worker_headcount_restat_f h
        JOIN {SCHEMA}.dim_department_d d ON h.department_sk = d.department_sk
        WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
            AND d.is_current = true
        GROUP BY d.department_id, d.department_name
        ORDER BY headcount DESC
        """
        department_data = self._execute_and_fetch(department_query)

        # Query 3: Headcount by location (using surrogate key join)
        location_query = f"""
        SELECT
            l.location_id,
            l.location_name,
            l.city,
            l.country_name,
            COUNT(DISTINCT h.employee_id) as headcount
        FROM {SCHEMA}.fct_worker_headcount_restat_f h
        JOIN {SCHEMA}.dim_location_d l ON h.location_sk = l.location_sk
        WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
            AND l.is_current = true
        GROUP BY l.location_id, l.location_name, l.city, l.country_name
        ORDER BY headcount DESC
        """
        location_data = self._execute_and_fetch(location_query)

        # Query 4: Headcount trend by snapshot date
        trend_query = f"""
        SELECT
            snapshot_date,
            COUNT(DISTINCT employee_id) as headcount
        FROM {SCHEMA}.fct_worker_headcount_restat_f
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
        """
        trend_data = self._execute_and_fetch(trend_query)

        headcount_summary = {
            'extraction_type': 'headcount',
            'timestamp': datetime.utcnow().isoformat(),
            'by_company': company_data,
            'by_department': department_data,
            'by_location': location_data,
            'trend': trend_data
        }

        logger.info(f"Headcount data extracted: {len(company_data)} companies, {len(department_data)} departments, {len(location_data)} locations")
        return headcount_summary

    def extract_movements(self) -> Dict[str, Any]:
        """
        Extract employee movement analytics.

        Returns:
            Dictionary with movement data including types, terminations, and trends
        """
        logger.info("Extracting movement data")

        # Query 1: Movement counts by type (sum of change count columns)
        movement_types_query = f"""
        SELECT
            'job_changes' as movement_type,
            COALESCE(SUM(job_change_count), 0) as count
        FROM {SCHEMA}.fct_worker_movement_f
        UNION ALL
        SELECT
            'location_changes' as movement_type,
            COALESCE(SUM(location_change_count), 0) as count
        FROM {SCHEMA}.fct_worker_movement_f
        UNION ALL
        SELECT
            'grade_changes' as movement_type,
            COALESCE(SUM(grade_change_count), 0) as count
        FROM {SCHEMA}.fct_worker_movement_f
        UNION ALL
        SELECT
            'management_level_changes' as movement_type,
            COALESCE(SUM(management_level_change_count), 0) as count
        FROM {SCHEMA}.fct_worker_movement_f
        UNION ALL
        SELECT
            'company_changes' as movement_type,
            COALESCE(SUM(company_change_count), 0) as count
        FROM {SCHEMA}.fct_worker_movement_f
        UNION ALL
        SELECT
            'supervisory_org_changes' as movement_type,
            COALESCE(SUM(supervisory_organization_change_count), 0) as count
        FROM {SCHEMA}.fct_worker_movement_f
        UNION ALL
        SELECT
            'work_model_changes' as movement_type,
            COALESCE(SUM(worker_model_change_count), 0) as count
        FROM {SCHEMA}.fct_worker_movement_f
        """
        movement_types = self._execute_and_fetch(movement_types_query)

        # Query 2: Regrettable terminations count
        terminations_query = f"""
        SELECT
            COALESCE(SUM(regrettable_termination_count), 0) as regrettable_terminations,
            COUNT(DISTINCT employee_id) as distinct_employees_with_terms
        FROM {SCHEMA}.fct_worker_movement_f
        WHERE regrettable_termination_count > 0
        """
        terminations = self._execute_and_fetch(terminations_query)

        # Restructure terminations data
        terminations_summary = {
            'regrettable_terminations': int(terminations[0]['regrettable_terminations']) if terminations else 0,
            'employees_with_regrettable_terms': int(terminations[0]['distinct_employees_with_terms']) if terminations else 0
        }

        # Query 3: Movement trend by month
        trend_query = f"""
        SELECT
            DATE_TRUNC('month', effective_date)::DATE as month,
            COUNT(DISTINCT employee_id) as distinct_employees,
            SUM(job_change_count) as job_changes,
            SUM(location_change_count) as location_changes,
            SUM(grade_change_count) as grade_changes,
            SUM(regrettable_termination_count) as regrettable_terms
        FROM {SCHEMA}.fct_worker_movement_f
        GROUP BY DATE_TRUNC('month', effective_date)
        ORDER BY month DESC
        LIMIT 12
        """
        trend_data = self._execute_and_fetch(trend_query)

        movements_summary = {
            'extraction_type': 'movements',
            'timestamp': datetime.utcnow().isoformat(),
            'by_type': movement_types,
            'terminations': terminations_summary,
            'trend': trend_data
        }

        logger.info(f"Movement data extracted: {terminations_summary['regrettable_terminations']} regrettable terminations")
        return movements_summary

    def extract_compensation(self) -> Dict[str, Any]:
        """
        Extract compensation analytics.

        Returns:
            Dictionary with compensation data by grade and job family
        """
        logger.info("Extracting compensation data")

        # Query 1: Average base pay by grade profile
        grade_query = f"""
        SELECT
            g.grade_id,
            g.grade_name,
            g.grade_profile_name,
            g.grade_profile_salary_range_minimjum as salary_minimum,
            g.grade_profile_salary_range_midpoint as salary_midpoint,
            g.grade_profile_salary_range_maximum as salary_maximum,
            COUNT(DISTINCT j.employee_id) as employee_count,
            ROUND(AVG(j.base_pay_proposed_amount), 2) as avg_base_pay,
            MIN(j.base_pay_proposed_amount) as min_base_pay,
            MAX(j.base_pay_proposed_amount) as max_base_pay
        FROM {SCHEMA}.dim_worker_job_d j
        LEFT JOIN {SCHEMA}.dim_grade_profile_d g ON j.compensation_grade_proposed = g.grade_profile_id
        WHERE j.is_current_job_row = true
            AND j.active = '1'
            AND j.base_pay_proposed_amount IS NOT NULL
            AND j.base_pay_proposed_amount > 0
            AND g.is_current = true
        GROUP BY g.grade_id, g.grade_name, g.grade_profile_name,
                 g.grade_profile_salary_range_minimjum, g.grade_profile_salary_range_midpoint,
                 g.grade_profile_salary_range_maximum
        ORDER BY salary_midpoint DESC
        """
        grade_data = self._execute_and_fetch(grade_query)

        # Query 2: Compensation distribution by job family
        family_query = f"""
        SELECT
            jf.job_family,
            jf.job_family_name,
            COUNT(DISTINCT j.employee_id) as employee_count,
            ROUND(AVG(j.base_pay_proposed_amount), 2) as avg_base_pay,
            MIN(j.base_pay_proposed_amount) as min_base_pay,
            MAX(j.base_pay_proposed_amount) as max_base_pay
        FROM {SCHEMA}.dim_worker_job_d j
        LEFT JOIN {SCHEMA}.dim_job_profile_d jf ON j.job_profile_id = jf.job_profile_id
        WHERE j.is_current_job_row = true
            AND j.active = '1'
            AND j.base_pay_proposed_amount IS NOT NULL
            AND j.base_pay_proposed_amount > 0
            AND jf.is_current = true
        GROUP BY jf.job_family, jf.job_family_name
        ORDER BY avg_base_pay DESC
        """
        family_data = self._execute_and_fetch(family_query)

        compensation_summary = {
            'extraction_type': 'compensation',
            'timestamp': datetime.utcnow().isoformat(),
            'by_grade': grade_data,
            'by_job_family': family_data
        }

        logger.info(f"Compensation data extracted: {len(grade_data)} grades, {len(family_data)} job families")
        return compensation_summary

    def extract_org_health(self) -> Dict[str, Any]:
        """
        Extract organizational health metrics.

        Returns:
            Dictionary with org structure and health metrics
        """
        logger.info("Extracting organizational health data")

        # Query 1: Department counts and sizes
        departments_query = f"""
        SELECT
            d.department_id,
            d.department_name,
            COUNT(DISTINCT j.employee_id) as department_size,
            COUNT(DISTINCT j.manager_id) as manager_count
        FROM {SCHEMA}.dim_department_d d
        LEFT JOIN {SCHEMA}.dim_worker_job_d j ON d.department_id = j.supervisory_organization AND j.is_current_job_row = true
        WHERE d.is_current = true
            AND d.active = '1'
        GROUP BY d.department_id, d.department_name
        ORDER BY department_size DESC
        """
        departments_data = self._execute_and_fetch(departments_query)

        # Query 2: Manager span of control (count direct reports per manager)
        span_query = f"""
        SELECT
            m.employee_id as manager_employee_id,
            m.business_title as manager_title,
            COUNT(DISTINCT e.employee_id) as direct_reports
        FROM {SCHEMA}.dim_worker_job_d m
        INNER JOIN {SCHEMA}.dim_worker_job_d e ON m.employee_id = e.manager_id
        WHERE m.is_current_job_row = true
            AND m.active = '1'
            AND e.is_current_job_row = true
            AND e.active = '1'
        GROUP BY m.employee_id, m.business_title
        ORDER BY direct_reports DESC
        LIMIT 100
        """
        span_data = self._execute_and_fetch(span_query)

        # Query 3: Location distribution
        location_query = f"""
        SELECT
            l.location_id,
            l.location_name,
            l.city,
            l.country_name,
            l.region_name,
            COUNT(DISTINCT j.employee_id) as headcount
        FROM {SCHEMA}.dim_location_d l
        LEFT JOIN {SCHEMA}.dim_worker_job_d j ON l.location_id = j.location AND j.is_current_job_row = true AND j.active = '1'
        WHERE l.is_current = true
        GROUP BY l.location_id, l.location_name, l.city, l.country_name, l.region_name
        ORDER BY headcount DESC
        """
        location_data = self._execute_and_fetch(location_query)

        # Query 4: Worker type distribution
        worker_type_query = f"""
        SELECT
            worker_type,
            worker_sub_type,
            COUNT(DISTINCT employee_id) as count
        FROM {SCHEMA}.dim_worker_job_d
        WHERE is_current_job_row = true
            AND active = '1'
        GROUP BY worker_type, worker_sub_type
        ORDER BY count DESC
        """
        worker_type_data = self._execute_and_fetch(worker_type_query)

        org_health_summary = {
            'extraction_type': 'org_health',
            'timestamp': datetime.utcnow().isoformat(),
            'departments': departments_data,
            'manager_span_of_control': span_data,
            'locations': location_data,
            'worker_types': worker_type_data
        }

        logger.info(f"Organizational health data extracted: {len(departments_data)} departments, {len(location_data)} locations")
        return org_health_summary


class S3DataPublisher:
    """Handles publishing extracted data to S3."""

    @staticmethod
    def publish(bucket: str, key: str, data: Dict[str, Any]) -> bool:
        """
        Publish data to S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            data: Data to publish as JSON

        Returns:
            True if successful, False otherwise
        """
        try:
            json_data = json.dumps(data, default=str)
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_data,
                ContentType='application/json'
            )
            logger.info(f"Published data to s3://{bucket}/{key}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish to S3: {str(e)}")
            return False


class CloudWatchMetricsPublisher:
    """Handles publishing metrics to CloudWatch."""

    @staticmethod
    def publish_kpi_metrics(metrics: Dict[str, Any], namespace: str) -> bool:
        """
        Publish KPI metrics to CloudWatch.

        Args:
            metrics: Dictionary of metric values
            namespace: CloudWatch namespace

        Returns:
            True if successful, False otherwise
        """
        try:
            metric_data = [
                {
                    'MetricName': 'ActiveHeadcount',
                    'Value': metrics.get('total_headcount', 0),
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'TotalMovements',
                    'Value': metrics.get('total_movements', 0),
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'AvgBasePay',
                    'Value': metrics.get('avg_base_pay', 0),
                    'Unit': 'None',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'ActiveCompanies',
                    'Value': metrics.get('active_companies', 0),
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'ActiveDepartments',
                    'Value': metrics.get('active_departments', 0),
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }
            ]

            cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data
            )
            logger.info(f"Published {len(metric_data)} metrics to CloudWatch namespace {namespace}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish CloudWatch metrics: {str(e)}")
            return False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function.

    Args:
        event: Lambda event containing 'extraction' parameter
        context: Lambda context object

    Returns:
        Dictionary with status and results
    """
    try:
        extraction_type = event.get('extraction', 'kpi_summary')
        logger.info(f"Processing extraction type: {extraction_type}")

        # Validate extraction type
        valid_extractions = ['kpi_summary', 'headcount', 'movements', 'compensation', 'org_health']
        if extraction_type not in valid_extractions:
            error_msg = f"Invalid extraction type: {extraction_type}. Must be one of {valid_extractions}"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_msg})
            }

        # Initialize executor and extractor
        executor = RedshiftQueryExecutor(CLUSTER_ID, DATABASE, DB_USER)
        extractor = DashboardDataExtractor(executor)

        # Execute appropriate extraction
        extraction_methods = {
            'kpi_summary': extractor.extract_kpi_summary,
            'headcount': extractor.extract_headcount,
            'movements': extractor.extract_movements,
            'compensation': extractor.extract_compensation,
            'org_health': extractor.extract_org_health
        }

        extracted_data = extraction_methods[extraction_type]()

        # Publish to S3
        s3_key = f"data/{extraction_type}.json"
        S3DataPublisher.publish(S3_BUCKET, s3_key, extracted_data)

        # Publish CloudWatch metrics if this is KPI summary
        if extraction_type == 'kpi_summary':
            CloudWatchMetricsPublisher.publish_kpi_metrics(
                extractor.kpi_metrics,
                CLOUDWATCH_NAMESPACE
            )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'{extraction_type} extraction completed successfully',
                'extraction_type': extraction_type,
                'timestamp': datetime.utcnow().isoformat(),
                's3_location': f"s3://{S3_BUCKET}/{s3_key}"
            })
        }

    except TimeoutError as e:
        logger.error(f"Query timeout error: {str(e)}")
        return {
            'statusCode': 504,
            'body': json.dumps({'error': f'Query timeout: {str(e)}'})
        }
    except Exception as e:
        logger.error(f"Unexpected error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal error: {str(e)}'})
        }


if __name__ == '__main__':
    # For local testing
    test_event = {'extraction': 'kpi_summary'}
    print(json.dumps(lambda_handler(test_event, None), indent=2))
