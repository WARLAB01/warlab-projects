"""
CloudWatch Metrics Publisher Lambda Function

This Lambda function queries KPI metrics from Redshift and publishes them
as CloudWatch custom metrics. It's designed to run on a schedule (e.g., hourly)
to keep CloudWatch metrics synchronized with the Redshift data warehouse.

Metrics published to the "WarLabHRDashboard" namespace:
- ActiveHeadcount: Total active employees
- TotalMovements: Total employee movements recorded
- AvgBasePay: Average base compensation
- ActiveCompanies: Number of active companies
- ActiveDepartments: Number of active departments

These metrics can be used for CloudWatch dashboards, alarms, and monitoring.
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
cloudwatch_client = boto3.client('cloudwatch')

# Configuration constants
CLUSTER_ID = 'warlab-hr-datamart'
DATABASE = 'dev'
DB_USER = 'admin'
SCHEMA = 'l3_workday'
CLOUDWATCH_NAMESPACE = 'WarLabHRDashboard'
QUERY_TIMEOUT_SECONDS = 300
POLL_INTERVAL_SECONDS = 1


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
            column_names = None

            while True:
                params = {'Id': query_id}
                if next_token:
                    params['NextToken'] = next_token

                response = redshift_data_client.get_statement_result(**params)

                # Extract column names from first response
                if column_names is None:
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


class KPIMetricsExtractor:
    """Extracts KPI metrics from Redshift for CloudWatch publishing."""

    def __init__(self, executor: RedshiftQueryExecutor):
        """
        Initialize the metrics extractor.

        Args:
            executor: RedshiftQueryExecutor instance
        """
        self.executor = executor

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

    def extract_kpi_metrics(self) -> Dict[str, Any]:
        """
        Extract all KPI metrics from Redshift.

        Returns:
            Dictionary with metric names and values

        Raises:
            Exception: If any query fails
        """
        logger.info("Extracting KPI metrics from Redshift")

        metrics = {}

        try:
            # Metric 1: Total active headcount
            logger.info("Extracting active headcount metric")
            headcount_query = f"""
            SELECT COUNT(DISTINCT employee_id) as value
            FROM {SCHEMA}.fct_worker_headcount_restat_f
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
            """
            headcount_result = self._execute_and_fetch(headcount_query)
            metrics['ActiveHeadcount'] = float(headcount_result[0]['value']) if headcount_result else 0.0
            logger.info(f"ActiveHeadcount: {metrics['ActiveHeadcount']}")

        except Exception as e:
            logger.error(f"Failed to extract ActiveHeadcount: {str(e)}")
            raise

        try:
            # Metric 2: Total movements
            logger.info("Extracting total movements metric")
            movements_query = f"""
            SELECT COUNT(*) as value
            FROM {SCHEMA}.fct_worker_movement_f
            """
            movements_result = self._execute_and_fetch(movements_query)
            metrics['TotalMovements'] = float(movements_result[0]['value']) if movements_result else 0.0
            logger.info(f"TotalMovements: {metrics['TotalMovements']}")

        except Exception as e:
            logger.error(f"Failed to extract TotalMovements: {str(e)}")
            raise

        try:
            # Metric 3: Average base pay
            logger.info("Extracting average base pay metric")
            compensation_query = f"""
            SELECT AVG(CAST(base_pay AS DECIMAL(15,2))) as value
            FROM {SCHEMA}.dim_worker_job_d
            WHERE is_current_job_row = true
            """
            compensation_result = self._execute_and_fetch(compensation_query)
            metrics['AvgBasePay'] = float(compensation_result[0]['value']) if compensation_result and compensation_result[0]['value'] else 0.0
            logger.info(f"AvgBasePay: {metrics['AvgBasePay']}")

        except Exception as e:
            logger.error(f"Failed to extract AvgBasePay: {str(e)}")
            raise

        try:
            # Metric 4: Active companies
            logger.info("Extracting active companies metric")
            companies_query = f"""
            SELECT COUNT(DISTINCT company_id) as value
            FROM {SCHEMA}.dim_company_d
            WHERE is_current = true
            """
            companies_result = self._execute_and_fetch(companies_query)
            metrics['ActiveCompanies'] = float(companies_result[0]['value']) if companies_result else 0.0
            logger.info(f"ActiveCompanies: {metrics['ActiveCompanies']}")

        except Exception as e:
            logger.error(f"Failed to extract ActiveCompanies: {str(e)}")
            raise

        try:
            # Metric 5: Active departments
            logger.info("Extracting active departments metric")
            departments_query = f"""
            SELECT COUNT(DISTINCT department_id) as value
            FROM {SCHEMA}.dim_department_d
            WHERE is_current = true
            """
            departments_result = self._execute_and_fetch(departments_query)
            metrics['ActiveDepartments'] = float(departments_result[0]['value']) if departments_result else 0.0
            logger.info(f"ActiveDepartments: {metrics['ActiveDepartments']}")

        except Exception as e:
            logger.error(f"Failed to extract ActiveDepartments: {str(e)}")
            raise

        return metrics


class CloudWatchMetricsPublisher:
    """Handles publishing metrics to CloudWatch."""

    @staticmethod
    def publish_metrics(metrics: Dict[str, float], namespace: str) -> bool:
        """
        Publish metrics to CloudWatch.

        Args:
            metrics: Dictionary mapping metric names to float values
            namespace: CloudWatch namespace

        Returns:
            True if successful, False otherwise

        Raises:
            Exception: If publishing fails
        """
        try:
            metric_data = []
            current_timestamp = datetime.utcnow()

            # Build metric data list
            metric_units = {
                'ActiveHeadcount': 'Count',
                'TotalMovements': 'Count',
                'AvgBasePay': 'None',
                'ActiveCompanies': 'Count',
                'ActiveDepartments': 'Count'
            }

            for metric_name, metric_value in metrics.items():
                metric_data.append({
                    'MetricName': metric_name,
                    'Value': metric_value,
                    'Unit': metric_units.get(metric_name, 'None'),
                    'Timestamp': current_timestamp
                })

            # Publish metrics in batches (CloudWatch has a 20 metric limit per put_metric_data call)
            batch_size = 20
            for i in range(0, len(metric_data), batch_size):
                batch = metric_data[i:i + batch_size]
                cloudwatch_client.put_metric_data(
                    Namespace=namespace,
                    MetricData=batch
                )
                logger.info(f"Published batch of {len(batch)} metrics to CloudWatch namespace {namespace}")

            logger.info(f"Successfully published {len(metrics)} metrics to CloudWatch")
            return True

        except Exception as e:
            logger.error(f"Failed to publish CloudWatch metrics: {str(e)}")
            raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function.

    Extracts KPI metrics from Redshift and publishes them to CloudWatch.
    Can be invoked manually or on a schedule (e.g., via CloudWatch Events).

    Args:
        event: Lambda event (can be empty for scheduled execution)
        context: Lambda context object

    Returns:
        Dictionary with execution status and published metrics
    """
    try:
        logger.info("Starting CloudWatch metrics publisher")

        # Initialize extractor and executor
        executor = RedshiftQueryExecutor(CLUSTER_ID, DATABASE, DB_USER)
        extractor = KPIMetricsExtractor(executor)

        # Extract metrics from Redshift
        metrics = extractor.extract_kpi_metrics()

        # Publish to CloudWatch
        CloudWatchMetricsPublisher.publish_metrics(metrics, CLOUDWATCH_NAMESPACE)

        # Prepare response
        response_body = {
            'message': 'KPI metrics published successfully',
            'timestamp': datetime.utcnow().isoformat(),
            'namespace': CLOUDWATCH_NAMESPACE,
            'metrics_published': list(metrics.keys()),
            'metric_values': metrics
        }

        logger.info(f"Execution completed successfully: {json.dumps(response_body, default=str)}")

        return {
            'statusCode': 200,
            'body': json.dumps(response_body, default=str)
        }

    except TimeoutError as e:
        logger.error(f"Query timeout error: {str(e)}")
        return {
            'statusCode': 504,
            'body': json.dumps({
                'error': f'Query timeout: {str(e)}'
            })
        }
    except Exception as e:
        logger.error(f"Unexpected error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Internal error: {str(e)}'
            })
        }


if __name__ == '__main__':
    # For local testing
    print(json.dumps(lambda_handler({}, None), indent=2))
