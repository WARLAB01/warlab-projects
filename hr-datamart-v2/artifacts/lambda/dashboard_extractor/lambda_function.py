"""
HR Datamart V2 - Dashboard Data Extractor Lambda
Queries v2_l3_star schema in Redshift, publishes JSON to S3 for dashboard consumption.

Extractions: kpi_summary, headcount, movements, compensation, org_health
Invocation: {"extraction": "kpi_summary"} or {"extraction": "all"}

Architecture: Redshift Data API -> JSON -> S3 -> CloudFront -> Static HTML/ECharts
"""
import json
import time
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ============================================================
# Configuration
# ============================================================
CLUSTER_ID = 'warlab-hr-datamart'
DATABASE = 'dev'
DB_USER = 'admin'
SCHEMA = 'v2_l3_star'
S3_BUCKET = 'warlab-hr-dashboard'
S3_PREFIX = 'v2/data'
CLOUDWATCH_NAMESPACE = 'WarLabHRDashboard-V2'
QUERY_TIMEOUT_SECONDS = 300


# ============================================================
# Redshift Query Executor
# ============================================================
class RedshiftQueryExecutor:
    def __init__(self):
        self.client = boto3.client('redshift-data', region_name='us-east-1')

    def execute_query(self, sql, description=''):
        logger.info(f"Executing: {description}")
        resp = self.client.execute_statement(
            ClusterIdentifier=CLUSTER_ID,
            Database=DATABASE,
            DbUser=DB_USER,
            Sql=sql
        )
        statement_id = resp['Id']
        elapsed = 0
        while elapsed < QUERY_TIMEOUT_SECONDS:
            time.sleep(2)
            elapsed += 2
            status = self.client.describe_statement(Id=statement_id)
            if status['Status'] in ('FINISHED', 'FAILED', 'ABORTED'):
                break
        if status['Status'] != 'FINISHED':
            raise Exception(f"Query failed ({description}): {status.get('Error', status['Status'])}")
        return statement_id

    def get_results(self, statement_id):
        result = self.client.get_statement_result(Id=statement_id)
        columns = [col['name'] for col in result['ColumnMetadata']]
        rows = []
        for record in result.get('Records', []):
            row = {}
            for i, field in enumerate(record):
                if 'isNull' in field:
                    row[columns[i]] = None
                elif 'stringValue' in field:
                    row[columns[i]] = field['stringValue']
                elif 'longValue' in field:
                    row[columns[i]] = field['longValue']
                elif 'doubleValue' in field:
                    row[columns[i]] = field['doubleValue']
                elif 'booleanValue' in field:
                    row[columns[i]] = field['booleanValue']
                else:
                    row[columns[i]] = str(field)
            rows.append(row)
        return rows

    def query(self, sql, description=''):
        sid = self.execute_query(sql, description)
        return self.get_results(sid)


# ============================================================
# Dashboard Data Extractor
# ============================================================
class DashboardDataExtractor:
    def __init__(self):
        self.db = RedshiftQueryExecutor()
        self.s3 = boto3.client('s3')

    def extract_kpi_summary(self):
        """Extract 5 KPI metrics."""
        # Total active headcount (latest snapshot)
        headcount = self.db.query(f"""
            SELECT COUNT(DISTINCT employee_id) as total_headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f
            ) AND headcount = 1
        """, "KPI: Total Headcount")

        # Total movements
        movements = self.db.query(f"""
            SELECT COUNT(*) as total_movements
            FROM {SCHEMA}.fct_worker_movement_f
        """, "KPI: Total Movements")

        # Average base pay (active workers) - regex filter for numeric values
        avg_pay = self.db.query(f"""
            SELECT ROUND(AVG(base_pay_proposed_amount::DECIMAL(18,2)), 2) as avg_base_pay
            FROM {SCHEMA}.dim_worker_job_d
            WHERE is_current = 'Y'
              AND worker_status = 'Active'
              AND base_pay_proposed_amount ~ '^[0-9]'
        """, "KPI: Avg Base Pay")

        # Active companies
        companies = self.db.query(f"""
            SELECT COUNT(DISTINCT company_id) as active_companies
            FROM {SCHEMA}.dim_company_d
        """, "KPI: Active Companies")

        # Active departments
        departments = self.db.query(f"""
            SELECT COUNT(DISTINCT department_id) as active_departments
            FROM {SCHEMA}.dim_supervisory_org_d
        """, "KPI: Active Departments")

        metrics = {
            'total_headcount': int(headcount[0]['total_headcount']) if headcount else 0,
            'total_movements': int(movements[0]['total_movements']) if movements else 0,
            'avg_base_pay': float(avg_pay[0]['avg_base_pay']) if avg_pay and avg_pay[0]['avg_base_pay'] else 0,
            'active_companies': int(companies[0]['active_companies']) if companies else 0,
            'active_departments': int(departments[0]['active_departments']) if departments else 0,
        }

        return self._publish('kpi_summary', metrics)

    def extract_headcount(self):
        """Extract headcount by company, department, location, and trend."""
        # By Company - derive from sup_org hierarchy (company_id not populated in worker_job)
        # Use top-level sup_org (sup_org_level_1) as company proxy
        by_company = self.db.query(f"""
            SELECT
                COALESCE(s.sup_org_level_1_id, 'Unknown') as company_id,
                COALESCE(s.sup_org_level_1_name, 'Unknown') as company_name,
                COUNT(DISTINCT h.employee_id) as headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f h
            LEFT JOIN {SCHEMA}.dim_supervisory_org_d s ON h.sup_org_id = s.department_id
            WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
              AND h.headcount = 1
            GROUP BY COALESCE(s.sup_org_level_1_id, 'Unknown'), COALESCE(s.sup_org_level_1_name, 'Unknown')
            ORDER BY headcount DESC
        """, "Headcount by Company")

        # By Department (top 20, natural key join)
        by_dept = self.db.query(f"""
            SELECT h.sup_org_id, s.department_name, COUNT(DISTINCT h.employee_id) as headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f h
            LEFT JOIN {SCHEMA}.dim_supervisory_org_d s ON h.sup_org_id = s.department_id
            WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
              AND h.headcount = 1
              AND h.sup_org_id IS NOT NULL AND h.sup_org_id <> ''
            GROUP BY h.sup_org_id, s.department_name
            ORDER BY headcount DESC
            LIMIT 20
        """, "Headcount by Department")

        # By Location (natural key join)
        by_location = self.db.query(f"""
            SELECT h.location_id, l.location_name, COUNT(DISTINCT h.employee_id) as headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f h
            LEFT JOIN {SCHEMA}.dim_location_d l ON h.location_id = l.location_id
            WHERE h.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.fct_worker_headcount_restat_f)
              AND h.headcount = 1
              AND h.location_id IS NOT NULL AND h.location_id <> ''
            GROUP BY h.location_id, l.location_name
            ORDER BY headcount DESC
        """, "Headcount by Location")

        # 24-month trend
        trend = self.db.query(f"""
            SELECT snapshot_date, COUNT(DISTINCT employee_id) as headcount
            FROM {SCHEMA}.fct_worker_headcount_restat_f
            WHERE headcount = 1
            GROUP BY snapshot_date
            ORDER BY snapshot_date
        """, "Headcount Trend")

        data = {
            'by_company': by_company,
            'by_department': by_dept,
            'by_location': by_location,
            'trend': trend,
        }

        return self._publish('headcount', data)

    def extract_movements(self):
        """Extract movement analytics by type, terminations, and trends."""
        # Movement counts by type
        by_type = self.db.query(f"""
            SELECT
                SUM(external_hire_count) as hires,
                SUM(termination_count) as terminations,
                SUM(promotion_count_business_process) as promotions,
                SUM(demotion_count) as demotions,
                SUM(lateral_move_count) as lateral_moves,
                SUM(job_change_count) as job_changes,
                SUM(base_pay_change_count) as pay_changes,
                SUM(voluntary_termination_count) as voluntary_terminations,
                SUM(involuntary_termination_count) as involuntary_terminations,
                SUM(regrettable_termination_count) as regrettable_terminations,
                SUM(grade_change_count) as grade_changes,
                SUM(company_change_count) as company_changes,
                SUM(cost_center_change_count) as cost_center_changes,
                SUM(supervisory_organization_change_count) as org_changes,
                SUM(worker_model_change_count) as work_model_changes
            FROM {SCHEMA}.fct_worker_movement_f
        """, "Movement by Type")

        # Termination breakdown
        terminations = self.db.query(f"""
            SELECT primary_termination_reason, COUNT(*) as cnt
            FROM {SCHEMA}.fct_worker_movement_f
            WHERE termination_count = 1
              AND primary_termination_reason IS NOT NULL
              AND TRIM(primary_termination_reason) <> ''
            GROUP BY primary_termination_reason
            ORDER BY cnt DESC
            LIMIT 15
        """, "Termination Breakdown")

        # Monthly trend (by fiscal month)
        monthly_trend = self.db.query(f"""
            SELECT
                TO_CHAR(DATE_TRUNC('month', effective_date), 'YYYY-MM') as month,
                SUM(external_hire_count) as hires,
                SUM(termination_count) as terminations,
                SUM(promotion_count_business_process) as promotions,
                SUM(job_change_count) as job_changes,
                SUM(base_pay_change_count) as pay_changes,
                SUM(voluntary_termination_count) as vol_terms,
                SUM(involuntary_termination_count) as invol_terms
            FROM {SCHEMA}.fct_worker_movement_f
            GROUP BY DATE_TRUNC('month', effective_date)
            ORDER BY month
        """, "Movement Monthly Trend")

        # Turnover & promotion rates (last 12 months)
        rates = self.db.query(f"""
            WITH monthly AS (
                SELECT
                    TO_CHAR(DATE_TRUNC('month', m.effective_date), 'YYYY-MM') as month,
                    SUM(m.termination_count) as terms,
                    SUM(m.promotion_count_business_process) as promos
                FROM {SCHEMA}.fct_worker_movement_f m
                GROUP BY DATE_TRUNC('month', m.effective_date)
            ),
            hc AS (
                SELECT snapshot_date, COUNT(DISTINCT employee_id) as headcount
                FROM {SCHEMA}.fct_worker_headcount_restat_f
                WHERE headcount = 1
                GROUP BY snapshot_date
            )
            SELECT mo.month,
                mo.terms, mo.promos,
                hc.headcount,
                CASE WHEN hc.headcount > 0 THEN ROUND(mo.terms::DECIMAL * 100.0 / hc.headcount, 2) ELSE 0 END as turnover_rate,
                CASE WHEN hc.headcount > 0 THEN ROUND(mo.promos::DECIMAL * 100.0 / hc.headcount, 2) ELSE 0 END as promotion_rate
            FROM monthly mo
            LEFT JOIN hc ON LAST_DAY((mo.month || '-01')::DATE) = hc.snapshot_date
            ORDER BY mo.month
        """, "Turnover & Promotion Rates")

        data = {
            'by_type': by_type[0] if by_type else {},
            'terminations': terminations,
            'monthly_trend': monthly_trend,
            'rates': rates,
        }

        return self._publish('movements', data)

    def extract_compensation(self):
        """Extract compensation analytics by grade and job family."""
        # By Grade (no MEDIAN - can't combine with other aggregates in Redshift)
        by_grade = self.db.query(f"""
            SELECT
                dwj.compensation_grade as grade_id,
                g.grade_name,
                COUNT(DISTINCT dwj.employee_id) as worker_count,
                ROUND(AVG(dwj.base_pay_proposed_amount::DECIMAL(18,2)), 2) as avg_pay,
                ROUND(MIN(dwj.base_pay_proposed_amount::DECIMAL(18,2)), 2) as min_pay,
                ROUND(MAX(dwj.base_pay_proposed_amount::DECIMAL(18,2)), 2) as max_pay
            FROM {SCHEMA}.dim_worker_job_d dwj
            LEFT JOIN {SCHEMA}.dim_grade_profile_d g ON dwj.compensation_grade = g.grade_id
            WHERE dwj.is_current = 'Y'
              AND dwj.worker_status = 'Active'
              AND dwj.base_pay_proposed_amount ~ '^[0-9]'
            GROUP BY dwj.compensation_grade, g.grade_name
            ORDER BY avg_pay DESC
        """, "Compensation by Grade")

        # By Job Family
        by_job_family = self.db.query(f"""
            SELECT
                dwj.job_profile_id,
                jp.job_profile_name as job_family,
                COUNT(DISTINCT dwj.employee_id) as worker_count,
                ROUND(AVG(dwj.base_pay_proposed_amount::DECIMAL(18,2)), 2) as avg_pay,
                ROUND(MIN(dwj.base_pay_proposed_amount::DECIMAL(18,2)), 2) as min_pay,
                ROUND(MAX(dwj.base_pay_proposed_amount::DECIMAL(18,2)), 2) as max_pay
            FROM {SCHEMA}.dim_worker_job_d dwj
            LEFT JOIN {SCHEMA}.dim_job_profile_d jp ON dwj.job_profile_id = jp.job_profile_id
            WHERE dwj.is_current = 'Y'
              AND dwj.worker_status = 'Active'
              AND dwj.base_pay_proposed_amount ~ '^[0-9]'
            GROUP BY dwj.job_profile_id, jp.job_profile_name
            ORDER BY avg_pay DESC
            LIMIT 20
        """, "Compensation by Job Family")

        data = {
            'by_grade': by_grade,
            'by_job_family': by_job_family,
        }

        return self._publish('compensation', data)

    def extract_org_health(self):
        """Extract org health: departments, span of control, locations, worker types."""
        # Departments by size
        departments = self.db.query(f"""
            SELECT
                dwj.supervisory_organization as dept_id,
                s.department_name,
                COUNT(DISTINCT dwj.employee_id) as worker_count
            FROM {SCHEMA}.dim_worker_job_d dwj
            LEFT JOIN {SCHEMA}.dim_supervisory_org_d s ON dwj.supervisory_organization = s.department_id
            WHERE dwj.is_current = 'Y' AND dwj.worker_status = 'Active'
            GROUP BY dwj.supervisory_organization, s.department_name
            ORDER BY worker_count DESC
            LIMIT 20
        """, "Org Health: Departments")

        # Manager span of control (manager_worker_id = direct manager)
        span_of_control = self.db.query(f"""
            SELECT
                manager_worker_id,
                manager_preferred_name,
                COUNT(DISTINCT employee_id) as direct_reports
            FROM {SCHEMA}.dim_report_to_d
            WHERE manager_worker_id IS NOT NULL
              AND TRIM(manager_worker_id) <> ''
            GROUP BY manager_worker_id, manager_preferred_name
            ORDER BY direct_reports DESC
        """, "Org Health: Span of Control")

        # Span of control distribution
        span_dist = self.db.query(f"""
            WITH spans AS (
                SELECT manager_worker_id, COUNT(DISTINCT employee_id) as direct_reports
                FROM {SCHEMA}.dim_report_to_d
                WHERE manager_worker_id IS NOT NULL AND TRIM(manager_worker_id) <> ''
                GROUP BY manager_worker_id
            )
            SELECT
                CASE
                    WHEN direct_reports <= 3 THEN '1-3'
                    WHEN direct_reports <= 6 THEN '4-6'
                    WHEN direct_reports <= 10 THEN '7-10'
                    WHEN direct_reports <= 15 THEN '11-15'
                    ELSE '16+'
                END as span_range,
                COUNT(*) as manager_count
            FROM spans
            GROUP BY 1
            ORDER BY MIN(direct_reports)
        """, "Org Health: Span Distribution")

        # Location distribution
        locations = self.db.query(f"""
            SELECT
                dwj.location as location_id,
                l.location_name,
                COUNT(DISTINCT dwj.employee_id) as worker_count
            FROM {SCHEMA}.dim_worker_job_d dwj
            LEFT JOIN {SCHEMA}.dim_location_d l ON dwj.location = l.location_id
            WHERE dwj.is_current = 'Y' AND dwj.worker_status = 'Active'
            GROUP BY dwj.location, l.location_name
            ORDER BY worker_count DESC
        """, "Org Health: Locations")

        # Worker status distribution
        worker_status = self.db.query(f"""
            SELECT worker_status, COUNT(DISTINCT employee_id) as worker_count
            FROM {SCHEMA}.dim_worker_job_d
            WHERE is_current = 'Y'
            GROUP BY worker_status
            ORDER BY worker_count DESC
        """, "Org Health: Worker Status")

        data = {
            'departments': departments,
            'span_of_control': span_of_control[:20],
            'span_distribution': span_dist,
            'locations': locations,
            'worker_status': worker_status,
        }

        return self._publish('org_health', data)

    def _publish(self, extraction_type, data):
        """Publish extraction results to S3."""
        payload = {
            'extraction_type': extraction_type,
            'schema': SCHEMA,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': data,
        }
        key = f"{S3_PREFIX}/{extraction_type}.json"
        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(payload, default=str),
            ContentType='application/json'
        )
        logger.info(f"Published {extraction_type} to s3://{S3_BUCKET}/{key}")
        return payload


# ============================================================
# CloudWatch Metrics Publisher
# ============================================================
class CloudWatchMetricsPublisher:
    def __init__(self):
        self.cw = boto3.client('cloudwatch', region_name='us-east-1')

    def publish_kpi_metrics(self, kpi_data):
        """Publish KPI metrics to CloudWatch."""
        metrics = kpi_data.get('data', kpi_data)
        metric_data = []
        for name, value in metrics.items():
            if isinstance(value, (int, float)):
                metric_data.append({
                    'MetricName': name,
                    'Value': float(value),
                    'Unit': 'Count' if 'count' in name.lower() else 'None',
                    'Timestamp': datetime.now(timezone.utc),
                })
        if metric_data:
            self.cw.put_metric_data(
                Namespace=CLOUDWATCH_NAMESPACE,
                MetricData=metric_data
            )
            logger.info(f"Published {len(metric_data)} metrics to CloudWatch")


# ============================================================
# Lambda Handler
# ============================================================
def lambda_handler(event, context):
    """
    Main handler. Event format: {"extraction": "kpi_summary"|"headcount"|"movements"|"compensation"|"org_health"|"all"}
    """
    extraction = event.get('extraction', 'all')
    extractor = DashboardDataExtractor()
    results = {}

    extraction_map = {
        'kpi_summary': extractor.extract_kpi_summary,
        'headcount': extractor.extract_headcount,
        'movements': extractor.extract_movements,
        'compensation': extractor.extract_compensation,
        'org_health': extractor.extract_org_health,
    }

    if extraction == 'all':
        for name, func in extraction_map.items():
            try:
                results[name] = func()
                logger.info(f"Completed: {name}")
            except Exception as e:
                logger.error(f"Failed: {name} - {str(e)}")
                results[name] = {'error': str(e)}

        # Publish KPI metrics to CloudWatch
        if 'kpi_summary' in results and 'error' not in results['kpi_summary']:
            try:
                cw_publisher = CloudWatchMetricsPublisher()
                cw_publisher.publish_kpi_metrics(results['kpi_summary'])
            except Exception as e:
                logger.error(f"CloudWatch publish failed: {str(e)}")
    elif extraction in extraction_map:
        try:
            results[extraction] = extraction_map[extraction]()
        except Exception as e:
            logger.error(f"Failed: {extraction} - {str(e)}")
            results[extraction] = {'error': str(e)}
    else:
        return {'statusCode': 400, 'body': f"Unknown extraction: {extraction}"}

    return {
        'statusCode': 200,
        'body': json.dumps({'extractions': list(results.keys()), 'timestamp': datetime.now(timezone.utc).isoformat()}, default=str)
    }
