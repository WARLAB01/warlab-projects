"""
Test cases and example invocations for People Analytics Dashboard Lambda functions.

This module provides test events and example usage patterns for both Lambda functions.
Can be used for local testing, CI/CD pipelines, or CloudWatch Events scheduling.
"""

import json
from datetime import datetime

# Test events for Dashboard Extractor Lambda
DASHBOARD_EXTRACTOR_TESTS = {
    'kpi_summary': {
        'event': {
            'extraction': 'kpi_summary'
        },
        'description': 'Extract KPI summary metrics (headcount, movements, avg pay, etc.)',
        'expected_output': {
            'statusCode': 200,
            'body': {
                'message': 'kpi_summary extraction completed successfully',
                'extraction_type': 'kpi_summary',
                'timestamp': '2024-01-15T10:30:00.000000',
                's3_location': 's3://warlab-hr-dashboard/data/kpi_summary.json'
            }
        },
        'expected_s3_content': {
            'extraction_type': 'kpi_summary',
            'timestamp': '2024-01-15T10:30:00.000000',
            'metrics': {
                'total_headcount': 5000,
                'total_movements': 1200,
                'avg_base_pay': 85000.50,
                'active_companies': 15,
                'active_departments': 120
            }
        }
    },
    'headcount': {
        'event': {
            'extraction': 'headcount'
        },
        'description': 'Extract headcount data by company, department, location, and trend',
        'expected_output': {
            'statusCode': 200,
            'body': {
                'message': 'headcount extraction completed successfully',
                'extraction_type': 'headcount',
                'timestamp': '2024-01-15T10:30:00.000000',
                's3_location': 's3://warlab-hr-dashboard/data/headcount.json'
            }
        },
        'expected_s3_content': {
            'extraction_type': 'headcount',
            'timestamp': '2024-01-15T10:30:00.000000',
            'by_company': [
                {
                    'company_id': 'COMP001',
                    'company_name': 'Acme Corp',
                    'headcount': 2500
                }
            ],
            'by_department': [
                {
                    'department_id': 'DEPT001',
                    'department_name': 'Engineering',
                    'headcount': 1200
                }
            ],
            'by_location': [
                {
                    'location_id': 'LOC001',
                    'location_name': 'San Francisco',
                    'city': 'San Francisco',
                    'country_code': 'US',
                    'headcount': 1500
                }
            ],
            'trend': [
                {
                    'snapshot_date': '2024-01-01',
                    'headcount': 4900
                },
                {
                    'snapshot_date': '2024-01-15',
                    'headcount': 5000
                }
            ]
        }
    },
    'movements': {
        'event': {
            'extraction': 'movements'
        },
        'description': 'Extract employee movement data (job changes, terminations, trends)',
        'expected_output': {
            'statusCode': 200,
            'body': {
                'message': 'movements extraction completed successfully',
                'extraction_type': 'movements',
                'timestamp': '2024-01-15T10:30:00.000000',
                's3_location': 's3://warlab-hr-dashboard/data/movements.json'
            }
        },
        'expected_s3_content': {
            'extraction_type': 'movements',
            'timestamp': '2024-01-15T10:30:00.000000',
            'by_type': [
                {'movement_type': 'job_change', 'count': 450},
                {'movement_type': 'location_change', 'count': 200},
                {'movement_type': 'compensation_change', 'count': 550}
            ],
            'terminations': {
                'total_terminations': 85,
                'regrettable_terminations': 15,
                'other_terminations': 70
            },
            'trend': [
                {
                    'month': '2023-12-01',
                    'total_movements': 150,
                    'job_changes': 50,
                    'location_changes': 30,
                    'compensation_changes': 70
                }
            ]
        }
    },
    'compensation': {
        'event': {
            'extraction': 'compensation'
        },
        'description': 'Extract compensation data by grade and job family',
        'expected_output': {
            'statusCode': 200,
            'body': {
                'message': 'compensation extraction completed successfully',
                'extraction_type': 'compensation',
                'timestamp': '2024-01-15T10:30:00.000000',
                's3_location': 's3://warlab-hr-dashboard/data/compensation.json'
            }
        },
        'expected_s3_content': {
            'extraction_type': 'compensation',
            'timestamp': '2024-01-15T10:30:00.000000',
            'by_grade': [
                {
                    'grade_id': 'GR01',
                    'grade_code': '1',
                    'grade_name': 'Entry Level',
                    'employee_count': 500,
                    'avg_base_pay': 45000.00,
                    'min_base_pay': 40000.00,
                    'max_base_pay': 50000.00
                }
            ],
            'by_job_family': [
                {
                    'job_family_id': 'ENG',
                    'job_family_name': 'Engineering',
                    'employee_count': 1200,
                    'avg_base_pay': 120000.00,
                    'min_base_pay': 80000.00,
                    'max_base_pay': 180000.00,
                    'median_base_pay': 115000.00
                }
            ]
        }
    },
    'org_health': {
        'event': {
            'extraction': 'org_health'
        },
        'description': 'Extract organizational health metrics (departments, span of control, locations)',
        'expected_output': {
            'statusCode': 200,
            'body': {
                'message': 'org_health extraction completed successfully',
                'extraction_type': 'org_health',
                'timestamp': '2024-01-15T10:30:00.000000',
                's3_location': 's3://warlab-hr-dashboard/data/org_health.json'
            }
        },
        'expected_s3_content': {
            'extraction_type': 'org_health',
            'timestamp': '2024-01-15T10:30:00.000000',
            'departments': [
                {
                    'department_id': 'DEPT001',
                    'department_name': 'Engineering',
                    'department_size': 1200,
                    'manager_count': 25
                }
            ],
            'manager_span_of_control': [
                {
                    'manager_id': 'EMP12345',
                    'direct_reports': 8,
                    'avg_tenure_years': 5
                }
            ],
            'locations': [
                {
                    'location_id': 'LOC001',
                    'location_name': 'San Francisco',
                    'city': 'San Francisco',
                    'country_code': 'US',
                    'headcount': 1500
                }
            ],
            'worker_types': [
                {
                    'worker_type_id': 'FTE',
                    'worker_type_name': 'Full Time Employee',
                    'count': 4500
                }
            ]
        }
    }
}

# Test event for CloudWatch Metrics Publisher Lambda
CLOUDWATCH_PUBLISHER_TEST = {
    'event': {},
    'description': 'Publish KPI metrics from Redshift to CloudWatch',
    'expected_output': {
        'statusCode': 200,
        'body': {
            'message': 'KPI metrics published successfully',
            'timestamp': '2024-01-15T10:30:00.000000',
            'namespace': 'WarLabHRDashboard',
            'metrics_published': [
                'ActiveHeadcount',
                'TotalMovements',
                'AvgBasePay',
                'ActiveCompanies',
                'ActiveDepartments'
            ],
            'metric_values': {
                'ActiveHeadcount': 5000.0,
                'TotalMovements': 1200.0,
                'AvgBasePay': 85000.50,
                'ActiveCompanies': 15.0,
                'ActiveDepartments': 120.0
            }
        }
    }
}

# Error test cases
ERROR_TEST_CASES = {
    'invalid_extraction': {
        'event': {
            'extraction': 'invalid_type'
        },
        'expected_status': 400,
        'expected_error': 'Invalid extraction type'
    },
    'missing_extraction': {
        'event': {},
        'expected_behavior': 'Should default to kpi_summary'
    },
    'redshift_timeout': {
        'scenario': 'Redshift query times out after 300 seconds',
        'expected_status': 504,
        'expected_error': 'Query timeout'
    },
    's3_write_failure': {
        'scenario': 'S3 bucket not accessible',
        'expected_status': 500,
        'expected_error': 'Failed to publish to S3'
    }
}


def print_test_summary():
    """Print a summary of all test cases."""
    print("=" * 80)
    print("PEOPLE ANALYTICS DASHBOARD - LAMBDA FUNCTION TEST CASES")
    print("=" * 80)

    print("\n--- DASHBOARD EXTRACTOR TESTS ---\n")
    for test_name, test_case in DASHBOARD_EXTRACTOR_TESTS.items():
        print(f"Test: {test_name}")
        print(f"  Description: {test_case['description']}")
        print(f"  Event: {json.dumps(test_case['event'])}")
        print(f"  Expected Status: {test_case['expected_output']['statusCode']}")
        print()

    print("\n--- CLOUDWATCH PUBLISHER TEST ---\n")
    print(f"Test: KPI Metrics Publisher")
    print(f"  Description: {CLOUDWATCH_PUBLISHER_TEST['description']}")
    print(f"  Event: {json.dumps(CLOUDWATCH_PUBLISHER_TEST['event'])}")
    print(f"  Expected Status: {CLOUDWATCH_PUBLISHER_TEST['expected_output']['statusCode']}")
    print()

    print("\n--- ERROR TEST CASES ---\n")
    for error_name, error_case in ERROR_TEST_CASES.items():
        print(f"Test: {error_name}")
        if 'scenario' in error_case:
            print(f"  Scenario: {error_case['scenario']}")
        if 'expected_status' in error_case:
            print(f"  Expected Status: {error_case['expected_status']}")
        if 'expected_error' in error_case:
            print(f"  Expected Error: {error_case['expected_error']}")
        print()


def get_test_event(extraction_type: str) -> dict:
    """
    Get test event for a specific extraction type.

    Args:
        extraction_type: Type of extraction (kpi_summary, headcount, etc.)

    Returns:
        Test event dictionary
    """
    return DASHBOARD_EXTRACTOR_TESTS.get(extraction_type, {}).get('event', {})


def get_cloudwatch_test_event() -> dict:
    """Get test event for CloudWatch metrics publisher."""
    return CLOUDWATCH_PUBLISHER_TEST['event']


# CloudWatch Events Schedule Examples
CLOUDWATCH_SCHEDULE_EXAMPLES = {
    'hourly': {
        'Name': 'WarLabHRMetricsPublisher-Hourly',
        'ScheduleExpression': 'rate(1 hour)',
        'Description': 'Publish KPI metrics every hour'
    },
    'daily_midnight': {
        'Name': 'WarLabHRMetricsPublisher-DailyMidnight',
        'ScheduleExpression': 'cron(0 0 * * ? *)',
        'Description': 'Publish KPI metrics at midnight UTC daily'
    },
    'daily_morning': {
        'Name': 'WarLabHRMetricsPublisher-DailyMorning',
        'ScheduleExpression': 'cron(0 8 * * ? *)',
        'Description': 'Publish KPI metrics at 8 AM UTC daily'
    },
    'business_hours': {
        'Name': 'WarLabHRMetricsPublisher-BusinessHours',
        'ScheduleExpression': 'cron(0 9-17 ? * MON-FRI *)',
        'Description': 'Publish KPI metrics every hour during business hours (9 AM - 5 PM UTC)'
    }
}


# AWS CLI command examples
AWS_CLI_EXAMPLES = {
    'invoke_kpi_summary': """
aws lambda invoke \\
    --function-name dashboard-extractor \\
    --payload '{"extraction": "kpi_summary"}' \\
    response.json

cat response.json | jq .
    """,
    'invoke_headcount': """
aws lambda invoke \\
    --function-name dashboard-extractor \\
    --payload '{"extraction": "headcount"}' \\
    response.json

cat response.json | jq .
    """,
    'invoke_cloudwatch_publisher': """
aws lambda invoke \\
    --function-name cloudwatch-publisher \\
    response.json

cat response.json | jq .
    """,
    'tail_logs': """
aws logs tail /aws/lambda/dashboard-extractor --follow
    """,
    'check_metrics': """
aws cloudwatch get-metric-statistics \\
    --namespace WarLabHRDashboard \\
    --metric-name ActiveHeadcount \\
    --start-time 2024-01-15T00:00:00Z \\
    --end-time 2024-01-15T23:59:59Z \\
    --period 3600 \\
    --statistics Average
    """
}


if __name__ == '__main__':
    print_test_summary()

    print("\n--- CLOUDWATCH EVENT SCHEDULES ---\n")
    for schedule_type, schedule in CLOUDWATCH_SCHEDULE_EXAMPLES.items():
        print(f"Schedule: {schedule_type}")
        print(f"  Name: {schedule['Name']}")
        print(f"  Expression: {schedule['ScheduleExpression']}")
        print(f"  Description: {schedule['Description']}")
        print()

    print("\n--- AWS CLI EXAMPLES ---\n")
    for command_name, command in AWS_CLI_EXAMPLES.items():
        print(f"Command: {command_name}")
        print(command.strip())
        print()
