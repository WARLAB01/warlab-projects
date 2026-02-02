# Architecture Overview

## High-Level Data Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Workday   │────▶│     S3      │────▶│    Glue     │────▶│  Redshift   │
│     HCM     │     │   Bucket    │     │   ETL Job   │     │ Serverless  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │                    │                    │
                          ▼                    ▼                    ▼
                    Raw CSV Files       Transformation      Analytics-Ready
                    (Landing Zone)         & Load              Tables
```

## AWS Components

### Storage Layer

**Amazon S3** (`hr-workday-data-{account}`)
- Landing zone for Workday extracts
- Stores raw CSV files organized by date
- Also stores Glue job scripts

```
s3://hr-workday-data-{account}/
├── raw/hr_data/
│   ├── core_hr_employees/
│   ├── job_movement_transactions/
│   ├── compensation_change_transactions/
│   └── worker_movement_transactions/
├── glue_scripts/
└── temp/glue/
```

### Processing Layer

**AWS Glue**
- **Glue Database**: `hr_workday_catalog` - metadata catalog
- **Glue Crawler**: `hr-workday-s3-crawler` - schema discovery
- **Glue ETL Job**: `hr-workday-load-to-redshift` - data loading
- **Glue Trigger**: `hr-workday-daily-load` - scheduled execution

The ETL job uses Redshift COPY commands for optimal bulk loading performance.

### Data Warehouse Layer

**Amazon Redshift Serverless**
- **Workgroup**: `hr-workday-wg`
- **Namespace**: `hr-workday-ns`
- **Database**: `hr_workday_db`
- **Schema**: `hr_workday`

Redshift Serverless automatically scales compute based on query demand.

### Security Layer

**IAM Roles**
- `hr-workday-glue-role`: Permissions for Glue jobs
- `hr-workday-redshift-role`: S3 access for COPY commands

**Encryption**
- S3: Server-side encryption (SSE-S3)
- Redshift: Encryption at rest enabled

## Daily Processing Flow

```
5:00 AM UTC  │  Workday export job runs
             ▼
5:30 AM UTC  │  CSV files land in S3
             ▼
6:00 AM UTC  │  Glue trigger fires
             ▼
6:00 AM UTC  │  ETL job starts
             │  ├─ Truncate target tables
             │  ├─ Execute COPY commands
             │  └─ Validate row counts
             ▼
6:15 AM UTC  │  Job completes (typical)
             ▼
6:30 AM UTC  │  Data available for queries
```

## Network Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         AWS VPC                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   Private Subnets                    │    │
│  │  ┌─────────────┐    ┌─────────────┐                 │    │
│  │  │  Redshift   │    │    Glue     │                 │    │
│  │  │ Serverless  │◀───│  ETL Job    │                 │    │
│  │  └─────────────┘    └──────┬──────┘                 │    │
│  │         │                  │                         │    │
│  │         │     ┌────────────┘                         │    │
│  │         │     │                                      │    │
│  │         ▼     ▼                                      │    │
│  │  ┌─────────────────┐                                 │    │
│  │  │   VPC Endpoint  │ ◀──── S3 Access                │    │
│  │  │      (S3)       │                                 │    │
│  │  └─────────────────┘                                 │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   Public Subnet                      │    │
│  │  ┌─────────────┐                                     │    │
│  │  │  NAT Gateway │ ──── Internet Access               │    │
│  │  └─────────────┘                                     │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Monitoring & Alerting

| Component | Monitoring | Alert Threshold |
|-----------|------------|-----------------|
| Glue Job | CloudWatch Metrics | Job failure |
| Glue Job Duration | CloudWatch | > 60 minutes |
| Redshift Storage | CloudWatch | > 80% capacity |
| Data Quality | Custom SQL checks | Row count variance > 5% |

Alerts are sent via SNS to PagerDuty and email distribution lists.

## Disaster Recovery

| Scenario | Recovery Method | RTO | RPO |
|----------|-----------------|-----|-----|
| Job failure | Re-run job | 30 min | 0 |
| Data corruption | Reload from S3 | 1 hour | 24 hours |
| Redshift failure | Restore from snapshot | 2 hours | 24 hours |
| Full DR | Redeploy pipeline | 4 hours | 24 hours |

## Cost Optimization

- **Redshift Serverless**: Pay only for compute used during queries
- **S3 Lifecycle**: Archive data older than 90 days to Glacier
- **Glue Python Shell**: Uses minimal DPU (0.0625) for simple ETL

---

For detailed architecture documentation, see [ARCHITECTURE.md](../docs/technical/ARCHITECTURE.md).
