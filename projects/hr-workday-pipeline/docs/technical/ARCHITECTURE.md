# Technical Architecture Document
## HR Workday Data Migration & Consolidation

### Document Control

| Version | Date | Author | Status |
|---------|------|--------|--------|
| 1.0 | 2026-02-02 | Data Engineering Team | Approved |

---

## 1. Architecture Overview

### 1.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE LAYER                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐                                                            │
│  │   Workday   │  CSV Exports (Daily)                                       │
│  │     HCM     │────────────────────┐                                       │
│  └─────────────┘                    │                                       │
└─────────────────────────────────────│───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              LANDING LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Amazon S3                                    │   │
│  │  s3://hr-workday-data-{account}/                                    │   │
│  │  ├── raw/hr_data/YYYY/MM/DD/     (Daily partitioned landing)       │   │
│  │  ├── processed/                   (Post-load archive)               │   │
│  │  ├── rejected/                    (Failed records)                  │   │
│  │  └── temp/glue/                   (ETL working space)               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────│───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PROCESSING LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────┐    ┌─────────────────────┐                        │
│  │    AWS Glue         │    │   AWS Glue          │                        │
│  │    Crawlers         │───▶│   Data Catalog      │                        │
│  │  (Schema Discovery) │    │   (Metadata Store)  │                        │
│  └─────────────────────┘    └─────────────────────┘                        │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      AWS Glue ETL Jobs                               │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │   │
│  │  │  Validate   │─▶│  Transform  │─▶│    Load     │                  │   │
│  │  │   (DQ)      │  │  (Business  │  │  (COPY to   │                  │   │
│  │  │             │  │   Rules)    │  │  Redshift)  │                  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────│───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            SERVING LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Amazon Redshift Serverless                        │   │
│  │  Namespace: hr-workday-ns                                           │   │
│  │  Workgroup: hr-workday-wg                                           │   │
│  │  Database:  hr_workday_db                                           │   │
│  │                                                                      │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │                    Schema: hr_workday                        │   │   │
│  │  │  ┌──────────────────┐  ┌──────────────────────────────┐    │   │   │
│  │  │  │ core_hr_employees│  │ job_movement_transactions    │    │   │   │
│  │  │  └──────────────────┘  └──────────────────────────────┘    │   │   │
│  │  │  ┌──────────────────────────────┐  ┌───────────────────┐   │   │   │
│  │  │  │compensation_change_trans     │  │worker_movement_   │   │   │   │
│  │  │  │                              │  │transactions       │   │   │   │
│  │  │  └──────────────────────────────┘  └───────────────────┘   │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────│───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONSUMPTION LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  Redshift   │  │    BI       │  │   Data      │  │   Custom    │        │
│  │  Query      │  │   Tools     │  │   Science   │  │   Apps      │        │
│  │  Editor v2  │  │  (Tableau)  │  │  (Python)   │  │   (API)     │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Component Summary

| Component | AWS Service | Purpose |
|-----------|-------------|---------|
| Data Landing | S3 | Raw file storage, partitioned by date |
| Metadata Catalog | Glue Data Catalog | Schema management, table definitions |
| ETL Orchestration | Glue Jobs | Data transformation and loading |
| Scheduling | Glue Triggers | Automated job execution |
| Data Warehouse | Redshift Serverless | Analytical data storage and queries |
| Access Control | IAM | Authentication and authorization |
| Secrets | Secrets Manager | Credential storage |
| Monitoring | CloudWatch | Logs, metrics, alerts |

---

## 2. Data Flow Architecture

### 2.1 Daily Batch Process Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DAILY BATCH PROCESS                               │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  06:00 UTC                                                               │
│     │                                                                     │
│     ▼                                                                     │
│  ┌─────────────────┐                                                     │
│  │ Glue Trigger    │  Scheduled trigger fires                            │
│  │ (Daily 6 AM)    │                                                     │
│  └────────┬────────┘                                                     │
│           │                                                               │
│           ▼                                                               │
│  ┌─────────────────┐                                                     │
│  │ Start Glue Job  │  Job: hr-workday-load-to-redshift                  │
│  └────────┬────────┘                                                     │
│           │                                                               │
│           ▼                                                               │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    FOR EACH TABLE                                │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │    │
│  │  │ 1. Truncate │─▶│ 2. COPY     │─▶│ 3. Update   │              │    │
│  │  │    Table    │  │    from S3  │  │    Audit    │              │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘              │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│           │                                                               │
│           ▼                                                               │
│  ┌─────────────────┐                                                     │
│  │ Log Results     │  Write to CloudWatch Logs                           │
│  └────────┬────────┘                                                     │
│           │                                                               │
│           ▼                                                               │
│  ┌─────────────────┐                                                     │
│  │ Send Alert      │  SNS notification (success/failure)                 │
│  └─────────────────┘                                                     │
│                                                                           │
│  ~06:30 UTC (typical completion)                                         │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Lineage

```
SOURCE                    TRANSFORMATION                 TARGET
──────                    ──────────────                 ──────

Workday HCM               AWS Glue ETL                   Redshift
┌─────────────┐          ┌─────────────┐                ┌─────────────────┐
│ Worker      │          │ • Validate  │                │ core_hr_        │
│ Business    │─────────▶│ • Type cast │───────────────▶│ employees       │
│ Object      │          │ • Add audit │                │                 │
└─────────────┘          └─────────────┘                └─────────────────┘

┌─────────────┐          ┌─────────────┐                ┌─────────────────┐
│ Job Change  │          │ • Validate  │                │ job_movement_   │
│ Event       │─────────▶│ • Map codes │───────────────▶│ transactions    │
│             │          │ • Add audit │                │                 │
└─────────────┘          └─────────────┘                └─────────────────┘

┌─────────────┐          ┌─────────────┐                ┌─────────────────┐
│ Compensation│          │ • Validate  │                │ compensation_   │
│ Change      │─────────▶│ • Calculate │───────────────▶│ change_         │
│ Event       │          │ • Add audit │                │ transactions    │
└─────────────┘          └─────────────┘                └─────────────────┘

┌─────────────┐          ┌─────────────┐                ┌─────────────────┐
│ Position    │          │ • Validate  │                │ worker_movement_│
│ Movement    │─────────▶│ • Enrich    │───────────────▶│ transactions    │
│ Event       │          │ • Add audit │                │                 │
└─────────────┘          └─────────────┘                └─────────────────┘
```

---

## 3. Infrastructure Architecture

### 3.1 AWS Infrastructure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS Account                                     │
│                              Region: us-east-1                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                            VPC                                      │    │
│  │                      (Default or Custom)                            │    │
│  │  ┌─────────────────────────────────────────────────────────────┐   │    │
│  │  │                     Private Subnets                          │   │    │
│  │  │  ┌─────────────────┐  ┌─────────────────┐                   │   │    │
│  │  │  │ Redshift        │  │ Glue            │                   │   │    │
│  │  │  │ Serverless      │  │ Connection      │                   │   │    │
│  │  │  │ Endpoint        │  │ (if needed)     │                   │   │    │
│  │  │  └─────────────────┘  └─────────────────┘                   │   │    │
│  │  └─────────────────────────────────────────────────────────────┘   │    │
│  │                                                                     │    │
│  │  ┌─────────────────────────────────────────────────────────────┐   │    │
│  │  │                     Public Subnets                           │   │    │
│  │  │  ┌─────────────────┐                                        │   │    │
│  │  │  │ NAT Gateway     │  (for Glue internet access)            │   │    │
│  │  │  └─────────────────┘                                        │   │    │
│  │  └─────────────────────────────────────────────────────────────┘   │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│  │ S3 Bucket       │  │ Secrets Manager │  │ CloudWatch      │            │
│  │ (Regional)      │  │ (Credentials)   │  │ (Logs/Metrics)  │            │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Security Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SECURITY LAYERS                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  IDENTITY & ACCESS                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  IAM Roles                                                           │   │
│  │  ├── hr-workday-glue-role      (Glue job execution)                 │   │
│  │  ├── hr-workday-redshift-role  (Redshift S3 access)                 │   │
│  │  └── hr-workday-crawler-role   (Glue crawler)                       │   │
│  │                                                                      │   │
│  │  IAM Policies                                                        │   │
│  │  ├── S3 read/write (specific bucket)                                │   │
│  │  ├── Glue catalog access                                            │   │
│  │  ├── Redshift data API                                              │   │
│  │  └── CloudWatch logs                                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  NETWORK SECURITY                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Security Groups                                                     │   │
│  │  ├── Redshift SG: Inbound 5439 from authorized IPs/VPC             │   │
│  │  └── Glue SG: Outbound HTTPS to AWS services                        │   │
│  │                                                                      │   │
│  │  VPC Endpoints (Optional)                                           │   │
│  │  ├── S3 Gateway Endpoint                                            │   │
│  │  ├── Glue Interface Endpoint                                        │   │
│  │  └── Secrets Manager Interface Endpoint                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  DATA PROTECTION                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Encryption at Rest                                                  │   │
│  │  ├── S3: SSE-S3 (AES-256)                                           │   │
│  │  └── Redshift: AWS managed keys                                     │   │
│  │                                                                      │   │
│  │  Encryption in Transit                                               │   │
│  │  ├── S3: HTTPS enforced                                             │   │
│  │  ├── Redshift: SSL required                                         │   │
│  │  └── All API calls: TLS 1.2+                                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Database Design

### 4.1 Schema Design Principles

- **Distribution Style**: DISTKEY on `employee_id` for join optimization
- **Sort Keys**: Compound sort on `employee_id`, `effective_date` for range queries
- **Compression**: Automatic encoding for optimal storage
- **Constraints**: Primary keys for data integrity (not enforced, but documented)

### 4.2 Table Relationships

```
                    ┌──────────────────────┐
                    │  core_hr_employees   │
                    │  (Master Data)       │
                    │──────────────────────│
                    │ PK: employee_id      │
                    └──────────┬───────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ job_movement_    │ │ compensation_    │ │ worker_movement_ │
│ transactions     │ │ change_trans     │ │ transactions     │
│──────────────────│ │──────────────────│ │──────────────────│
│ PK: txn_id       │ │ PK: txn_id       │ │ PK: txn_id       │
│ FK: employee_id  │ │ FK: employee_id  │ │ FK: employee_id  │
│ SK: effective_dt │ │ SK: effective_dt │ │ SK: effective_dt │
└──────────────────┘ └──────────────────┘ └──────────────────┘
```

### 4.3 Storage Estimates

| Table | Rows | Avg Row Size | Estimated Size |
|-------|------|--------------|----------------|
| core_hr_employees | 10,000 | 800 bytes | 8 MB |
| job_movement_transactions | 7,000 | 500 bytes | 3.5 MB |
| compensation_change_transactions | 20,000 | 600 bytes | 12 MB |
| worker_movement_transactions | 13,000 | 700 bytes | 9 MB |
| **Total** | **50,000** | | **~35 MB** |

*Note: With Redshift compression, actual storage is typically 3-4x smaller.*

---

## 5. Integration Architecture

### 5.1 Inbound Integrations

| Source | Method | Frequency | Format | Authentication |
|--------|--------|-----------|--------|----------------|
| Workday | S3 file drop | Daily | CSV | IAM role |

### 5.2 Outbound Integrations

| Target | Method | Protocol | Authentication |
|--------|--------|----------|----------------|
| BI Tools | JDBC/ODBC | SQL | IAM/User credentials |
| Redshift Query Editor | Web | HTTPS | IAM |
| Custom Applications | Data API | REST | IAM |
| Data Science | JDBC | SQL | IAM |

---

## 6. Monitoring Architecture

### 6.1 Observability Stack

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MONITORING & ALERTING                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        CloudWatch                                    │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │   │
│  │  │    Logs     │  │   Metrics   │  │   Alarms    │                  │   │
│  │  │ /aws-glue/  │  │ Glue job    │  │ Job failure │                  │   │
│  │  │ /aws-redshift│  │ duration    │  │ Long runtime│                  │   │
│  │  │             │  │ Row counts  │  │ Data quality│                  │   │
│  │  └─────────────┘  └─────────────┘  └──────┬──────┘                  │   │
│  └───────────────────────────────────────────│──────────────────────────┘   │
│                                              │                               │
│                                              ▼                               │
│                                    ┌─────────────────┐                      │
│                                    │   SNS Topic     │                      │
│                                    │ (Notifications) │                      │
│                                    └────────┬────────┘                      │
│                                             │                                │
│                              ┌──────────────┼──────────────┐                │
│                              ▼              ▼              ▼                │
│                        ┌─────────┐    ┌─────────┐    ┌─────────┐          │
│                        │  Email  │    │  Slack  │    │ PagerDuty│          │
│                        └─────────┘    └─────────┘    └─────────┘          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Key Metrics

| Metric | Source | Threshold | Alert |
|--------|--------|-----------|-------|
| Glue job status | Glue | FAILED | Critical |
| Job duration | Glue | > 60 min | Warning |
| Row count variance | Custom | > 5% | Warning |
| Redshift storage | Redshift | > 80% | Warning |
| Query queue time | Redshift | > 30 sec | Warning |

---

## 7. Disaster Recovery

### 7.1 Backup Strategy

| Component | Backup Method | Frequency | Retention |
|-----------|---------------|-----------|-----------|
| S3 data | Versioning | Continuous | 30 days |
| Redshift | Snapshots | Daily | 7 days |
| Glue catalog | AWS managed | Continuous | N/A |
| Glue scripts | S3 + Git | On change | Indefinite |

### 7.2 Recovery Procedures

| Scenario | RTO | RPO | Procedure |
|----------|-----|-----|-----------|
| Glue job failure | 1 hour | 24 hours | Re-run job manually |
| Data corruption | 4 hours | 24 hours | Restore from S3 source |
| Redshift failure | 2 hours | 24 hours | Restore from snapshot |
| Region outage | 24 hours | 24 hours | Redeploy in DR region |

---

## Appendix A: Technology Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Data Warehouse | Redshift Serverless | Cost-effective, auto-scaling, managed |
| ETL Tool | AWS Glue | Native AWS integration, serverless |
| Storage | S3 | Durable, cost-effective, universal access |
| IaC | CloudFormation | Native AWS, no additional tooling |
| Load Method | COPY command | Fastest bulk loading for Redshift |

## Appendix B: Capacity Planning

| Metric | Current | 1 Year | 3 Year |
|--------|---------|--------|--------|
| Employees | 10,000 | 12,000 | 18,000 |
| Daily transactions | 150 | 200 | 350 |
| Storage (compressed) | 10 MB | 15 MB | 40 MB |
| Monthly AWS cost | $100 | $130 | $200 |
