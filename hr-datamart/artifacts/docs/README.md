# HR Datamart - Portable Synthetic Data Platform

## 1. PROJECT OVERVIEW

### Purpose
The HR Datamart is a portable, synthetic data platform that simulates real-world Workday HR system feeds. It provides a complete data warehouse demonstrating enterprise data modeling, transformation, and analytics patterns using AWS cloud services.

### Architecture
```
Data Flow: S3 → Redshift L1 (Staging) → Redshift L3 (Star Schema)

S3 (Inbound CSV Files)
    ↓
[glue_s3_to_l1_job.py]
    ↓
Redshift L1 (Raw Staging Tables)
    ├─ stg_employees
    ├─ stg_jobs
    ├─ stg_organizations
    ├─ stg_compensation
    └─ 8 other staging tables
    ↓
[glue_l1_to_l3_job.py]
    ↓
Redshift L3 (Conformed Data & Analytics Layer)
    ├─ Dimension Tables (Star Schema)
    │  ├─ dim_worker_d
    │  ├─ dim_job_d
    │  ├─ dim_organization_d
    │  └─ dim_day_d
    │
    └─ Fact Tables
       ├─ fct_worker_movement_f
       ├─ fct_worker_compensation_f
       └─ fct_worker_status_f
```

### Technology Stack
- **AWS S3**: Data lake for inbound CSV feeds
- **AWS Redshift**: Data warehouse for staging (L1) and analytics (L3) layers
- **AWS Glue**: Python Shell jobs for ETL orchestration
- **Python 3.9+**: pg8000 library for Redshift connectivity
- **Git**: Version control for SQL and Python scripts

---

## 2. MODELING STANDARDS

### 2.1 Conceptual Data Model (CDM)
Business entities represented in the datamart:

| Entity | Purpose | Source Tables |
|--------|---------|---|
| **Workers** | Employee master with employment history | INT010 (Employee), INT040 (Worker Status) |
| **Jobs** | Job assignments with effective dating | INT020 (Job), INT050 (Job Classification) |
| **Organizations** | Company hierarchy and business units | INT030 (Organization) |
| **Compensation** | Pay, benefits, and financial events | INT060 (Compensation), INT070 (Benefits) |
| **Employment Status** | Worker employment lifecycle states | INT040, INT270 (Termination) |
| **Dates & Time** | Calendar dimension for temporal analysis | Derived from all feeds |

### 2.2 Logical Data Model (LDM)
Star schema optimized for BI tools and analytical queries.

**Dimension Tables (denormalized, slowly changing)**
- `dim_worker_d`: Employee master with employment history
- `dim_job_d`: Job assignment details
- `dim_organization_d`: Company structure
- `dim_day_d`: Calendar dimension (YYYYMMDD keys)
- `dim_worker_status_d`: Employment status tracking
- `dim_job_classification_d`: Job category hierarchy

**Fact Tables (transactions and metrics)**
- `fct_worker_movement_f`: Job changes, transfers, promotions
- `fct_worker_compensation_f`: Salary, bonus, benefit transactions
- `fct_worker_status_f`: Employment status changes

### 2.3 Physical Data Model (PDM)
Redshift-specific implementation optimizing for cloud data warehouse performance.

**Key Principles:**
- All surrogate keys: `BIGINT IDENTITY` (auto-increment)
- Business keys: Retained for traceability
- Distribution style: `DISTSTYLE AUTO` (Redshift decides based on table size)
- Sort keys: Primary join dimensions for query optimization
- Slowly Changing Dimension Type 2 (SCD2): `valid_from`, `valid_to`, `is_current`
- Compression: ENCODE LZ4 for large columns

**Example Table Structure:**
```sql
CREATE TABLE l3_workday.dim_worker_d (
    worker_key BIGINT IDENTITY(1, 1) PRIMARY KEY,
    worker_business_key VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    department_name VARCHAR(255),
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL,
    hash_diff VARCHAR(32),
    etl_load_ts TIMESTAMP NOT NULL,
    DISTSTYLE AUTO
)
SORTKEY (worker_business_key, valid_from);
```

### 2.4 Naming Conventions

| Object Type | Pattern | Example | Notes |
|---|---|---|---|
| Schema | `l1_<source>`, `l3_<domain>` | `l1_workday`, `l3_workday` | L1 = staging, L3 = analytics |
| Staging Table | `stg_<entity>` | `stg_employees` | Raw, 1:1 with source |
| Dimension | `dim_<entity>_d` | `dim_worker_d` | `-d` suffix standard |
| Fact | `fct_<subject>_f` | `fct_worker_movement_f` | `-f` suffix standard |
| Bridge | `brdg_<table1>_<table2>` | `brdg_worker_job` | Many-to-many relationships |
| View | `<table>_vw` | `dim_worker_current_vw` | Views filtered for current records |
| Column | `snake_case` | `first_name`, `hire_date` | All lowercase |
| Surrogate Key | `<entity>_key` | `worker_key` | IDENTITY, BIGINT |
| Business Key | `<entity>_business_key` | `worker_business_key` | Source natural key |
| Date Key | `<date_concept>_key` | `hire_date_key` | YYYYMMDD integer format |
| Flag | `is_<attribute>` | `is_current`, `is_active` | Boolean columns |
| Timestamp | `<action>_ts` | `etl_load_ts`, `create_ts` | UTC, TIMESTAMP WITH TIMEZONE |

**Restrictions:**
- All column names lowercase, no spaces
- Avoid Redshift reserved words (see [Reserved Words](https://docs.aws.amazon.com/redshift/latest/dg/keywords.html))
- No special characters except underscore
- Prefix data quality columns with `dq_` for lineage clarity

### 2.5 Star vs Snowflake Schema Rationale

**Star Schema Adopted (Denormalized)**
- ✅ Single join from facts to dimensions
- ✅ Simpler, faster BI tool queries
- ✅ Better Redshift query performance
- ✅ Reduced query complexity
- ✅ Dimension tables <100GB each

**Snowflake Avoided (Normalized)**
- ❌ Multiple joins required
- ❌ Complex query logic for BI tools
- ❌ Slower performance in cloud DWH
- ❌ Not justified for feature count

---

## 3. KEYING STRATEGY

### 3.1 Surrogate Keys
All dimension tables use Redshift `BIGINT IDENTITY` for surrogate keys.

```sql
-- Surrogate key pattern
worker_key BIGINT IDENTITY(1, 1) PRIMARY KEY  -- Auto-incrementing from 1
```

**Advantages:**
- Stable across source system changes
- Optimal join performance
- Supports dimension versioning (SCD2)
- Protects against PII exposure in foreign keys

### 3.2 Business Keys
Natural keys retained from source systems for traceability and audit.

```sql
-- Business key pattern
worker_business_key VARCHAR(50) NOT NULL  -- Source employee_id
organization_business_key VARCHAR(50) NOT NULL  -- Source company_id
```

**Constraints:**
- NOT NULL on all dimensions
- Unique per version (with valid_from date)
- Used in source-to-target reconciliation

### 3.3 Date Keys
Calendar dimensions use integer YYYYMMDD format for efficient joins and partitioning.

```sql
-- dim_day_d example
CREATE TABLE l3_workday.dim_day_d (
    day_key INT NOT NULL PRIMARY KEY,  -- 20231225 format
    calendar_date DATE NOT NULL,
    year_of_day INT,
    month_of_year INT,
    day_of_month INT,
    day_of_week_name VARCHAR(10),
    week_of_year INT,
    is_weekend BOOLEAN
);
```

**Pattern:** `CAST(REPLACE(calendar_date, '-', '') AS INT)` → 20231225

### 3.4 Slowly Changing Dimension Type 2 (SCD2)
Tracks historical changes to dimension attributes while maintaining current records.

```sql
-- SCD2 pattern
valid_from DATE NOT NULL,      -- Record effective date
valid_to DATE,                 -- Record expiration date (NULL = current)
is_current BOOLEAN NOT NULL    -- True if valid_to IS NULL
```

**Key Rules:**
1. New record inserted when business key attribute changes
2. Previous record's `valid_to` = new record's `valid_from` - 1 day
3. `is_current` flag = 1 only for active record
4. `valid_to` IS NULL for current records

**Example Workflow:**
```
Event: Employee changes department from Finance → HR on 2024-01-15

Before:
worker_key=1, worker_business_key=E001, department=Finance,
valid_from=2024-01-01, valid_to=NULL, is_current=1

After Insert:
worker_key=1, worker_business_key=E001, department=Finance,
valid_from=2024-01-01, valid_to=2024-01-14, is_current=0

After Insert:
worker_key=2, worker_business_key=E001, department=HR,
valid_from=2024-01-15, valid_to=NULL, is_current=1
```

### 3.5 Hash Diff for Change Detection
MD5 hash of dimension attributes used to detect changes.

```sql
-- Hash diff pattern
hash_diff VARCHAR(32) NOT NULL  -- MD5 of concatenated attributes

-- Calculation example
MD5(CONCAT(first_name, '|', last_name, '|', email, '|', department))
```

**Use Cases:**
- Avoid inserting duplicate records
- Identify which dimensions changed
- Performance optimization (hash compare faster than column-by-column)

---

## 4. DATA FLOW

### 4.1 Layer Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     S3 Data Lake                            │
│              (Inbound CSV Files - INT feeds)                │
└────────────────────┬────────────────────────────────────────┘
                     │ CSV files from Workday export
                     │
        ┌────────────▼───────────────┐
        │  glue_s3_to_l1_job.py       │
        │  COPY from S3 to Redshift   │
        └────────────┬────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│            L1 Schema: l1_workday (Staging)                  │
│          Raw tables, 1:1 with source feeds                  │
│  (stg_employees, stg_jobs, stg_organizations, etc.)         │
└────────────────────┬────────────────────────────────────────┘
                     │ Raw data, minimal transformations
                     │
        ┌────────────▼──────────────────────┐
        │  glue_l1_to_l3_job.py             │
        │  1. Load L3 source tables          │
        │  2. Load dimensions (SCD2)         │
        │  3. Load fact tables               │
        └────────────┬──────────────────────┘
                     │
┌────────────────────▼──────────────────────────────────────┐
│     L3 Schema: l3_workday (Analytics / Star Schema)        │
│                                                             │
│  Dimensions:          Facts:                              │
│  ├─ dim_worker_d      ├─ fct_worker_movement_f           │
│  ├─ dim_job_d         ├─ fct_worker_compensation_f       │
│  ├─ dim_organization_d └─ fct_worker_status_f            │
│  └─ dim_day_d                                             │
└────────────────────┬──────────────────────────────────────┘
                     │ Conformed dimensions & facts
                     │
        ┌────────────▼─────────────────┐
        │   BI Tools / Analytics       │
        │   (Tableau, QuickSight, etc) │
        └──────────────────────────────┘
```

### 4.2 Feed-to-Table Mapping

| INT Code | Feed Name | Source File | L1 Staging Table | L3 Usage |
|---|---|---|---|---|
| INT010 | Employee Master | `int_010_employee.csv` | `stg_employees` | dim_worker_d, fct_worker_movement_f |
| INT020 | Job Assignment | `int_020_job.csv` | `stg_jobs` | dim_job_d, fct_worker_movement_f |
| INT030 | Organization | `int_030_organization.csv` | `stg_organizations` | dim_organization_d, dim_worker_d |
| INT040 | Worker Status | `int_040_worker_status.csv` | `stg_worker_status` | dim_worker_status_d, fct_worker_status_f |
| INT050 | Job Classification | `int_050_job_classification.csv` | `stg_job_classification` | dim_job_classification_d |
| INT060 | Compensation | `int_060_compensation.csv` | `stg_compensation` | fct_worker_compensation_f |
| INT070 | Benefits | `int_070_benefits.csv` | `stg_benefits` | fct_worker_compensation_f |
| INT080 | Salary History | `int_080_salary_history.csv` | `stg_salary_history` | fct_worker_compensation_f |
| INT090 | Hire Events | `int_090_hire_events.csv` | `stg_hire_events` | fct_worker_movement_f |
| INT100 | Transfer Events | `int_100_transfer_events.csv` | `stg_transfer_events` | fct_worker_movement_f |
| INT110 | Promotion Events | `int_110_promotion_events.csv` | `stg_promotion_events` | fct_worker_movement_f |
| INT270 | Termination / Rescind | `int_270_termination.csv` | `stg_termination` | dim_worker_d, fct_worker_status_f |

### 4.3 Dependency Graph
```
INT010 (Employee)
  ├─→ dim_worker_d
  ├─→ fct_worker_movement_f
  └─→ dim_worker_status_d

INT020 (Job)
  ├─→ dim_job_d
  └─→ fct_worker_movement_f

INT030 (Organization)
  ├─→ dim_organization_d
  └─→ dim_worker_d (denormalized)

INT040 (Worker Status)
  ├─→ dim_worker_status_d
  └─→ fct_worker_status_f

INT050 (Job Classification)
  └─→ dim_job_classification_d

INT060-080 (Compensation)
  └─→ fct_worker_compensation_f

INT090-110 (Movement Events)
  └─→ fct_worker_movement_f

INT270 (Termination/Rescind)
  ├─→ dim_worker_d
  └─→ fct_worker_status_f
```

---

## 5. FEED INVENTORY

### 5.1 Complete Feed Listing
All 12 feeds with metadata:

| INT | Feed Name | CSV File | Approx Rows | Cadence | PII | Refresh Dependencies |
|---|---|---|---|---|---|---|
| 010 | Employee Master | int_010_employee.csv | 2,500 | Daily | ✓ | - |
| 020 | Job Assignment | int_020_job.csv | 3,200 | Daily | - | INT010 (employees) |
| 030 | Organization | int_030_organization.csv | 150 | Weekly | - | - |
| 040 | Worker Status | int_040_worker_status.csv | 12,000 | Daily | ✓ | INT010 (employees) |
| 050 | Job Classification | int_050_job_classification.csv | 85 | Monthly | - | - |
| 060 | Compensation | int_060_compensation.csv | 2,800 | Monthly | ✓ | INT010 (employees) |
| 070 | Benefits | int_070_benefits.csv | 5,200 | Monthly | ✓ | INT010 (employees) |
| 080 | Salary History | int_080_salary_history.csv | 8,500 | Monthly | ✓ | INT010 (employees) |
| 090 | Hire Events | int_090_hire_events.csv | 180 | Daily | ✓ | INT010 (employees) |
| 100 | Transfer Events | int_100_transfer_events.csv | 320 | Daily | - | INT010, INT020, INT030 |
| 110 | Promotion Events | int_110_promotion_events.csv | 240 | Daily | - | INT010, INT020, INT050 |
| 270 | Termination/Rescind | int_270_termination.csv | 95 | Daily | ✓ | INT010 (employees) |

**Load Order:** Must respect dependencies
1. INT010, INT030, INT050 (independent)
2. INT020, INT040, INT060, INT070, INT080, INT090 (depend on INT010)
3. INT100 (depends on INT010, INT020, INT030)
4. INT110 (depends on INT010, INT020, INT050)
5. INT270 (depends on INT010)

---

## 6. SCHEMA INVENTORY

### 6.1 L1 (Staging) Schema: `l1_workday`

**Staging Tables:**
```
stg_employees (2,500 rows)
├─ Columns: employee_id, first_name, last_name, email, hire_date, ...
├─ Primary Key: employee_id
└─ Load: COPY from s3://bucket/workday/int_010/

stg_jobs (3,200 rows)
├─ Columns: job_id, employee_id, job_title, department, start_date, end_date, ...
├─ Primary Key: job_id
└─ Load: COPY from s3://bucket/workday/int_020/

stg_organizations (150 rows)
├─ Columns: org_id, org_name, parent_org_id, org_level, ...
├─ Primary Key: org_id
└─ Load: COPY from s3://bucket/workday/int_030/

stg_worker_status (12,000 rows)
├─ Columns: status_id, employee_id, status_code, effective_date, ...
├─ Primary Key: status_id
└─ Load: COPY from s3://bucket/workday/int_040/

stg_job_classification (85 rows)
├─ Columns: job_class_id, job_class_code, job_class_title, ...
├─ Primary Key: job_class_id
└─ Load: COPY from s3://bucket/workday/int_050/

stg_compensation (2,800 rows)
├─ Columns: comp_id, employee_id, comp_type, amount, effective_date, ...
├─ Primary Key: comp_id
└─ Load: COPY from s3://bucket/workday/int_060/

stg_benefits (5,200 rows)
├─ Columns: benefit_id, employee_id, benefit_type, enroll_date, ...
├─ Primary Key: benefit_id
└─ Load: COPY from s3://bucket/workday/int_070/

stg_salary_history (8,500 rows)
├─ Columns: salary_id, employee_id, salary_amount, effective_date, ...
├─ Primary Key: salary_id
└─ Load: COPY from s3://bucket/workday/int_080/

stg_hire_events (180 rows)
├─ Columns: hire_event_id, employee_id, hire_date, hire_type, ...
├─ Primary Key: hire_event_id
└─ Load: COPY from s3://bucket/workday/int_090/

stg_transfer_events (320 rows)
├─ Columns: transfer_id, employee_id, from_org_id, to_org_id, transfer_date, ...
├─ Primary Key: transfer_id
└─ Load: COPY from s3://bucket/workday/int_100/

stg_promotion_events (240 rows)
├─ Columns: promo_id, employee_id, from_job_id, to_job_id, promo_date, ...
├─ Primary Key: promo_id
└─ Load: COPY from s3://bucket/workday/int_110/

stg_termination (95 rows)
├─ Columns: term_id, employee_id, termination_date, rehire_date, ...
├─ Primary Key: term_id
└─ Load: COPY from s3://bucket/workday/int_270/
```

### 6.2 L3 Source Tables: `l3_workday._src`

Source tables act as intermediate transformation layer, staging data before dimension/fact loads.

```
_src_employees
├─ Denormalizes stg_employees with organization details
└─ Used by: dim_worker_d

_src_jobs
├─ Denormalizes stg_jobs with job classification
└─ Used by: dim_job_d

_src_worker_status
├─ Enriches stg_worker_status with effective dating
└─ Used by: dim_worker_status_d, fct_worker_status_f
```

### 6.3 L3 Dimension Tables: `l3_workday`

```
dim_worker_d (2,500 unique keys)
├─ Attributes: worker_key, worker_business_key, first_name, last_name, email,
│              hire_date, department_name, manager_name, organization_name,
│              job_title, employment_status, ...
├─ Keys: PK=worker_key, BK=worker_business_key
├─ SCD2: valid_from, valid_to, is_current
├─ Distribution: DISTSTYLE AUTO
└─ Sort Key: worker_business_key, valid_from

dim_job_d (400 unique keys)
├─ Attributes: job_key, job_business_key, job_title, job_classification,
│              department, organizational_level, salary_range, ...
├─ Keys: PK=job_key, BK=job_business_key
├─ SCD2: valid_from, valid_to, is_current
├─ Distribution: DISTSTYLE AUTO
└─ Sort Key: job_business_key, valid_from

dim_organization_d (150 unique keys)
├─ Attributes: organization_key, organization_business_key, organization_name,
│              parent_organization_key, organizational_level, region, ...
├─ Keys: PK=organization_key, BK=organization_business_key
├─ SCD2: valid_from, valid_to, is_current
├─ Distribution: DISTSTYLE AUTO
└─ Sort Key: organization_business_key

dim_worker_status_d (50+ unique keys)
├─ Attributes: status_key, status_business_key, status_code, status_name,
│              status_category, ...
├─ Keys: PK=status_key, BK=status_business_key
├─ Distribution: DISTSTYLE AUTO
└─ Sort Key: status_code

dim_job_classification_d (85 unique keys)
├─ Attributes: job_class_key, job_class_business_key, job_class_code,
│              job_class_title, job_family, ...
├─ Keys: PK=job_class_key, BK=job_class_business_key
├─ Distribution: DISTSTYLE AUTO
└─ Sort Key: job_class_code

dim_day_d (365+ days)
├─ Attributes: day_key (YYYYMMDD), calendar_date, year, month, day,
│              day_of_week, week_number, is_weekend, is_holiday, ...
├─ Keys: PK=day_key
├─ Distribution: ALL (small, frequently joined)
└─ Sort Key: calendar_date
```

### 6.4 L3 Fact Tables: `l3_workday`

```
fct_worker_movement_f (2,500+ rows)
├─ Grain: One row per worker movement event (hire, transfer, promotion)
├─ Dimensions:
│   ├─ worker_key FK → dim_worker_d
│   ├─ from_job_key FK → dim_job_d
│   ├─ to_job_key FK → dim_job_d
│   ├─ from_organization_key FK → dim_organization_d
│   ├─ to_organization_key FK → dim_organization_d
│   └─ effective_date_key FK → dim_day_d
├─ Measures: (none - events are facts)
├─ Attributes: movement_type, movement_reason
├─ Distribution: DISTSTYLE AUTO
└─ Sort Key: worker_key, effective_date_key

fct_worker_compensation_f (15,000+ rows)
├─ Grain: One row per compensation event (salary change, bonus, benefit)
├─ Dimensions:
│   ├─ worker_key FK → dim_worker_d
│   ├─ job_key FK → dim_job_d
│   ├─ effective_date_key FK → dim_day_d
│   └─ compensation_type_key FK → dim_compensation_type_d (not yet defined)
├─ Measures: amount, percentage
├─ Distribution: DISTSTYLE AUTO
└─ Sort Key: worker_key, effective_date_key

fct_worker_status_f (12,000+ rows)
├─ Grain: One row per worker status change
├─ Dimensions:
│   ├─ worker_key FK → dim_worker_d
│   ├─ status_key FK → dim_worker_status_d
│   ├─ effective_date_key FK → dim_day_d
│   └─ organization_key FK → dim_organization_d
├─ Measures: (none - events are facts)
├─ Attributes: status_code, status_name
├─ Distribution: DISTSTYLE AUTO
└─ Sort Key: worker_key, effective_date_key
```

---

## 7. IDP FIELD SPECIFICATIONS

IDP (Intermediate Data Platform) fields are calculated columns supporting advanced transformations.

### 7.1 idp_calc_end_date
**Purpose:** Determine effective end date for current records using window function.

**Calculation:**
```sql
LEAD(effective_date) OVER (
    PARTITION BY employee_id, attribute_type
    ORDER BY effective_date
) - INTERVAL 1 DAY AS idp_calc_end_date
```

**Logic:**
- For each employee attribute sequence, calculate when the next change occurs
- Subtract 1 day to get end date (day before change)
- NULL if current (no next change)

**Example:**
```
employee_id | effective_date | idp_calc_end_date
100         | 2024-01-01     | 2024-03-14
100         | 2024-03-15     | 2024-06-30
100         | 2024-07-01     | NULL (current)
```

### 7.2 idp_obsolete_date
**Purpose:** Identify termination/rescind dates from INT270 feed.

**Calculation:**
```sql
LEFT JOIN stg_termination t
    ON s.employee_id = t.employee_id
SELECT COALESCE(t.termination_date, s.idp_calc_end_date) AS idp_obsolete_date
```

**Logic:**
- Join to INT270 (termination records)
- Termination date overrides calculated end date
- If rescinded (rehire_date > termination_date), treat as rescind
- NULL if employee still active

### 7.3 idp_max_entry_ts
**Purpose:** Identify the maximum timestamp entry for each grain.

**Calculation:**
```sql
MAX(record_timestamp) OVER (
    PARTITION BY employee_id, job_id, effective_date
) AS idp_max_entry_ts
```

**Logic:**
- Handle multiple records per day (intra-day updates)
- Use latest version if multiple entries
- Prevents duplicate FK resolution

### 7.4 idp_min_seq_num
**Purpose:** Get minimum sequence number at the maximum timestamp (resolution tiebreaker).

**Calculation:**
```sql
MIN(sequence_number) OVER (
    PARTITION BY employee_id, job_id, effective_date
    WINDOW employee_id ORDER BY record_timestamp DESC, sequence_number ASC
) FILTER (record_timestamp = idp_max_entry_ts) AS idp_min_seq_num
```

**Logic:**
- When multiple records at max timestamp, select lowest sequence number
- Deterministic tiebreaker for FK resolution
- Supports intra-day change ordering

### 7.5 idp_employee_status
**Purpose:** Derive employment status (Active, Inactive, Terminated, On Leave).

**Calculation:**
```sql
CASE
    WHEN t.employee_id IS NOT NULL
         AND t.termination_date <= current_date
         AND (t.rehire_date IS NULL OR t.rehire_date > current_date)
    THEN 'Terminated'

    WHEN ws.status_code IN ('LEAVE_OF_ABSENCE', 'UNPAID_LEAVE')
         AND ws.effective_date <= current_date
         AND COALESCE(ws.idp_calc_end_date, DATE '9999-12-31') >= current_date
    THEN 'On Leave'

    WHEN ws.status_code = 'ACTIVE'
         AND ws.effective_date <= current_date
         AND COALESCE(ws.idp_calc_end_date, DATE '9999-12-31') >= current_date
    THEN 'Active'

    ELSE 'Inactive'
END AS idp_employee_status
```

**Status Hierarchy:**
1. Terminated: termination_date in past, no rehire
2. On Leave: leave status active
3. Active: active status current
4. Inactive: all other cases

---

## 8. ALGORITHM REFERENCE

### 8.1 dim_worker_job_d Union/As-Of Algorithm

**Objective:** Create unified worker-job dimension from separate tables.

**Algorithm:**
```
1. Create union of INT010 (employee) + INT020 (job) attributes
2. For each employee-job pair:
   a. Find overlapping dates between employee and job records
   b. Create one record per unique date combination
   c. Use as-of join logic (effective date <= current date < next date)
3. Apply SCD2 logic:
   a. Hash previous & current record attributes
   b. If hash differs, insert new record with:
      - Previous: valid_to = today - 1, is_current = FALSE
      - Current: valid_from = today, is_current = TRUE
4. Handle gaps/overlaps:
   a. If gap between job records: insert "no job" record
   b. If overlapping: use MAX(job_key) to select primary
```

**SQL Pattern:**
```sql
SELECT
    e.employee_id,
    j.job_id,
    e.first_name,
    j.job_title,
    GREATEST(e.effective_date, j.start_date) AS effective_from,
    LEAST(COALESCE(e.valid_to, '9999-12-31'),
          COALESCE(j.end_date, '9999-12-31')) AS effective_to
FROM stg_employees e
LEFT JOIN stg_jobs j
    ON e.employee_id = j.employee_id
    AND j.start_date <= COALESCE(e.valid_to, '9999-12-31')
    AND e.effective_date <= COALESCE(j.end_date, '9999-12-31')
```

### 8.2 dim_worker_status_d Derivation

**Objective:** Create normalized worker status dimension from raw status events.

**Algorithm:**
```
1. Load stg_worker_status (INT040) - all status records
2. Denormalize with status code lookup (if reference table exists)
3. Calculate idp_calc_end_date (LEAD over employee, order by date)
4. Classify status category:
   - ACTIVE → 'Active'
   - LEAVE_OF_ABSENCE, UNPAID_LEAVE → 'On Leave'
   - TERM, TERMINATED → 'Terminated' (if no rehire)
   - Other → 'Inactive'
5. Apply SCD2 type 1 (non-historic, current only):
   - Only keep current status per employee
   - valid_to IS NULL, is_current = TRUE
6. Deduplicate by status_code (distinct status types)
```

### 8.3 fct_worker_movement_f FK Resolution

**Objective:** Create movement fact table with proper FK references to dimensions.

**Algorithm:**
```
1. Union all movement event types:
   a. INT090 (Hire Events) → movement_type = 'Hire'
   b. INT100 (Transfer Events) → movement_type = 'Transfer'
   c. INT110 (Promotion Events) → movement_type = 'Promotion'

2. For each movement event:
   a. Lookup worker_key: JOIN to dim_worker_d on business_key, effective_date
   b. Lookup from_job_key: JOIN to dim_job_d on business_key, date <= effective_date
   c. Lookup to_job_key: JOIN to dim_job_d on business_key, effective_date >= date
   d. Lookup from_org_key: JOIN to dim_organization_d on business_key
   e. Lookup to_org_key: JOIN to dim_organization_d on business_key

3. Handle FK resolution logic:
   - If multiple versions of dimension record on event date:
     * Use idp_max_entry_ts to get latest
     * Use idp_min_seq_num as tiebreaker
   - If no matching dimension record:
     * Log error, reject record
     * Investigate source data quality

4. Grain validation:
   - One row per unique (worker, event_type, event_date, event_id)
   - No duplicates after FK resolution
```

### 8.4 Rescind Handling

**Objective:** Properly handle termination/rescind cycles where employee terminated and rehired.

**Algorithm:**
```
1. Load INT270 (Termination/Rescind events)

2. For each termination record:
   a. If rehire_date IS NULL:
      - Employee permanently terminated
      - idp_obsolete_date = termination_date
      - Status = 'Terminated'

   b. If rehire_date IS NOT NULL:
      - Employee rescinded (rehired)
      - If rehire_date > termination_date:
         * Insert gap records for period between termination and rehire
         * Create new worker_key version with valid_from = rehire_date
      - If rehire_date <= termination_date:
         * Data quality issue, log warning
         * Treat as rescind (no gap)

3. Update dimension:
   - Set valid_to = termination_date - 1 for previous version
   - Set is_current = FALSE
   - Insert new record with valid_from = termination_date for gap period
   - Insert new record with valid_from = rehire_date for rehired employee

4. Validation:
   - No gaps in SCD2 timeline (except between term and rehire)
   - valid_from <= valid_to + 1 day (continuous chain)
   - is_current = TRUE only for active status
```

---

## 9. ENVIRONMENT SETUP

### 9.1 Prerequisites
- AWS Account with Redshift cluster running
- AWS Glue service enabled
- S3 bucket for data lake
- IAM role with appropriate permissions
- Python 3.9+ with boto3, pg8000

### 9.2 Required Environment Variables

```bash
# Redshift Configuration
export REDSHIFT_HOST="redshift-cluster-1.c9akciq32.us-east-1.redshift.amazonaws.com"
export REDSHIFT_PORT="5439"
export REDSHIFT_DATABASE="dev"
export REDSHIFT_SCHEMA_L1="l1_workday"
export REDSHIFT_SCHEMA_L3="l3_workday"

# S3 Configuration
export S3_BUCKET="hr-datamart-lake"
export S3_PREFIX_INBOUND="workday/inbound"
export S3_PREFIX_SCRIPTS="workday/scripts"

# AWS Glue Configuration
export REDSHIFT_IAM_ROLE="arn:aws:iam::ACCOUNT:role/glue-redshift-role"

# Data Parameters
export DATA_DATE="2024-01-31"
export ETL_BATCH_ID="batch_001"
```

### 9.3 S3 Bucket Structure
```
s3://hr-datamart-lake/
├── workday/
│   ├── inbound/
│   │   ├── int_010/
│   │   │   ├── int_010_employee_20240131.csv
│   │   │   └── int_010_employee_20240201.csv
│   │   ├── int_020/
│   │   │   └── int_020_job_20240131.csv
│   │   └── ... (10 more feed directories)
│   ├── scripts/
│   │   ├── glue_s3_to_l1_job.py
│   │   ├── glue_l1_to_l3_job.py
│   │   └── sql/
│   │       ├── l3_source_loads.sql
│   │       ├── l3_dimension_loads.sql
│   │       └── l3_fact_loads.sql
│   ├── archive/
│   │   └── (processed files)
│   └── qa/
│       └── (QA report files)
```

### 9.4 IAM Role Requirements

**Glue to Redshift Role:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::hr-datamart-lake/*",
        "arn:aws:s3:::hr-datamart-lake"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "redshift:DescribeClusters",
        "redshift-data:ExecuteStatement",
        "redshift-data:GetStatementResult"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::hr-datamart-lake/qa/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

**Redshift User Permissions:**
```sql
-- Create Redshift user for Glue
CREATE USER glue_user PASSWORD 'password';

-- Grant schema creation
GRANT CREATE ON DATABASE dev TO glue_user;

-- Grant schema permissions
GRANT USAGE ON SCHEMA l1_workday TO glue_user;
GRANT CREATE ON SCHEMA l1_workday TO glue_user;
GRANT USAGE ON SCHEMA l3_workday TO glue_user;
GRANT CREATE ON SCHEMA l3_workday TO glue_user;

-- Grant table permissions (for all current and future tables)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA l1_workday TO glue_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA l3_workday TO glue_user;
```

---

## 10. EXECUTION GUIDE

### 10.1 Quick Start (3 Steps)

**Step 1: Prepare S3 Data**
```bash
# Upload CSV files to S3
aws s3 sync ./data/workday/ s3://hr-datamart-lake/workday/inbound/
```

**Step 2: Run L1 Load Job**
```bash
aws glue start-job-run \
  --job-name glue_s3_to_l1_job \
  --arguments '{
    "--s3_bucket": "hr-datamart-lake",
    "--s3_prefix": "workday/inbound",
    "--redshift_host": "redshift-cluster-1.c9akciq32.us-east-1.redshift.amazonaws.com",
    "--redshift_db": "dev",
    "--redshift_schema": "l1_workday",
    "--redshift_iam_role": "arn:aws:iam::ACCOUNT:role/glue-redshift-role",
    "--data_date": "2024-01-31",
    "--etl_batch_id": "batch_001"
  }'
```

**Step 3: Run L3 Load Job**
```bash
aws glue start-job-run \
  --job-name glue_l1_to_l3_job \
  --arguments '{
    "--s3_bucket": "hr-datamart-lake",
    "--s3_prefix": "workday/scripts",
    "--redshift_host": "redshift-cluster-1.c9akciq32.us-east-1.redshift.amazonaws.com",
    "--redshift_db": "dev",
    "--redshift_schema": "l3_workday",
    "--redshift_iam_role": "arn:aws:iam::ACCOUNT:role/glue-redshift-role",
    "--data_date": "2024-01-31",
    "--etl_batch_id": "batch_001"
  }'
```

### 10.2 Full Execution Runbook

**Phase 1: Data Validation**
```bash
# Check S3 inbound directory
aws s3 ls s3://hr-datamart-lake/workday/inbound/ --recursive --human-readable --summarize

# Verify all 12 feed files present
# Expected: int_010_employee.csv, int_020_job.csv, ..., int_270_termination.csv
```

**Phase 2: L1 Staging Load**
```bash
# Run L1 Glue job with full error handling
aws glue start-job-run \
  --job-name glue_s3_to_l1_job \
  --arguments {
    --s3_bucket hr-datamart-lake,
    --s3_prefix workday/inbound,
    --redshift_host $REDSHIFT_HOST,
    --redshift_port 5439,
    --redshift_db dev,
    --redshift_schema l1_workday,
    --redshift_iam_role $REDSHIFT_IAM_ROLE,
    --data_date 2024-01-31,
    --etl_batch_id batch_001
  }

# Monitor job execution
aws glue get-job-runs --job-name glue_s3_to_l1_job --query 'JobRuns[0].[Id,State,StartedOn,CompletedOn]'

# Query row counts in L1 after load
psql -h $REDSHIFT_HOST -U glue_user -d dev << EOF
SELECT table_name, row_count FROM svv_table_info WHERE schema_name = 'l1_workday' ORDER BY table_name;
EOF
```

**Phase 3: L3 Transformation Load**
```bash
# Run L3 Glue job
aws glue start-job-run \
  --job-name glue_l1_to_l3_job \
  --arguments {
    --s3_bucket hr-datamart-lake,
    --s3_prefix workday/scripts,
    --redshift_host $REDSHIFT_HOST,
    --redshift_port 5439,
    --redshift_db dev,
    --redshift_schema l3_workday,
    --redshift_iam_role $REDSHIFT_IAM_ROLE,
    --data_date 2024-01-31,
    --etl_batch_id batch_001
  }

# Monitor execution
aws glue get-job-runs --job-name glue_l1_to_l3_job --query 'JobRuns[0].[Id,State,StartedOn,CompletedOn]'
```

**Phase 4: Data Validation**
```bash
# Query L3 dimensions
psql -h $REDSHIFT_HOST -U glue_user -d dev << EOF
SELECT 'dim_worker_d' as table_name, COUNT(*) as row_count FROM l3_workday.dim_worker_d
UNION ALL SELECT 'dim_job_d', COUNT(*) FROM l3_workday.dim_job_d
UNION ALL SELECT 'dim_organization_d', COUNT(*) FROM l3_workday.dim_organization_d
UNION ALL SELECT 'fct_worker_movement_f', COUNT(*) FROM l3_workday.fct_worker_movement_f
UNION ALL SELECT 'fct_worker_compensation_f', COUNT(*) FROM l3_workday.fct_worker_compensation_f
UNION ALL SELECT 'fct_worker_status_f', COUNT(*) FROM l3_workday.fct_worker_status_f
ORDER BY table_name;
EOF
```

### 10.3 Dry-Run Mode

Run Glue jobs without modifying database (logs only):

```bash
# Modify glue_s3_to_l1_job.py to add DRY_RUN check
export DRY_RUN=true

aws glue start-job-run \
  --job-name glue_s3_to_l1_job \
  --arguments '{
    "--s3_bucket": "hr-datamart-lake",
    "--dry_run": "true",
    ... other arguments
  }'
```

### 10.4 Restatement Process

Complete re-run of all data (purge & reload):

```bash
# Step 1: Drop existing tables in L1 and L3
psql -h $REDSHIFT_HOST -U admin -d dev << EOF
DROP SCHEMA IF EXISTS l1_workday CASCADE;
DROP SCHEMA IF EXISTS l3_workday CASCADE;
EOF

# Step 2: Run L1 Glue job normally (creates fresh L1 schema/tables)
aws glue start-job-run --job-name glue_s3_to_l1_job --arguments {...}

# Step 3: Run L3 Glue job (creates fresh L3 schema/tables with SCD2 v1 records)
aws glue start-job-run --job-name glue_l1_to_l3_job --arguments {...}
```

---

## 11. QA & TESTING

### 11.1 Test Categories

**Data Quality Tests:**
- Null validation: Required fields present
- Referential integrity: Foreign keys exist in dimensions
- Uniqueness: No duplicate surrogate keys
- Grain validation: Fact tables match expected grain
- Cardinality checks: Reasonable row counts per table

**Transformation Tests:**
- Completeness: All source records accounted for
- Accuracy: Calculated fields correct (hash_diff, derived status)
- Consistency: SCD2 windows non-overlapping, is_current logic correct
- Timeliness: Load completes within SLA

**Source-to-Target Reconciliation:**
- Row count matching (source → L1 → L3)
- PII fields masked correctly
- Surrogate key generation deterministic
- Date range logic (valid_from/valid_to)

### 11.2 Test Queries

**Row Count Reconciliation:**
```sql
-- L1 vs Source file count
SELECT
  'stg_employees' AS table_name,
  (SELECT COUNT(*) FROM l1_workday.stg_employees) AS l1_count,
  2500 AS expected_count,
  CASE WHEN COUNT(*) = 2500 THEN 'PASS' ELSE 'FAIL' END AS result
FROM l1_workday.stg_employees;
```

**SCD2 Validation:**
```sql
-- Check for overlapping SCD2 windows
SELECT
  worker_key,
  valid_from,
  valid_to,
  CASE
    WHEN valid_to IS NOT NULL AND LAG(valid_to) OVER (PARTITION BY worker_key ORDER BY valid_from) >= valid_from
      THEN 'OVERLAPPING'
    ELSE 'OK'
  END AS scd2_status
FROM l3_workday.dim_worker_d
WHERE scd2_status = 'OVERLAPPING';
```

**FK Validation:**
```sql
-- Check for orphaned foreign keys
SELECT
  'fct_worker_movement_f' AS fact_table,
  COUNT(*) AS orphaned_fk_count
FROM l3_workday.fct_worker_movement_f f
LEFT JOIN l3_workday.dim_worker_d d ON f.worker_key = d.worker_key AND d.is_current = TRUE
WHERE d.worker_key IS NULL;
```

### 11.3 How to Read QA Results

QA report file: `s3://hr-datamart-lake/qa/qa_report_{data_date}_{etl_batch_id}.json`

```json
{
  "execution_date": "2024-01-31",
  "batch_id": "batch_001",
  "overall_status": "PASSED",
  "tests": [
    {
      "test_name": "l1_row_count_stg_employees",
      "status": "PASSED",
      "expected": 2500,
      "actual": 2500,
      "message": "Row count matches expected"
    },
    {
      "test_name": "l3_scd2_overlap_check",
      "status": "PASSED",
      "expected": 0,
      "actual": 0,
      "message": "No overlapping SCD2 windows detected"
    },
    {
      "test_name": "l3_fk_orphan_check",
      "status": "PASSED",
      "orphaned_records": 0,
      "message": "All foreign keys resolved"
    }
  ]
}
```

### 11.4 Acceptance Criteria

Load is considered successful if all the following pass:

| Criterion | Threshold | Check Method |
|---|---|---|
| L1 completeness | 100% of L1 rows match source file count | Row count query |
| L1 null validation | <1% missing values in required fields | NULL count per column |
| L3 SCD2 validity | 0 overlapping windows, continuous chains | valid_from/valid_to check |
| L3 FK resolution | >99% of FKs resolved, <1% orphaned | LEFT JOIN validation |
| L3 grain validation | Grain matches specification | COUNT(DISTINCT grain) |
| L3 hash consistency | 100% match between current & prior runs | Hash diff comparison |
| Load SLA | Completes within 30 minutes (L1) + 45 minutes (L3) | Job execution time |
| Data freshness | Data loaded for requested data_date | MAX(effective_date) check |

---

## 12. DIRECTORY STRUCTURE

Complete file tree of `/artifacts/`:

```
/sessions/pensive-epic-lamport/mnt/WesBarlow/hr-datamart/artifacts/
├── docs/
│   ├── README.md                                    ← This file
│   ├── ARCHITECTURE.md                             ← Detailed architecture diagrams
│   ├── DATA_DICTIONARY.md                          ← Column-level documentation
│   └── TROUBLESHOOTING.md                          ← Common issues & solutions
│
├── glue/
│   ├── glue_s3_to_l1_job.py                        ← AWS Glue: S3 to L1 staging load
│   ├── glue_l1_to_l3_job.py                        ← AWS Glue: L1 to L3 transformation
│   └── requirements.txt                            ← Python dependencies (pg8000, boto3)
│
├── sql/
│   ├── ddl/
│   │   ├── l1_workday_schema_ddl.sql               ← L1 table definitions
│   │   └── l3_workday_schema_ddl.sql               ← L3 table definitions
│   │
│   ├── dml/
│   │   ├── l3_source_loads.sql                     ← L3 source table population
│   │   ├── l3_dimension_loads.sql                  ← L3 dimension loads (SCD2)
│   │   └── l3_fact_loads.sql                       ← L3 fact table loads
│   │
│   └── qa/
│       ├── qa_row_counts.sql                       ← Row count validation queries
│       ├── qa_null_checks.sql                      ← NULL field validation
│       ├── qa_scd2_validation.sql                  ← SCD2 window overlap checks
│       └── qa_fk_validation.sql                    ← Foreign key orphan checks
│
├── config/
│   ├── feeds_inventory.json                        ← Metadata for all 12 feeds
│   ├── etl_environment.sh                          ← Environment variable definitions
│   └── glue_job_config.json                        ← Glue job parameter templates
│
├── data/
│   ├── synthetic/
│   │   ├── int_010_employee.csv                    ← Sample employee data (2,500 rows)
│   │   ├── int_020_job.csv                         ← Sample job assignment (3,200 rows)
│   │   ├── int_030_organization.csv                ← Sample organization (150 rows)
│   │   ├── int_040_worker_status.csv               ← Sample worker status (12,000 rows)
│   │   ├── int_050_job_classification.csv          ← Sample job classification (85 rows)
│   │   ├── int_060_compensation.csv                ← Sample compensation (2,800 rows)
│   │   ├── int_070_benefits.csv                    ← Sample benefits (5,200 rows)
│   │   ├── int_080_salary_history.csv              ← Sample salary history (8,500 rows)
│   │   ├── int_090_hire_events.csv                 ← Sample hire events (180 rows)
│   │   ├── int_100_transfer_events.csv             ← Sample transfer events (320 rows)
│   │   ├── int_110_promotion_events.csv            ← Sample promotion events (240 rows)
│   │   └── int_270_termination.csv                 ← Sample termination/rescind (95 rows)
│   │
│   └── reference/
│       ├── status_codes.csv                        ← Status code lookup table
│       └── job_classifications.csv                 ← Job classification codes
│
└── tests/
    ├── test_l1_loads.py                            ← Unit tests for L1 loading
    ├── test_l3_transformations.py                  ← Unit tests for L3 logic
    └── test_integration.py                         ← End-to-end integration tests
```

---

## Appendix A: Glossary

| Term | Definition |
|---|---|
| **CDM** | Conceptual Data Model - Business entity definitions |
| **LDM** | Logical Data Model - Star/snowflake schema design |
| **PDM** | Physical Data Model - Redshift-specific implementation |
| **L1** | Layer 1 - Raw staging layer (1:1 with source) |
| **L3** | Layer 3 - Conformed analytics layer (star schema) |
| **SCD2** | Slowly Changing Dimension Type 2 - Historical tracking |
| **IDP** | Intermediate Data Platform - Calculated transformation fields |
| **FK** | Foreign Key - Reference to dimension |
| **Grain** | Level of detail in fact table (one row per...) |
| **DISTSTYLE** | Redshift distribution style (how rows distributed across nodes) |
| **Rescind** | Reversal of termination (employee rehired) |
| **Hash Diff** | MD5 checksum of attributes for change detection |
| **ETL Batch ID** | Unique identifier for load execution |
| **Data Date** | Business date for which data loaded |

---

## Appendix B: Contact & Support

- **Data Engineering**: data-engineering@company.com
- **Issue Tracking**: https://jira.company.com/browse/DATAMART
- **Documentation**: https://confluence.company.com/display/DATA/HR+Datamart
- **Slack Channel**: #hr-datamart-engineering

---

**Document Version:** 1.0
**Last Updated:** 2024-01-31
**Author:** Data Engineering Team
**Status:** Published
