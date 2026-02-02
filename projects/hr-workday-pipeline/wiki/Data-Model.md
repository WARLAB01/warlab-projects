# Data Model

Complete reference for the HR Workday data model in Redshift.

---

## Schema Overview

All HR data resides in the `hr_workday` schema within the `hr_workday_db` database.

```
hr_workday_db
└── hr_workday (schema)
    ├── core_hr_employees           (10,000 records)
    ├── job_movement_transactions   (~7,000 records)
    ├── compensation_change_transactions (~20,000 records)
    └── worker_movement_transactions (~13,000 records)
```

---

## Entity Relationship Diagram

```
┌─────────────────────────┐
│   core_hr_employees     │
│─────────────────────────│
│ PK employee_id          │◄─────────────────────────────────────┐
│    worker_id            │                                      │
│    first_name           │                                      │
│    last_name            │                                      │
│    hire_date            │                                      │
│    worker_status        │                                      │
│    business_unit        │                                      │
│    department           │                                      │
│ FK manager_employee_id  │──┐ (self-reference)                  │
│    ...                  │  │                                   │
└─────────────────────────┘  │                                   │
         ▲                   │                                   │
         │                   └───────────────────────────────────┤
         │                                                       │
┌────────┴────────────────┐  ┌─────────────────────────┐        │
│ job_movement_           │  │ compensation_change_    │        │
│    transactions         │  │    transactions         │        │
│─────────────────────────│  │─────────────────────────│        │
│ PK transaction_id       │  │ PK transaction_id       │        │
│ FK employee_id          │──┤ FK employee_id          │────────┤
│    effective_date       │  │    effective_date       │        │
│    transaction_type     │  │    transaction_type     │        │
│    prior_job_profile    │  │    prior_base_salary    │        │
│    new_job_profile      │  │    new_base_salary      │        │
│    ...                  │  │    ...                  │        │
└─────────────────────────┘  └─────────────────────────┘        │
                                                                 │
                             ┌─────────────────────────┐        │
                             │ worker_movement_        │        │
                             │    transactions         │        │
                             │─────────────────────────│        │
                             │ PK transaction_id       │        │
                             │ FK employee_id          │────────┘
                             │    effective_date       │
                             │    movement_type        │
                             │    prior_location       │
                             │    new_location         │
                             │    ...                  │
                             └─────────────────────────┘
```

---

## Table: core_hr_employees

Current-state snapshot of all employees.

### Key Information
| Property | Value |
|----------|-------|
| **Primary Key** | `employee_id` |
| **Distribution Key** | `employee_id` |
| **Sort Key** | `employee_id`, `hire_date` |
| **Update Frequency** | Daily (full refresh) |

### Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `employee_id` | VARCHAR(20) | No | Primary identifier (EMP-XXXXXX) |
| `worker_id` | VARCHAR(20) | Yes | Workday worker ID |
| `first_name` | VARCHAR(100) | Yes | Legal first name |
| `last_name` | VARCHAR(100) | Yes | Legal last name |
| `preferred_name` | VARCHAR(100) | Yes | Preferred/display name |
| `legal_full_name` | VARCHAR(200) | Yes | Full legal name |
| `email_work` | VARCHAR(200) | Yes | Work email address |
| `gender` | VARCHAR(10) | Yes | Gender |
| `original_hire_date` | DATE | Yes | First hire date (for rehires) |
| `hire_date` | DATE | Yes | Current/most recent hire date |
| `termination_date` | DATE | Yes | Termination date (NULL if active) |
| `worker_status` | VARCHAR(50) | Yes | Active, Terminated, On Leave |
| `worker_type` | VARCHAR(50) | Yes | Employee, Contractor, etc. |
| `business_title` | VARCHAR(200) | Yes | Current job title |
| `job_profile` | VARCHAR(200) | Yes | Standardized job code |
| `job_family` | VARCHAR(100) | Yes | Job family grouping |
| `job_level` | INTEGER | Yes | Numeric level (1-12) |
| `management_level` | VARCHAR(50) | Yes | Executive, Manager, IC |
| `supervisory_organization` | VARCHAR(200) | Yes | Reporting org name |
| `manager_employee_id` | VARCHAR(20) | Yes | Manager's employee_id |
| `business_unit` | VARCHAR(100) | Yes | Top-level org |
| `division` | VARCHAR(100) | Yes | Division within BU |
| `department` | VARCHAR(100) | Yes | Department |
| `team` | VARCHAR(200) | Yes | Team name |
| `cost_center` | VARCHAR(50) | Yes | Cost center code |
| `location` | VARCHAR(100) | Yes | Work location |
| `country` | VARCHAR(50) | Yes | Country |
| `region` | VARCHAR(50) | Yes | Geographic region |
| `pay_rate_type` | VARCHAR(50) | Yes | Salaried, Hourly |
| `fte` | DECIMAL(5,2) | Yes | Full-time equivalent |
| `base_salary` | DECIMAL(15,2) | Yes | Annual base salary* |
| `bonus_target_percent` | DECIMAL(5,4) | Yes | Bonus target %* |
| `bonus_target_amount` | DECIMAL(15,2) | Yes | Bonus target $* |
| `annual_equity_grant` | DECIMAL(15,2) | Yes | Annual equity value* |
| `total_compensation` | DECIMAL(15,2) | Yes | Total comp value* |
| `currency` | VARCHAR(10) | Yes | Currency code |
| `car_allowance` | DECIMAL(15,2) | Yes | Car allowance* |
| `phone_allowance` | DECIMAL(15,2) | Yes | Phone allowance* |
| `executive_perquisite` | DECIMAL(15,2) | Yes | Executive perks* |
| `last_performance_rating` | VARCHAR(50) | Yes | Most recent rating* |
| `years_of_service` | INTEGER | Yes | Tenure in years |
| `time_in_position` | INTEGER | Yes | Months in current role |
| `is_manager` | BOOLEAN | Yes | Has direct reports |
| `loaded_at` | TIMESTAMP | Yes | Load timestamp |
| `source_file` | VARCHAR(500) | Yes | Source file path |

*\*Restricted access - requires Compensation Analyst role*

---

## Table: job_movement_transactions

Historical record of job-related changes.

### Key Information
| Property | Value |
|----------|-------|
| **Primary Key** | `transaction_id` |
| **Foreign Key** | `employee_id` → `core_hr_employees.employee_id` |
| **Distribution Key** | `employee_id` |
| **Sort Key** | `employee_id`, `effective_date` |

### Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `transaction_id` | VARCHAR(20) | No | Transaction ID (JOB-XXXXXX) |
| `employee_id` | VARCHAR(20) | No | Employee identifier |
| `worker_id` | VARCHAR(20) | Yes | Workday worker ID |
| `effective_date` | DATE | No | When change took effect |
| `transaction_type` | VARCHAR(50) | Yes | Promotion, Transfer, Hire, etc. |
| `transaction_status` | VARCHAR(50) | Yes | Completed, Pending |
| `reason_code` | VARCHAR(100) | Yes | Reason for change |
| `prior_job_profile` | VARCHAR(200) | Yes | Previous job code |
| `new_job_profile` | VARCHAR(200) | Yes | New job code |
| `prior_job_level` | INTEGER | Yes | Previous level |
| `new_job_level` | INTEGER | Yes | New level |
| `prior_business_unit` | VARCHAR(100) | Yes | Previous BU |
| `new_business_unit` | VARCHAR(100) | Yes | New BU |
| `prior_division` | VARCHAR(100) | Yes | Previous division |
| `new_division` | VARCHAR(100) | Yes | New division |
| `prior_department` | VARCHAR(100) | Yes | Previous department |
| `new_department` | VARCHAR(100) | Yes | New department |
| `prior_manager_id` | VARCHAR(20) | Yes | Previous manager |
| `new_manager_id` | VARCHAR(20) | Yes | New manager |
| `prior_location` | VARCHAR(100) | Yes | Previous location |
| `new_location` | VARCHAR(100) | Yes | New location |
| `prior_worker_type` | VARCHAR(50) | Yes | Previous worker type |
| `new_worker_type` | VARCHAR(50) | Yes | New worker type |
| `initiated_by` | VARCHAR(100) | Yes | Who initiated |
| `initiated_date` | DATE | Yes | When initiated |
| `completed_date` | DATE | Yes | When completed |
| `comments` | VARCHAR(500) | Yes | Notes |
| `loaded_at` | TIMESTAMP | Yes | Load timestamp |
| `source_file` | VARCHAR(500) | Yes | Source file path |

### Transaction Types
- `Hire` - New hire
- `Promotion` - Job level increase
- `Demotion` - Job level decrease
- `Lateral Transfer` - Same level, different role
- `Department Transfer` - Different department
- `Termination` - Employment end

---

## Table: compensation_change_transactions

Historical record of pay changes.

### Key Information
| Property | Value |
|----------|-------|
| **Primary Key** | `transaction_id` |
| **Foreign Key** | `employee_id` → `core_hr_employees.employee_id` |
| **Distribution Key** | `employee_id` |
| **Sort Key** | `employee_id`, `effective_date` |
| **Access** | Restricted - Compensation Analyst role required |

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | VARCHAR(20) | Transaction ID (COMP-XXXXXX) |
| `employee_id` | VARCHAR(20) | Employee identifier |
| `effective_date` | DATE | When change took effect |
| `transaction_type` | VARCHAR(50) | Merit, Promotion, Adjustment, etc. |
| `prior_base_salary` | DECIMAL(15,2) | Previous base salary |
| `new_base_salary` | DECIMAL(15,2) | New base salary |
| `base_change_amount` | DECIMAL(15,2) | Dollar change |
| `base_change_percent` | DECIMAL(8,2) | Percent change |
| `prior_bonus_target_percent` | DECIMAL(5,4) | Previous bonus % |
| `new_bonus_target_percent` | DECIMAL(5,4) | New bonus % |
| `currency` | VARCHAR(10) | Currency code |
| `performance_rating` | VARCHAR(50) | Associated rating |

### Transaction Types
- `Merit Increase` - Annual merit adjustment
- `Promotion Increase` - Pay raise with promotion
- `Market Adjustment` - Competitive adjustment
- `Equity Refresh` - Stock grant refresh
- `Bonus Adjustment` - Target change

---

## Table: worker_movement_transactions

Historical record of location and org changes.

### Key Information
| Property | Value |
|----------|-------|
| **Primary Key** | `transaction_id` |
| **Foreign Key** | `employee_id` → `core_hr_employees.employee_id` |
| **Distribution Key** | `employee_id` |
| **Sort Key** | `employee_id`, `effective_date` |

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | VARCHAR(20) | Transaction ID (WM-XXXXXX) |
| `employee_id` | VARCHAR(20) | Employee identifier |
| `effective_date` | DATE | When change took effect |
| `movement_type` | VARCHAR(50) | Relocation, Transfer, etc. |
| `prior_location` | VARCHAR(100) | Previous location |
| `new_location` | VARCHAR(100) | New location |
| `prior_country` | VARCHAR(50) | Previous country |
| `new_country` | VARCHAR(50) | New country |
| `prior_department` | VARCHAR(100) | Previous department |
| `new_department` | VARCHAR(100) | New department |
| `relocation_package` | VARCHAR(100) | Relo package type |
| `remote_work_arrangement` | VARCHAR(50) | Remote/Hybrid/Office |

### Movement Types
- `Office Relocation` - New office location
- `Remote Transition` - To remote work
- `Department Transfer` - New department
- `International Transfer` - New country

---

## Common Joins

### Employee with Manager

```sql
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name as employee_name,
    m.first_name || ' ' || m.last_name as manager_name
FROM hr_workday.core_hr_employees e
LEFT JOIN hr_workday.core_hr_employees m
    ON e.manager_employee_id = m.employee_id;
```

### Employee with Job History

```sql
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name as name,
    j.effective_date,
    j.transaction_type,
    j.new_job_profile
FROM hr_workday.core_hr_employees e
JOIN hr_workday.job_movement_transactions j
    ON e.employee_id = j.employee_id
ORDER BY e.employee_id, j.effective_date;
```

---

For the complete data dictionary with all field details, see [DATA_DICTIONARY.md](../docs/technical/DATA_DICTIONARY.md).
