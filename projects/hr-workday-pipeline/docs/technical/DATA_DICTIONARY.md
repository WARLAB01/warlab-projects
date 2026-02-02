# Data Dictionary
## HR Workday Data Warehouse

### Document Control

| Version | Date | Author | Status |
|---------|------|--------|--------|
| 1.0 | 2026-02-02 | Data Engineering Team | Current |

---

## Overview

This data dictionary provides detailed descriptions of all tables, columns, and data elements in the HR Workday data warehouse. The data warehouse consists of four primary tables within the `hr_workday` schema.

---

## Table: core_hr_employees

**Description:** Non-transactional master data containing current state of all employees (active and terminated). This is the primary reference table for employee demographics, job information, organizational hierarchy, and compensation.

**Primary Key:** `employee_id`
**Distribution Key:** `employee_id`
**Sort Key:** `employee_id`, `hire_date`
**Update Frequency:** Daily (full refresh)
**Approximate Row Count:** 10,000

### Column Definitions

| Column Name | Data Type | Nullable | Description | Example |
|-------------|-----------|----------|-------------|---------|
| `employee_id` | VARCHAR(20) | No | Unique employee identifier (EIN format). Primary key for all employee-related joins. | EMP123456 |
| `worker_id` | VARCHAR(20) | Yes | Workday internal worker identifier. Secondary identifier used for system integrations. | WD12345678 |
| `first_name` | VARCHAR(100) | Yes | Employee's legal first name as recorded in Workday. | John |
| `last_name` | VARCHAR(100) | Yes | Employee's legal last name/surname. | Smith |
| `preferred_name` | VARCHAR(100) | Yes | Employee's preferred first name (may differ from legal name). | Johnny |
| `legal_full_name` | VARCHAR(200) | Yes | Concatenated legal first and last name. | John Smith |
| `email_work` | VARCHAR(200) | Yes | Corporate email address. Format: firstname.lastname@globalfinancial.com | john.smith@globalfinancial.com |
| `gender` | VARCHAR(10) | Yes | Gender code. Values: 'M' (Male), 'F' (Female). | M |
| `original_hire_date` | DATE | Yes | Date employee was first hired by the organization. Does not change for rehires. | 2020-03-15 |
| `hire_date` | DATE | Yes | Current hire date. May differ from original_hire_date for rehired employees. | 2020-03-15 |
| `termination_date` | DATE | Yes | Date of employment termination. NULL for active employees. | 2025-06-30 |
| `worker_status` | VARCHAR(50) | Yes | Current employment status. Values: 'Active', 'Terminated'. | Active |
| `worker_type` | VARCHAR(50) | Yes | Employment classification. Values: 'Regular', 'Temporary', 'Contractor', 'Intern'. | Regular |
| `business_title` | VARCHAR(200) | Yes | Employee's official business title as displayed externally. | Senior Financial Analyst |
| `job_profile` | VARCHAR(200) | Yes | Standardized job profile from Workday job catalog. | Senior Analyst |
| `job_family` | VARCHAR(100) | Yes | Grouping of related job profiles. | Finance |
| `job_level` | INTEGER | Yes | Numeric job level (1-12). Higher numbers indicate more senior positions. | 5 |
| `management_level` | VARCHAR(50) | Yes | Management classification. Values: 'Individual Contributor', 'Management', 'Executive'. | Individual Contributor |
| `supervisory_organization` | VARCHAR(200) | Yes | Name of the supervisory organization the employee belongs to. | FP&A (FP&A - Team 2) |
| `manager_employee_id` | VARCHAR(20) | Yes | Employee_ID of the employee's direct manager. NULL for top-level executives. | EMP789012 |
| `business_unit` | VARCHAR(100) | Yes | Highest level of organizational hierarchy. | Investment Banking |
| `division` | VARCHAR(100) | Yes | Second level of organizational hierarchy within business unit. | Capital Markets |
| `department` | VARCHAR(100) | Yes | Third level of organizational hierarchy within division. | Research |
| `team` | VARCHAR(200) | Yes | Lowest level of organizational hierarchy. Specific team assignment. | Research - Team 1 |
| `cost_center` | VARCHAR(50) | Yes | Financial cost center code for budget allocation. Format: CC-XX-XXX. | CC-20-456 |
| `location` | VARCHAR(100) | Yes | Primary work location (city, state/province). | New York, NY |
| `country` | VARCHAR(50) | Yes | Country of primary work location. Values: 'USA', 'Canada'. | USA |
| `region` | VARCHAR(50) | Yes | Geographic region. Values: 'Northeast', 'Southeast', 'Midwest', 'West', 'Southwest', 'Mountain', 'Ontario', 'Quebec'. | Northeast |
| `pay_rate_type` | VARCHAR(50) | Yes | Compensation payment type. Values: 'Salary', 'Hourly'. | Salary |
| `fte` | DECIMAL(5,2) | Yes | Full-time equivalent. 1.0 = full-time, 0.5 = half-time. | 1.00 |
| `base_salary` | DECIMAL(15,2) | Yes | Annual base salary amount in local currency. | 125000.00 |
| `bonus_target_percent` | DECIMAL(5,4) | Yes | Target bonus as percentage of base salary (decimal format). 0.15 = 15%. | 0.1500 |
| `bonus_target_amount` | DECIMAL(15,2) | Yes | Target bonus amount in local currency (base_salary × bonus_target_percent). | 18750.00 |
| `annual_equity_grant` | DECIMAL(15,2) | Yes | Annual equity grant value in local currency. 0 if not equity-eligible. | 25000.00 |
| `total_compensation` | DECIMAL(15,2) | Yes | Sum of base_salary + bonus_target_amount + annual_equity_grant. | 168750.00 |
| `currency` | VARCHAR(10) | Yes | Currency code for compensation values. Values: 'USD', 'CAD'. | USD |
| `car_allowance` | DECIMAL(15,2) | Yes | Annual car allowance amount. 0 if not eligible. | 12000.00 |
| `phone_allowance` | DECIMAL(15,2) | Yes | Annual phone/mobile allowance amount. 0 if not eligible. | 1800.00 |
| `executive_perquisite` | DECIMAL(15,2) | Yes | Annual executive perquisite allowance. 0 if not eligible. | 0.00 |
| `last_performance_rating` | VARCHAR(50) | Yes | Most recent annual performance rating. Values: 'Exceptional', 'Exceeds Expectations', 'Meets Expectations', 'Needs Improvement', 'Unsatisfactory'. | Meets Expectations |
| `years_of_service` | INTEGER | Yes | Calculated years since original_hire_date. | 5 |
| `time_in_position` | INTEGER | Yes | Years in current position/role. | 2 |
| `is_manager` | BOOLEAN | Yes | Flag indicating if employee has direct reports. | FALSE |
| `loaded_at` | TIMESTAMP | Yes | Timestamp when record was loaded to data warehouse. System-generated. | 2026-02-02 06:15:00 |
| `source_file` | VARCHAR(500) | Yes | S3 path of source file for data lineage tracking. | s3://hr-workday-data/raw/... |

### Business Rules

1. `employee_id` is immutable and persists across rehires
2. `termination_date` is NULL for all active employees
3. `manager_employee_id` must exist in the employee table (referential integrity)
4. `total_compensation` = `base_salary` + `bonus_target_amount` + `annual_equity_grant`
5. `years_of_service` calculated from `original_hire_date`, not `hire_date`

---

## Table: job_movement_transactions

**Description:** Transactional history of employee job changes including hires, promotions, terminations, lateral moves, and demotions. Each row represents a single job event with before/after state.

**Primary Key:** `transaction_id`
**Distribution Key:** `employee_id`
**Sort Key:** `employee_id`, `effective_date`
**Update Frequency:** Daily (full refresh)
**Approximate Row Count:** 7,000/year

### Column Definitions

| Column Name | Data Type | Nullable | Description | Example |
|-------------|-----------|----------|-------------|---------|
| `transaction_id` | VARCHAR(20) | No | Unique transaction identifier. Format: JM + 7 digits. | JM1234567 |
| `employee_id` | VARCHAR(20) | No | Foreign key to core_hr_employees. | EMP123456 |
| `worker_id` | VARCHAR(20) | Yes | Workday worker identifier. | WD12345678 |
| `effective_date` | DATE | No | Date when the job change takes effect. | 2025-07-01 |
| `transaction_type` | VARCHAR(50) | Yes | Type of job movement. Values: 'Hire', 'Termination', 'Promotion', 'Demotion', 'Lateral Move'. | Promotion |
| `transaction_status` | VARCHAR(50) | Yes | Processing status. Values: 'Completed', 'Pending', 'Cancelled'. | Completed |
| `reason_code` | VARCHAR(100) | Yes | Business reason for the transaction. See reason code reference table. | Performance Based |
| `prior_job_profile` | VARCHAR(200) | Yes | Job profile before the change. NULL for new hires. | Analyst II |
| `new_job_profile` | VARCHAR(200) | Yes | Job profile after the change. NULL for terminations. | Senior Analyst |
| `prior_job_level` | INTEGER | Yes | Job level before the change. NULL for new hires. | 3 |
| `new_job_level` | INTEGER | Yes | Job level after the change. NULL for terminations. | 4 |
| `prior_business_unit` | VARCHAR(100) | Yes | Business unit before the change. | Investment Banking |
| `new_business_unit` | VARCHAR(100) | Yes | Business unit after the change. | Investment Banking |
| `prior_division` | VARCHAR(100) | Yes | Division before the change. | Capital Markets |
| `new_division` | VARCHAR(100) | Yes | Division after the change. | Capital Markets |
| `prior_department` | VARCHAR(100) | Yes | Department before the change. | Research |
| `new_department` | VARCHAR(100) | Yes | Department after the change. | Research |
| `prior_manager_id` | VARCHAR(20) | Yes | Manager's employee_id before the change. | EMP789012 |
| `new_manager_id` | VARCHAR(20) | Yes | Manager's employee_id after the change. | EMP789012 |
| `prior_location` | VARCHAR(100) | Yes | Work location before the change. | New York, NY |
| `new_location` | VARCHAR(100) | Yes | Work location after the change. | New York, NY |
| `prior_worker_type` | VARCHAR(50) | Yes | Worker type before the change. | Regular |
| `new_worker_type` | VARCHAR(50) | Yes | Worker type after the change. | Regular |
| `initiated_by` | VARCHAR(100) | Yes | Source of the transaction. Values: 'Manager', 'HR', 'Employee', 'HR System', 'Recruiter'. | Manager |
| `initiated_date` | DATE | Yes | Date the transaction was initiated/submitted. | 2025-06-01 |
| `completed_date` | DATE | Yes | Date the transaction was completed/approved. | 2025-07-01 |
| `comments` | VARCHAR(500) | Yes | Free-text notes about the transaction. | Promotion from Level 3 to Level 4 |
| `loaded_at` | TIMESTAMP | Yes | Data warehouse load timestamp. | 2026-02-02 06:15:00 |
| `source_file` | VARCHAR(500) | Yes | S3 source file path. | s3://hr-workday-data/raw/... |

### Transaction Type Reference

| Transaction Type | Description | Prior Fields | New Fields |
|-----------------|-------------|--------------|------------|
| Hire | New employee joining | All NULL | All populated |
| Termination | Employee leaving | All populated | All NULL |
| Promotion | Job level increase | Populated | Populated (higher level) |
| Demotion | Job level decrease | Populated | Populated (lower level) |
| Lateral Move | Role change, same level | Populated | Populated (same level) |

---

## Table: compensation_change_transactions

**Description:** Transactional history of compensation changes including merit increases, promotions, market adjustments, equity grants, and allowance modifications.

**Primary Key:** `transaction_id`
**Distribution Key:** `employee_id`
**Sort Key:** `employee_id`, `effective_date`
**Update Frequency:** Daily (full refresh)
**Approximate Row Count:** 20,000/year

### Column Definitions

| Column Name | Data Type | Nullable | Description | Example |
|-------------|-----------|----------|-------------|---------|
| `transaction_id` | VARCHAR(20) | No | Unique transaction identifier. Format: CC + 7 digits. | CC1234567 |
| `employee_id` | VARCHAR(20) | No | Foreign key to core_hr_employees. | EMP123456 |
| `worker_id` | VARCHAR(20) | Yes | Workday worker identifier. | WD12345678 |
| `effective_date` | DATE | No | Date when compensation change takes effect. | 2025-04-01 |
| `transaction_type` | VARCHAR(50) | Yes | Type of compensation change. See reference table below. | Merit Increase |
| `transaction_status` | VARCHAR(50) | Yes | Processing status. Values: 'Completed', 'Pending'. | Completed |
| `reason_code` | VARCHAR(100) | Yes | Business reason for the change. | Annual Merit Increase |
| `prior_base_salary` | DECIMAL(15,2) | Yes | Base salary before the change. | 100000.00 |
| `new_base_salary` | DECIMAL(15,2) | Yes | Base salary after the change. | 104000.00 |
| `base_change_amount` | DECIMAL(15,2) | Yes | Dollar change in base salary (new - prior). | 4000.00 |
| `base_change_percent` | DECIMAL(8,2) | Yes | Percentage change in base salary. | 4.00 |
| `prior_bonus_target_percent` | DECIMAL(5,4) | Yes | Bonus target percentage before change. | 0.1500 |
| `new_bonus_target_percent` | DECIMAL(5,4) | Yes | Bonus target percentage after change. | 0.1500 |
| `prior_bonus_target_amount` | DECIMAL(15,2) | Yes | Bonus target amount before change. | 15000.00 |
| `new_bonus_target_amount` | DECIMAL(15,2) | Yes | Bonus target amount after change. | 15600.00 |
| `prior_annual_equity` | DECIMAL(15,2) | Yes | Annual equity grant before change. | 20000.00 |
| `new_annual_equity` | DECIMAL(15,2) | Yes | Annual equity grant after change. | 25000.00 |
| `allowance_type` | VARCHAR(100) | Yes | Type of allowance if applicable. | Car Allowance |
| `allowance_amount` | DECIMAL(15,2) | Yes | Allowance amount if applicable. | 12000.00 |
| `currency` | VARCHAR(10) | Yes | Currency code for all monetary values. | USD |
| `performance_rating` | VARCHAR(50) | Yes | Performance rating driving the change (if applicable). | Exceeds Expectations |
| `compa_ratio_prior` | DECIMAL(8,4) | Yes | Compa-ratio before change (salary ÷ midpoint). | 0.9500 |
| `compa_ratio_new` | DECIMAL(8,4) | Yes | Compa-ratio after change. | 0.9900 |
| `initiated_by` | VARCHAR(100) | Yes | Source of the transaction. | Annual Compensation Cycle |
| `approved_by` | VARCHAR(100) | Yes | Approver of the transaction. | Compensation Committee |
| `initiated_date` | DATE | Yes | Date transaction was initiated. | 2025-02-01 |
| `completed_date` | DATE | Yes | Date transaction was completed. | 2025-04-01 |
| `comments` | VARCHAR(500) | Yes | Free-text notes. | Annual merit increase - Exceeds Expectations |
| `loaded_at` | TIMESTAMP | Yes | Data warehouse load timestamp. | 2026-02-02 06:15:00 |
| `source_file` | VARCHAR(500) | Yes | S3 source file path. | s3://hr-workday-data/raw/... |

### Compensation Transaction Type Reference

| Transaction Type | Description | Typical Timing |
|-----------------|-------------|----------------|
| Merit Increase | Annual performance-based salary increase | April (annual cycle) |
| Promotion Adjustment | Salary increase due to promotion | Throughout year |
| Market Adjustment | Salary increase to match market rates | As needed |
| Equity Refresh | Annual or ad-hoc equity grant | March (annual cycle) |
| Retention | Retention bonus or salary increase | As needed |
| Allowance Change | New or modified allowance | As needed |

---

## Table: worker_movement_transactions

**Description:** Transactional history of worker movements including transfers, relocations, reporting line changes, and organizational restructuring events.

**Primary Key:** `transaction_id`
**Distribution Key:** `employee_id`
**Sort Key:** `employee_id`, `effective_date`
**Update Frequency:** Daily (full refresh)
**Approximate Row Count:** 13,000/year

### Column Definitions

| Column Name | Data Type | Nullable | Description | Example |
|-------------|-----------|----------|-------------|---------|
| `transaction_id` | VARCHAR(20) | No | Unique transaction identifier. Format: WM + 7 digits. | WM1234567 |
| `employee_id` | VARCHAR(20) | No | Foreign key to core_hr_employees. | EMP123456 |
| `worker_id` | VARCHAR(20) | Yes | Workday worker identifier. | WD12345678 |
| `effective_date` | DATE | No | Date when movement takes effect. | 2025-08-01 |
| `movement_type` | VARCHAR(50) | Yes | Type of worker movement. See reference table below. | Internal Transfer |
| `movement_status` | VARCHAR(50) | Yes | Processing status. | Completed |
| `reason_code` | VARCHAR(100) | Yes | Business reason for the movement. | Career Development |
| `prior_location` | VARCHAR(100) | Yes | Work location before the change. | New York, NY |
| `new_location` | VARCHAR(100) | Yes | Work location after the change. | Chicago, IL |
| `prior_country` | VARCHAR(50) | Yes | Country before the change. | USA |
| `new_country` | VARCHAR(50) | Yes | Country after the change. | USA |
| `prior_region` | VARCHAR(50) | Yes | Region before the change. | Northeast |
| `new_region` | VARCHAR(50) | Yes | Region after the change. | Midwest |
| `prior_business_unit` | VARCHAR(100) | Yes | Business unit before the change. | Investment Banking |
| `new_business_unit` | VARCHAR(100) | Yes | Business unit after the change. | Commercial Banking |
| `prior_division` | VARCHAR(100) | Yes | Division before the change. | Capital Markets |
| `new_division` | VARCHAR(100) | Yes | Division after the change. | Corporate Lending |
| `prior_department` | VARCHAR(100) | Yes | Department before the change. | Research |
| `new_department` | VARCHAR(100) | Yes | Department after the change. | Underwriting |
| `prior_team` | VARCHAR(200) | Yes | Team before the change. | Research - Team 1 |
| `new_team` | VARCHAR(200) | Yes | Team after the change. | Underwriting - Team 3 |
| `prior_cost_center` | VARCHAR(50) | Yes | Cost center before the change. | CC-20-456 |
| `new_cost_center` | VARCHAR(50) | Yes | Cost center after the change. | CC-40-789 |
| `prior_manager_id` | VARCHAR(20) | Yes | Manager before the change. | EMP789012 |
| `new_manager_id` | VARCHAR(20) | Yes | Manager after the change. | EMP345678 |
| `prior_supervisory_org` | VARCHAR(200) | Yes | Supervisory org before the change. | Research (Research - Team 1) |
| `new_supervisory_org` | VARCHAR(200) | Yes | Supervisory org after the change. | Underwriting (Underwriting - Team 3) |
| `relocation_package` | VARCHAR(100) | Yes | Relocation package tier if applicable. | Tier 1 - Full |
| `remote_work_arrangement` | VARCHAR(50) | Yes | Work arrangement. Values: 'Office', 'Hybrid', 'Remote'. | Hybrid |
| `initiated_by` | VARCHAR(100) | Yes | Source of the transaction. | Employee |
| `approved_by` | VARCHAR(100) | Yes | Approver of the transaction. | HR Director |
| `initiated_date` | DATE | Yes | Date transaction was initiated. | 2025-06-15 |
| `completed_date` | DATE | Yes | Date transaction was completed. | 2025-08-01 |
| `comments` | VARCHAR(500) | Yes | Free-text notes. | Transfer from Investment Banking to Commercial Banking |
| `loaded_at` | TIMESTAMP | Yes | Data warehouse load timestamp. | 2026-02-02 06:15:00 |
| `source_file` | VARCHAR(500) | Yes | S3 source file path. | s3://hr-workday-data/raw/... |

### Movement Type Reference

| Movement Type | Description |
|--------------|-------------|
| Relocation | Geographic location change |
| Internal Transfer | Department or business unit change |
| Reporting Change | Manager or reporting line change only |
| Org Restructure | Organizational restructuring event |
| Work Arrangement Change | Change in remote/hybrid/office status |

---

## Reference Data

### Organization Hierarchy

| Level | Field | Example Values |
|-------|-------|----------------|
| 1 | Business Unit | Retail Banking, Investment Banking, Wealth Management, Commercial Banking, Risk & Compliance, Technology, Corporate Functions |
| 2 | Division | Consumer Lending, Capital Markets, Private Banking, etc. |
| 3 | Department | Mortgage Origination, Research, Client Advisory, etc. |
| 4 | Team | Research - Team 1, Client Advisory - Team 2, etc. |

### Location Reference

| Location | Country | Region |
|----------|---------|--------|
| New York, NY | USA | Northeast |
| Charlotte, NC | USA | Southeast |
| Chicago, IL | USA | Midwest |
| San Francisco, CA | USA | West |
| Boston, MA | USA | Northeast |
| Toronto, ON | Canada | Ontario |
| Dallas, TX | USA | Southwest |
| Los Angeles, CA | USA | West |
| Atlanta, GA | USA | Southeast |
| Denver, CO | USA | Mountain |
| Montreal, QC | Canada | Quebec |
| Phoenix, AZ | USA | Southwest |

### Job Level Reference

| Level | Management Level | Typical Titles |
|-------|-----------------|----------------|
| 1-5 | Individual Contributor | Analyst, Associate, Specialist, Lead |
| 6-10 | Management | Supervisor, Manager, Director |
| 11-12 | Executive | Vice President, Chief Officer |

---

## Data Quality Rules

| Rule ID | Table | Column(s) | Rule | Severity |
|---------|-------|-----------|------|----------|
| DQ-001 | All | employee_id | Must match pattern 'EMP' + 6 digits | Error |
| DQ-002 | All | effective_date | Must be valid date, not future | Error |
| DQ-003 | core_hr_employees | email_work | Must contain '@' | Warning |
| DQ-004 | core_hr_employees | base_salary | Must be > 0 for active employees | Warning |
| DQ-005 | job_movement | prior/new_job_level | new_job_level > prior_job_level for promotions | Warning |
| DQ-006 | compensation | base_change_percent | Must equal (new - prior) / prior × 100 | Warning |
