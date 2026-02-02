# Data Governance Framework
## HR Workday Data Pipeline

### Document Control

| Version | Date | Author | Status |
|---------|------|--------|--------|
| 1.0 | 2026-02-02 | Data Engineering Team | Current |

---

## 1. Overview

This document establishes the data governance framework for the HR Workday data pipeline, defining policies, standards, and procedures for managing HR data throughout its lifecycle.

### 1.1 Purpose

- Ensure data quality, integrity, and consistency
- Protect sensitive employee information
- Maintain regulatory compliance
- Enable trusted analytics and reporting

### 1.2 Scope

This framework applies to all HR data flowing through the pipeline:
- Core HR employee records
- Job movement transactions
- Compensation change transactions
- Worker movement transactions

---

## 2. Data Classification

### 2.1 Classification Levels

| Level | Description | Examples | Handling Requirements |
|-------|-------------|----------|----------------------|
| **Highly Confidential** | Data that could cause significant harm if exposed | SSN, salary details, performance ratings | Encryption at rest and in transit, strict access controls, audit logging |
| **Confidential** | Internal data with limited distribution | Employee names, job titles, org structure | Encryption, role-based access |
| **Internal** | General business data | Department codes, location names | Standard access controls |
| **Public** | Non-sensitive data | Generic job family names | Minimal restrictions |

### 2.2 Field-Level Classification

#### Core HR Employees Table

| Field | Classification | Justification |
|-------|---------------|---------------|
| employee_id | Confidential | Unique identifier |
| first_name, last_name | Confidential | PII |
| email_work | Confidential | Contact information |
| gender | Highly Confidential | Protected class |
| base_salary | Highly Confidential | Compensation data |
| bonus_target_percent | Highly Confidential | Compensation data |
| total_compensation | Highly Confidential | Compensation data |
| last_performance_rating | Highly Confidential | Performance data |
| business_unit | Internal | Org structure |
| location | Internal | Work location |

#### Transaction Tables

| Field Category | Classification |
|---------------|---------------|
| Salary/Compensation fields | Highly Confidential |
| Employee identifiers | Confidential |
| Organizational fields | Internal |
| Date/Status fields | Internal |

---

## 3. Data Quality Standards

### 3.1 Quality Dimensions

| Dimension | Definition | Target | Measurement |
|-----------|------------|--------|-------------|
| **Completeness** | Required fields populated | 99.5% | NULL count checks |
| **Accuracy** | Data reflects real-world values | 99% | Validation rules |
| **Consistency** | Data agrees across systems | 100% | Cross-reference checks |
| **Timeliness** | Data available when needed | T+1 | Load timestamp monitoring |
| **Uniqueness** | No duplicate records | 100% | Primary key validation |
| **Validity** | Data conforms to formats | 100% | Format validation |

### 3.2 Data Quality Rules

#### Required Fields (Non-NULL)

**Core HR Employees:**
- employee_id
- worker_id
- hire_date
- worker_status
- business_unit
- department

**Transaction Tables:**
- transaction_id
- employee_id
- effective_date
- transaction_type
- transaction_status

#### Format Validation

| Field | Format | Example |
|-------|--------|---------|
| employee_id | EMP-NNNNNN | EMP-001234 |
| worker_id | WRK-NNNNNN | WRK-001234 |
| transaction_id | [TYPE]-NNNNNN | JOB-000001, COMP-000001 |
| email_work | Valid email format | john.doe@company.com |
| dates | YYYY-MM-DD | 2026-01-15 |

#### Referential Integrity

- All `employee_id` values in transaction tables must exist in `core_hr_employees`
- All `manager_employee_id` values must reference valid employees
- Effective dates must be within reasonable business ranges

### 3.3 Quality Monitoring

```sql
-- Daily Data Quality Check
SELECT
    'Completeness' as check_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN employee_id IS NULL THEN 1 ELSE 0 END) as null_employee_id,
    SUM(CASE WHEN hire_date IS NULL THEN 1 ELSE 0 END) as null_hire_date,
    SUM(CASE WHEN worker_status IS NULL THEN 1 ELSE 0 END) as null_status
FROM hr_workday.core_hr_employees;

-- Duplicate Check
SELECT employee_id, COUNT(*)
FROM hr_workday.core_hr_employees
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- Orphan Transaction Check
SELECT COUNT(*) as orphan_transactions
FROM hr_workday.job_movement_transactions t
LEFT JOIN hr_workday.core_hr_employees e ON t.employee_id = e.employee_id
WHERE e.employee_id IS NULL;
```

---

## 4. Access Control

### 4.1 Role-Based Access Control (RBAC)

| Role | Description | Access Level |
|------|-------------|--------------|
| **Data Engineer** | Pipeline development and maintenance | Full technical access, no comp data |
| **Data Analyst** | Reporting and analytics | Read access, masked comp data |
| **HR Business Partner** | HR operations support | Read access to assigned business units |
| **HR Leadership** | Strategic HR decisions | Full read access |
| **Compensation Analyst** | Comp analysis | Full comp data access |
| **Auditor** | Compliance verification | Read-only audit access |

### 4.2 Redshift Access Implementation

```sql
-- Create groups
CREATE GROUP data_engineers;
CREATE GROUP data_analysts;
CREATE GROUP hr_business_partners;
CREATE GROUP hr_leadership;
CREATE GROUP compensation_analysts;

-- Grant schema access
GRANT USAGE ON SCHEMA hr_workday TO GROUP data_analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA hr_workday TO GROUP data_analysts;

-- Revoke sensitive columns from general analysts
REVOKE SELECT (base_salary, bonus_target_percent, total_compensation,
               last_performance_rating)
ON hr_workday.core_hr_employees FROM GROUP data_analysts;

-- Create view with masked compensation for analysts
CREATE VIEW hr_workday.employees_analyst_view AS
SELECT
    employee_id, worker_id, first_name, last_name,
    hire_date, worker_status, business_title,
    business_unit, division, department,
    location, country,
    -- Mask sensitive fields
    'RESTRICTED' as base_salary,
    'RESTRICTED' as total_compensation
FROM hr_workday.core_hr_employees;
```

### 4.3 AWS IAM Policies

Access to AWS resources follows least-privilege principles:

- **Glue Job Role**: S3 read, Redshift write, Secrets Manager read
- **Redshift Role**: S3 read for COPY operations only
- **Analytics Users**: Redshift read-only via Query Editor

---

## 5. Data Retention

### 5.1 Retention Schedule

| Data Type | Retention Period | Archive Method | Disposal |
|-----------|-----------------|----------------|----------|
| Current employee records | Active + 7 years | S3 Glacier | Secure delete |
| Terminated employee records | 7 years post-termination | S3 Glacier | Secure delete |
| Transaction history | 7 years | S3 Glacier | Secure delete |
| Audit logs | 7 years | CloudWatch Logs | Auto-expire |
| ETL job logs | 90 days | CloudWatch Logs | Auto-expire |

### 5.2 Archival Process

1. **Monthly Archive Job**: Move records older than retention threshold to archive tables
2. **Quarterly S3 Archive**: Export archived data to S3 Glacier
3. **Annual Review**: Verify retention compliance, execute disposal

```bash
# Archive to S3 Glacier
aws s3 cp s3://hr-workday-data-{account}/archive/2025/ \
    s3://hr-workday-archive-{account}/2025/ \
    --recursive \
    --storage-class GLACIER
```

---

## 6. Compliance

### 6.1 Regulatory Requirements

| Regulation | Requirement | Implementation |
|------------|-------------|----------------|
| **SOX** | Financial controls, audit trails | Audit logging, change tracking |
| **GDPR** (if applicable) | Data subject rights, consent | Data masking, deletion capability |
| **CCPA** (California) | Consumer privacy rights | Data inventory, access controls |
| **HIPAA** (if health data) | PHI protection | Encryption, access controls |
| **SOC 2** | Security controls | AWS compliance inheritance |

### 6.2 Audit Trail Requirements

All data changes must be traceable:

- **loaded_at**: Timestamp of when record was loaded
- **source_file**: Source file that provided the data
- **ETL Job Run ID**: Glue job run identifier
- **CloudTrail**: AWS API activity logging

### 6.3 Right to Deletion (GDPR/CCPA)

Process for handling deletion requests:

1. Receive verified deletion request
2. Identify all records for the employee across tables
3. Execute deletion or anonymization
4. Document completion
5. Verify deletion from backups within retention period

---

## 7. Data Stewardship

### 7.1 Roles and Responsibilities

| Role | Responsibility | Contact |
|------|----------------|---------|
| **Data Owner** | HR Operations Director | hr-data-owner@company.com |
| **Data Steward** | HR Data Manager | hr-data-steward@company.com |
| **Technical Steward** | Data Engineering Lead | data-engineering@company.com |
| **Privacy Officer** | Chief Privacy Officer | privacy@company.com |

### 7.2 Stewardship Activities

| Activity | Frequency | Owner |
|----------|-----------|-------|
| Data quality review | Weekly | Data Steward |
| Access review | Quarterly | Data Owner |
| Policy review | Annual | Privacy Officer |
| Classification review | Annual | Data Steward |
| Retention compliance | Annual | Technical Steward |

---

## 8. Change Management

### 8.1 Schema Changes

All schema changes must follow this process:

1. **Request**: Submit change request with business justification
2. **Review**: Data Steward reviews impact on downstream systems
3. **Approve**: Data Owner approves change
4. **Implement**: Data Engineering implements in non-prod first
5. **Test**: Validate data quality and downstream compatibility
6. **Deploy**: Promote to production
7. **Document**: Update data dictionary and documentation

### 8.2 Change Request Template

```
Change Request: [CR-YYYY-NNN]
Requestor: [Name]
Date: [Date]
Type: [Schema Change | New Field | Field Removal | Data Type Change]

Description:
[Detailed description of the change]

Business Justification:
[Why this change is needed]

Impact Assessment:
- Upstream systems: [List]
- Downstream systems: [List]
- Reports affected: [List]

Rollback Plan:
[How to reverse if issues occur]
```

---

## 9. Incident Management

### 9.1 Data Incident Categories

| Category | Description | Response Time |
|----------|-------------|---------------|
| **Critical** | Data breach, unauthorized access | Immediate |
| **High** | Data corruption, significant quality issue | 2 hours |
| **Medium** | Minor quality issue, delayed load | 4 hours |
| **Low** | Documentation gap, minor discrepancy | Next business day |

### 9.2 Incident Response

1. **Detect**: Identify incident through monitoring or report
2. **Assess**: Determine severity and scope
3. **Contain**: Stop further impact
4. **Investigate**: Identify root cause
5. **Remediate**: Fix the issue
6. **Recover**: Restore normal operations
7. **Review**: Document lessons learned

### 9.3 Breach Notification

If a data breach occurs involving PII:
- Notify Privacy Officer within 1 hour
- Follow company incident response plan
- Regulatory notification within required timeframes (72 hours for GDPR)

---

## 10. Glossary

| Term | Definition |
|------|------------|
| **PII** | Personally Identifiable Information |
| **PHI** | Protected Health Information |
| **Data Owner** | Business stakeholder accountable for data |
| **Data Steward** | Individual responsible for data quality |
| **Data Classification** | Categorization of data by sensitivity |
| **Retention Period** | Duration data must be kept |
| **Right to Deletion** | Individual's right to have data removed |

---

## Appendix A: Compliance Checklist

- [ ] Data classification completed for all fields
- [ ] Access controls implemented per classification
- [ ] Encryption enabled at rest and in transit
- [ ] Audit logging configured
- [ ] Retention policies documented
- [ ] Backup and recovery tested
- [ ] Incident response plan documented
- [ ] Privacy impact assessment completed
- [ ] Staff training completed
- [ ] Annual review scheduled
