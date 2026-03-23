# HR Datamart V3 — Change Plan

**Date:** 2026-03-23
**Baseline:** HR Datamart V2 (`hr-datamart-v2/`)
**Target:** HR Datamart V3 (`hr-datamart-v3/`)
**Seed:** 42
**Timestamp:** 20260323060000

---

## Summary of Change Requests

| CR | INT | Feed Name | Type | Description |
|----|-----|-----------|------|-------------|
| CR1 | INT6021 | Job Profile | Field Reorder | Job_Profile_ID, Job_Profile_WID moved to columns 0 and 1 |
| CR2 | INT6022 | Job Classification | Schema Overhaul | Normalized to parent/child structure; new PK Job_Classification_ID |
| CR3 | INT6027 | Matrix Organization | New Field | Matrix_Organization_Description (varchar 200) appended |
| CR4 | INT6028 | Department Hierarchy | New Field | Owner_EIN_WID (varchar 32) inserted after Owner_EIN |
| CR5 | INT6031 | Worker Profile | Field Reorder + New Fields | Worker_ID/WID lead; Address_Line_1 + Address_Line_2 added; postal code typo fixed; INDIGENOUS deduped |
| CR6 | INT0095E | Worker Job | Field Rename | Worker_Sub-Type → Worker_Sub_Type (hyphen removed) |

---

## Detailed Change Specifications

### CR1 — INT6021 Job Profile (Field Reorder)
**Ticket:** INT6021
**Files modified:** `generators/reference_data.py`

**V2 field order (first 3 columns):**
```
Compensation_Grade, Critical_Job_Flag, Difficult_to_Fill_Flag, ...
```

**V3 field order (first 3 columns):**
```
Job_Profile_ID, Job_Profile_WID, Compensation_Grade, ...
```

**Full V3 field order:**
```
Job_Profile_ID, Job_Profile_WID, Compensation_Grade, Critical_Job_Flag,
Difficult_to_Fill_Flag, Inactive_Flag, Job_Category_Code, Job_Category_Name,
Job_Exempt_Canada, Job_Exempt_US, Job_Family, Job_Family_Group,
Job_Family_Group_Name, Job_Family_Name, Job_Level_Code, Job_Level_Name,
Job_Profile_Code, Job_Profile_Description, Job_Profile_Name,
Job_Profile_Summary, Job_Title, Management_Level_Code, Management_Level_Name,
Pay_Rate_Type, Public_Job, Work_Shift_Required, JOB_MATRIX,
IS_PEOPLE_MANAGER, IS_MANAGER, FREQUENCY
```

**Impact:** Data content unchanged; column positions shifted. Downstream consumers ordering by column index must update to use named columns.

---

### CR2 — INT6022 Job Classification (Schema Overhaul)
**Ticket:** INT6022
**Files modified:** `generators/reference_data.py`, `generators/config.py`

**V2 schema (13 columns, one row per job profile):**
```
Job_Profile_ID, Job_Profile_WID, AAP_Job_Group, Bonus_Eligibility,
Customer_Facing, EEO1_Code, Job_Collection, Loan_Originator_Code,
National_Occupation_Code, Occupation_Code, Recruitment_Channel,
Standard_Occupation_Code, Stock
```

**V3 schema (6 columns, normalized parent/child):**
```
Job_Classification_ID  — PK (JCL_NNNN)
Job_Classification_WID — Workday ID (32-char hex)
Job_Classification_Name — e.g. "Software Engineer - Classification"
Job_Classification_Group_ID — references classification group (JCL_GRP_*)
Job_Classification_Group_Name — human-readable group label
Job_Profile_ID — FK to INT6021.Job_Profile_ID
```

**Classification groups defined in `config.JOB_CLASSIFICATION_GROUPS`:**
| Group ID | Group Name |
|----------|-----------|
| JCL_GRP_EXEC | Executive Leadership |
| JCL_GRP_TECH | Technology Professional |
| JCL_GRP_FIN | Finance & Accounting |
| JCL_GRP_RISK | Risk & Compliance |
| JCL_GRP_OPS | Operations |
| JCL_GRP_HR | Human Resources |
| JCL_GRP_LEGAL | Legal & Compliance |
| JCL_GRP_SALES | Sales & Advisory |
| JCL_GRP_MKT | Marketing & Communications |
| JCL_GRP_INVEST | Investment Management |
| JCL_GRP_ADMIN | Administrative Support |

**Impact:** Breaking change. Consumers reading old flat fields (AAP_Job_Group, EEO1_Code, etc.) must migrate to new structure. Row count unchanged (94 rows, one per job profile), but cardinality model changes.

---

### CR3 — INT6027 Matrix Organization (New Field)
**Ticket:** INT6027
**Files modified:** `generators/reference_data.py`, `generators/config.py`

**New field:** `Matrix_Organization_Description` (varchar 200), appended as last column.

**V3 field order:**
```
Matrix_Organization_ID, Matrix_Organization_Status, Maxtrix_Organization_Name,
Maxtrix_Organization_Code, Matrix_Organization_Type, Matrix_Organization_SubType,
Matrix_Organization_Description
```
*(Note: "Maxtrix" typos preserved from source schema.)*

**Content:** Each matrix org now carries a business-context description of ~150–200 chars. All 15 rows populated with non-empty descriptions.

**Impact:** Additive. Existing consumers can ignore the new column without breakage.

---

### CR4 — INT6028 Department Hierarchy (New Field)
**Ticket:** INT6028
**Files modified:** `generators/reference_data.py`

**New field:** `Owner_EIN_WID` (varchar 32), inserted immediately after `Owner_EIN`.

**V3 field order:**
```
Department_ID, Department_WID, Department_Name, Dept_Name_with_Manager_Name,
Active, Parent_Dept_ID, Owner_EIN, Owner_EIN_WID, Department_Level,
PRIMARY_LOCATION_CODE, Type, Subtype
```

**Semantics:** References `Worker_Workday_ID` from INT6031 (Worker Profile). In V3 synthetic data, `Owner_EIN_WID` is populated with a generated WID at department-creation time. For production loads, this should be updated after employee data is loaded.

**Impact:** Additive (new column). `Owner_EIN` remains present and empty (same as V2).

---

### CR5 — INT6031 Worker Profile (Field Reorder + New Fields)
**Ticket:** INT6031
**Files modified:** `generators/transactional_data.py`, `generators/employee_timeline.py`

**V2 → V3 changes:**
1. `Worker_ID` moved to column 0 (was near column 22)
2. `Worker_Workday_ID` moved to column 1 (was final column)
3. `Address_Line_1` (varchar 200) — **new field**, inserted after `Indigenous`
4. `Address_Line_2` (varchar 200) — **new field**, inserted after `Address_Line_1`
5. `HOME_ADDRESS_COUNTRY_NAME` / `HOME_ADDRESS_REGION_NAME` moved inline with address block (were at end of record)
6. `Home_Addres_Postal_Code` typo corrected → `Home_Address_Postal_Code`
7. Duplicate `INDIGENOUS` column removed (was appearing twice)
8. `Worker_Workday_ID` removed from end (now at position 1)

**V3 field order (37 columns):**
```
Worker_ID, Worker_Workday_ID, Bank_of_the_West_Employee_ID, Date_of_Birth,
Enterprise_ID, Race_Ethnicity, Gender, Gender_Identity, Indigenous,
Address_Line_1, Address_Line_2, Home_Address_City, Home_Address_Country,
HOME_ADDRESS_COUNTRY_NAME, Home_Address_Region, HOME_ADDRESS_REGION_NAME,
Home_Address_Postal_Code, Last_Name, Legal_First_Name, Legal_Full_Name,
Legal_Full_Name_Formatted, Military_Status, Preferred_First_Name,
Preferred_Full_Name, Preferred_Full_Name_Formatted, Primary_Work_Email_Address,
Secondary_Work_Email_Address, Sexual_Orientation, Junior_Senior,
Product_Sector_Group, Preferred_Language,
Bonus/Equity_Earliest_Retirement_Date, Class_Year, Admin_FTE,
CONSOLIDATED_TITLE, GENERATION, PENSIONABLE_YRS_OF_SERVICE
```

**Impact:** Breaking change. All consumers must update column references. Address fields now available for geo-analytics.

---

### CR6 — INT0095E Worker Job (Field Rename)
**Ticket:** INT0095E
**Files modified:** `generators/transactional_data.py`

**Change:** `Worker_Sub-Type` → `Worker_Sub_Type` (hyphen replaced with underscore in column header and dict key)

**Impact:** Breaking change for any consumer referencing the column by name. Data values unchanged.

---

## Files Changed from V2 Baseline

| File | Change Type | CRs |
|------|-------------|-----|
| `generators/config.py` | Modified | CR2 (new groups), CR3 (descriptions in MATRIX_ORGS), seed/timestamp |
| `generators/reference_data.py` | Modified | CR1, CR2, CR3, CR4 |
| `generators/transactional_data.py` | Modified | CR5, CR6 |
| `generators/employee_timeline.py` | Modified | CR5 (new EmployeeProfile fields) |
| `generators/utils.py` | Unchanged | — |
| `generate_all.py` | Minor update | Banner only |

## Backward Compatibility

| Feed | Breaking | Notes |
|------|----------|-------|
| INT6020 | No | Unchanged |
| INT6021 | Partial | Column positions shifted; named-column access safe |
| INT6022 | Yes | Completely new schema |
| INT6023 | No | Unchanged |
| INT6024 | No | Unchanged |
| INT6025 | No | Unchanged |
| INT6027 | No | New column additive |
| INT6028 | No | New column additive |
| INT6031 | Yes | Column positions shifted; new fields added |
| INT6032 | No | Unchanged |
| INT0095E | Partial | Column rename; data values unchanged |
| INT0096 | No | Unchanged |
| INT0098 | No | Unchanged |
| INT270 | No | Unchanged |
