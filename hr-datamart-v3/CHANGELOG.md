# HR Datamart V3 — Changelog

## [3.0.0] — 2026-03-23

### Summary
HR Datamart V3 applies six change requests to the V2 generator baseline. The synthetic data uses seed 42 and timestamp 20260323060000. All 14 feeds generated with 0 validation errors.

### Generation stats
| Metric | Value |
|--------|-------|
| Unique employees | 20,959 |
| Total events | 118,569 |
| Active at end-date | 10,055 |
| Worker profiles (INT6031) | 20,959 |
| Positions (INT6032) | 20,959 |
| Rescinded transactions (INT270) | 1,778 |
| Run time | ~28 seconds |

---

### Changed

#### CR1 — INT6021 Job Profile — Field Reorder
- `Job_Profile_ID` moved to column position 0
- `Job_Profile_WID` moved to column position 1
- All other fields follow; content unchanged

#### CR2 — INT6022 Job Classification — Schema Normalization
- **Removed:** `AAP_Job_Group`, `Bonus_Eligibility`, `Customer_Facing`, `EEO1_Code`, `Job_Collection`, `Loan_Originator_Code`, `National_Occupation_Code`, `Occupation_Code`, `Recruitment_Channel`, `Standard_Occupation_Code`, `Stock`
- **Added:** `Job_Classification_ID` (new PK), `Job_Classification_WID`, `Job_Classification_Name`, `Job_Classification_Group_ID`, `Job_Classification_Group_Name`
- `Job_Profile_ID` retained as foreign key reference to INT6021

#### CR3 — INT6027 Matrix Organization — New Field
- **Added:** `Matrix_Organization_Description` (varchar 200) as final column
- All 15 matrix organizations populated with descriptive text

#### CR4 — INT6028 Department Hierarchy — New Field
- **Added:** `Owner_EIN_WID` (varchar 32) after `Owner_EIN`
- Semantically references `Worker_Workday_ID` from INT6031

#### CR5 — INT6031 Worker Profile — Field Reorder and New Fields
- **Added:** `Address_Line_1` (varchar 200)
- **Added:** `Address_Line_2` (varchar 200)
- **Fixed:** `Home_Addres_Postal_Code` typo → `Home_Address_Postal_Code`
- **Moved:** `Worker_ID` to column 0, `Worker_Workday_ID` to column 1
- **Moved:** `HOME_ADDRESS_COUNTRY_NAME`, `HOME_ADDRESS_REGION_NAME` inline with address block
- **Removed:** Duplicate `INDIGENOUS` column (was appearing at end of record)
- **Removed:** `Worker_Workday_ID` from end position (now leads at column 1)

#### CR6 — INT0095E Worker Job — Field Rename
- **Renamed:** `Worker_Sub-Type` → `Worker_Sub_Type` (hyphen replaced by underscore)

---

### Unchanged
- INT6020 Grade Profile
- INT6023 Location
- INT6024 Company
- INT6025 Cost Center
- INT6032 Positions
- INT0096 Worker Organization
- INT0098 Worker Compensation
- INT270 Rescinded Transactions

---

### Configuration Changes (V2 → V3)
| Setting | V2 | V3 |
|---------|----|----|
| Seed | 20160213 | 42 |
| Feed timestamp | 20260213120000 | 20260323060000 |
| Output directory | data/feeds | data |

---

## [2.0.0] — 2026-02-13

Initial V2 release. Baseline for V3 changes.
