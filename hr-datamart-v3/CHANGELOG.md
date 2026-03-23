# HR Datamart V3 — Changelog

## [v3.0.0] — 2026-03-23

### Added
- **INT6027** `Matrix_Organization_Description` (VARCHAR 200) — new field at end of Matrix Organization feed. Auto-generated description based on type, subtype, and name.
- **INT6028** `Owner_EIN_WID` — new FK field after `Owner_EIN`, referencing `Worker_Workday_ID` from worker profile. Completes the 12-field department hierarchy spec.
- **INT6031** `Address_Line_1` — physical street address line 1 for each worker profile (e.g., "1234 Elm Street").
- **INT6031** `Address_Line_2` — secondary address line (e.g., "Apt 42", or empty for ~60% of workers).

### Changed
- **INT6021** Field order fixed. `Job_Profile_ID` is now column 1 (was column 17), `Job_Profile_WID` is now column 2 (was column 20). All 30 fields present and unchanged in content.
- **INT6022** Full schema normalization. Old 13-field flat format replaced with normalized 6-field parent/child structure: `Job_Classification_ID`, `Job_Classification_WID`, `Job_Classification_Name`, `Job_Classification_Group_ID`, `Job_Classification_Group_Name`, `Job_Profile_ID`. One classification row per job profile; classification groups derived from job family groups.
- **INT6031** Field order fixed. `Worker_ID` is now column 1 (PK), `Worker_Workday_ID` column 2. `Address_Line_1` and `Address_Line_2` added at columns 10–11.
- **INT6031** Postal code column renamed: `Home_Addres_Postal_Code` → `Home_Address_Postal_Code` (typo fixed).
- **INT0095E** `Worker_Sub-Type` renamed to `Worker_Sub_Type` (hyphen replaced with underscore for SQL compatibility).

### Removed
- **INT6031** Duplicate `INDIGENOUS` column removed (was redundant with `Indigenous`; column count changes from 36 to 37 due to the two new address fields).

### Infrastructure
- Generator version string updated to V3.
- `validate_dataset.py` data path made absolute for portability.
- All changes are deterministic (seed 20160213) and fully reproducible.

---

## [v2.0.0] — 2026-02-13 (Baseline)

Initial v2 release. See `hr-datamart-v2/` for details.
