# HR Datamart V3 — Change Plan

**Date:** 2026-03-23
**Base:** hr-datamart-v2
**Target:** hr-datamart-v3

---

## Change Requests Applied

### INT6021 — Job Profile
**File:** `workday.hrdp.dly_job_profile.full.*`
**Type:** Schema / Field Order
**Change:** Fix field order to match spec. Job_Profile_ID is now column 1, Job_Profile_WID column 2. All 30 fields present (JOB_MATRIX, IS_PEOPLE_MANAGER, IS_MANAGER, FREQUENCY were already in v2).
**Approach:** Update `FIELD_ORDERS["INT6021"]` in `reference_data.py`. No data-generation logic changes needed — all fields were already being generated.

---

### INT6022 — Job Classification
**File:** `workday.hrdp.dly_job_classification.full.*`
**Type:** Full Schema Normalization
**Change:** Replace 13-field flat format with normalized 6-field parent/child structure:
- `Job_Classification_ID` (PK, generated as `JC_NNNN`)
- `Job_Classification_WID` (32-char hex WID)
- `Job_Classification_Name` (derived from Job_Profile_Name)
- `Job_Classification_Group_ID` (parent group ID, generated as `JCG_NNNN`)
- `Job_Classification_Group_Name` (derived from Job_Family_Group_Name)
- `Job_Profile_ID` (FK to INT6021)
**Approach:** Rewrite `_gen_job_classifications()` in `reference_data.py`. Build group map from job_profiles, then generate one classification row per profile.

---

### INT6027 — Matrix Organization
**File:** `workday.hrdp.dly_matrix_organization.full.*`
**Type:** New Field
**Change:** Add `Matrix_Organization_Description` (VARCHAR 200) at end of each row.
**Approach:** Add field to `_gen_matrix_orgs()` dict and `FIELD_ORDERS["INT6027"]`. Description is auto-generated from type/subtype/name.

---

### INT6028 — Department Hierarchy
**File:** `workday.hrdp.dly_department_hierarchy.full.*`
**Type:** New Field (FK)
**Change:** Add `Owner_EIN_WID` after `Owner_EIN`. Full 12-field spec complete.
**Approach:** Generate synthetic WID for each department using seeded RNG. Field is a FK placeholder to Worker_Workday_ID from worker profile (populated with synthetic WIDs since department/employee generation order prevents exact FK assignment at generation time).

---

### INT6031 — Worker Profile
**File:** `workday.hrdp.dly_worker_profile.full.*`
**Type:** New Fields + Field Order + Bug Fixes
**Changes:**
- `Address_Line_1` added (e.g., "1234 Elm Street")
- `Address_Line_2` added (e.g., "Apt 42" or empty)
- Field order fixed: `Worker_ID` is now col 1 (PK), `Worker_Workday_ID` col 2
- Postal code field name typo fixed: `Home_Addres_Postal_Code` → `Home_Address_Postal_Code`
- Duplicate `INDIGENOUS` column removed (was redundant with `Indigenous`)
- Total: 37 fields
**Approach:** Add `address_line_1`, `address_line_2` to `EmployeeProfile` dataclass in `employee_timeline.py`. Populate using `faker.street_name()` with seeded RNG. Update `_profile_to_6031()` and `FIELD_ORDERS["INT6031"]` in `transactional_data.py`.

---

### INT0095E — Worker Job
**File:** `workday.hrdp.dly_worker_job.full.*`
**Type:** Field Rename
**Change:** `Worker_Sub-Type` → `Worker_Sub_Type` (hyphen to underscore)
**Approach:** Update `FIELD_ORDERS["INT0095E"]` and `_event_to_095e()` dict key in `transactional_data.py`.

---

## Files Modified

| File | Changes |
|------|---------|
| `generators/reference_data.py` | INT6021 FIELD_ORDERS, INT6022 generator + FIELD_ORDERS, INT6027 + FIELD_ORDERS, INT6028 + FIELD_ORDERS |
| `generators/employee_timeline.py` | EmployeeProfile dataclass: address_line_1, address_line_2; _gen_profile() |
| `generators/transactional_data.py` | INT0095E FIELD_ORDERS + dict key, INT6031 FIELD_ORDERS + _profile_to_6031() |

## Files Unchanged

| File | Reason |
|------|--------|
| `generators/config.py` | No config changes required |
| `generators/utils.py` | No utility changes required |
| `generate_all.py` | Pipeline unchanged (version string updated only) |

## Backward Compatibility

v2 data and generators are untouched in `hr-datamart-v2/`. The v3 dataset is a separate independent artifact.
