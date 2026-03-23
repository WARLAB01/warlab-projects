# HR Datamart V3 — Validation Summary

**Generated:** 2026-03-23
**Seed:** 20160213
**Period:** 2016-02-13 → 2026-02-13
**Result:** ✅ ALL CHECKS PASSED (15/15)

---

## Feed Row Counts

| Feed | Rows | Cols | Description |
|------|-----:|-----:|-------------|
| INT6020 | 15 | 15 | Grade Profiles |
| INT6021 | 94 | **30** | Job Profiles (field order fixed) |
| INT6022 | 94 | **6** | Job Classification (normalized) |
| INT6023 | 16 | 19 | Locations |
| INT6024 | 8 | 7 | Companies |
| INT6025 | 198 | 6 | Cost Centers |
| INT6027 | 15 | **7** | Matrix Organizations (+Description) |
| INT6028 | 144 | **12** | Department Hierarchy (+Owner_EIN_WID) |
| INT6031 | 20,970 | **37** | Worker Profiles (+Address_Line_1/2) |
| INT6032 | 20,970 | 11 | Positions |
| INT0095E | 117,805 | 81 | Worker Job (Worker_Sub_Type renamed) |
| INT0096 | 353,415 | 9 | Worker Organization |
| INT0098 | 117,805 | 19 | Worker Compensation |
| INT270 | 1,767 | 3 | Rescinded Transactions |

---

## V3 Schema Changes Verified

| CR | Feed | Change | Status |
|----|------|--------|--------|
| INT6021 | Job Profile | Field order fixed: Job_Profile_ID first (col 1), Job_Profile_WID second (col 2); 30 fields | ✅ |
| INT6022 | Job Classification | Full normalization: 6-field parent/child (Job_Classification_ID PK + 5 fields) | ✅ |
| INT6027 | Matrix Organization | Matrix_Organization_Description (VARCHAR 200) added at end | ✅ |
| INT6028 | Department Hierarchy | Owner_EIN_WID added after Owner_EIN; 12-field spec complete | ✅ |
| INT6031 | Worker Profile | Address_Line_1 + Address_Line_2 added; Worker_ID is PK (col 1); postal code typo fixed; duplicate INDIGENOUS removed; 37 fields | ✅ |
| INT0095E | Worker Job | Worker_Sub-Type → Worker_Sub_Type (col 9) | ✅ |

---

## Validation Checks (15/15 PASS)

### Row Count Checks
- ✅ INT0095E has > 100k events (117,805)
- ✅ INT0096 = 3× INT0095E (353,415 = 3 × 117,805)
- ✅ INT0098 = INT0095E (117,805)
- ✅ INT6031 profiles > 10k (20,970)
- ✅ INT6032 positions = INT6031 profiles (20,970)

### Referential Integrity
- ✅ WID set: INT0095E == INT0098
- ✅ WID set: INT0096 WIDs ⊆ INT0095E WIDs
- ✅ Employee IDs: INT0095E == INT6031
- ✅ Job Profile IDs: INT0095E ⊆ INT6021
- ✅ Company IDs: INT0096 ⊆ INT6024
- ✅ Cost Center IDs: INT0096 ⊆ INT6025
- ✅ Location names: INT0095E ⊆ INT6023
- ✅ INT270 WIDs ⊆ transactional WIDs
- ✅ No events before founding date (2016-02-13)

### Headcount
- ✅ Final headcount 10,055 (target: 10,000–11,000)

### Warnings
- ⚠️ matplotlib not installed — charts not generated (non-blocking)

---

## Headcount Curve

| Year | Actual | Target |
|------|-------:|-------:|
| 2016 | 2,011 | ~2,000 |
| 2017 | 5,013 | ~5,000 |
| 2018 | 8,015 | ~8,000 |
| 2019 | 9,508 | ~9,500 |
| 2020 | 10,006 | ~10,000 |
| 2021 | 10,206 | ~10,000 |
| 2022 | 9,802 | ~10,000 |
| 2023 | 10,109 | ~10,000 |
| 2024 | 10,304 | ~10,000 |
| 2025 | 10,002 | ~10,000 |
| 2026 | 10,055 | ~10,000 |

---

## Demographics

**Gender:** Male 51.7% | Female 45.2% | Non-Binary 2.0% | Not Disclosed 1.2%
**Generations:** Millennial 41.5% | Gen X 28.0% | Gen Z 18.3% | Baby Boomer 12.1%
**Attrition mix:** Voluntary 58.0% | Involuntary 24.5% | Retirement 10.4% | EOC 6.2% | Death 0.9%
