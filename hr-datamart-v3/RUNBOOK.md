# HR Datamart V3 — Runbook

## Overview

HR Datamart V3 is a synthetic Workday HRDP data generator for WARLab. It produces 14 CSV feeds covering employee lifecycle events, organizational hierarchy, compensation, and reference data.

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Python | 3.9+ |
| Faker | `pip install faker` |

Install dependencies:
```bash
pip install faker
```

---

## Directory Structure

```
hr-datamart-v3/
├── generate_all.py          # Main orchestrator — run this
├── validate_v3.py           # Post-generation validation script
├── CHANGE_PLAN.md           # Detailed CR specifications
├── CHANGELOG.md             # Version history
├── RUNBOOK.md               # This file
├── generators/
│   ├── __init__.py
│   ├── config.py            # Seed, dates, org structure, feed config
│   ├── reference_data.py    # INT6020-6028 generators
│   ├── employee_timeline.py # Employee event simulation + INT6031 profiles
│   ├── transactional_data.py # INT0095E/0096/0098/270/6031 writers
│   └── utils.py             # Shared utilities
└── data/
    └── workday.hrdp.dly_*.full.20260323060000.csv
```

---

## Generating Data

From the `hr-datamart-v3/` directory:

```bash
cd hr-datamart-v3
python3 generate_all.py
```

Expected runtime: **~30 seconds** on a modern Mac Studio.

Expected output to `data/`:
```
workday.hrdp.dly_grade_profile.full.20260323060000.csv          15 rows
workday.hrdp.dly_job_profile.full.20260323060000.csv            94 rows
workday.hrdp.dly_job_classification.full.20260323060000.csv     94 rows
workday.hrdp.dly_location.full.20260323060000.csv               16 rows
workday.hrdp.dly_company.full.20260323060000.csv                 8 rows
workday.hrdp.dly_cost_center.full.20260323060000.csv           198 rows
workday.hrdp.dly_matrix_organization.full.20260323060000.csv    15 rows
workday.hrdp.dly_department_hierarchy.full.20260323060000.csv  144 rows
workday.hrdp.dly_positions.full.20260323060000.csv          20,959 rows
workday.hrdp.dly_worker_job.full.20260323060000.csv        118,569 rows
workday.hrdp.dly_worker_organization.full.20260323060000.csv 355,707 rows
workday.hrdp.dly_worker_compensation.full.20260323060000.csv 118,569 rows
workday.hrdp.dly_rescinded_transactions.full.20260323060000.csv 1,778 rows
workday.hrdp.dly_worker_profile.full.20260323060000.csv     20,959 rows
```

---

## Validation

Run after every generation to confirm schema, PKs, and referential integrity:

```bash
python3 validate_v3.py
```

The script exits with code 0 on pass, 1 on failure. Checks performed:

| Check | Feeds |
|-------|-------|
| Schema conformance (required columns present) | All 14 |
| Primary key uniqueness | All keyed feeds |
| Column order (CR1: INT6021 col 0, CR5: INT6031 col 0) | INT6021, INT6031 |
| Deprecated column absent (CR5 typo, CR6 rename) | INT6031, INT0095E |
| New field non-empty (CR3 description) | INT6027 |
| Referential integrity | INT6022→INT6021, INT6032→INT6021, INT0095E→INT6031, INT0095E→INT6021 |

---

## Configuration

Key settings in `generators/config.py`:

| Setting | Value | Notes |
|---------|-------|-------|
| `SEED` | 42 | Controls all randomness; change to get different data |
| `FEED_TIMESTAMP` | `20260323060000` | Embedded in all filenames |
| `OUTPUT_DIR` | `data` | Relative to `hr-datamart-v3/` |
| `DATA_END_DATE` | 2026-02-13 | Employee timeline end |
| `COMPANY_FOUNDED` | 2016-02-13 | Employee timeline start |

To regenerate with a different seed, update `SEED` in `config.py` and re-run `generate_all.py`.

---

## Feed Reference

### Reference Feeds (generated first)

| Feed | File Pattern | PK | Key V3 Changes |
|------|-------------|-----|----------------|
| INT6020 | `grade_profile` | Grade_ID | None |
| INT6021 | `job_profile` | Job_Profile_ID | **CR1**: ID/WID columns lead |
| INT6022 | `job_classification` | Job_Classification_ID | **CR2**: Normalized schema |
| INT6023 | `location` | Location_ID | None |
| INT6024 | `company` | Company_ID | None |
| INT6025 | `cost_center` | Cost_Center_ID | None |
| INT6027 | `matrix_organization` | Matrix_Organization_ID | **CR3**: Description added |
| INT6028 | `department_hierarchy` | Department_ID | **CR4**: Owner_EIN_WID added |
| INT6032 | `positions` | Position_ID | None |

### Transactional Feeds (generated from employee timelines)

| Feed | File Pattern | PK | Key V3 Changes |
|------|-------------|-----|----------------|
| INT0095E | `worker_job` | Transaction_WID | **CR6**: Worker_Sub_Type renamed |
| INT0096 | `worker_organization` | — (3 rows/event) | None |
| INT0098 | `worker_compensation` | Transaction_WID | None |
| INT270 | `rescinded_transactions` | workday_id | None |
| INT6031 | `worker_profile` | Worker_ID | **CR5**: Reorder + Address fields |

---

## CR-Specific Notes

### CR2 Migration (INT6022)
The old V2 classification fields (EEO1, NOC, SOC, Bonus_Eligibility, etc.) are no longer in this feed. If downstream systems need EEO or occupation codes, they should be sourced from an alternate classification lookup or HR system of record.

### CR4 Owner_EIN_WID (INT6028)
The `Owner_EIN_WID` field in synthetic data is populated with a generated WID at department-creation time. In production, this field should be loaded as a post-processing step after employee WIDs are established, using a department-to-manager assignment lookup.

### CR5 Address Fields (INT6031)
`Address_Line_1` and `Address_Line_2` are generated using Faker with street number, name, and type components. They represent home addresses, not work locations (work location is in INT0095E.Business_Site_ID).

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `ModuleNotFoundError: faker` | Faker not installed | `pip install faker` |
| `FileNotFoundError` in validate_v3.py | Data not generated | Run `generate_all.py` first |
| Validation FAIL on FK check | Seed or config mismatch | Regenerate with correct seed |
| Different row counts than expected | Seed changed | Verify `config.SEED = 42` |

---

## Git

Branch: `claude/friendly-cannon`
Remote: `https://github.com/WARLAB01/warlab-projects.git`

```bash
git log --oneline -5
git diff main...HEAD --stat
```
