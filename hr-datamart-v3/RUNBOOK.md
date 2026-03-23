# HR Datamart V3 — Runbook

## Overview

This runbook covers environment setup, generation, validation, and troubleshooting for the WARLab HR Datamart V3 synthetic dataset.

**Seed:** `20160213` (fully deterministic)
**Period:** 2016-02-13 → 2026-02-13
**Output:** 14 CSV feeds in `data/feeds/`

---

## Prerequisites

```bash
python3 --version   # 3.9+
pip3 install faker pandas numpy
```

---

## Generate All CSV Feeds

```bash
cd hr-datamart-v3/
python3 generate_all.py
```

Expected output:
- INT6020: 15 rows
- INT6021: 94 rows (30 cols)
- INT6022: 94 rows (6 cols, normalized)
- INT6023: 16 rows
- INT6024: 8 rows
- INT6025: 198 rows
- INT6027: 15 rows (7 cols)
- INT6028: 144 rows (12 cols)
- INT6031: ~20,970 rows (37 cols)
- INT6032: ~20,970 rows
- INT0095E: ~117,805 rows
- INT0096: ~353,415 rows
- INT0098: ~117,805 rows
- INT270: ~1,767 rows

Runtime: ~30 seconds on a modern machine.

---

## Validate Dataset

```bash
python3 validate_dataset.py
```

Expected: `15/15 checks PASS`. See `qa_evidence/VALIDATION_SUMMARY.md` for details.

---

## V3 Schema Changes Quick Reference

| Feed | Change | Key Detail |
|------|--------|-----------|
| INT6021 | Field order | `Job_Profile_ID` col 1, `Job_Profile_WID` col 2 |
| INT6022 | Normalization | 6 fields: ID, WID, Name, Group_ID, Group_Name, Profile_ID |
| INT6027 | New field | `Matrix_Organization_Description` at end |
| INT6028 | New field | `Owner_EIN_WID` after `Owner_EIN` |
| INT6031 | New fields + reorder | `Address_Line_1`, `Address_Line_2`; `Worker_ID` is col 1 |
| INT0095E | Rename | `Worker_Sub-Type` → `Worker_Sub_Type` |

---

## Output Files

All CSVs land in `data/feeds/` with the naming convention:

```
workday.hrdp.dly_{feed_name}.full.{timestamp}.csv
```

Where `timestamp = 20260213120000`.

---

## Reproducibility

Running `generate_all.py` multiple times will produce byte-for-byte identical output (deterministic RNG, fixed seed 20160213, no wall-clock or UUID dependencies).

---

## Backward Compatibility

- v2 data remains untouched in `hr-datamart-v2/`.
- v3 CSVs are a separate dataset — they are not updates to v2 files.
- SQL DDL for v3 should be derived from v2 DDL with the following adjustments:
  - `dly_job_profile`: reorder columns (no type changes)
  - `dly_job_classification`: drop all old columns; add 6 new columns
  - `dly_matrix_organization`: add `Matrix_Organization_Description VARCHAR(200)`
  - `dly_department_hierarchy`: add `Owner_EIN_WID VARCHAR(32)` after `Owner_EIN`
  - `dly_worker_profile`: add `Address_Line_1 VARCHAR(200)`, `Address_Line_2 VARCHAR(200)`; rename `Home_Addres_Postal_Code` → `Home_Address_Postal_Code`; drop `INDIGENOUS` (duplicate)
  - `dly_worker_job`: rename `Worker_Sub-Type` → `Worker_Sub_Type`

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ModuleNotFoundError: faker` | faker not installed | `pip3 install faker` |
| `ModuleNotFoundError: pandas` | pandas not installed | `pip3 install pandas numpy` |
| Different row counts than expected | RNG state affected by code change | Normal — any change to generation order shifts RNG trajectory |
| Charts not generated | matplotlib not installed | `pip3 install matplotlib` (optional, non-blocking) |
