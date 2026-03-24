#!/usr/bin/env python3
"""
validate_v3.py - Post-generation validation for HR Datamart V3.

Checks:
  1. Schema conformance  — all required columns present in each feed
  2. PK uniqueness       — primary key column has no duplicates
  3. Referential integrity — FK values exist in referenced feed
  4. CR-specific checks  — new/changed fields contain expected values

Usage:
    python3 validate_v3.py
"""

import csv
import os
import sys
from typing import Dict, List, Set, Tuple

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
TS = "20260323060000"

# ============================================================
# Feed definitions
# ============================================================

FEEDS: Dict[str, Dict] = {
    "INT6020": {
        "file": f"workday.hrdp.dly_grade_profile.full.{TS}.csv",
        "pk": "Grade_ID",
        "required_cols": ["Grade_ID", "Grade_Name", "Grade_Profile_Currency_Code"],
    },
    # CR1: Job_Profile_ID and Job_Profile_WID must be the first two columns
    "INT6021": {
        "file": f"workday.hrdp.dly_job_profile.full.{TS}.csv",
        "pk": "Job_Profile_ID",
        "required_cols": ["Job_Profile_ID", "Job_Profile_WID", "Compensation_Grade",
                          "JOB_MATRIX", "IS_PEOPLE_MANAGER", "IS_MANAGER", "FREQUENCY"],
        "col_order_check": ("Job_Profile_ID", 0),   # must be column 0
    },
    # CR2: Normalized schema
    # INT6022/CR: 11 standard groups; each job profile has exactly 11 rows
    "INT6022": {
        "file": f"workday.hrdp.dly_job_classification.full.{TS}.csv",
        "pk": "Job_Classification_ID",
        "required_cols": ["Job_Classification_ID", "Job_Classification_WID",
                          "Job_Classification_Name", "Job_Classification_Group_ID",
                          "Job_Classification_Group_Name", "Job_Profile_ID"],
        "fk": [("Job_Profile_ID", "INT6021", "Job_Profile_ID")],
        "classification_groups_check": True,  # verifies 11 groups present and N×11 row model
    },
    "INT6023": {
        "file": f"workday.hrdp.dly_location.full.{TS}.csv",
        "pk": "Location_ID",
        "required_cols": ["Location_ID", "Location_WID", "Location_Name"],
    },
    "INT6024": {
        "file": f"workday.hrdp.dly_company.full.{TS}.csv",
        "pk": "Company_ID",
        "required_cols": ["Company_ID", "Company_WID", "Company_Name"],
    },
    "INT6025": {
        "file": f"workday.hrdp.dly_cost_center.full.{TS}.csv",
        "pk": "Cost_Center_ID",
        "required_cols": ["Cost_Center_ID", "Cost_Center_WID", "Cost_Center_Name"],
    },
    # CR3: Matrix_Organization_Description must exist and be non-empty
    "INT6027": {
        "file": f"workday.hrdp.dly_matrix_organization.full.{TS}.csv",
        "pk": "Matrix_Organization_ID",
        "required_cols": ["Matrix_Organization_ID", "Matrix_Organization_Status",
                          "Maxtrix_Organization_Name", "Matrix_Organization_Description"],
        "non_empty_cols": ["Matrix_Organization_Description"],
    },
    # CR4: Owner_EIN_WID must be present
    "INT6028": {
        "file": f"workday.hrdp.dly_department_hierarchy.full.{TS}.csv",
        "pk": "Department_ID",
        "required_cols": ["Department_ID", "Department_WID", "Department_Name",
                          "Owner_EIN", "Owner_EIN_WID", "Department_Level",
                          "PRIMARY_LOCATION_CODE", "Type", "Subtype"],
    },
    # INT6032/CR: 8 new fields added
    "INT6032": {
        "file": f"workday.hrdp.dly_positions.full.{TS}.csv",
        "pk": "Position_ID",
        "required_cols": [
            "Position_ID", "Supervisory_Organization", "Worker_Type",
            "Work_Space", "Pay_Rate_Type", "Schedule_Weekly_Hours",
            "Scheduled_FTE", "Default_Weekly_Hours", "Employee_Type",
            "shift_number", "Exclude_From_Headcount",
        ],
        "fk": [("Job_Profile", "INT6021", "Job_Profile_ID")],
    },
    # CR6: Worker_Sub_Type (not Worker_Sub-Type)
    "INT0095E": {
        "file": f"workday.hrdp.dly_worker_job.full.{TS}.csv",
        "pk": "Transaction_WID",
        "required_cols": ["Employee_ID", "Transaction_WID", "Worker_Sub_Type",
                          "Worker_Workday_ID"],
        "absent_cols": ["Worker_Sub-Type"],   # old name must not appear
        "fk": [("Employee_ID", "INT6031", "Worker_ID"),
               ("Job_Profile_ID", "INT6021", "Job_Profile_ID")],
    },
    "INT0096": {
        "file": f"workday.hrdp.dly_worker_organization.full.{TS}.csv",
        "pk": None,
        "required_cols": ["Employee_ID", "Transaction_WID", "Organization_ID",
                          "Organization_Type", "Worker_Workday_ID"],
    },
    "INT0098": {
        "file": f"workday.hrdp.dly_worker_compensation.full.{TS}.csv",
        "pk": "Transaction_WID",
        "required_cols": ["Employee_ID", "Transaction_WID", "Worker_Workday_ID"],
    },
    "INT270": {
        "file": f"workday.hrdp.dly_rescinded_transactions.full.{TS}.csv",
        "pk": "workday_id",
        "required_cols": ["workday_id", "idp_table", "rescinded_moment"],
    },
    # CR5: Worker_ID and Worker_Workday_ID lead; Address_Line_1/2 exist;
    #      Home_Address_Postal_Code (not Home_Addres_Postal_Code); no INDIGENOUS dup
    "INT6031": {
        "file": f"workday.hrdp.dly_worker_profile.full.{TS}.csv",
        "pk": "Worker_ID",
        "required_cols": ["Worker_ID", "Worker_Workday_ID", "Address_Line_1",
                          "Address_Line_2", "Home_Address_Postal_Code",
                          "HOME_ADDRESS_COUNTRY_NAME", "HOME_ADDRESS_REGION_NAME"],
        "col_order_check": ("Worker_ID", 0),
        "absent_cols": ["Home_Addres_Postal_Code"],  # typo must be gone
    },
}


# ============================================================
# Helpers
# ============================================================

def load_feed(filepath: str) -> Tuple[List[str], List[Dict]]:
    """Load CSV feed, return (headers, rows)."""
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = reader.fieldnames or []
    return list(headers), rows


def check_pk_unique(rows: List[Dict], pk_col: str) -> List[str]:
    seen: Set[str] = set()
    dupes = []
    for row in rows:
        val = row.get(pk_col, "")
        if val in seen:
            dupes.append(val)
        seen.add(val)
    return dupes


def check_fk(child_rows: List[Dict], fk_col: str,
             parent_rows: List[Dict], parent_col: str) -> List[str]:
    parent_vals = {r.get(parent_col, "") for r in parent_rows}
    broken = []
    for row in child_rows:
        val = row.get(fk_col, "")
        if val and val not in parent_vals:
            broken.append(val)
    return broken[:10]  # cap at 10 examples


# ============================================================
# Main validation
# ============================================================

def main():
    print("=" * 70)
    print("  HR Datamart V3 — Validation Report")
    print(f"  Data directory: {DATA_DIR}")
    print("=" * 70)

    all_rows: Dict[str, List[Dict]] = {}
    errors = 0
    warnings = 0

    for feed_key, spec in FEEDS.items():
        filepath = os.path.join(DATA_DIR, spec["file"])
        print(f"\n[{feed_key}] {spec['file']}")

        if not os.path.exists(filepath):
            print(f"  ERROR: File not found")
            errors += 1
            continue

        headers, rows = load_feed(filepath)
        all_rows[feed_key] = rows
        print(f"  Rows: {len(rows):,}   Columns: {len(headers)}")

        # 1. Schema conformance — required columns present
        missing = [c for c in spec.get("required_cols", []) if c not in headers]
        if missing:
            print(f"  ERROR: Missing columns: {missing}")
            errors += 1
        else:
            print(f"  OK schema conformance ({len(spec.get('required_cols',[]))} required cols present)")

        # 2. Column order check
        order_check = spec.get("col_order_check")
        if order_check:
            col_name, expected_pos = order_check
            actual_pos = headers.index(col_name) if col_name in headers else -1
            if actual_pos != expected_pos:
                print(f"  ERROR: '{col_name}' is at position {actual_pos}, expected {expected_pos}")
                errors += 1
            else:
                print(f"  OK column order: '{col_name}' is at position {expected_pos}")

        # 3. Absent columns (renamed/removed)
        for col in spec.get("absent_cols", []):
            if col in headers:
                print(f"  ERROR: Deprecated column still present: '{col}'")
                errors += 1
            else:
                print(f"  OK absent: '{col}' correctly not present")

        # 4. PK uniqueness
        pk = spec.get("pk")
        if pk and pk in headers:
            dupes = check_pk_unique(rows, pk)
            if dupes:
                print(f"  ERROR: PK '{pk}' has {len(dupes)} duplicates, e.g.: {dupes[:3]}")
                errors += 1
            else:
                print(f"  OK PK uniqueness on '{pk}'")

        # 5a. INT6022/CR: verify 11 standard groups, N×11 row model
        if spec.get("classification_groups_check") and rows:
            EXPECTED_GROUPS = {
                "AAP Job Group", "Bonus Eligibility", "Customer Facing", "EEO1 Code",
                "Job Collection", "Loan Originator Code", "National Occupation Code",
                "Occupation Code", "Recruitment Channel", "Standard Occupation Code", "Stock",
            }
            actual_groups = {r.get("Job_Classification_Group_Name", "") for r in rows}
            missing_groups = EXPECTED_GROUPS - actual_groups
            extra_groups   = actual_groups - EXPECTED_GROUPS
            if missing_groups:
                print(f"  ERROR: Missing classification groups: {sorted(missing_groups)}")
                errors += 1
            elif extra_groups:
                print(f"  ERROR: Unexpected classification groups: {sorted(extra_groups)}")
                errors += 1
            else:
                print(f"  OK all 11 standard classification groups present")
            # Verify each job profile has exactly 11 rows
            from collections import Counter
            per_profile = Counter(r.get("Job_Profile_ID", "") for r in rows)
            bad_profiles = {jp: cnt for jp, cnt in per_profile.items() if cnt != 11}
            if bad_profiles:
                examples = list(bad_profiles.items())[:3]
                print(f"  ERROR: {len(bad_profiles)} job profile(s) don't have exactly 11 rows, e.g. {examples}")
                errors += 1
            else:
                print(f"  OK N×11 model: all {len(per_profile):,} job profiles have exactly 11 classifications")

        # 5b. Non-empty columns
        for col in spec.get("non_empty_cols", []):
            empty_count = sum(1 for r in rows if not r.get(col, "").strip())
            if empty_count > 0:
                print(f"  WARN: '{col}' has {empty_count} empty values")
                warnings += 1
            else:
                print(f"  OK non-empty: '{col}' populated in all {len(rows)} rows")

    # 6. Referential integrity (after all feeds loaded)
    print("\n--- Referential Integrity ---")
    for feed_key, spec in FEEDS.items():
        for fk_col, parent_key, parent_col in spec.get("fk", []):
            if feed_key not in all_rows or parent_key not in all_rows:
                continue
            broken = check_fk(all_rows[feed_key], fk_col,
                               all_rows[parent_key], parent_col)
            if broken:
                print(f"  ERROR [{feed_key}] '{fk_col}' -> [{parent_key}] '{parent_col}': "
                      f"{len(broken)} broken FKs, e.g. {broken[:3]}")
                errors += 1
            else:
                child_count = len([r for r in all_rows[feed_key] if r.get(fk_col)])
                print(f"  OK [{feed_key}] '{fk_col}' -> [{parent_key}] all {child_count:,} non-null FKs resolve")

    print("\n" + "=" * 70)
    print(f"  Validation complete: {errors} error(s), {warnings} warning(s)")
    if errors == 0:
        print("  PASSED")
    else:
        print("  FAILED")
    print("=" * 70)

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
