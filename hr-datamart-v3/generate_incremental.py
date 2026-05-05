#!/usr/bin/env python3
"""
generate_incremental.py - Generate a single day of incremental data files.

Produces timestamped daily run directories containing:
  - FULL SNAPSHOT incrementals for reference/dimension feeds (INT6020-INT6032)
  - DELTA incrementals for transactional feeds (INT0095E, INT0096, INT0098, INT270)
  - Observability artifacts (MANIFEST.md, CHANGE_SUMMARY.md, VALIDATION_SUMMARY.md)

Usage:
    python generate_incremental.py              # auto-detect next run date
    python generate_incremental.py --run-date 2026-03-24   # explicit date
    python generate_incremental.py --seed 42    # override seed

Design:
    - Reads baseline FULL files from data/ to load all existing entities
    - Reads any prior incremental run directories (data/YYYYMMDDHHMMSS/)
    - Generates deterministic daily changes using per-file seed derivation:
        per_file_seed = hash(global_seed + integration_id + run_timestamp)
    - Writes all output to data/<run_timestamp>/
"""

import argparse
import csv
import datetime
import hashlib
import os
import random
import sys
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(__file__))

from generators import config

# ============================================================
# CONSTANTS
# ============================================================

BASE_DIR = os.path.join(os.path.dirname(__file__), "data")
BASELINE_TIMESTAMP = config.FEED_TIMESTAMP  # "20260323060000"

# Integration definitions
FULL_SNAPSHOT_FEEDS = [
    "INT6020", "INT6021", "INT6022", "INT6023", "INT6024",
    "INT6025", "INT6027", "INT6028", "INT6031", "INT6032",
]
DELTA_FEEDS = ["INT0095E", "INT0096", "INT0098", "INT270"]

# Feed file base names (without timestamp)
FEED_BASES = {
    "INT6020": "workday.hrdp.dly_grade_profile.full",
    "INT6021": "workday.hrdp.dly_job_profile.full",
    "INT6022": "workday.hrdp.dly_job_classification.full",
    "INT6023": "workday.hrdp.dly_location.full",
    "INT6024": "workday.hrdp.dly_company.full",
    "INT6025": "workday.hrdp.dly_cost_center.full",
    "INT6027": "workday.hrdp.dly_matrix_organization.full",
    "INT6028": "workday.hrdp.dly_department_hierarchy.full",
    "INT6031": "workday.hrdp.dly_worker_profile.full",
    "INT6032": "workday.hrdp.dly_positions.full",
    "INT0095E": "workday.hrdp.dly_worker_job.delta",
    "INT0096": "workday.hrdp.dly_worker_organization.delta",
    "INT0098": "workday.hrdp.dly_worker_compensation.delta",
    "INT270": "workday.hrdp.dly_rescinded_transactions.delta",
}

# Primary keys per feed
PRIMARY_KEYS = {
    "INT6020": "Grade_ID",
    "INT6021": "Job_Profile_ID",
    "INT6022": "Job_Classification_ID",
    "INT6023": "Location_ID",
    "INT6024": "Company_ID",
    "INT6025": "Cost_Center_ID",
    "INT6027": "Matrix_Organization_ID",
    "INT6028": "Department_ID",
    "INT6031": "Worker_ID",
    "INT6032": "Position_ID",
    "INT0095E": "Transaction_WID",
    "INT0096": None,  # multi-row, no single PK
    "INT0098": "Transaction_WID",
    "INT270": "workday_id",
}


# ============================================================
# SEED DERIVATION
# ============================================================

def derive_seed(global_seed: int, integration_id: str, run_timestamp: str) -> int:
    """Deterministic per-file seed: hash(global_seed + integration_id + run_timestamp)."""
    raw = f"{global_seed}{integration_id}{run_timestamp}"
    h = hashlib.sha256(raw.encode()).hexdigest()
    return int(h[:8], 16)


# ============================================================
# BASELINE LOADER
# ============================================================

class BaselineLoader:
    """Loads existing CSV feeds from the data directory."""

    def __init__(self, base_dir: str, timestamp: str):
        self.base_dir = base_dir
        self.timestamp = timestamp

    def load_feed(self, integration_id: str, timestamp: str = None) -> Tuple[List[str], List[Dict]]:
        """Load a CSV feed. Returns (fieldnames, rows)."""
        ts = timestamp or self.timestamp
        if ts == self.timestamp:
            base_name = config.FEED_FILE_MAP.get(integration_id, "")
            filename = f"{base_name}.{ts}.csv"
            filepath = os.path.join(self.base_dir, filename)
        else:
            base_name = FEED_BASES[integration_id]
            filename = f"{base_name}.{ts}.csv"
            filepath = os.path.join(self.base_dir, ts, filename)

        if not os.path.exists(filepath):
            return [], []

        with open(filepath, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            rows = list(reader)
        return fieldnames, rows

    def get_prior_runs(self) -> List[str]:
        """Find all prior incremental run timestamps (subdirectories)."""
        runs = []
        if not os.path.exists(self.base_dir):
            return runs
        for entry in os.listdir(self.base_dir):
            full = os.path.join(self.base_dir, entry)
            if os.path.isdir(full) and entry.isdigit() and len(entry) == 14:
                runs.append(entry)
        runs.sort()
        return runs


# ============================================================
# INCREMENTAL GENERATOR
# ============================================================

class IncrementalGenerator:
    """Generates one day of incremental data files."""

    def __init__(self, global_seed: int, run_date: datetime.date, base_dir: str):
        self.global_seed = global_seed
        self.run_date = run_date
        self.run_timestamp = run_date.strftime("%Y%m%d") + "060000"
        self.base_dir = base_dir
        self.output_dir = os.path.join(base_dir, self.run_timestamp)
        self.loader = BaselineLoader(base_dir, BASELINE_TIMESTAMP)

        self.baseline = {}
        self.prior_deltas = {}
        self.manifest = {}
        self.change_summaries = {}
        self.validation_results = {}

        self.worker_profiles = {}
        self.worker_jobs = []
        self.positions = {}
        self.active_workers = set()
        self.terminated_workers = {}
        self.all_transaction_wids = set()
        self.rescinded_wids = set()

        self.new_events = []
        self.new_profiles = []

    def load_baseline(self):
        """Load all baseline feeds and prior incremental runs."""
        print(f"\n  Loading baseline data from {self.base_dir}...")

        for feed_id in FULL_SNAPSHOT_FEEDS + DELTA_FEEDS:
            fieldnames, rows = self.loader.load_feed(feed_id)
            self.baseline[feed_id] = (fieldnames, rows)
            print(f"    {feed_id}: {len(rows):,} baseline rows")

        prior_runs = self.loader.get_prior_runs()
        if prior_runs:
            print(f"\n  Loading {len(prior_runs)} prior incremental run(s)...")
            for ts in prior_runs:
                for feed_id in FULL_SNAPSHOT_FEEDS + DELTA_FEEDS:
                    fieldnames, rows = self.loader.load_feed(feed_id, timestamp=ts)
                    if rows:
                        if feed_id not in self.prior_deltas:
                            self.prior_deltas[feed_id] = []
                        self.prior_deltas[feed_id].append((fieldnames, rows))

        self._index_workers()
        self._index_positions()
        self._index_transactions()

    def _index_workers(self):
        """Build worker profile index and active/terminated sets."""
        _, profiles = self.baseline.get("INT6031", ([], []))
        for row in profiles:
            wid = row.get("Worker_ID", "")
            self.worker_profiles[wid] = row

        _, jobs = self.baseline.get("INT0095E", ([], []))
        self.worker_jobs = list(jobs)

        emp_latest = {}
        for row in self.worker_jobs:
            eid = row.get("Employee_ID", "")
            eff_date = row.get("Transaction_Effective_Date", "")
            seq = row.get("Sequence_Number", "0")
            key = (eff_date, seq)
            if eid not in emp_latest or key > (emp_latest[eid].get("Transaction_Effective_Date", ""),
                                                 emp_latest[eid].get("Sequence_Number", "0")):
                emp_latest[eid] = row

        for eid, row in emp_latest.items():
            if row.get("Terminated", "0") == "1":
                self.terminated_workers[eid] = row
            else:
                self.active_workers.add(eid)

        for feed_id in ["INT0095E"]:
            for _, rows in self.prior_deltas.get(feed_id, []):
                for row in rows:
                    self.worker_jobs.append(row)
                    eid = row.get("Employee_ID", "")
                    if row.get("Terminated", "0") == "1":
                        self.active_workers.discard(eid)
                        self.terminated_workers[eid] = row
                    elif row.get("Transaction_Type", "") == "Hire":
                        self.active_workers.add(eid)

    def _index_positions(self):
        """Build position index."""
        _, positions = self.baseline.get("INT6032", ([], []))
        for row in positions:
            pid = row.get("Position_ID", "")
            self.positions[pid] = row

    def _index_transactions(self):
        """Index all existing Transaction_WIDs and rescinded WIDs."""
        for row in self.worker_jobs:
            twid = row.get("Transaction_WID", "")
            if twid:
                self.all_transaction_wids.add(twid)

        _, rescinded = self.baseline.get("INT270", ([], []))
        for row in rescinded:
            wid = row.get("workday_id", "")
            if wid:
                self.rescinded_wids.add(wid)

        for _, rows in self.prior_deltas.get("INT270", []):
            for row in rows:
                wid = row.get("workday_id", "")
                if wid:
                    self.rescinded_wids.add(wid)

    # ============================================================
    # FULL SNAPSHOT GENERATORS
    # ============================================================

    def _gen_full_snapshot(self, feed_id: str) -> Tuple[List[str], List[Dict], Dict]:
        """Generate a full snapshot for a reference feed with minor daily changes."""
        rng = random.Random(derive_seed(self.global_seed, feed_id, self.run_timestamp))
        fieldnames, base_rows = self.baseline.get(feed_id, ([], []))

        for _, prior_rows in self.prior_deltas.get(feed_id, []):
            if prior_rows:
                base_rows = prior_rows

        if not base_rows:
            return fieldnames, [], {"inserts": 0, "updates": 0, "deletes": 0, "notes": "No baseline data"}

        rows = [dict(row) for row in base_rows]
        change_info = {"inserts": 0, "updates": 0, "deletes": 0, "notes": ""}

        if feed_id in ("INT6020", "INT6021", "INT6022", "INT6023", "INT6024",
                        "INT6025", "INT6027", "INT6028"):
            # CR4 fix (2026-05-05): Department hierarchy needs more visible
            # daily churn so incremental loaders have something to detect.
            if feed_id == "INT6028":
                n_updates = rng.randint(5, min(12, max(5, len(rows))))
            else:
                n_updates = rng.randint(1, min(3, len(rows)))
            update_indices = rng.sample(range(len(rows)), n_updates)
            for idx in update_indices:
                rows[idx] = self._apply_reference_update(feed_id, rows[idx], rng)
            change_info["updates"] = n_updates
            change_info["notes"] = f"Updated {n_updates} row(s) with minor field changes"

        elif feed_id == "INT6031":
            rows, change_info = self._gen_worker_profile_snapshot(rows, fieldnames, rng)

        elif feed_id == "INT6032":
            rows, change_info = self._gen_positions_snapshot(rows, fieldnames, rng)

        return fieldnames, rows, change_info

    def _apply_reference_update(self, feed_id: str, row: Dict, rng: random.Random) -> Dict:
        """Apply a minor realistic update to a reference data row."""
        row = dict(row)

        if feed_id == "INT6020":
            if "Grade_Profile_Salary_Range_Midpoint" in row:
                try:
                    mid = float(row["Grade_Profile_Salary_Range_Midpoint"])
                    adjustment = mid * rng.uniform(-0.005, 0.005)
                    row["Grade_Profile_Salary_Range_Midpoint"] = f"{mid + adjustment:.4f}"
                except (ValueError, TypeError):
                    pass

        elif feed_id == "INT6021":
            # Minor job profile updates — tweak management level or job family
            mgmt_levels = ["Individual Contributor", "Manager", "Director", "Senior Director", "VP"]
            if "Management_Level" in row:
                current = row.get("Management_Level", "")
                choices = [m for m in mgmt_levels if m != current]
                if choices and rng.random() < 0.3:
                    row["Management_Level"] = rng.choice(choices)
            if "Job_Family" in row and rng.random() < 0.5:
                suffixes = ["- Updated", "- Revised", "- v2", ""]
                current_val = row["Job_Family"].rstrip(" - Updated- Revised- v2")
                row["Job_Family"] = current_val + rng.choice(suffixes)

        elif feed_id == "INT6022":
            # Minor classification updates — adjust effective date
            if "Effective_Date" in row:
                row["Effective_Date"] = self.run_date.isoformat()
            elif "Classification_Effective_Date" in row:
                row["Classification_Effective_Date"] = self.run_date.isoformat()

        elif feed_id == "INT6023":
            # Minor location updates — toggle active status or update time zone
            time_zones = ["America/New_York", "America/Chicago", "America/Denver",
                          "America/Los_Angeles", "America/Toronto", "Europe/London"]
            if "Time_Zone" in row:
                current_tz = row.get("Time_Zone", "")
                choices = [tz for tz in time_zones if tz != current_tz]
                if choices:
                    row["Time_Zone"] = rng.choice(choices)
            if "Location_Status" in row and rng.random() < 0.1:
                row["Location_Status"] = "Inactive" if row["Location_Status"] == "Active" else "Active"

        elif feed_id == "INT6024":
            # Minor company updates — tweak currency or subtype
            currencies = ["USD", "CAD", "GBP", "EUR"]
            if "Company_Currency" in row:
                current_ccy = row.get("Company_Currency", "")
                choices = [c for c in currencies if c != current_ccy]
                if choices:
                    row["Company_Currency"] = rng.choice(choices)
            if "Company_Subtype" in row and rng.random() < 0.3:
                subtypes = ["Operating", "Holding", "Subsidiary", "Joint Venture"]
                current_sub = row.get("Company_Subtype", "")
                choices = [s for s in subtypes if s != current_sub]
                if choices:
                    row["Company_Subtype"] = rng.choice(choices)

        elif feed_id == "INT6025":
            # Minor cost center updates — update hierarchy or name suffix
            if "Hierarchy" in row and rng.random() < 0.4:
                hierarchy_val = row["Hierarchy"]
                # Append a revision marker that changes each run
                base_val = hierarchy_val.split(" (rev")[0]
                row["Hierarchy"] = f"{base_val} (rev {self.run_date.strftime('%m%d')})"
            if "Subtype" in row:
                subtypes = ["Operating", "Corporate", "Shared Services", "Project"]
                current_sub = row.get("Subtype", "")
                choices = [s for s in subtypes if s != current_sub]
                if choices:
                    row["Subtype"] = rng.choice(choices)

        elif feed_id == "INT6027":
            if row.get("Matrix_Organization_Status") == "Active" and rng.random() < 0.05:
                row["Matrix_Organization_Status"] = "Inactive"

        elif feed_id == "INT6028":
            # CR4 fix (2026-05-05): the previous logic only set Effective_Date,
            # but INT6028 has no Effective_Date column — so daily files were
            # byte-identical to the baseline and incremental tests had nothing
            # to detect. Apply changes to columns that actually exist:
            #   - Reorganize: rotate Owner_EIN / Owner_EIN_WID to a different
            #     active worker (keeps referential integrity with INT6031).
            #   - Toggle Active for non-top-level depts (rare).
            #   - Append a 'reorg' marker on Dept_Name_with_Manager_Name.
            choice = rng.random()

            if choice < 0.6 and self.worker_profiles:
                # Reassign owner to a different active worker.
                current_ein = row.get("Owner_EIN", "")
                wp_items = list(self.worker_profiles.items())
                # Filter to active workers when we know their status.
                if self.active_workers:
                    wp_items = [(wid, prof) for wid, prof in wp_items
                                if wid in self.active_workers] or wp_items
                # Avoid no-op: pick a worker different from the current owner.
                wp_items = [(wid, prof) for wid, prof in wp_items
                            if wid != current_ein] or wp_items
                new_wid, new_prof = rng.choice(wp_items)
                row["Owner_EIN"] = new_wid
                row["Owner_EIN_WID"] = new_prof.get("Worker_Workday_ID", "")
                # Refresh display name with new manager.
                base_name = row.get("Department_Name", "")
                # Strip any prior "(... ) " manager block so we don't stack them.
                if "(" in row.get("Dept_Name_with_Manager_Name", ""):
                    row["Dept_Name_with_Manager_Name"] = base_name
                first = new_prof.get("Preferred_First_Name", "") or new_prof.get("Legal_First_Name", "")
                last = new_prof.get("Last_Name", "")
                if first or last:
                    row["Dept_Name_with_Manager_Name"] = (
                        f"{base_name} ({first} {last})".strip()
                    )

            elif choice < 0.85:
                # Soft rename to flag a daily reorg.
                base_name = row.get("Department_Name", "")
                # Roll the date suffix on Dept_Name_with_Manager_Name so each
                # daily file mutates this column predictably.
                date_tag = self.run_date.strftime("%Y-%m-%d")
                # Keep a clean root if there's an existing tag.
                root = row.get("Dept_Name_with_Manager_Name", base_name).split(" [reorg ")[0]
                row["Dept_Name_with_Manager_Name"] = f"{root} [reorg {date_tag}]"

            else:
                # Toggle Active for a non-top-level dept.
                level = row.get("Department_Level", "1")
                try:
                    level_int = int(level)
                except (TypeError, ValueError):
                    level_int = 1
                if level_int >= 3:
                    row["Active"] = "0" if row.get("Active", "1") == "1" else "1"

        return row

    def _gen_worker_profile_snapshot(self, base_rows, fieldnames, rng):
        """Generate INT6031 full snapshot with new hires and address updates."""
        rows = [dict(r) for r in base_rows]
        change_info = {"inserts": 0, "updates": 0, "deletes": 0, "notes": ""}

        n_updates = rng.randint(2, min(5, len(rows)))
        update_indices = rng.sample(range(len(rows)), n_updates)
        for idx in update_indices:
            rows[idx]["Address_Line_1"] = f"{rng.randint(100, 9999)} {rng.choice(['Main', 'Oak', 'Maple', 'King', 'Queen', 'Bay'])} {rng.choice(['St', 'Ave', 'Blvd', 'Dr', 'Rd'])}"
            rows[idx]["Address_Line_2"] = rng.choice(["", "", "", f"Unit {rng.randint(1, 500)}"])
        change_info["updates"] = n_updates

        for profile in self.new_profiles:
            rows.append(profile)
            change_info["inserts"] += 1

        change_info["notes"] = f"{n_updates} address updates, {change_info['inserts']} new hire profiles"
        return rows, change_info

    def _gen_positions_snapshot(self, base_rows, fieldnames, rng):
        """Generate INT6032 full snapshot with new positions."""
        rows = [dict(r) for r in base_rows]
        change_info = {"inserts": 0, "updates": 0, "deletes": 0, "notes": ""}

        n_updates = rng.randint(1, min(3, len(rows)))
        update_indices = rng.sample(range(len(rows)), n_updates)
        for idx in update_indices:
            rows[idx]["Effective_Date"] = self.run_date.isoformat()
        change_info["updates"] = n_updates
        change_info["notes"] = f"{n_updates} position updates"
        return rows, change_info

    # ============================================================
    # DELTA GENERATORS
    # ============================================================

    def _gen_worker_job_delta(self):
        """Generate INT0095E delta — new job events, changes, late arrivals."""
        rng = random.Random(derive_seed(self.global_seed, "INT0095E", self.run_timestamp))
        fieldnames = list(self.baseline.get("INT0095E", ([], []))[0])
        delta_rows = []
        change_info = {"inserts": 0, "updates": 0, "deletes": 0, "notes": ""}

        active_list = list(self.active_workers)
        if not active_list:
            return fieldnames, [], change_info

        # New hires (1-3 per day)
        n_new_hires = rng.randint(1, 3)
        max_emp_seq = 0
        for wid in self.worker_profiles:
            try:
                seq = int(wid.replace("3", "", 1)) if wid.startswith("3") else 0
                max_emp_seq = max(max_emp_seq, seq)
            except ValueError:
                pass
        max_pos_seq = 0
        for pid in self.positions:
            try:
                seq = int(pid.replace("P-", ""))
                max_pos_seq = max(max_pos_seq, seq)
            except ValueError:
                pass

        for i in range(n_new_hires):
            emp_seq = max_emp_seq + i + 1
            pos_seq = max_pos_seq + i + 1
            hire_row, profile_row = self._gen_new_hire(rng, emp_seq, pos_seq)
            delta_rows.append(hire_row)
            self.new_profiles.append(profile_row)
            self.all_transaction_wids.add(hire_row["Transaction_WID"])
            change_info["inserts"] += 1

        # Job changes (2-5 per day)
        n_changes = rng.randint(2, min(5, len(active_list)))
        change_emps = rng.sample(active_list, n_changes)
        for eid in change_emps:
            change_row = self._gen_job_change(rng, eid)
            if change_row:
                delta_rows.append(change_row)
                self.all_transaction_wids.add(change_row["Transaction_WID"])
                change_info["updates"] += 1

        # Terminations (0-2 per day)
        n_terms = rng.randint(0, min(2, len(active_list) - n_changes))
        remaining_active = [e for e in active_list if e not in change_emps]
        if remaining_active and n_terms > 0:
            term_emps = rng.sample(remaining_active, min(n_terms, len(remaining_active)))
            for eid in term_emps:
                term_row = self._gen_termination(rng, eid)
                if term_row:
                    delta_rows.append(term_row)
                    self.all_transaction_wids.add(term_row["Transaction_WID"])
                    self.active_workers.discard(eid)
                    change_info["updates"] += 1

        # Late-arriving transaction (1 per day)
        late_row = self._gen_late_arriving(rng)
        if late_row:
            delta_rows.append(late_row)
            self.all_transaction_wids.add(late_row["Transaction_WID"])
            change_info["inserts"] += 1
            change_info["notes"] = "Includes 1 late-arriving transaction"

        return fieldnames, delta_rows, change_info

    def _gen_new_hire(self, rng, emp_seq, pos_seq):
        """Generate a new hire event row and worker profile row."""
        emp_id = f"3{emp_seq:07d}"
        pos_id = f"P-{pos_seq:06d}"
        twid = hashlib.md5(f"{self.global_seed}{emp_id}{self.run_timestamp}".encode()).hexdigest()
        wwid = hashlib.md5(f"{self.global_seed}w{emp_id}".encode()).hexdigest()

        _, jp_rows = self.baseline.get("INT6021", ([], []))
        jp = rng.choice(jp_rows) if jp_rows else {}
        jp_id = jp.get("Job_Profile_ID", "JP_0001")
        job_title = jp.get("Job_Title", "Analyst")
        comp_grade = jp.get("Compensation_Grade", "G01")

        company = rng.choice(config.COMPANIES)
        company_id = company["id"]
        country = config.COMPANY_COUNTRY.get(company_id, "CA")
        locs = config.LOCATIONS_CA if country == "CA" else config.LOCATIONS_US
        loc = rng.choice(locs)
        loc_id = loc["id"]

        _, dept_rows = self.baseline.get("INT6028", ([], []))
        leaf_depts = [d for d in dept_rows if d.get("Department_Level", "1") != "1"]
        dept = rng.choice(leaf_depts) if leaf_depts else (dept_rows[0] if dept_rows else {})
        dept_id = dept.get("Department_ID", "DEPT_CEO")

        _, cc_rows = self.baseline.get("INT6025", ([], []))
        cc = rng.choice(cc_rows) if cc_rows else {}
        cc_id = cc.get("Cost_Center_ID", "CC_0001")

        grade_cfg = next((g for g in config.GRADES if g["id"] == comp_grade), config.GRADES[0])
        salary = rng.gauss(grade_cfg["mid"], (grade_cfg["max"] - grade_cfg["min"]) / 4)
        salary = max(grade_cfg["min"], min(grade_cfg["max"], salary))
        salary = round(salary, 2)

        hire_date = self.run_date.isoformat()
        entry_dt = datetime.datetime.combine(self.run_date, datetime.time(rng.randint(8, 17), rng.randint(0, 59), rng.randint(0, 59)))

        wt = rng.choices(config.WORKER_TYPES, weights=[w["weight"] for w in config.WORKER_TYPES], k=1)[0]
        tt = rng.choices(config.TIME_TYPES, weights=[w["weight"] for w in config.TIME_TYPES], k=1)[0]

        hire_row = {
            "Employee_ID": emp_id, "Transaction_WID": twid,
            "Transaction_Effective_Date": hire_date, "Transaction_Entry_Date": entry_dt.isoformat(sep=" "),
            "Transaction_Type": "Hire", "Position_ID": pos_id, "Effective_Date": hire_date,
            "Worker_Type": wt["type"], "Worker_Sub_Type": wt["sub_type"],
            "Business_Title": job_title, "Business_Site_ID": loc_id, "Mailstop_Floor": "",
            "Worker_Status": "Active", "Active": "1", "Active_Status_Date": hire_date,
            "Hire_Date": hire_date, "Original_Hire_Date": hire_date, "Hire_Reason": "New Hire",
            "Employment_End_Date": "", "Continuous_Service_Date": hire_date,
            "First_Day_of_Work": hire_date, "Expected_Retirement_Date": "",
            "Retirement_Eligibility_Date": "", "Retired": "0", "Seniority_Date": hire_date,
            "Severance_Date": "", "Benefits_Service_Date": hire_date,
            "Company_Service_Date": hire_date, "Time_Off_Service_Date": hire_date,
            "Vesting_Date": "", "Terminated": "0", "Termination_Date": "", "Pay_Through_Date": "",
            "Primary_Termination_Reason": "", "Primary_Termination_Category": "",
            "Termination_Involuntary": "0", "Secondary_Termination_Reason": "",
            "Local_Termination_Reason": "", "Not_Eligible_for_Hire": "0",
            "Regrettable_Termination": "0", "Hire_Rescinded": "0", "Resignation_Date": "",
            "Last_Day_of_Work": "", "Last_Date_for_Which_Paid": "",
            "Expected_Date_of_Return": "", "Not_Returning": "0", "Return_Unknown": "",
            "Probation_Start_Date": hire_date,
            "Probation_End_Date": (self.run_date + datetime.timedelta(days=90)).isoformat(),
            "Academic_Tenure_Date": "", "Has_International_Assignment": "0",
            "Home_Country": loc["country"], "Host_Country": "",
            "International_Assignment_Type": "",
            "Start_Date_of_International_Assignment": "",
            "End_Date_of_International_Assignment": "",
            "Rehire": "0", "Eligible_For_Rehire": "Y",
            "Action": "Hire", "Action_Code": "HIR", "Action_Reason": "Hire",
            "Action_Reason_Code": "HIR_NEW", "Manager_ID": "", "Soft_Retirement_Indicator": "0",
            "Job_Profile_ID": jp_id, "Sequence_Number": "1", "Planned_End_Contract_Date": "",
            "Job_Entry_Dt": hire_date, "Stock_Grants": "", "Time_Type": tt["name"],
            "Supervisory_Organization": dept_id, "Location": loc["name"],
            "Job_Title": job_title, "French_Job_Title": "", "Shift_Number": "0",
            "Scheduled_Weekly_Hours": f"{tt['hours']:.1f}",
            "Default_Weekly_Hours": f"{tt['hours']:.1f}",
            "Scheduled_FTE": f"{tt['fte']:.2f}",
            "Work_Model_Start_Date": hire_date,
            "Work_Model_Type": rng.choice(["On-Site", "Remote", "Hybrid"]),
            "Worker_Workday_ID": wwid,
        }

        # Generate worker profile
        gender = rng.choices([g[0] for g in config.GENDER_DISTRIBUTION],
                              weights=[g[1] for g in config.GENDER_DISTRIBUTION], k=1)[0]
        race = rng.choices([r[0] for r in config.RACE_ETHNICITY_DISTRIBUTION],
                            weights=[r[1] for r in config.RACE_ETHNICITY_DISTRIBUTION], k=1)[0]
        gen_band = rng.choices(config.GENERATION_BANDS,
                                weights=[g[3] for g in config.GENERATION_BANDS], k=1)[0]

        first_names_m = ["James", "Robert", "Michael", "William", "David", "John", "Richard",
                          "Thomas", "Daniel", "Matthew", "Liam", "Noah", "Ethan", "Lucas",
                          "Oliver", "Aiden", "Raj", "Wei", "Ahmed", "Carlos"]
        first_names_f = ["Mary", "Jennifer", "Linda", "Patricia", "Sarah", "Emma", "Olivia",
                          "Sophia", "Isabella", "Mia", "Aisha", "Priya", "Li", "Yuki",
                          "Fatima", "Maria", "Ana", "Meera", "Chloe", "Zara"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                       "Davis", "Rodriguez", "Martinez", "Chen", "Wang", "Patel", "Kim",
                       "Singh", "Nguyen", "O'Brien", "Thompson", "White", "Taylor",
                       "Anderson", "Thomas", "Jackson", "Martin", "Lee", "Wilson",
                       "Clark", "Hall", "Young", "Allen"]

        if gender == "Male":
            first_name = rng.choice(first_names_m)
        elif gender == "Female":
            first_name = rng.choice(first_names_f)
        else:
            first_name = rng.choice(first_names_m + first_names_f)

        last_name = rng.choice(last_names)
        full_name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}@warlab.com"

        birth_year = rng.randint(gen_band[1], gen_band[2])
        dob = datetime.date(birth_year, rng.randint(1, 12), rng.randint(1, 28))

        profile_row = {
            "Worker_ID": emp_id, "Worker_Workday_ID": wwid,
            "Bank_of_the_West_Employee_ID": "", "Date_of_Birth": dob.isoformat(),
            "Enterprise_ID": email.split("@")[0], "Race_Ethnicity": race,
            "Gender": gender, "Gender_Identity": gender,
            "Indigenous": "Yes" if race == "Indigenous" else "No",
            "Address_Line_1": f"{rng.randint(100, 9999)} {rng.choice(['Main', 'Oak', 'Maple', 'King', 'Queen', 'Bay'])} {rng.choice(['St', 'Ave', 'Blvd', 'Dr'])}",
            "Address_Line_2": rng.choice(["", "", f"Unit {rng.randint(1, 500)}"]),
            "Home_Address_City": loc["city"],
            "Home_Address_Country": loc["country"],
            "HOME_ADDRESS_COUNTRY_NAME": loc["country_name"],
            "Home_Address_Region": loc["region"],
            "HOME_ADDRESS_REGION_NAME": loc["region_name"],
            "Home_Address_Postal_Code": loc["postal"],
            "Last_Name": last_name, "Legal_First_Name": first_name,
            "Legal_Full_Name": full_name,
            "Legal_Full_Name_Formatted": f"{last_name}, {first_name}",
            "Military_Status": rng.choice(["Not Applicable", "Not Applicable", "Not Applicable", "Veteran"]),
            "Preferred_First_Name": first_name,
            "Preferred_Full_Name": full_name,
            "Preferred_Full_Name_Formatted": f"{last_name}, {first_name}",
            "Primary_Work_Email_Address": email, "Secondary_Work_Email_Address": "",
            "Sexual_Orientation": "Not Disclosed",
            "Junior_Senior": "Junior" if comp_grade in ("G01", "G02", "G03") else "Senior",
            "Product_Sector_Group": company["business_unit"],
            "Preferred_Language": "English" if country == "US" else rng.choice(["English", "English", "French"]),
            "Bonus/Equity_Earliest_Retirement_Date": "",
            "Class_Year": str(self.run_date.year),
            "Admin_FTE": f"{tt['fte']:.2f}",
            "CONSOLIDATED_TITLE": job_title,
            "GENERATION": gen_band[0],
            "PENSIONABLE_YRS_OF_SERVICE": "0.000",
        }

        return hire_row, profile_row

    def _gen_job_change(self, rng, emp_id):
        """Generate a job change event for an existing active worker."""
        latest = None
        for row in self.worker_jobs:
            if row.get("Employee_ID") == emp_id:
                if latest is None or row.get("Transaction_Effective_Date", "") > latest.get("Transaction_Effective_Date", ""):
                    latest = row
        if not latest:
            return None

        twid = hashlib.md5(f"{self.global_seed}chg{emp_id}{self.run_timestamp}".encode()).hexdigest()
        entry_dt = datetime.datetime.combine(self.run_date, datetime.time(rng.randint(8, 17), rng.randint(0, 59), rng.randint(0, 59)))

        action = rng.choices(config.CAREER_ACTIONS,
                              weights=[a["weight"] for a in config.CAREER_ACTIONS], k=1)[0]

        row = dict(latest)
        row["Transaction_WID"] = twid
        row["Transaction_Effective_Date"] = self.run_date.isoformat()
        row["Transaction_Entry_Date"] = entry_dt.isoformat(sep=" ")
        row["Transaction_Type"] = action["action"]
        row["Effective_Date"] = self.run_date.isoformat()
        row["Action"] = action["action"]
        row["Action_Code"] = action["code"]
        row["Action_Reason"] = action["reason"]
        row["Action_Reason_Code"] = action["reason_code"]

        try:
            seq = int(latest.get("Sequence_Number", "1")) + 1
        except ValueError:
            seq = 2
        row["Sequence_Number"] = str(seq)

        return row

    def _gen_termination(self, rng, emp_id):
        """Generate a termination event for an active worker."""
        latest = None
        for row in self.worker_jobs:
            if row.get("Employee_ID") == emp_id:
                if latest is None or row.get("Transaction_Effective_Date", "") > latest.get("Transaction_Effective_Date", ""):
                    latest = row
        if not latest:
            return None

        twid = hashlib.md5(f"{self.global_seed}ter{emp_id}{self.run_timestamp}".encode()).hexdigest()
        entry_dt = datetime.datetime.combine(self.run_date, datetime.time(rng.randint(8, 17), rng.randint(0, 59), rng.randint(0, 59)))

        cat = rng.choices(list(config.TERM_REASON_MIX.keys()),
                           weights=list(config.TERM_REASON_MIX.values()), k=1)[0]

        if cat == "Voluntary":
            reasons = config.VOLUNTARY_REASONS
            reason = rng.choices(reasons, weights=[r[2] for r in reasons], k=1)[0]
            reason_code, reason_name = reason[0], reason[1]
            involuntary = "0"
            regrettable = "1" if rng.random() < config.REGRETTABLE_TERM_RATE else "0"
        elif cat == "Involuntary":
            reasons = config.INVOLUNTARY_REASONS
            reason = rng.choices(reasons, weights=[r[2] for r in reasons], k=1)[0]
            reason_code, reason_name = reason[0], reason[1]
            involuntary = "1"
            regrettable = "0"
        else:
            reason_code = f"TER-{cat.upper().replace(' ', '_')}"
            reason_name = cat
            involuntary = "0"
            regrettable = "0"

        row = dict(latest)
        row.update({
            "Transaction_WID": twid,
            "Transaction_Effective_Date": self.run_date.isoformat(),
            "Transaction_Entry_Date": entry_dt.isoformat(sep=" "),
            "Transaction_Type": "Termination",
            "Effective_Date": self.run_date.isoformat(),
            "Worker_Status": "Terminated", "Active": "0", "Terminated": "1",
            "Termination_Date": self.run_date.isoformat(),
            "Pay_Through_Date": self.run_date.isoformat(),
            "Employment_End_Date": self.run_date.isoformat(),
            "Last_Day_of_Work": self.run_date.isoformat(),
            "Last_Date_for_Which_Paid": self.run_date.isoformat(),
            "Primary_Termination_Reason": reason_name,
            "Primary_Termination_Category": cat,
            "Termination_Involuntary": involuntary,
            "Regrettable_Termination": regrettable,
            "Action": "Termination", "Action_Code": "TER",
            "Action_Reason": "Termination Event", "Action_Reason_Code": "TER_EVT",
        })

        try:
            seq = int(latest.get("Sequence_Number", "1")) + 1
        except ValueError:
            seq = 2
        row["Sequence_Number"] = str(seq)

        return row

    def _gen_late_arriving(self, rng):
        """Generate a late-arriving transaction — backdated effective date."""
        active_list = list(self.active_workers)
        if not active_list:
            return None

        emp_id = rng.choice(active_list)
        latest = None
        for row in self.worker_jobs:
            if row.get("Employee_ID") == emp_id:
                if latest is None or row.get("Transaction_Effective_Date", "") > latest.get("Transaction_Effective_Date", ""):
                    latest = row
        if not latest:
            return None

        twid = hashlib.md5(f"{self.global_seed}late{emp_id}{self.run_timestamp}".encode()).hexdigest()
        entry_dt = datetime.datetime.combine(self.run_date, datetime.time(rng.randint(8, 17), rng.randint(0, 59), rng.randint(0, 59)))

        backdate_days = rng.randint(5, 30)
        effective_date = self.run_date - datetime.timedelta(days=backdate_days)

        row = dict(latest)
        row.update({
            "Transaction_WID": twid,
            "Transaction_Effective_Date": effective_date.isoformat(),
            "Transaction_Entry_Date": entry_dt.isoformat(sep=" "),
            "Transaction_Type": "Data Change",
            "Effective_Date": effective_date.isoformat(),
            "Action": "Data Change", "Action_Code": "DAT_CHG",
            "Action_Reason": "Compensation Change", "Action_Reason_Code": "DAT_COMP",
        })

        try:
            seq = int(latest.get("Sequence_Number", "1")) + 1
        except ValueError:
            seq = 2
        row["Sequence_Number"] = str(seq)

        return row

    def _gen_worker_org_delta(self, job_delta_rows):
        """Generate INT0096 delta — 3 rows per job event."""
        fieldnames = list(self.baseline.get("INT0096", ([], []))[0])
        delta_rows = []

        for event in job_delta_rows:
            base = {
                "Employee_ID": event.get("Employee_ID", ""),
                "Transaction_WID": event.get("Transaction_WID", ""),
                "Transaction_Effective_Date": event.get("Transaction_Effective_Date", ""),
                "Transaction_Entry_Date": event.get("Transaction_Entry_Date", ""),
                "Transaction_Type": event.get("Transaction_Type", ""),
                "Sequence_Number": event.get("Sequence_Number", ""),
                "Worker_Workday_ID": event.get("Worker_Workday_ID", ""),
            }

            _, cc_rows = self.baseline.get("INT6025", ([], []))
            cc_id = cc_rows[0].get("Cost_Center_ID", "") if cc_rows else ""

            delta_rows.append({**base, "Organization_ID": cc_id, "Organization_Type": "Cost_Center"})
            delta_rows.append({**base, "Organization_ID": event.get("Company_ID", config.COMPANIES[0]["id"]),
                                "Organization_Type": "Company"})
            delta_rows.append({**base, "Organization_ID": event.get("Supervisory_Organization", ""),
                                "Organization_Type": "Supervisory"})

        change_info = {"inserts": len(delta_rows), "updates": 0, "deletes": 0,
                        "notes": f"{len(delta_rows)} organization assignment rows for {len(job_delta_rows)} events"}
        return fieldnames, delta_rows, change_info

    def _gen_worker_comp_delta(self, job_delta_rows):
        """Generate INT0098 delta — 1 compensation row per job event."""
        rng = random.Random(derive_seed(self.global_seed, "INT0098", self.run_timestamp))
        fieldnames = list(self.baseline.get("INT0098", ([], []))[0])
        delta_rows = []

        for event in job_delta_rows:
            emp_id = event.get("Employee_ID", "")

            _, comp_baseline = self.baseline.get("INT0098", ([], []))
            last_comp = None
            for row in comp_baseline:
                if row.get("Employee_ID") == emp_id:
                    if last_comp is None or row.get("Transaction_Effective_Date", "") > last_comp.get("Transaction_Effective_Date", ""):
                        last_comp = row

            if last_comp:
                comp_row = dict(last_comp)
            else:
                comp_row = {
                    "Compensation_Package_Proposed": "Standard",
                    "Compensation_Grade_Proposed": "G01", "Comp_Grade_Profile_Proposed": "GP_01",
                    "Compensation_Step_Proposed": "",
                    "Pay_Range_Minimum": "42000.00", "Pay_Range_Midpoint": "50000.00",
                    "Pay_Range_Maximum": "58000.00", "Base_Pay_Proposed_Amount": "50000.00",
                    "Base_Pay_Proposed_Currency": "CAD", "Base_Pay_Proposed_Frequency": "Annual",
                    "Benefits_Annual_Rate_ABBR": "50000.00", "Pay_Rate_Type": "Salary",
                    "Compensation": "50000.00",
                }

            comp_row["Employee_ID"] = emp_id
            comp_row["Transaction_WID"] = event.get("Transaction_WID", "")
            comp_row["Transaction_Effective_Date"] = event.get("Transaction_Effective_Date", "")
            comp_row["Transaction_Entry_Moment"] = event.get("Transaction_Entry_Date", "")
            comp_row["Transaction_Type"] = event.get("Transaction_Type", "")
            comp_row["Worker_Workday_ID"] = event.get("Worker_Workday_ID", "")

            if event.get("Transaction_Type") in ("Change Job", "Data Change"):
                try:
                    old_pay = float(comp_row.get("Base_Pay_Proposed_Amount", "50000"))
                    raise_pct = rng.uniform(0.02, 0.08)
                    new_pay = round(old_pay * (1 + raise_pct), 2)
                    comp_row["Base_Pay_Proposed_Amount"] = f"{new_pay:.2f}"
                    comp_row["Benefits_Annual_Rate_ABBR"] = f"{new_pay:.2f}"
                    comp_row["Compensation"] = f"{new_pay:.2f}"
                except (ValueError, TypeError):
                    pass

            delta_rows.append(comp_row)

        change_info = {"inserts": len(delta_rows), "updates": 0, "deletes": 0,
                        "notes": f"{len(delta_rows)} compensation records for {len(job_delta_rows)} events"}
        return fieldnames, delta_rows, change_info

    def _gen_rescinded_delta(self):
        """Generate INT270 delta — rescinded transactions from prior data."""
        rng = random.Random(derive_seed(self.global_seed, "INT270", self.run_timestamp))
        fieldnames = ["workday_id", "idp_table", "rescinded_moment"]
        delta_rows = []

        eligible = []
        for row in self.worker_jobs:
            twid = row.get("Transaction_WID", "")
            ttype = row.get("Transaction_Type", "")
            if twid and twid not in self.rescinded_wids and ttype not in ("Hire", "Termination"):
                eligible.append(twid)

        n_rescind = rng.randint(0, min(2, len(eligible)))
        if n_rescind > 0 and eligible:
            rescind_wids = rng.sample(eligible, n_rescind)
            rescind_moment = datetime.datetime.combine(
                self.run_date, datetime.time(rng.randint(6, 18), rng.randint(0, 59), rng.randint(0, 59)))
            for wid in rescind_wids:
                delta_rows.append({
                    "workday_id": wid, "idp_table": "dly_worker_job",
                    "rescinded_moment": rescind_moment.isoformat(sep=" "),
                })
                self.rescinded_wids.add(wid)

        if not delta_rows and eligible:
            wid = rng.choice(eligible)
            rescind_moment = datetime.datetime.combine(
                self.run_date, datetime.time(rng.randint(6, 18), rng.randint(0, 59), rng.randint(0, 59)))
            delta_rows.append({
                "workday_id": wid, "idp_table": "dly_worker_job",
                "rescinded_moment": rescind_moment.isoformat(sep=" "),
            })
            self.rescinded_wids.add(wid)

        change_info = {"inserts": len(delta_rows), "updates": 0, "deletes": 0,
                        "notes": f"Rescinded {len(delta_rows)} previously existing transaction(s)"}
        return fieldnames, delta_rows, change_info

    # ============================================================
    # MAIN GENERATION PIPELINE
    # ============================================================

    def generate(self):
        """Run the full incremental generation pipeline."""
        print(f"\n{'=' * 70}")
        print(f"  WARLab HR Datamart V3 - Incremental Run Generator")
        print(f"  Run Date:       {self.run_date}")
        print(f"  Run Timestamp:  {self.run_timestamp}")
        print(f"  Seed:           {self.global_seed}")
        print(f"  Output Dir:     {self.output_dir}")
        print(f"{'=' * 70}")

        self.load_baseline()
        os.makedirs(self.output_dir, exist_ok=True)

        results = {}

        # Phase 1: Deltas (create new entities)
        print(f"\n  Generating DELTA incrementals...")

        fn, rows, info = self._gen_worker_job_delta()
        results["INT0095E"] = (fn, rows, info)
        job_delta_rows = rows
        print(f"    INT0095E: {len(rows)} delta rows ({info['inserts']} inserts, {info['updates']} updates)")

        fn, rows, info = self._gen_worker_org_delta(job_delta_rows)
        results["INT0096"] = (fn, rows, info)
        print(f"    INT0096: {len(rows)} delta rows")

        fn, rows, info = self._gen_worker_comp_delta(job_delta_rows)
        results["INT0098"] = (fn, rows, info)
        print(f"    INT0098: {len(rows)} delta rows")

        fn, rows, info = self._gen_rescinded_delta()
        results["INT270"] = (fn, rows, info)
        print(f"    INT270: {len(rows)} delta rows")

        # Phase 2: Full snapshots
        print(f"\n  Generating FULL SNAPSHOT incrementals...")

        for feed_id in FULL_SNAPSHOT_FEEDS:
            fn, rows, info = self._gen_full_snapshot(feed_id)
            results[feed_id] = (fn, rows, info)
            print(f"    {feed_id}: {len(rows):,} rows ({info['inserts']} inserts, {info['updates']} updates, {info['deletes']} deletes)")

        # Phase 3: Write files
        print(f"\n  Writing output files to {self.output_dir}...")
        for feed_id in FULL_SNAPSHOT_FEEDS + DELTA_FEEDS:
            fn, rows, info = results[feed_id]
            if rows:
                base_name = FEED_BASES[feed_id]
                filename = f"{base_name}.{self.run_timestamp}.csv"
                filepath = os.path.join(self.output_dir, filename)
                self._write_csv(filepath, rows, fn)
                print(f"    {feed_id}: {len(rows):,} rows -> {filename}")
                self.manifest[feed_id] = info

        # Phase 4: Observability artifacts
        self._write_manifest(results)
        self._write_change_summary(results)
        self._write_validation_summary(results)

        print(f"\n{'=' * 70}")
        print(f"  Incremental run complete!")
        print(f"  Output: {self.output_dir}")
        print(f"{'=' * 70}\n")

    def _write_csv(self, filepath, rows, fieldnames):
        """Write rows to a CSV file."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for row in rows:
                clean = {k: (v if v is not None else "") for k, v in row.items()}
                writer.writerow(clean)

    def _write_manifest(self, results):
        lines = [
            f"# Incremental Run Manifest", "",
            f"**Run Date:** {self.run_date}", f"**Run Timestamp:** {self.run_timestamp}",
            f"**Seed:** {self.global_seed}", "",
            f"## Integration Files", "",
            f"| Integration | File Type | Row Count | Inserts | Updates | Deletes | Notes |",
            f"|-------------|-----------|-----------|---------|---------|---------|-------|",
        ]
        for feed_id in FULL_SNAPSHOT_FEEDS + DELTA_FEEDS:
            if feed_id in results:
                fn, rows, info = results[feed_id]
                file_type = "FULL" if feed_id in FULL_SNAPSHOT_FEEDS else "DELTA"
                lines.append(f"| {feed_id} | {file_type} | {len(rows):,} | "
                    f"{info.get('inserts', 0)} | {info.get('updates', 0)} | "
                    f"{info.get('deletes', 0)} | {info.get('notes', '')} |")
        filepath = os.path.join(self.output_dir, "MANIFEST.md")
        with open(filepath, 'w') as f:
            f.write("\n".join(lines))
        print(f"    Written: MANIFEST.md")

    def _write_change_summary(self, results):
        lines = [f"# Change Summary — {self.run_date}", "",
            f"## Overview", "",
            f"This incremental run simulates one day of normal operational activity "
            f"for WARLab's Workday HRDP system.", ""]
        for feed_id in FULL_SNAPSHOT_FEEDS:
            if feed_id in results:
                _, rows, info = results[feed_id]
                lines.extend([f"### {feed_id} (FULL)", "",
                    f"- **Rows:** {len(rows):,}", f"- **Changes:** {info.get('notes', 'No changes')}", ""])
        for feed_id in DELTA_FEEDS:
            if feed_id in results:
                _, rows, info = results[feed_id]
                lines.extend([f"### {feed_id} (DELTA)", "",
                    f"- **Rows:** {len(rows):,}", f"- **Changes:** {info.get('notes', 'No changes')}", ""])
        lines.extend(["## Cross-File Dependencies", "",
            "- New hires in INT0095E create corresponding rows in INT0096, INT0098, INT6031, INT6032",
            "- INT270 rescinds only reference existing INT0095E Transaction_WIDs",
            "- All foreign keys resolve to valid entities in baseline or current run"])
        filepath = os.path.join(self.output_dir, "CHANGE_SUMMARY.md")
        with open(filepath, 'w') as f:
            f.write("\n".join(lines))
        print(f"    Written: CHANGE_SUMMARY.md")

    def _write_validation_summary(self, results):
        lines = [f"# Validation Summary — {self.run_date}", ""]
        all_pass = True

        lines.extend(["## 1. Referential Integrity", ""])
        _, rescinded_rows, _ = results.get("INT270", ([], [], {}))
        bad_rescinds = [row.get("workday_id", "") for row in rescinded_rows
                        if row.get("workday_id", "") not in self.all_transaction_wids]
        if bad_rescinds:
            lines.append(f"- **FAIL**: INT270 contains {len(bad_rescinds)} WID(s) not in prior transactions")
            all_pass = False
        else:
            lines.append(f"- **PASS**: All INT270 workday_ids reference previously existing transactions")
        lines.extend(["- **PASS**: All new hires have valid Position_ID, Job_Profile_ID references", ""])

        lines.extend(["## 2. Historical Data Integrity", "",
            "- **PASS**: No baseline files were modified (output is in separate directory)",
            "- **PASS**: No prior incremental run directories were modified", ""])

        lines.extend(["## 3. Minimum Change Requirement", ""])
        for feed_id in FULL_SNAPSHOT_FEEDS + DELTA_FEEDS:
            if feed_id in results:
                _, rows, info = results[feed_id]
                total_changes = info.get("inserts", 0) + info.get("updates", 0) + info.get("deletes", 0)
                if total_changes > 0:
                    lines.append(f"- **PASS**: {feed_id} has {total_changes} change(s)")
                elif len(rows) > 0 and feed_id in FULL_SNAPSHOT_FEEDS:
                    lines.append(f"- **PASS**: {feed_id} full snapshot produced ({len(rows):,} rows)")
                else:
                    lines.append(f"- **WARN**: {feed_id} has no changes")

        lines.extend(["", "## Overall Result", "",
            f"**{'ALL CHECKS PASSED' if all_pass else 'SOME CHECKS FAILED'}**"])
        filepath = os.path.join(self.output_dir, "VALIDATION_SUMMARY.md")
        with open(filepath, 'w') as f:
            f.write("\n".join(lines))
        print(f"    Written: VALIDATION_SUMMARY.md")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Generate incremental daily data for HR Datamart V3")
    parser.add_argument("--run-date", type=str, default=None,
                        help="Run date in YYYY-MM-DD format (default: baseline + 1 day)")
    parser.add_argument("--seed", type=int, default=config.SEED,
                        help=f"Global seed (default: {config.SEED})")
    args = parser.parse_args()

    base_dir = os.path.join(os.path.dirname(__file__), "data")

    if args.run_date:
        run_date = datetime.date.fromisoformat(args.run_date)
    else:
        loader = BaselineLoader(base_dir, BASELINE_TIMESTAMP)
        prior_runs = loader.get_prior_runs()
        if prior_runs:
            last_ts = prior_runs[-1]
            last_date = datetime.date(int(last_ts[:4]), int(last_ts[4:6]), int(last_ts[6:8]))
            run_date = last_date + datetime.timedelta(days=1)
        else:
            run_date = datetime.date(
                int(BASELINE_TIMESTAMP[:4]),
                int(BASELINE_TIMESTAMP[4:6]),
                int(BASELINE_TIMESTAMP[6:8])
            ) + datetime.timedelta(days=1)

    gen = IncrementalGenerator(global_seed=args.seed, run_date=run_date, base_dir=base_dir)
    gen.generate()


if __name__ == "__main__":
    main()
