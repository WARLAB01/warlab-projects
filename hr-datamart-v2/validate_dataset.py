#!/usr/bin/env python3
"""
validate_dataset.py - Local Python analysis of generated synthetic HR data.

Performs comprehensive validation:
  1. Row counts & feed summary
  2. Referential integrity (tic-and-tie) across all 14 feeds
  3. Headcount curve analysis
  4. Attrition & hiring rate analysis
  5. Demographic distributions
  6. Compensation distribution by grade
  7. Organizational structure validation
  8. Timeline event type breakdown
  9. Generates matplotlib charts

Usage:
    python validate_dataset.py
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# Configuration
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "feeds")
TIMESTAMP = "20260213120000"
COMPANY_FOUNDED = date(2016, 2, 13)
DATA_END = date(2026, 2, 13)

FEED_FILES = {
    "INT6020": f"workday.hrdp.dly_grade_profile.full.{TIMESTAMP}.csv",
    "INT6021": f"workday.hrdp.dly_job_profile.full.{TIMESTAMP}.csv",
    "INT6022": f"workday.hrdp.dly_job_classification.full.{TIMESTAMP}.csv",
    "INT6023": f"workday.hrdp.dly_location.full.{TIMESTAMP}.csv",
    "INT6024": f"workday.hrdp.dly_company.full.{TIMESTAMP}.csv",
    "INT6025": f"workday.hrdp.dly_cost_center.full.{TIMESTAMP}.csv",
    "INT6027": f"workday.hrdp.dly_matrix_organization.full.{TIMESTAMP}.csv",
    "INT6028": f"workday.hrdp.dly_department_hierarchy.full.{TIMESTAMP}.csv",
    "INT6031": f"workday.hrdp.dly_worker_profile.full.{TIMESTAMP}.csv",
    "INT6032": f"workday.hrdp.dly_positions.full.{TIMESTAMP}.csv",
    "INT0095E": f"workday.hrdp.dly_worker_job.full.{TIMESTAMP}.csv",
    "INT0096": f"workday.hrdp.dly_worker_organization.full.{TIMESTAMP}.csv",
    "INT0098": f"workday.hrdp.dly_worker_compensation.full.{TIMESTAMP}.csv",
    "INT270": f"workday.hrdp.dly_rescinded_transactions.full.{TIMESTAMP}.csv",
}

pass_count = 0
fail_count = 0
warn_count = 0


def check(label, condition, detail=""):
    global pass_count, fail_count
    if condition:
        print(f"  [PASS] {label}")
        pass_count += 1
    else:
        print(f"  [FAIL] {label} -- {detail}")
        fail_count += 1


def warn(label, detail=""):
    global warn_count
    print(f"  [WARN] {label} -- {detail}")
    warn_count += 1


def load_feed(key):
    path = os.path.join(DATA_DIR, FEED_FILES[key])
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def main():
    global pass_count, fail_count, warn_count

    print("=" * 70)
    print("  WARLab HR Datamart V2 - Dataset Validation Report")
    print("=" * 70)

    # Load all feeds
    print("\n[1] Loading feeds...")
    feeds = {}
    for key in FEED_FILES:
        feeds[key] = load_feed(key)
        print(f"    {key:10s}: {len(feeds[key]):>10,} rows  x {len(feeds[key].columns):>3} cols")

    # -------------------------------------------------------
    # Section 2: Row Count Sanity Checks
    # -------------------------------------------------------
    print("\n[2] Row count sanity checks...")

    wj = feeds["INT0095E"]
    wo = feeds["INT0096"]
    wc = feeds["INT0098"]

    check("INT0095E has > 100k events", len(wj) > 100000, f"got {len(wj)}")
    check("INT0096 = 3x INT0095E", len(wo) == 3 * len(wj),
          f"expected {3 * len(wj)}, got {len(wo)}")
    check("INT0098 = INT0095E", len(wc) == len(wj),
          f"expected {len(wj)}, got {len(wc)}")
    check("INT6031 profiles > 10k", len(feeds["INT6031"]) > 10000,
          f"got {len(feeds['INT6031'])}")
    check("INT6032 positions = INT6031 profiles", len(feeds["INT6032"]) == len(feeds["INT6031"]),
          f"positions={len(feeds['INT6032'])}, profiles={len(feeds['INT6031'])}")

    # -------------------------------------------------------
    # Section 3: Referential Integrity (Tic-and-Tie)
    # -------------------------------------------------------
    print("\n[3] Referential integrity (tic-and-tie)...")

    # 3a: Same Transaction_WID set across INT0095E and INT0098
    wids_095 = set(wj["Transaction_WID"])
    wids_098 = set(wc["Transaction_WID"])
    check("WID set: INT0095E == INT0098", wids_095 == wids_098,
          f"diff: {len(wids_095.symmetric_difference(wids_098))}")

    # 3b: INT0096 WIDs are subset of INT0095E (each event produces 3 org rows)
    wids_096 = set(wo["Transaction_WID"])
    check("WID set: INT0096 WIDs ⊆ INT0095E WIDs", wids_096.issubset(wids_095),
          f"extra: {len(wids_096 - wids_095)}")

    # 3c: Employee IDs in INT0095E == INT6031
    emp_ids_095 = set(wj["Employee_ID"])
    emp_ids_6031 = set(feeds["INT6031"]["Worker_ID"])
    check("Employee IDs: INT0095E == INT6031",
          emp_ids_095 == emp_ids_6031,
          f"only in 095E: {len(emp_ids_095 - emp_ids_6031)}, only in 6031: {len(emp_ids_6031 - emp_ids_095)}")

    # 3d: All Job_Profile_IDs in INT0095E exist in INT6021
    jp_ids_ref = set(feeds["INT6021"]["Job_Profile_ID"])
    jp_ids_used = set(wj["Job_Profile_ID"].dropna()) - {""}
    missing_jp = jp_ids_used - jp_ids_ref
    check("Job Profile IDs: INT0095E ⊆ INT6021", len(missing_jp) == 0,
          f"missing: {len(missing_jp)}")

    # 3e: All Company IDs in INT0096 exist in INT6024
    co_ids_ref = set(feeds["INT6024"]["Company_ID"])
    org_company = wo[wo["Organization_Type"] == "Company"]
    co_ids_used = set(org_company["Organization_ID"].dropna()) - {""}
    missing_co = co_ids_used - co_ids_ref
    check("Company IDs: INT0096 ⊆ INT6024", len(missing_co) == 0,
          f"missing: {missing_co}")

    # 3f: All Cost Center IDs in INT0096 exist in INT6025
    cc_ids_ref = set(feeds["INT6025"]["Cost_Center_ID"])
    org_cc = wo[wo["Organization_Type"] == "Cost_Center"]
    cc_ids_used = set(org_cc["Organization_ID"].dropna()) - {""}
    missing_cc = cc_ids_used - cc_ids_ref
    check("Cost Center IDs: INT0096 ⊆ INT6025", len(missing_cc) == 0,
          f"missing: {len(missing_cc)}")

    # 3g: All Location values in INT0095E exist in INT6023
    loc_ids_ref = set(feeds["INT6023"]["Location_Name"])
    loc_ids_used = set(wj["Location"].dropna()) - {""}
    missing_loc = loc_ids_used - loc_ids_ref
    check("Location names: INT0095E ⊆ INT6023", len(missing_loc) == 0,
          f"missing: {missing_loc}")

    # 3h: INT270 rescinded WIDs exist in INT0095E (or INT0096/INT0098)
    rescinded_wids = set(feeds["INT270"]["workday_id"])
    all_wids = wids_095 | wids_096 | wids_098
    missing_rescind = rescinded_wids - all_wids
    check("INT270 WIDs ⊆ transactional WIDs", len(missing_rescind) == 0,
          f"orphan rescinded: {len(missing_rescind)}")

    # 3i: No events before company founding date
    eff_dates = pd.to_datetime(wj["Effective_Date"])
    earliest = eff_dates.min().date()
    check("No events before founding date",
          earliest >= COMPANY_FOUNDED,
          f"earliest event: {earliest}")

    # -------------------------------------------------------
    # Section 4: Headcount Curve Analysis
    # -------------------------------------------------------
    print("\n[4] Headcount curve analysis...")

    wj_copy = wj.copy()
    wj_copy["eff_date"] = pd.to_datetime(wj_copy["Effective_Date"])
    wj_copy["year"] = wj_copy["eff_date"].dt.year

    # Approximate headcount at year-end by counting unique active employees
    # For each year, get the last event per employee and count actives
    yearly_hc = {}
    for yr in range(2016, 2027):
        yr_events = wj_copy[wj_copy["eff_date"] <= f"{yr}-12-31"]
        if len(yr_events) == 0:
            yearly_hc[yr] = 0
            continue
        latest = yr_events.sort_values("eff_date").groupby("Employee_ID").last()
        active_count = (latest["Active"] == "1").sum()
        yearly_hc[yr] = active_count

    print("    Year-end headcount estimates:")
    for yr, hc in sorted(yearly_hc.items()):
        target = ""
        if yr == 2016: target = " (target: ~2,000)"
        elif yr == 2017: target = " (target: ~5,000)"
        elif yr == 2018: target = " (target: ~8,000)"
        elif yr == 2019: target = " (target: ~9,500)"
        elif yr >= 2020: target = " (target: ~10,000)"
        print(f"      {yr}: {hc:>6,}{target}")

    check("Final headcount 9,000-11,000",
          9000 <= yearly_hc.get(2025, 0) <= 11000,
          f"got {yearly_hc.get(2025, 0)}")

    # -------------------------------------------------------
    # Section 5: Attrition & Hiring Analysis
    # -------------------------------------------------------
    print("\n[5] Attrition & hiring analysis...")

    term_events = wj_copy[wj_copy["Action"] == "Termination"]
    hire_events = wj_copy[wj_copy["Action"] == "Hire"]

    print("    Terminations by year:")
    term_by_year = term_events.groupby("year").size()
    for yr, cnt in term_by_year.items():
        hc = yearly_hc.get(yr, 1)
        rate = cnt / max(hc, 1) * 100
        print(f"      {yr}: {cnt:>5,} terms  ({rate:.1f}% of headcount)")

    print("\n    Hires by year:")
    hire_by_year = hire_events.groupby("year").size()
    for yr, cnt in hire_by_year.items():
        print(f"      {yr}: {cnt:>5,} hires")

    # Termination categories
    print("\n    Termination category mix:")
    if "Primary_Termination_Category" in term_events.columns:
        cat_counts = term_events["Primary_Termination_Category"].value_counts()
        total_terms = len(term_events)
        for cat, cnt in cat_counts.items():
            if cat:
                print(f"      {cat:25s}: {cnt:>5,}  ({cnt/total_terms*100:.1f}%)")

    # -------------------------------------------------------
    # Section 6: Demographics (INT6031)
    # -------------------------------------------------------
    print("\n[6] Demographic distributions (INT6031)...")

    prof = feeds["INT6031"]

    print("    Gender:")
    for g, cnt in prof["Gender"].value_counts().items():
        print(f"      {g:20s}: {cnt:>6,}  ({cnt/len(prof)*100:.1f}%)")

    print("\n    Race/Ethnicity (top 8):")
    for r, cnt in prof["Race_Ethnicity"].value_counts().head(8).items():
        print(f"      {r:30s}: {cnt:>6,}  ({cnt/len(prof)*100:.1f}%)")

    print("\n    Generation:")
    for g, cnt in prof["GENERATION"].value_counts().items():
        print(f"      {g:20s}: {cnt:>6,}  ({cnt/len(prof)*100:.1f}%)")

    # -------------------------------------------------------
    # Section 7: Compensation Analysis
    # -------------------------------------------------------
    print("\n[7] Compensation analysis...")

    # Get latest event per employee (active only)
    latest_events = wj_copy.sort_values("eff_date").groupby("Employee_ID").last()
    active_latest = latest_events[latest_events["Active"] == "1"]

    # Merge with compensation data
    wc_copy = wc.copy()
    wc_copy["base_pay"] = pd.to_numeric(wc_copy["Base_Pay_Proposed_Amount"], errors="coerce")
    wc_copy["grade"] = wc_copy["Compensation_Grade_Proposed"]

    # Get latest comp per active employee
    wc_copy["eff_date"] = pd.to_datetime(wc_copy["Transaction_Effective_Date"])
    latest_comp = wc_copy.sort_values("eff_date").groupby("Employee_ID").last()
    active_comp = latest_comp[latest_comp.index.isin(active_latest.index)]

    print("    Active employee compensation by grade:")
    grade_stats = active_comp.groupby("grade")["base_pay"].agg(["count", "mean", "median", "min", "max"])
    grade_stats = grade_stats.sort_index()
    print(f"      {'Grade':8s} {'Count':>6s} {'Mean':>12s} {'Median':>12s} {'Min':>12s} {'Max':>12s}")
    for grade, row in grade_stats.iterrows():
        if pd.notna(row["mean"]):
            print(f"      {grade:8s} {int(row['count']):>6,} {row['mean']:>12,.0f} {row['median']:>12,.0f} {row['min']:>12,.0f} {row['max']:>12,.0f}")

    # -------------------------------------------------------
    # Section 8: Organizational Structure
    # -------------------------------------------------------
    print("\n[8] Organizational structure...")

    dept = feeds["INT6028"]
    print(f"    Total departments: {len(dept)}")
    dept_levels = dept["Department_Level"].astype(int)
    for lvl, cnt in dept_levels.value_counts().sort_index().items():
        print(f"      Level {lvl}: {cnt} departments")

    print(f"\n    Companies: {len(feeds['INT6024'])}")
    for _, row in feeds["INT6024"].iterrows():
        print(f"      {row['Company_ID']:15s}  {row['Company_Name']}")

    # -------------------------------------------------------
    # Section 9: Event Type Breakdown
    # -------------------------------------------------------
    print("\n[9] Event type breakdown...")

    action_counts = wj_copy["Action"].value_counts()
    for action, cnt in action_counts.items():
        print(f"      {action:25s}: {cnt:>8,}  ({cnt/len(wj)*100:.1f}%)")

    # -------------------------------------------------------
    # Section 10: Generate Charts
    # -------------------------------------------------------
    print("\n[10] Generating charts...")
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        chart_dir = os.path.join(os.path.dirname(__file__), "analysis")
        os.makedirs(chart_dir, exist_ok=True)

        # Chart 1: Headcount curve
        fig, ax = plt.subplots(figsize=(12, 6))
        years = sorted(yearly_hc.keys())
        hc_values = [yearly_hc[y] for y in years]
        ax.plot(years, hc_values, 'b-o', linewidth=2, markersize=8)
        ax.set_title("WARLab Headcount Curve (2016-2026)", fontsize=14)
        ax.set_xlabel("Year")
        ax.set_ylabel("Active Headcount")
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, max(hc_values) * 1.1)
        plt.tight_layout()
        plt.savefig(os.path.join(chart_dir, "headcount_curve.png"), dpi=150)
        plt.close()
        print("    Saved headcount_curve.png")

        # Chart 2: Hires vs Terminations
        fig, ax = plt.subplots(figsize=(12, 6))
        years_ht = sorted(set(hire_by_year.index) | set(term_by_year.index))
        hires = [hire_by_year.get(y, 0) for y in years_ht]
        terms = [term_by_year.get(y, 0) for y in years_ht]
        x = np.arange(len(years_ht))
        width = 0.35
        ax.bar(x - width/2, hires, width, label='Hires', color='green', alpha=0.7)
        ax.bar(x + width/2, terms, width, label='Terminations', color='red', alpha=0.7)
        ax.set_xticks(x)
        ax.set_xticklabels(years_ht)
        ax.set_title("Hires vs Terminations by Year", fontsize=14)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(os.path.join(chart_dir, "hires_vs_terms.png"), dpi=150)
        plt.close()
        print("    Saved hires_vs_terms.png")

        # Chart 3: Compensation distribution
        fig, ax = plt.subplots(figsize=(12, 6))
        active_pay = active_comp["base_pay"].dropna()
        ax.hist(active_pay, bins=50, color='steelblue', edgecolor='white', alpha=0.8)
        ax.set_title("Base Pay Distribution (Active Employees)", fontsize=14)
        ax.set_xlabel("Base Pay ($)")
        ax.set_ylabel("Count")
        ax.axvline(active_pay.median(), color='red', linestyle='--', label=f'Median: ${active_pay.median():,.0f}')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(os.path.join(chart_dir, "comp_distribution.png"), dpi=150)
        plt.close()
        print("    Saved comp_distribution.png")

        # Chart 4: Gender and Generation pie charts
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        gender_counts = prof["Gender"].value_counts()
        ax1.pie(gender_counts, labels=gender_counts.index, autopct='%1.1f%%',
                colors=['#4e79a7', '#f28e2b', '#76b7b2', '#e15759'])
        ax1.set_title("Gender Distribution")
        gen_counts = prof["GENERATION"].value_counts()
        ax2.pie(gen_counts, labels=gen_counts.index, autopct='%1.1f%%',
                colors=['#59a14f', '#edc948', '#b07aa1', '#ff9da7'])
        ax2.set_title("Generation Distribution")
        plt.tight_layout()
        plt.savefig(os.path.join(chart_dir, "demographics.png"), dpi=150)
        plt.close()
        print("    Saved demographics.png")

        # Chart 5: Attrition rate over time
        fig, ax = plt.subplots(figsize=(12, 6))
        attrition_rates = []
        for yr in range(2017, 2026):
            hc = yearly_hc.get(yr, 1)
            terms_yr = term_by_year.get(yr, 0)
            attrition_rates.append(terms_yr / max(hc, 1) * 100)
        ax.plot(range(2017, 2026), attrition_rates, 'r-o', linewidth=2, markersize=8)
        ax.set_title("Annual Attrition Rate", fontsize=14)
        ax.set_xlabel("Year")
        ax.set_ylabel("Attrition Rate (%)")
        ax.set_ylim(0, 25)
        ax.axhline(y=12, color='gray', linestyle=':', label='12% lower bound')
        ax.axhline(y=18, color='gray', linestyle=':', label='18% upper bound')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(chart_dir, "attrition_rate.png"), dpi=150)
        plt.close()
        print("    Saved attrition_rate.png")

    except ImportError:
        warn("matplotlib not available", "charts not generated")

    # -------------------------------------------------------
    # Final Summary
    # -------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"  Validation Summary")
    print(f"  Passed: {pass_count}  |  Failed: {fail_count}  |  Warnings: {warn_count}")
    if fail_count == 0:
        print("  STATUS: ALL CHECKS PASSED")
    else:
        print("  STATUS: SOME CHECKS FAILED - review above")
    print("=" * 70)

    return fail_count


if __name__ == "__main__":
    sys.exit(main())
