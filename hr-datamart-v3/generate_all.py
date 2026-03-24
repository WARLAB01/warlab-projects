#!/usr/bin/env python3
"""
generate_all.py - Orchestrator for WARLab HR Datamart V3 synthetic data generation.

V3 change requests applied:
  CR1      - INT6021 (Job Profile):    Field order resequenced (ID/WID lead)
  CR2      - INT6022 (Job Class):      Normalized parent/child schema
  CR3      - INT6027 (Matrix Org):     Added Matrix_Organization_Description
  CR4      - INT6028 (Dept Hierarchy): Added Owner_EIN_WID field
  CR5      - INT6031 (Worker Profile): Fixed field order + added Address_Line_1/2
  CR6      - INT0095E (Worker Job):    Renamed Worker_Sub-Type → Worker_Sub_Type
  INT6022/CR - Job Classification: 11 standard groups; N×11 row model
  INT6032/CR - Positions: Added Work_Space, Pay_Rate_Type, Schedule_Weekly_Hours,
                          Scheduled_FTE, Default_Weekly_Hours, Employee_Type,
                          shift_number, Exclude_From_Headcount

Usage:
    python generate_all.py
"""

import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from generators import config
from generators import utils
from generators.reference_data import ReferenceDataGenerator
from generators.employee_timeline import EmployeeTimelineGenerator
from generators.transactional_data import TransactionalDataWriter


def main():
    start_time = time.time()
    print("=" * 70)
    print(f"  WARLab HR Datamart V3 - Synthetic Data Generator")
    print(f"  Company: {config.COMPANY_NAME}")
    print(f"  Period:  {config.COMPANY_FOUNDED} to {config.DATA_END_DATE}")
    print(f"  Seed:    {config.SEED}")
    print(f"  CRs:     INT6021/CR1 INT6022/CR2 INT6027/CR3 INT6028/CR4 INT6031/CR5 INT0095E/CR6 INT6022/CR INT6032/CR")
    print("=" * 70)

    rng = utils.get_rng(config.SEED)

    print("\n[Step 1/5] Generating reference data...")
    ref_gen = ReferenceDataGenerator(rng=rng)
    ref_feeds = ref_gen.generate_all()

    print("\n[Step 2/5] Generating employee timelines...")
    timeline_gen = EmployeeTimelineGenerator(ref_data=ref_gen, rng=rng)
    timeline_gen.generate_all()

    print("\n[Step 3/5] Generating rescinded transactions...")
    timeline_gen.generate_rescinded()

    print("\n[Step 4/5] Generating position records...")
    positions = ref_gen.generate_positions(timeline_gen.position_assignments)

    print("\n[Step 5/5] Writing all CSV feeds...")
    output_dir = os.path.join(os.path.dirname(__file__), config.OUTPUT_DIR)
    writer = TransactionalDataWriter(output_dir=output_dir)
    writer.write_all(
        events=timeline_gen.all_events,
        profiles=timeline_gen.profiles,
        rescinded=timeline_gen.rescinded_wids,
        ref_feeds=ref_feeds,
        ref_positions=positions,
    )

    elapsed = time.time() - start_time
    print("=" * 70)
    print(f"  Generation complete in {elapsed:.1f} seconds")
    print(f"  Total unique employees: {timeline_gen.employee_seq:,}")
    print(f"  Total events:           {len(timeline_gen.all_events):,}")
    print(f"  Active at end:          {len(timeline_gen.active_employees):,}")
    print(f"  Profiles (INT6031):     {len(timeline_gen.profiles):,}")
    print(f"  Positions (INT6032):    {len(positions):,}")
    print(f"  Rescinded (INT270):     {len(timeline_gen.rescinded_wids):,}")
    print(f"  Output directory:       {output_dir}")
    print("=" * 70)


if __name__ == "__main__":
    main()
