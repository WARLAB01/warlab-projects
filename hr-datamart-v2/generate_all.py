#!/usr/bin/env python3
"""
generate_all.py - Orchestrator for WARLab HR Datamart V2 synthetic data generation.

Runs the full pipeline:
  1. Generate reference data (grades, jobs, locations, companies, etc.)
  2. Generate employee timelines (master lifecycle simulation)
  3. Generate rescinded transactions
  4. Generate positions from employee assignments
  5. Write all feeds to CSV files

Usage:
    python generate_all.py
"""

import time
import sys
import os

# Ensure the project root is on sys.path
sys.path.insert(0, os.path.dirname(__file__))

from generators import config
from generators import utils
from generators.reference_data import ReferenceDataGenerator
from generators.employee_timeline import EmployeeTimelineGenerator
from generators.transactional_data import TransactionalDataWriter


def main():
    start_time = time.time()
    print("=" * 70)
    print(f"  WARLab HR Datamart V2 - Synthetic Data Generator")
    print(f"  Company: {config.COMPANY_NAME}")
    print(f"  Period:  {config.COMPANY_FOUNDED} to {config.DATA_END_DATE}")
    print(f"  Seed:    {config.SEED}")
    print("=" * 70)

    # Initialize seeded RNG
    rng = utils.get_rng(config.SEED)

    # -------------------------------------------------------
    # Step 1: Generate Reference Data
    # -------------------------------------------------------
    print("\n[Step 1/5] Generating reference data...")
    ref_gen = ReferenceDataGenerator(rng=rng)
    ref_feeds = ref_gen.generate_all()

    # -------------------------------------------------------
    # Step 2: Generate Employee Timelines
    # -------------------------------------------------------
    print("\n[Step 2/5] Generating employee timelines...")
    timeline_gen = EmployeeTimelineGenerator(ref_data=ref_gen, rng=rng)
    timeline_gen.generate_all()

    # -------------------------------------------------------
    # Step 3: Generate Rescinded Transactions
    # -------------------------------------------------------
    print("\n[Step 3/5] Generating rescinded transactions...")
    timeline_gen.generate_rescinded()

    # -------------------------------------------------------
    # Step 4: Generate Positions from assignments
    # -------------------------------------------------------
    print("\n[Step 4/5] Generating position records...")
    positions = ref_gen.generate_positions(timeline_gen.position_assignments)

    # -------------------------------------------------------
    # Step 5: Write all CSV feeds
    # -------------------------------------------------------
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

    # -------------------------------------------------------
    # Summary
    # -------------------------------------------------------
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
