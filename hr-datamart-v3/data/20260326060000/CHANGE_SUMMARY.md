# Change Summary — 2026-03-26

## Overview

This incremental run simulates one day of normal operational activity for WARLab's Workday HRDP system.

### INT6020 (FULL)

- **Rows:** 15
- **Changes:** Updated 3 row(s) with minor field changes

### INT6021 (FULL)

- **Rows:** 94
- **Changes:** Updated 3 row(s) with minor field changes

### INT6022 (FULL)

- **Rows:** 1,034
- **Changes:** Updated 2 row(s) with minor field changes

### INT6023 (FULL)

- **Rows:** 16
- **Changes:** Updated 1 row(s) with minor field changes

### INT6024 (FULL)

- **Rows:** 8
- **Changes:** Updated 1 row(s) with minor field changes

### INT6025 (FULL)

- **Rows:** 198
- **Changes:** Updated 3 row(s) with minor field changes

### INT6027 (FULL)

- **Rows:** 15
- **Changes:** Updated 3 row(s) with minor field changes

### INT6028 (FULL)

- **Rows:** 144
- **Changes:** Updated 2 row(s) with minor field changes

### INT6031 (FULL)

- **Rows:** 20,931
- **Changes:** 5 address updates, 2 new hire profiles

### INT6032 (FULL)

- **Rows:** 20,923
- **Changes:** 3 position updates

### INT0095E (DELTA)

- **Rows:** 10
- **Changes:** Includes 1 late-arriving transaction

### INT0096 (DELTA)

- **Rows:** 30
- **Changes:** 30 organization assignment rows for 10 events

### INT0098 (DELTA)

- **Rows:** 10
- **Changes:** 10 compensation records for 10 events

### INT270 (DELTA)

- **Rows:** 1
- **Changes:** Rescinded 1 previously existing transaction(s)

## Cross-File Dependencies

- New hires in INT0095E create corresponding rows in INT0096, INT0098, INT6031, INT6032
- INT270 rescinds only reference existing INT0095E Transaction_WIDs
- All foreign keys resolve to valid entities in baseline or current run