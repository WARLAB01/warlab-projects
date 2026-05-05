# Change Summary — 2026-03-25

## Overview

This incremental run simulates one day of normal operational activity for WARLab's Workday HRDP system.

### INT6020 (FULL)

- **Rows:** 15
- **Changes:** Updated 1 row(s) with minor field changes

### INT6021 (FULL)

- **Rows:** 94
- **Changes:** Updated 1 row(s) with minor field changes

### INT6022 (FULL)

- **Rows:** 1,034
- **Changes:** Updated 3 row(s) with minor field changes

### INT6023 (FULL)

- **Rows:** 16
- **Changes:** Updated 1 row(s) with minor field changes

### INT6024 (FULL)

- **Rows:** 8
- **Changes:** Updated 2 row(s) with minor field changes

### INT6025 (FULL)

- **Rows:** 198
- **Changes:** Updated 1 row(s) with minor field changes

### INT6027 (FULL)

- **Rows:** 15
- **Changes:** Updated 3 row(s) with minor field changes

### INT6028 (FULL)

- **Rows:** 144
- **Changes:** Updated 12 row(s) with minor field changes

### INT6031 (FULL)

- **Rows:** 20,929
- **Changes:** 5 address updates, 3 new hire profiles

### INT6032 (FULL)

- **Rows:** 20,923
- **Changes:** 2 position updates

### INT0095E (DELTA)

- **Rows:** 7
- **Changes:** Includes 1 late-arriving transaction

### INT0096 (DELTA)

- **Rows:** 21
- **Changes:** 21 organization assignment rows for 7 events

### INT0098 (DELTA)

- **Rows:** 7
- **Changes:** 7 compensation records for 7 events

### INT270 (DELTA)

- **Rows:** 1
- **Changes:** Rescinded 1 previously existing transaction(s)

## Cross-File Dependencies

- New hires in INT0095E create corresponding rows in INT0096, INT0098, INT6031, INT6032
- INT270 rescinds only reference existing INT0095E Transaction_WIDs
- All foreign keys resolve to valid entities in baseline or current run