# FOLDER ORGANIZATION AND AI INDEXING

**Date:** 2026-04-25
**Subject:** SYNTHESIS folder review, reorganization, and agent-facing index

## What was accomplished

This task delivered three things in sequence: a comprehensive review of the SYNTHESIS folder, a reorganization of its contents into a clean lifecycle-based layout, and a structured index designed for AI agents to use as a routing layer when answering questions against the package.

### 1. Review and recommendation

Every file under SYNTHESIS and its subfolders was inventoried, opened, and characterized — strategy documents, performance decks, executive PDFs, workforce data CSVs, the three installable analytics skills, and every generated skill output. The review captured what was present, identified friction points (duplicated skill output runs in two parallel folders, missing data dictionary, no top-level README, an orphan CSV at the root, mixed source/tool/output content, and `__pycache__` clutter), and laid out three reorganization options with trade-offs. Option B — a lifecycle-based split into `inputs / skills / outputs / misc` — was recommended and subsequently approved.

### 2. Reorganization executed

The folder was restructured per Option B. Source content moved into a single `inputs/nbfg/` tree with sub-folders for strategy, decks, reports, and data. The three analytics skills stayed put under `skills/`. All generated skill outputs were consolidated: the most recent run of each of the six artifacts was placed under `outputs/latest/` with clean, timestamp-free filenames, while every historical timestamped run was preserved under `outputs/archive/` organized by skill. The orphan `vantrax_submission_log.csv` was moved to a new `misc/` folder with an explanatory note. All `__pycache__` directories were removed.

The three skills were updated additively to point at the new data location while retaining the legacy path as a fallback, so any externally-installed copy keeps working without modification.

### 3. AI-friendly index

A machine-readable JSON index was built and refreshed to match the new layout. Each of the 62 files in the package is described by its lifecycle stage, domain, scope, time period, owner, schema (for CSVs), key topics, and example queries it can answer. A topical index maps common query themes (Wealth strategy-vs-actual, Toronto Tech compensation/attrition, Capital Markets engagement paradox, gender pay gap, enterprise 2025 results, and others) to ranked file lists. Routing hints, code tables for LOBs and levels, and a changelog round out the index so agents can reason about the package without re-deriving structure on every call.

A human-readable data dictionary was added next to the workforce CSVs, documenting every column, every coded value, and which skill consumes which file.

## Verification

The final state was verified end-to-end: the JSON parses cleanly, every file on disk maps to exactly one indexed entry with no duplicates, every topical-index reference resolves to a real file ID, and no `__pycache__` remains.

## Artifacts created

- `SYNTHESIS_REVIEW.md` — folder review and reorganization options write-up
- `FILE_INDEX.json` — agent-facing index of every file in the package
- `README.md` — top-level entry point describing the new layout
- `DATA_DICTIONARY.md` — schema reference and value glossary for the workforce CSVs
- `NOTE.md` — context for the orphan file parked in `misc/`
- `FOLDER_ORGANIZATION_AND_AI_INDEXING.md` — this summary document

## Artifacts updated

- The three `SKILL.md` files (one per analytics skill) — recommended sample data paths refreshed
- `data_discovery.py` — default-path resolver updated to try the new layout first, with the legacy path retained as a fallback
