# SYNTHESIS Folder Review

> **Status (2026-04-25):** Option B has been implemented. The folder has been reorganized into `inputs/`, `skills/`, `outputs/`, and `misc/`. See the root [`README.md`](./README.md) for the current layout and [`FILE_INDEX.json`](./FILE_INDEX.json) for the agent-facing index. The sections below are kept as a record of the pre-reorg state and the rationale for the reorg.

**Reviewed:** 2026-04-25  
**Scope:** Every file under `/SYNTHESIS` and its subfolders (59 files at review time, ~5.0 MB)  
**Companion file:** [`FILE_INDEX.json`](./FILE_INDEX.json) — machine-readable index optimized for AI agents

---

## 1. What's in the folder today

The SYNTHESIS folder holds an end-to-end demonstration package built around a fictional Canadian/US bank, **Northbridge Financial Group (NBFG)**. It bundles three things that today sit side-by-side without a clear separator:

1. **Source content** — the strategy documents, performance decks, executive reports, and workforce data CSVs that make up the "ground truth" for NBFG.
2. **Analytical tooling** — three installable Claude skills (`strategic-alignment-check`, `pay-equity-review`, `risk-hot-spots`) plus shared helpers and an `INSTALL.sh`.
3. **Generated outputs** — PDF executive briefs and Excel detail workbooks produced by running the skills against the NBFG data, scattered across two `_skill_outputs/` folders with multiple timestamped runs of the same analyses.

A single small CSV (`vantrax_submission_log.csv`) sits at the root with no folder, no header narrative, and no apparent connection to the NBFG demo — likely an orphan from another workstream.

### Folder map (current)

```
SYNTHESIS/
├── vantrax_submission_log.csv          ← orphan, 20 rows, employee submission tracking
├── NBFG_Demo_Pack/                     ← the fictional bank's content + data
│   ├── strategy_documents/             ← 4 .docx (enterprise + 3 LOB/function strategies)
│   ├── performance_decks/              ← 4 .pptx (enterprise + 3 LOB business reviews)
│   ├── reports/                        ← 3 .pdf (engagement summary, comp philosophy, CEO letter)
│   ├── data/                           ← 8 .csv (workforce data: HC, attrition, hiring, comp, etc.)
│   ├── data/_skill_outputs/            ← 6 generated artifacts (duplicates of 1758/1801 runs)
│   └── _skill_outputs/                 ← 10 generated artifacts (1752, 1802, 1815 runs)
└── skills/                             ← 3 installable analytics skills
    ├── INSTALL.sh
    ├── _shared/                        ← data_discovery.py, skill_outputs.py
    ├── strategic-alignment-check/
    ├── pay-equity-review/
    └── risk-hot-spots/
```

### Content themes (NBFG_Demo_Pack)

The NBFG package tells one coherent story across formats. The strategy docs and CEO letter declare ambitions; the workforce CSVs record what actually happened; the performance decks reconcile the two. Every artifact is internally consistent:

- **Wealth Management** is the highest-priority growth franchise (target: +5% advisors, +12% AUM). Actual results landed flat-to-negative on every metric — a textbook strategy/execution gap.
- **Toronto Technology** has a documented compensation problem (12-15% below market median) driving senior engineering attrition; this thread runs through the Tech strategy, the engagement survey, the CEO letter, the engagement CSV, and the comp CSV.
- **Capital Markets** had a record financial year ($820M NIBT, +18%) with the lowest engagement score in the bank (52). It's the canonical "good numbers, bad people-signal" narrative.
- **Canadian P&C** is the steady-eddy: flat revenue, highest engagement (78), no big issues.

The three skills are purpose-built to surface exactly these tensions from this data shape.

---

## 2. What works, and what doesn't

**Working well**
- The NBFG_Demo_Pack is internally consistent — strategy claims line up with data trends, decks reconcile cleanly, and the skills produce findings that map to the real narrative.
- The skills are well-structured (separate `analyze.py`, `build_outputs.py`, `validate.py`, with a shared helper layer).
- The strategy docs and reports are well-named (numeric prefixes give a clear reading order).

**Friction points for humans and AI agents**
- **No top-level README or index.** A new reader (human or agent) lands on the folder with no map.
- **Duplicated, undated skill outputs.** Running the three skills twice has produced ten timestamped artifacts in two different `_skill_outputs/` folders, and there's no manifest explaining which run is canonical. An agent searching for "the latest pay equity brief" has to scan timestamps across two locations.
- **Source vs. tool vs. output is mixed.** `skills/` (executable code) and `NBFG_Demo_Pack/` (content) both live at the root, but the relationship between them is implicit. New users can't tell which folder to feed to which tool.
- **Data dictionary missing.** Eight CSVs with column names like `lob_code`, `level`, `regrettable`, `participation_rate_pct` — but no schema reference, no value glossary (what's an `L4`?), no cross-reference of which skill consumes which CSV.
- **Orphan file.** `vantrax_submission_log.csv` at the root looks like a leftover. Either retire it or give it a folder + brief context note.
- **`__pycache__` directories.** Shouldn't be in a shared package; bloats the index and adds noise for agents.
- **Filename inconsistency in outputs.** The generated PDFs have timestamp suffixes; the inputs don't. There's no naming convention documented.

---

## 3. Reorganization options

Three options, ordered from least to most disruptive. **Recommended: Option B.**

### Option A — Light cleanup, keep the current shape
Keep `NBFG_Demo_Pack/` and `skills/` at the root. Just:
- Add a `README.md` at the root explaining the package.
- Consolidate the two `_skill_outputs/` folders into one, drop the duplicate runs, keep only the latest.
- Delete `__pycache__/` directories.
- Add a `DATA_DICTIONARY.md` inside `NBFG_Demo_Pack/data/`.
- Either delete `vantrax_submission_log.csv` or move it to a clearly labelled "misc" folder with a sentence of context.

**Trade-offs:** Minimal disruption. Existing skill scripts and pathing keep working. Doesn't fix the conceptual mixing of "source content" vs. "tools" vs. "generated outputs" — an AI agent still has to know that `skills/` runs against `NBFG_Demo_Pack/data/` and writes into `_skill_outputs/`.

### Option B — Reorganize by lifecycle stage (Recommended)
Split the folder along the natural pipeline: **inputs → tools → outputs.** Every file maps to exactly one stage; AI agents can be told "look in `/inputs` for source-of-truth questions, look in `/outputs` for already-computed analyses."

```
SYNTHESIS/
├── README.md
├── FILE_INDEX.json                     ← agent-facing index
├── inputs/                             ← all source content (read-only)
│   └── nbfg/
│       ├── strategy/                   ← .docx strategies (rename without numeric prefix)
│       ├── decks/                      ← .pptx business reviews
│       ├── reports/                    ← .pdf executive reports
│       ├── data/                       ← the 8 .csv files
│       └── DATA_DICTIONARY.md
├── skills/                             ← unchanged (installable)
│   ├── INSTALL.sh
│   ├── _shared/
│   ├── strategic-alignment-check/
│   ├── pay-equity-review/
│   └── risk-hot-spots/
├── outputs/                            ← every generated artifact
│   ├── latest/                         ← canonical "most recent" run, no timestamps
│   │   ├── strategic_alignment_brief.pdf
│   │   ├── strategic_alignment_detail.xlsx
│   │   ├── pay_equity_brief.pdf
│   │   ├── pay_equity_detail.xlsx
│   │   ├── risk_hotspots_brief.pdf
│   │   └── risk_hotspots_detail.xlsx
│   └── archive/                        ← historical runs, timestamped (kept for diffs)
└── misc/
    └── vantrax_submission_log.csv      ← with a note explaining what it is
```

**Trade-offs:** Cleanest mental model for both humans and AI agents. Requires updating any hard-coded paths in the skills (the `_shared/data_discovery.py` already does discovery, so impact should be small — worth a check). Breaks any external links pointing into the current structure.

**Why I recommend it:** an AI agent answering "what's our Wealth strategy" only needs to look in `/inputs/nbfg/strategy/`, and an agent answering "what did the latest pay-equity run find" only needs to look in `/outputs/latest/`. The current structure forces the agent to understand the implicit pipeline.

### Option C — Reorganize by business domain
Group everything around the *subject* rather than the lifecycle:

```
SYNTHESIS/
├── strategy-and-narrative/             ← strategy docs + decks + CEO letter + engagement summary
├── workforce-data/                     ← the 8 CSVs + dictionary + skill outputs
├── analytics-tooling/                  ← the skills
└── misc/
```

**Trade-offs:** Maps closely to how a human executive thinks ("show me the strategy stuff", "show me the workforce numbers"). But it blurs source vs. derived (strategy docs and CEO letter end up next to performance decks, which are themselves syntheses), and it splits skill inputs from skill outputs. Less ergonomic for an agent than Option B.

---

## 4. Recommended cleanup checklist (regardless of option chosen)

| Action | Reason |
|---|---|
| Add a root `README.md` describing the package, the skills, and the data | Single entry point for new readers |
| Add `NBFG_Demo_Pack/data/DATA_DICTIONARY.md` with column descriptions, value lists for `lob_code` / `level` / `region`, and which skill consumes which CSV | Eight CSVs + no glossary is the single biggest comprehension blocker |
| Consolidate skill outputs to one folder; keep one canonical "latest" run per skill | Removes duplicate-run confusion |
| Delete `__pycache__/` directories and add a `.gitignore` if this folder is versioned | Reduces noise; bytecode shouldn't be shared |
| Resolve `vantrax_submission_log.csv` (delete, move, or annotate) | Currently orphaned at the root |
| Adopt a consistent output filename convention (`{skill}_{artifact}_{YYYYMMDD_HHMM}.{ext}` or drop timestamps for "latest" copies) | Makes outputs predictable for both agents and humans |

---

## 5. Human-readable file index

Full structured index lives in [`FILE_INDEX.json`](./FILE_INDEX.json). Quick reference table:

### Source content (`NBFG_Demo_Pack/`)

| File | Type | Owner | What it covers | Best questions to send here |
|---|---|---|---|---|
| `strategy_documents/01_NBFG_Enterprise_Strategy_2025-2027.docx` | .docx | Office of the CEO | 3-year enterprise strategy, 5 pillars, 50K employees, $4.3B 2024 NIBT | Enterprise priorities, pillar list, financial baseline, talent commitments |
| `strategy_documents/02_Wealth_Management_Strategy_2025.docx` | .docx | James Whitford, Group Head Wealth | Wealth growth plan, +5% advisor target, +12% AUM target, $385B AUM by 2027 | Wealth strategy, advisor targets, AUM goals, talent investments |
| `strategy_documents/03_Technology_Talent_and_Modernization_Plan_2025.docx` | .docx | Priya Ramaswamy, CTO | Tech strategy, 6,000 engineers, Toronto comp gap, $2.4B 2025-2027 envelope | Tech strategy, Toronto attrition, comp benchmarking (McLean & Co.) |
| `strategy_documents/04_People_and_Culture_Strategy_2025.docx` | .docx | Diane Okafor-Lindgren, CHRO | P&C function strategy, engagement, attrition pockets, 5 priorities | HR strategy, engagement health, manager development plans |
| `performance_decks/01_Enterprise_Performance_Review_Q4_2025.pptx` | .pptx | Office of the CFO | 2025 full-year scorecard, $4.6B NIBT, 14.3% ROE, 70 engagement | Enterprise results, scorecard, LOB contribution mix |
| `performance_decks/02_Capital_Markets_Annual_Business_Review_2025.pptx` | .pptx | Sebastian Holm, Group Head CM | $820M NIBT (+18%), record FICC year, 52 engagement, ~7% VP gender comp gap | Capital Markets results, the "good numbers, bad signals" story |
| `performance_decks/03_Canadian_PC_Business_Review_2025.pptx` | .pptx | Marie-Claire Tremblay, Group Head CPC | $1.8B NIBT, flat revenue, 78 engagement, 180K net new customers | Canadian P&C results, retail banking trajectory |
| `performance_decks/04_Wealth_Management_Strategic_Update_Q4_2025.pptx` | .pptx | James Whitford | Plan-vs-actual on every Wealth metric — all behind plan | Wealth strategy/execution gap, advisor headcount miss |
| `reports/01_2025_Annual_Engagement_Survey_Executive_Summary.pdf` | .pdf | Diane Okafor-Lindgren, CHRO | 70 enterprise score, 82% participation, 28pt gap top-to-bottom LOB | Engagement headline, LOB-by-LOB scorecard, manager effectiveness story |
| `reports/02_2025_Total_Rewards_and_Compensation_Philosophy.pdf` | .pdf | Compensation Committee | 5 comp principles, pay-mix by level, market positioning policy (±5% of median) | Comp philosophy, pay-for-performance framework, LTI structure |
| `reports/03_CEO_Year_End_Letter_to_Employees_2025.pdf` | .pdf | Margaret Chen, CEO | 2025 reflection, $4.6B NIBT, Wealth shortfall, variable pool at 105% of target | CEO narrative, year-end reflection, 2026 priorities |

### Workforce data CSVs (`NBFG_Demo_Pack/data/`)

| File | Grain | Rows | Key columns | Used by skills |
|---|---|---|---|---|
| `attrition_data.csv` | month × LOB × region × city × level | 7,007 | departures, voluntary, involuntary, regrettable, top_reason | risk-hot-spots |
| `compensation_summary.csv` | year × LOB × region × level × gender | 1,055 | avg/median base & total comp, currency | pay-equity-review |
| `diversity_demographics.csv` | year × LOB × region × level × gender × ethnicity | 7,376 | headcount | (reference) |
| `engagement_survey_results.csv` | year × LOB × region × dimension | 529 | score, external_benchmark, participation_rate_pct | risk-hot-spots, strategic-alignment-check |
| `headcount_by_lob_region.csv` | quarter × LOB × region × city × level | 12,382 | headcount | strategic-alignment-check, risk-hot-spots |
| `hiring_data.csv` | month × LOB × region × city × level | 12,847 | hires, source, avg_time_to_fill_days | strategic-alignment-check, risk-hot-spots |
| `internal_movement.csv` | quarter × from_LOB → to_LOB × level | 3,480 | movement_type, count | (reference) |
| `promotion_rates.csv` | year × LOB × level × gender | 463 | eligible_count, promoted_count, promotion_rate_pct | (reference) |

### Analytics skills (`skills/`)

| Skill | Purpose | Inputs | Outputs |
|---|---|---|---|
| `strategic-alignment-check/` | Cross-references strategy docs against workforce data; flags strategy/execution gaps | strategy .docx/.pdf/.pptx + headcount/hiring/attrition/engagement CSVs | 1-pg PDF brief + ~5-tab Excel workbook |
| `pay-equity-review/` | Computes gender comp gaps at LOB+level grain; flags regional compression after FX-normalization | `compensation_summary.csv` | 1-pg PDF brief + ~7-tab Excel workbook |
| `risk-hot-spots/` | Composite risk score per LOB+region from engagement, attrition, hiring, comp signals | `attrition_data.csv`, `engagement_survey_results.csv`, `hiring_data.csv`, `headcount_by_lob_region.csv` | 1-pg PDF brief + ~6-tab Excel workbook |
| `_shared/data_discovery.py` | Helper: locates input CSVs in target folder | — | (utility) |
| `_shared/skill_outputs.py` | Helper: writes timestamped briefs and workbooks with BMO branding | — | (utility) |
| `INSTALL.sh` | Copies skills into `~/.claude/skills/` | — | (installer) |

### Generated outputs (`_skill_outputs/`)

All six artifacts are duplicated across multiple timestamped runs (1752, 1756, 1758, 1801, 1802, 1815). The most recent run per skill is:

| Artifact | Latest path |
|---|---|
| Strategic alignment brief (PDF) | `NBFG_Demo_Pack/_skill_outputs/strategic_alignment/strategic_alignment_brief_20260425_1815.pdf` |
| Strategic alignment detail (Excel) | `NBFG_Demo_Pack/_skill_outputs/strategic_alignment/strategic_alignment_detail_20260425_1815.xlsx` |
| Pay equity brief (PDF) | `NBFG_Demo_Pack/_skill_outputs/pay_equity_brief_20260425_1802.pdf` |
| Pay equity detail (Excel) | `NBFG_Demo_Pack/_skill_outputs/pay_equity_detail_20260425_1802.xlsx` |
| Risk hotspots brief (PDF) | `NBFG_Demo_Pack/_skill_outputs/risk_hotspots_brief_20260425_1802.pdf` |
| Risk hotspots detail (Excel) | `NBFG_Demo_Pack/_skill_outputs/risk_hotspots_detail_20260425_1802.xlsx` |

Older copies in `NBFG_Demo_Pack/data/_skill_outputs/` and at earlier timestamps in `NBFG_Demo_Pack/_skill_outputs/` are candidates for archive/delete.

### Orphan

| File | Notes |
|---|---|
| `vantrax_submission_log.csv` | 20-row CSV of employee_id / name / reference_number / status (all "submitted"). No clear connection to NBFG_Demo_Pack. Resolve before reorg. |

---

## 6. How to use the companion `FILE_INDEX.json`

`FILE_INDEX.json` is built for AI agents to load and use as a routing layer. Each file entry includes:

- `path` — relative to `SYNTHESIS/`
- `type` — file extension / format
- `lifecycle_stage` — `source` | `tool` | `generated_output` | `orphan`
- `domain` — `strategy`, `workforce_data`, `narrative_report`, `business_review`, `analytics_skill`, `analytics_artifact`, etc.
- `scope` — enterprise / LOB / function / region
- `time_period` — calendar coverage (e.g. "2025", "2025-2027", "Q4 2025")
- `owner` — named accountable executive (where stated in the document)
- `key_topics` — short tag list; tuned for keyword retrieval
- `summary` — one-sentence description
- `example_queries` — sample user questions this file is the right answer for
- `consumed_by` / `produced_by` — for CSVs and outputs, which skills reference them

Plus a top-level `topical_index` mapping common queries → ranked file lists (e.g. `"toronto_tech_attrition"` → the four files that touch this thread).
