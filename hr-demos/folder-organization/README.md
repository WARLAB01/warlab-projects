# SYNTHESIS

End-to-end demo package built around a fictional Canadian/US bank, **Northbridge Financial Group (NBFG)**. The folder is organized by **lifecycle stage** so both humans and AI agents know where to look:

```
SYNTHESIS/
├── README.md                ← this file
├── SYNTHESIS_REVIEW.md      ← review of structure & reorg rationale
├── FILE_INDEX.json          ← machine-readable index for AI agents
│
├── inputs/                  ← read-only source content
│   └── nbfg/
│       ├── strategy/        ← .docx strategy documents
│       ├── decks/           ← .pptx business reviews
│       ├── reports/         ← .pdf executive reports
│       ├── data/            ← .csv workforce data
│       └── data/DATA_DICTIONARY.md   ← schema + value glossary for the CSVs
│
├── skills/                  ← installable analytics tooling
│   ├── INSTALL.sh
│   ├── _shared/
│   ├── strategic-alignment-check/
│   ├── pay-equity-review/
│   └── risk-hot-spots/
│
├── outputs/                 ← generated artifacts
│   ├── latest/              ← canonical "most recent" run, no timestamps
│   └── archive/             ← historical runs, timestamped, organized by skill
│
└── misc/                    ← unrelated / orphan files (with NOTE.md)
```

## How to use this folder

**Looking for source-of-truth content** (strategy plans, business reviews, executive reports, the workforce data) — start in **`inputs/nbfg/`**.

**Want to run an analysis** — go to **`skills/`**. Each skill has a `SKILL.md` describing what it does and how to invoke it. The skills point at `inputs/nbfg/data/` by default; you can pass any folder.

**Want a pre-computed analysis** — go to **`outputs/latest/`**. Each skill produces two files: a 1-page PDF executive brief and a multi-tab Excel detail workbook. Older runs sit in `outputs/archive/{skill}/` if you need to diff.

**Investigating a column** — open **`inputs/nbfg/data/DATA_DICTIONARY.md`**. It explains every column, the meaning of `lob_code` / `level` / `region` values, and which skill consumes which CSV.

## For AI agents

`FILE_INDEX.json` at the root is the routing index. Each file entry includes its lifecycle stage, scope, owner, key topics, schema (for CSVs), and example queries it can answer. The top-level `topical_index` maps common query themes (e.g. `toronto_tech_compensation_attrition`, `wealth_management_strategy_vs_actual`) to ranked file lists.

Decision tree:
1. Is the user asking about NBFG content? → `lifecycle_stage == "source"`
2. Are they asking to run an analysis? → `lifecycle_stage == "tool"` (start with the matching `SKILL.md`)
3. Are they asking what a previous analysis found? → `lifecycle_stage == "generated_output"` AND `is_latest == true`

## Disclaimer

NBFG is a fictional bank invented for demonstration purposes. No figure, person, or claim in `inputs/nbfg/` is a real-world fact.
