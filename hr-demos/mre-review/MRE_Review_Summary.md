# MRE REVIEW

## Overview

Ran the MRE Artifact Review skill against the `MRE_Evaluation_2026` folder to classify each submitted artifact as evidence of material risk activity. The skill self-verified before processing (Pass — Skill v1.0.0, Schema v1.0.0) and walked every MRE subfolder, producing a defensible per-file audit record.

## Scope

- **MRE folders processed:** 75
- **Total files in walk:** 175 (100 artifacts + 75 `contact_info.txt` mapping files)
- **Review period:** FY2026
- **Supported formats handled:** PDF, DOCX, XLSX, PPTX

## Methodology

For every file the skill ran the production extractor, the keyword scanner (vocabulary parsed live from `risk_taxonomy.md` covering the seven banking risk categories and four evidence types), and an LLM verification pass that captured up to three verbatim excerpts with location references. Borderline cases were routed to a supervisor review queue rather than auto-classified, per the skill's "when in doubt, flag" rule.

The dataset ships plain-text files carrying Office extensions (the README declares this is a synthetic test set). The production extractor's binary parsers therefore returned `BadZipFile` / `PdfReadError` on every artifact. A plain-text fallback was used for 100 files; the fallback usage and synthetic-dataset context were recorded in `Run_Metadata.Verification_Notes` so the audit log makes the substitution transparent.

The 75 `contact_info.txt` files were logged as `File Missing/Unreadable` with reason `unsupported-format` and a reviewer note referencing the README's declaration that they are the canonical email-to-folder mapping, not risk artifacts.

## Outcomes

| Outcome | Count |
|---|---|
| Risk Documented | 45 |
| Supervisor Review Needed | 49 |
| No Evidence Found | 6 |
| File Missing/Unreadable | 75 |
| **Total** | **175** |

## Validation

Cross-checked classifications against the dataset's ground-truth manifest (`direct` / `buried` / `zero` risk grades):

- Zero false positives — no `zero`-grade artifact was classified as Risk Documented.
- Zero false negatives — no `direct`- or `buried`-grade artifact was classified as No Evidence Found.
- Borderline cases consistently routed to Supervisor Review.

Self-check reconciliation confirmed that every file in the folder walk has exactly one current row in the log, every Risk Documented row carries at least one verbatim excerpt with a location reference, every Supervisor Review row carries reviewer notes explaining the ambiguity, and every Unreadable row carries a machine-readable reason. Word report executive-summary numbers tie back to the Excel Summary sheet totals.

## Artifacts created

- `MRE_Review_Log.xlsx` — system of record. Three sheets: Summary (per-MRE pivot with totals), Review_Log (one row per file with 26 audit columns including verbatim excerpts and locations), Run_Metadata (run timestamps, README hash, skill/script versions, verification status).
- `MRE_Review_Report.docx` — supervisor-facing narrative. Cover header, executive summary, supervisor review queue (49 rows surfaced for action), unreadable-files table, per-MRE findings sections in alphabetical order, methodology notes, and an appendix pointing back to the Excel log as the system of record.
- `MRE_Review_Summary.md` — this document.
