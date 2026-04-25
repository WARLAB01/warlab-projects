# STRATEGIC ALIGNMENT CHECK

## Overview

This task identified situations where business actions may not be aligning to stated strategy. The analysis cross-referenced strategy documents (Word, PDF, PowerPoint) against quantitative workforce CSVs to surface measurable gaps between what the organization said it would do and what the data shows it actually did.

## What Was Accomplished

The analysis reviewed 14 documents — including 4 strategy documents, 3 executive reports, and 4 performance decks — and reconciled them against 8 workforce data sources covering headcount, hiring, attrition, engagement, compensation, diversity, internal movement, and promotions. For each line of business referenced in the documents, the skill inferred the stated strategic intent (growth, maintain, or reduce) and compared it to the observed two-year trend in the underlying data.

11 lines of business had sufficient metric coverage to evaluate. The analysis produced 36 individual findings, of which 21 were classified as critical or high severity, indicating material gaps between strategic intent and execution.

## Headline Findings

**Wealth Management — Critical severity.** The dedicated Wealth Management Strategy and the Enterprise Strategy frame this LOB as a talent-led growth priority. The data shows the opposite trajectory: hiring is down 15.6%, attrition is up 7 percentage points, and engagement has fallen from 68.8 to 61.8 (down 7 points). Every leading indicator is moving against the stated direction.

**Technology — High severity.** The Technology Talent and Modernization Plan calls for investment and capability growth. Headcount and hiring are both up modestly, but attrition rose 7 percentage points and engagement dropped 8.1 points (71.4 to 63.2) — the steepest engagement decline observed in the enterprise. Quantitative growth is happening, but the retention and morale signals are eroding underneath it.

## Methodology Summary

The analysis is fully deterministic. It walks the source folder, classifies CSVs by filename pattern, extracts plain text from each document, scans surrounding language for positive intent markers (grow, invest, accelerate, top-quartile) versus negative markers (wind-down, exit, divest), and computes two-year deltas in headcount, hiring, attrition rate, and engagement for each LOB. Severity is scored higher when growth intent collides with deterioration on multiple metrics simultaneously.

## Artifacts Created

- `strategic_alignment_brief_20260425_1815.pdf` — one-page executive brief with the top findings, severity-coded, in BMO Corporate Classic styling.
- `strategic_alignment_detail_20260425_1815.xlsx` — multi-tab detail workbook containing Summary, Findings, Documents Reviewed, LOB Trend Metrics, and Methodology tabs.
- `STRATEGIC_ALIGNMENT_CHECK.md` — this summary document.
