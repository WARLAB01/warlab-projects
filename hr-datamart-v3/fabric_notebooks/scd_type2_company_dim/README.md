# SCD Type 2 — COMPANY_DIM (L1 → L3)

Single-notebook SparkSQL implementation of an L1 → L3 Type 2 Slowly Changing
Dimension load for `l3_DM_COR_COMPANY_DIM` in a Microsoft Fabric Lakehouse.

Generated 2026-04-24 by MIA on branch `claude/friendly-cannon` for Chris Braden.

## Files

| File | Purpose |
|---|---|
| `scd_type2_company_dim.ipynb` | The Fabric notebook. Import directly into a Lakehouse-attached notebook. |
| `scd_type2_company_dim.sql`   | Pure-SQL mirror of the notebook cells for copy/paste into a fresh Fabric SparkSQL notebook. |
| `README.md`                   | This file. |

## Source / target

* **Source**: `<p_source_database_name>.<p_source_schema_name>.l1_company_dly`
  (columns: `Company_ID, Company_WID, Company_Name, Company_Code, Business_Unit, Company_Subtype, Company_Currency`)
* **Target**: `<p_target_database_name>.<p_target_schema_name>.l3_DM_COR_COMPANY_DIM`
* **Natural key**: `Company_ID`
* **Tracked attributes (in SCD hash)**: `COMPANY_WID`, `Company_Name`,
  `Company_Code`, `Business_Unit`, `Company_Subtype`, `Company_Currency`
* **Source system**: `WORKDAY`

## Source-to-target column mapping

| Source column      | Target column                    | Notes |
|--------------------|----------------------------------|-------|
| `Company_ID`       | `COMPANY_ID`                     | Natural key |
| `Company_Code`     | `COMPANY_CD`                     | |
| `Company_WID`      | `COMPANY_WID`                    | |
| `Company_Name`     | `COMPANY_NM_DESCR`               | |
| `Company_Subtype`  | `COMPANY_SUBTYPE_CD`             | |
| `Company_Currency` | `COMPANY_CURRENCY_CD`            | |
| `Business_Unit`    | `BUSINESS_UNIT_HIERARCHY_CD`     | |
| _(none)_           | `COMPANY_HIERARCHY_DESCR`        | Inserted as `NULL` per spec — no apparent source field |
| _(audit)_          | `VALID_FROM_TS`, `VALID_TO_TS`, `CURRENT_RECORD_IND`, `SOFT_DELETE_FLG`, `SOURCE_SYSTEM_CD`, `SOURCE_EXTRACT_TS`, `ETL_CREATE_TS`, `ETL_UPDATE_TS`, `ETL_CREATE_NUM`, `ETL_UPDATE_NUM` | Stamped from parameters per the audit + SCD rules |
| _(hash)_           | `SCD_HASH_CD`                    | MD5 over tracked attributes; audit fields excluded |
| _(identity)_       | `COMPANY_SK`                     | IDENTITY column — autopopulated, **not** in `INSERT` list |

## Notebook parameters

| Name                     | Type                | Example |
|--------------------------|---------------------|---------|
| `p_run_date`             | `DATE`              | `2026-04-24` |
| `p_run_ts`               | `TIMESTAMP`         | `2026-04-24 06:00:00` |
| `p_etl_run_id`           | `STRING` or `BIGINT`| `20260424060000` |
| `p_source_database_name` | `STRING`            | `lh_hr_l1` |
| `p_source_schema_name`   | `STRING`            | `workday` |
| `p_target_database_name` | `STRING`            | `lh_hr_l3` |
| `p_target_schema_name`   | `STRING`            | `dm_cor` |

The notebook binds these to Spark session variables in Phase 0 so each cell
runs independently.

## Phases

```
Phase 0  Bind parameters → session variables
Phase 1  Stage source feed (TRIM, COALESCE 'Ø', MD5 hash)
Phase 2  Snapshot current target rows (CURRENT_RECORD_IND = 'Y')
Phase 3  Classify staged rows: NEW / CHANGED / NO-OP
Phase 4  UPDATE — expire CHANGED current rows
Phase 5  INSERT — NEW rows + new versions of CHANGED rows
Phase 6  Diagnostic: counts by action
Phase 7  Integrity checks (uniqueness of open rows; validity window)
```

## Audit + SCD rules implemented

* `VALID_FROM_TS = p_run_date`, clamped to `MAX(p_run_date, 1800-01-01)`
* `VALID_TO_TS  = '9999-12-31'` for current rows
* `CURRENT_RECORD_IND = 'Y'` for current rows only
* `SOFT_DELETE_FLG = 'N'` on insert
* `SOURCE_SYSTEM_CD = 'WORKDAY'`
* `SCD_HASH_CD`: MD5 over `CONCAT_WS('||', COALESCE(TRIM(col), 'Ø'), ...)`
  for tracked attributes; audit fields excluded.

## Constraints honoured

* SparkSQL only — no Python or Scala in the notebook.
* No monolithic `MERGE`. Staged temp views, then a single `UPDATE` and a
  single `INSERT`.
* Target table is assumed to already exist with the exact layout supplied.
* Output is copy/paste-runnable in Fabric.
