-- ============================================================================
-- HR Datamart V3 — L1 → L3 Type 2 SCD Load: COMPANY_DIM
-- Target: Microsoft Fabric Lakehouse (SparkSQL on Delta tables)
-- Source : <p_source_database_name>.<p_source_schema_name>.l1_company_dly
-- Target : <p_target_database_name>.<p_target_schema_name>.l3_DM_COR_COMPANY_DIM
-- ----------------------------------------------------------------------------
-- This file mirrors the cells of scd_type2_company_dim.ipynb so the same logic
-- can be copy/pasted into a Fabric SparkSQL notebook. Each PHASE block below
-- corresponds to one notebook cell; the lines starting with `-- MARKDOWN:` are
-- the markdown-cell headers that precede the SQL cell.
-- ============================================================================
-- Author        : MIA (Wes Barlow's CoWork agent)
-- Generated for : Chris Braden — HR Datamart v3
-- Generated on  : 2026-04-24
-- Branch        : claude/friendly-cannon
-- ============================================================================


-- ============================================================================
-- PARAMETERS (notebook parameter cell — set by pipeline or run interactively)
-- ============================================================================
-- p_run_date              DATE       -- e.g. DATE'2026-04-24'
-- p_run_ts                TIMESTAMP  -- e.g. TIMESTAMP'2026-04-24 06:00:00'
-- p_etl_run_id            BIGINT     -- e.g. 20260424060000
-- p_source_database_name  STRING     -- e.g. 'lh_hr_l1'
-- p_source_schema_name    STRING     -- e.g. 'workday'
-- p_target_database_name  STRING     -- e.g. 'lh_hr_l3'
-- p_target_schema_name    STRING     -- e.g. 'dm_cor'
-- ============================================================================


-- ============================================================================
-- MARKDOWN: ## Phase 0 — Bind parameters as session variables
-- Spark SQL session variables (DECLARE / SET) make every downstream cell
-- self-contained and identical to a Fabric pipeline parameter binding.
-- Replace the SET literals below with your pipeline parameter values.
-- ============================================================================
DECLARE OR REPLACE VARIABLE p_run_date             DATE;
DECLARE OR REPLACE VARIABLE p_run_ts               TIMESTAMP;
DECLARE OR REPLACE VARIABLE p_etl_run_id           BIGINT;
DECLARE OR REPLACE VARIABLE p_source_database_name STRING;
DECLARE OR REPLACE VARIABLE p_source_schema_name   STRING;
DECLARE OR REPLACE VARIABLE p_target_database_name STRING;
DECLARE OR REPLACE VARIABLE p_target_schema_name   STRING;

SET VAR p_run_date             = DATE'2026-04-24';
SET VAR p_run_ts               = TIMESTAMP'2026-04-24 06:00:00';
SET VAR p_etl_run_id           = 20260424060000;
SET VAR p_source_database_name = 'lh_hr_l1';
SET VAR p_source_schema_name   = 'workday';
SET VAR p_target_database_name = 'lh_hr_l3';
SET VAR p_target_schema_name   = 'dm_cor';


-- ============================================================================
-- MARKDOWN: ## Phase 1 — Stage the source feed with computed SCD hash
-- Reads l1_company_dly, TRIMs strings, COALESCEs nulls to the sentinel 'Ø'
-- and computes SCD_HASH_CD (MD5 over the tracked attributes only — audit
-- columns are NOT part of the hash).
-- ============================================================================
CREATE OR REPLACE TEMPORARY VIEW v_stg_company AS
SELECT
    -- natural key + business attributes (trimmed)
    TRIM(src.Company_ID)         AS COMPANY_ID,
    TRIM(src.Company_Code)       AS COMPANY_CD,
    TRIM(src.Company_WID)        AS COMPANY_WID,
    TRIM(src.Company_Name)       AS COMPANY_NM_DESCR,
    TRIM(src.Company_Subtype)    AS COMPANY_SUBTYPE_CD,
    TRIM(src.Company_Currency)   AS COMPANY_CURRENCY_CD,
    TRIM(src.Business_Unit)      AS BUSINESS_UNIT_HIERARCHY_CD,
    -- columns with no apparent source field — insert NULL per spec
    CAST(NULL AS STRING)         AS COMPANY_HIERARCHY_DESCR,
    -- SCD hash over tracked attributes only (audit fields excluded)
    -- Tracked: COMPANY_WID, Company_Name, Company_Code, Business_Unit,
    --         Company_Subtype, Company_Currency
    MD5(CONCAT_WS('||',
        COALESCE(TRIM(src.Company_WID),      'Ø'),
        COALESCE(TRIM(src.Company_Name),     'Ø'),
        COALESCE(TRIM(src.Company_Code),     'Ø'),
        COALESCE(TRIM(src.Business_Unit),    'Ø'),
        COALESCE(TRIM(src.Company_Subtype),  'Ø'),
        COALESCE(TRIM(src.Company_Currency), 'Ø')
    ))                           AS SCD_HASH_CD
FROM IDENTIFIER(p_source_database_name || '.' || p_source_schema_name || '.l1_company_dly') AS src
WHERE src.Company_ID IS NOT NULL;


-- ============================================================================
-- MARKDOWN: ## Phase 2 — Snapshot the current target rows
-- Captures the current open version (CURRENT_RECORD_IND = 'Y') of every
-- COMPANY_ID. Used by Phases 3/4 to classify NEW vs CHANGED vs NO-OP.
-- ============================================================================
CREATE OR REPLACE TEMPORARY VIEW v_tgt_current AS
SELECT
    COMPANY_ID,
    SCD_HASH_CD AS TGT_SCD_HASH_CD
FROM IDENTIFIER(p_target_database_name || '.' || p_target_schema_name || '.l3_DM_COR_COMPANY_DIM')
WHERE CURRENT_RECORD_IND = 'Y';


-- ============================================================================
-- MARKDOWN: ## Phase 3 — Classify staged rows: NEW / CHANGED / NO-OP
-- Single LEFT JOIN of staging against current target. Three disjoint sets:
--   * NEW     — Company_ID not present in current target
--   * CHANGED — Company_ID present but hash differs
--   * NO-OP   — Company_ID present and hash matches (filtered out below)
-- ============================================================================
CREATE OR REPLACE TEMPORARY VIEW v_stg_classified AS
SELECT
    s.*,
    CASE
        WHEN t.COMPANY_ID IS NULL                 THEN 'NEW'
        WHEN t.TGT_SCD_HASH_CD <> s.SCD_HASH_CD   THEN 'CHANGED'
        ELSE 'NO-OP'
    END AS scd_action
FROM v_stg_company s
LEFT JOIN v_tgt_current t
       ON t.COMPANY_ID = s.COMPANY_ID;

CREATE OR REPLACE TEMPORARY VIEW v_stg_to_apply AS
SELECT * FROM v_stg_classified WHERE scd_action IN ('NEW','CHANGED');

CREATE OR REPLACE TEMPORARY VIEW v_stg_changed AS
SELECT COMPANY_ID FROM v_stg_classified WHERE scd_action = 'CHANGED';


-- ============================================================================
-- MARKDOWN: ## Phase 4 — Expire CHANGED rows in the target
-- For every CHANGED Company_ID, close out the current open version:
--   VALID_TO_TS        = p_run_date
--   CURRENT_RECORD_IND = 'N'
--   ETL_UPDATE_TS      = p_run_ts
--   ETL_UPDATE_NUM     = p_etl_run_id
-- We update only the open row (CURRENT_RECORD_IND = 'Y'). This is a Delta
-- UPDATE statement (not a MERGE), so the staged approach holds.
-- ============================================================================
UPDATE IDENTIFIER(p_target_database_name || '.' || p_target_schema_name || '.l3_DM_COR_COMPANY_DIM') AS tgt
   SET VALID_TO_TS        = CAST(p_run_date AS TIMESTAMP),
       CURRENT_RECORD_IND = 'N',
       ETL_UPDATE_TS      = p_run_ts,
       ETL_UPDATE_NUM     = CAST(p_etl_run_id AS INT)
 WHERE tgt.CURRENT_RECORD_IND = 'Y'
   AND tgt.COMPANY_ID IN (SELECT COMPANY_ID FROM v_stg_changed);


-- ============================================================================
-- MARKDOWN: ## Phase 5 — Insert NEW rows + new versions of CHANGED rows
-- COMPANY_SK is an IDENTITY column — DO NOT include it in the INSERT column
-- list; Fabric will autopopulate. Audit columns are stamped from parameters.
-- VALID_FROM_TS uses p_run_date (earliest baseline 1800-01-01 still applies
-- if a caller deliberately back-dates a re-load — clamp to MAX(p_run_date,
-- 1800-01-01) for safety).
-- ============================================================================
INSERT INTO IDENTIFIER(p_target_database_name || '.' || p_target_schema_name || '.l3_DM_COR_COMPANY_DIM') (
    COMPANY_ID,
    COMPANY_CD,
    COMPANY_WID,
    VALID_FROM_TS,
    VALID_TO_TS,
    CURRENT_RECORD_IND,
    SOFT_DELETE_FLG,
    SOURCE_SYSTEM_CD,
    SOURCE_EXTRACT_TS,
    ETL_CREATE_TS,
    ETL_UPDATE_TS,
    ETL_CREATE_NUM,
    ETL_UPDATE_NUM,
    SCD_HASH_CD,
    COMPANY_NM_DESCR,
    COMPANY_SUBTYPE_CD,
    COMPANY_CURRENCY_CD,
    COMPANY_HIERARCHY_DESCR,
    BUSINESS_UNIT_HIERARCHY_CD
)
SELECT
    s.COMPANY_ID,
    s.COMPANY_CD,
    s.COMPANY_WID,
    CAST(GREATEST(p_run_date, DATE'1800-01-01') AS TIMESTAMP) AS VALID_FROM_TS,
    TIMESTAMP'9999-12-31 00:00:00'                            AS VALID_TO_TS,
    'Y'                                                       AS CURRENT_RECORD_IND,
    'N'                                                       AS SOFT_DELETE_FLG,
    'WORKDAY'                                                 AS SOURCE_SYSTEM_CD,
    p_run_ts                                                  AS SOURCE_EXTRACT_TS,
    p_run_ts                                                  AS ETL_CREATE_TS,
    p_run_ts                                                  AS ETL_UPDATE_TS,
    CAST(p_etl_run_id AS INT)                                 AS ETL_CREATE_NUM,
    CAST(p_etl_run_id AS INT)                                 AS ETL_UPDATE_NUM,
    s.SCD_HASH_CD,
    s.COMPANY_NM_DESCR,
    s.COMPANY_SUBTYPE_CD,
    s.COMPANY_CURRENCY_CD,
    s.COMPANY_HIERARCHY_DESCR,
    s.BUSINESS_UNIT_HIERARCHY_CD
FROM v_stg_to_apply s;


-- ============================================================================
-- MARKDOWN: ## Phase 6 — Run summary (counts by action)
-- Quick diagnostic so the operator can confirm the run did what they expect.
-- Safe to leave in production — it only reads from the temp views.
-- ============================================================================
SELECT
    scd_action,
    COUNT(*) AS row_count
FROM v_stg_classified
GROUP BY scd_action
ORDER BY scd_action;


-- ============================================================================
-- MARKDOWN: ## Phase 7 — Post-load integrity checks
-- 1. Exactly one open row per Company_ID.
-- 2. No CHANGED Company_ID has more than one open row after the load.
-- These should both return ZERO rows in a healthy run.
-- ============================================================================
-- Check 1: Company_IDs with more than one open row (should be 0)
SELECT COMPANY_ID, COUNT(*) AS open_rows
FROM   IDENTIFIER(p_target_database_name || '.' || p_target_schema_name || '.l3_DM_COR_COMPANY_DIM')
WHERE  CURRENT_RECORD_IND = 'Y'
GROUP  BY COMPANY_ID
HAVING COUNT(*) > 1;

-- Check 2: rows where VALID_FROM_TS > VALID_TO_TS (should be 0)
SELECT COUNT(*) AS bad_validity_rows
FROM   IDENTIFIER(p_target_database_name || '.' || p_target_schema_name || '.l3_DM_COR_COMPANY_DIM')
WHERE  VALID_FROM_TS > VALID_TO_TS;
