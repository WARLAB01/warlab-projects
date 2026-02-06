# Redshift SQL Compatibility Guide

This document captures all Amazon Redshift SQL dialect differences encountered during the HR Datamart deployment. Use this as a reference when writing or porting SQL to Redshift.

---

## 1. String Concatenation

Redshift's `CONCAT()` function accepts **exactly 2 arguments**. For 3+ strings, use the `||` operator.

```sql
-- BAD: Fails on Redshift
CONCAT(col1, col2, col3)

-- GOOD: Works on Redshift
col1 || col2 || col3
```

**Recommendation:** Always use `||` for concatenation. It handles any number of arguments and is more readable.

---

## 2. Boolean to VARCHAR Casting

Redshift cannot directly cast `BOOLEAN` to `VARCHAR`. You must go through `INTEGER` first.

```sql
-- BAD: Fails on Redshift
active::VARCHAR

-- GOOD: Works on Redshift
active::INT::VARCHAR    -- true → '1', false → '0'
```

**DDL implication:** If your DML casts booleans to VARCHAR (via `::INT::VARCHAR`), the target column must be `VARCHAR(256)`, not `BOOLEAN`. Redshift enforces strict type checking — it will not implicitly coerce VARCHAR to BOOLEAN.

**Exception:** Columns populated by SQL `CASE` expressions producing literal `TRUE`/`FALSE` should remain `BOOLEAN`:
```sql
-- These columns stay BOOLEAN in DDL because the DML produces boolean literals
CASE WHEN day_of_week IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
```

---

## 3. VALUES Clause in Subqueries

Redshift does not support `VALUES` as a table constructor in subqueries.

```sql
-- BAD: Fails on Redshift
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)

-- GOOD: Use UNION ALL
SELECT 1 AS id, 'a' AS name
UNION ALL
SELECT 2, 'b'
```

---

## 4. CTE with INSERT (WITH...INSERT)

PostgreSQL allows `WITH cte AS (...) INSERT INTO table SELECT * FROM cte`. Redshift does not.

```sql
-- BAD: Fails on Redshift
WITH cte AS (SELECT ...)
INSERT INTO target SELECT * FROM cte;

-- GOOD: Put the CTE inside the INSERT
INSERT INTO target
WITH cte AS (SELECT ...)
SELECT * FROM cte;
```

---

## 5. Date Functions

Redshift uses different date functions from SQL Server and standard PostgreSQL:

```sql
-- BAD: SQL Server syntax, fails on Redshift
day(date_col)
month(date_col)
year(date_col)

-- GOOD: Redshift syntax
DATEPART(day, date_col)
DATEPART(month, date_col)
DATEPART(year, date_col)

-- Or use TO_CHAR for formatted extraction
CAST(TO_CHAR(date_col, 'DD') AS INTEGER)
```

---

## 6. INTERVAL Arithmetic

Redshift does not support multiplying intervals:

```sql
-- BAD: Fails on Redshift
date_col + INTERVAL '1 day' * n

-- GOOD: Use DATEADD
DATEADD(day, n, date_col)
```

---

## 7. Temp Tables and the Redshift Data API

When using `aws redshift-data execute-statement`, each call runs in its own session. Temp tables created in one call are **not visible** in subsequent calls.

**Solution:** Send the entire transaction as a single SQL block:

```bash
# BAD: Each statement runs in a separate session
aws redshift-data execute-statement --sql "CREATE TEMP TABLE staging AS ..."
aws redshift-data execute-statement --sql "UPDATE target FROM staging ..."
# staging table doesn't exist in this session!

# GOOD: Send everything as one block
aws redshift-data execute-statement --sql "$(cat full_script.sql)"
```

**Deployment script pattern:**
- **Block mode** for DML with temp tables (dim load, fact load): Send entire file as one `--sql` parameter
- **Statement mode** for DDL: Split on semicolons, execute each independently

---

## 8. COPY Command Best Practices

When loading pipe-delimited CSV files from S3:

```sql
COPY schema.table
FROM 's3://bucket/path/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/ROLE'
DELIMITER '|'
IGNOREHEADER 1          -- Skip CSV header row
ACCEPTINVCHARS          -- Handle unexpected characters gracefully
DATEFORMAT 'auto'       -- Auto-detect date formats
TIMEFORMAT 'auto'       -- Auto-detect timestamp formats
TRUNCATECOLUMNS         -- Truncate data that exceeds VARCHAR length
MAXERROR 0              -- Fail on any error (strict mode)
REGION 'us-east-1';     -- Required if S3 bucket is in different region
```

---

## 9. Column Name Consistency Across Views

Different Workday HRDP source views use different column names for similar concepts:

| View | Timestamp Column | Notes |
|------|-----------------|-------|
| `worker_job_dly_vw` | `transaction_entry_date` | DATE type |
| `worker_organization_dly_vw` | `transaction_entry_date` | DATE type |
| `worker_comp_dly_vw` | `transaction_entry_moment` | TIMESTAMP type |

Always verify column names against the actual view definition before referencing them in SQL.

---

## 10. Schema Awareness

Objects belong to the schema where they were **created**, not the schema of their source tables:

```sql
-- L3 Source views are in l3_workday schema (even though they query l1 tables)
FROM l3_workday.l3_workday_worker_job_dly_vw      -- CORRECT
FROM l1_workday.l1_workday_worker_job_dly_vw      -- WRONG (view doesn't exist here)
```

---

## 11. Table Name Cross-Reference

Always verify table names against the actual DDL. Common mismatches found:

| Wrong Name (DO NOT USE) | Correct Name |
|------------------------|-------------|
| `int6028_supervisory_organization` | `int6028_department_hierarchy` |
| `int6032_position` | `int6032_positions` |
| `int6022_job_profile_details` | `int6022_job_classification` |

---

## 12. Attribute Ownership in Star Schema

When building fact tables, verify which dimension owns each attribute:

| Attribute | Lives On | NOT On |
|-----------|---------|--------|
| `management_level_code` | `dim_job_profile_d` | `dim_worker_job_d` |
| `job_matrix` | `dim_job_profile_d` | `dim_worker_job_d` |

If a fact needs attributes from a related dimension, add an explicit JOIN:

```sql
FROM l3_workday.dim_worker_job_d dwj
LEFT JOIN l3_workday.dim_job_profile_d djp
    ON dwj.job_profile_id = djp.job_profile_id
    AND djp.is_current = true
WHERE dwj.is_current = true
```
