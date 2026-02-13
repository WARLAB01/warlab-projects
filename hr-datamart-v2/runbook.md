# HR Datamart V2 — End-to-End Runbook

## Environment Variables

```bash
export AWS_REGION=us-east-1
export REDSHIFT_CLUSTER=warlab-hr-datamart
export REDSHIFT_DB=dev
export REDSHIFT_USER=admin
export S3_BUCKET=warlab-hr-datamart-dev
export S3_DASHBOARD_BUCKET=warlab-hr-dashboard
export GLUE_WORKFLOW=warlab-hr-l1-load
export SCHEMA_L1=v2_l1_staging
export SCHEMA_L3_SRC=v2_l3_workday
export SCHEMA_L3_STAR=v2_l3_star
export DRY_RUN=false  # Set to true to validate SQL without executing
```

## Pipeline Execution Order

### Phase 1: Data Generation & S3 Upload

**Step 1 — Generate Synthetic Workday CSV Feeds**
```bash
# Generates 12 Workday HRDP CSV feeds with synthetic data
# Output: local CSV files per feed per day
python3 hr-datamart-v2/scripts/generate_data.py \
  --seed 42 \
  --workers 20000 \
  --days 730 \
  --output-dir /tmp/hrdp_feeds/
```

**Step 2 — Upload to S3**
```bash
# Upload partitioned feeds to S3 (dt=YYYY-MM-DD structure)
aws s3 sync /tmp/hrdp_feeds/ s3://${S3_BUCKET}/feeds/ --exclude "*.DS_Store"
```

### Phase 2: L1 Staging (S3 → Redshift)

**Step 3 — Load L1 via AWS Glue**
```bash
# Start the Glue workflow (12 parallel jobs, one per feed)
aws glue start-workflow-run --name ${GLUE_WORKFLOW}

# Monitor progress
aws glue get-workflow-run --name ${GLUE_WORKFLOW} \
  --run-id $(aws glue get-workflow-runs --name ${GLUE_WORKFLOW} \
    --query 'Runs[0].RunId' --output text)
```
- **ETL Script**: `s3://${S3_BUCKET}/glue-scripts/glue_s3_to_l1_etl.py`
- **Connection**: `warlab-redshift-connection` (JDBC)
- **12 feeds**: int6001–int6270 (worker_job, department_hierarchy, worker_profile, compensation, etc.)

**Alternative — Direct Redshift COPY (if Glue unavailable)**
```sql
-- Example for one feed (repeat for each)
COPY ${SCHEMA_L1}.int6029_worker_job
FROM 's3://${S3_BUCKET}/feeds/int6029_worker_job/'
IAM_ROLE '${IAM_ROLE_ARN}'
CSV IGNOREHEADER 1 TIMEFORMAT 'auto' DATEFORMAT 'auto'
BLANKSASNULL EMPTYASNULL;
```

### Phase 3: L3 Source Tables

**Step 4 — Create & Load L3 Source (DLY) Tables**
```bash
# Execute L3 source DDL
aws redshift-data execute-statement \
  --cluster-identifier ${REDSHIFT_CLUSTER} \
  --database ${REDSHIFT_DB} --db-user ${REDSHIFT_USER} \
  --sql "$(cat hr-datamart-v2/sql/v2_l3_source_ddl.sql)"

# Load L3 DLY tables from L1 (includes IDP column computation)
aws redshift-data execute-statement \
  --cluster-identifier ${REDSHIFT_CLUSTER} \
  --database ${REDSHIFT_DB} --db-user ${REDSHIFT_USER} \
  --sql "$(cat hr-datamart-v2/sql/v2_l3_source_load.sql)"
```

### Phase 4: L3 Star Schema — Dimensions

**Step 5 — Create & Load Dimension Tables**
```bash
# Execute dimension DDL (14 tables)
aws redshift-data execute-statement \
  --cluster-identifier ${REDSHIFT_CLUSTER} \
  --database ${REDSHIFT_DB} --db-user ${REDSHIFT_USER} \
  --sql "$(cat hr-datamart-v2/sql/v2_l3_star_dim_ddl.sql)"

# Load dimensions
aws redshift-data execute-statement \
  --cluster-identifier ${REDSHIFT_CLUSTER} \
  --database ${REDSHIFT_DB} --db-user ${REDSHIFT_USER} \
  --sql "$(cat hr-datamart-v2/sql/v2_l3_star_dim_load.sql)"

# Apply post-load fixes (hierarchy, report-to, status IDP filter, dedup)
aws redshift-data execute-statement \
  --cluster-identifier ${REDSHIFT_CLUSTER} \
  --database ${REDSHIFT_DB} --db-user ${REDSHIFT_USER} \
  --sql "$(cat hr-datamart-v2/sql/v2_l3_star_dim_fixes.sql)"
```

### Phase 5: L3 Star Schema — Fact Tables

**Step 6 — Create & Load Fact Tables**
```bash
# Execute fact DDL (2 tables)
aws redshift-data execute-statement \
  --cluster-identifier ${REDSHIFT_CLUSTER} \
  --database ${REDSHIFT_DB} --db-user ${REDSHIFT_USER} \
  --sql "$(cat hr-datamart-v2/sql/v2_l3_star_fact_ddl.sql)"

# Load facts (movement + headcount)
aws redshift-data execute-statement \
  --cluster-identifier ${REDSHIFT_CLUSTER} \
  --database ${REDSHIFT_DB} --db-user ${REDSHIFT_USER} \
  --sql "$(cat hr-datamart-v2/sql/v2_l3_star_fact_load.sql)"
```

### Phase 6: Dashboard Refresh

**Step 7 — Refresh V2 Dashboard Data**
```bash
# Invoke Lambda for all 5 extractions
aws lambda invoke \
  --function-name warlab-v2-dashboard-extractor \
  --payload '{"extraction":"all"}' \
  --cli-read-timeout 310 \
  /tmp/v2_dashboard_response.json

# Or invoke individually
for ext in kpi_summary headcount movements compensation org_health; do
  aws lambda invoke \
    --function-name warlab-v2-dashboard-extractor \
    --payload "{\"extraction\":\"${ext}\"}" \
    /tmp/v2_response_${ext}.json
done

# Invalidate CloudFront cache
aws cloudfront create-invalidation \
  --distribution-id E3RGFB9ROIS4KH \
  --paths "/v2/*"
```
- **Dashboard URL**: https://d142tokwl5q6ig.cloudfront.net/v2/
- **Auto-refresh**: EventBridge rule `warlab-v2-dashboard-refresh` runs every 6 hours

## Dry Run Mode

Set `DRY_RUN=true` to validate SQL without executing. Wrap commands:
```bash
if [ "$DRY_RUN" = "true" ]; then
  echo "[DRY RUN] Would execute: $SQL_FILE"
else
  aws redshift-data execute-statement ...
fi
```

## Dependency Chain

```
S3 Feeds → Glue ETL → L1 Staging
                          ↓
                    L3 Source (DLY)
                          ↓
                  L3 Star Dimensions
                          ↓
                    L3 Star Facts
                          ↓
                  Dashboard Lambda → S3 JSON → CloudFront
```

## Monitoring

- **Glue**: AWS Glue Console → Workflows → `warlab-hr-l1-load`
- **Lambda**: CloudWatch Logs → `/aws/lambda/warlab-v2-dashboard-extractor`
- **Dashboard**: CloudWatch Dashboard `WarLab-HR-Pipeline-Monitor`
- **Metrics**: CloudWatch Namespace `WarLabHRDashboard-V2`

## Troubleshooting

| Issue | Resolution |
|-------|-----------|
| Glue job fails | Check Glue connection `warlab-redshift-connection`, verify Redshift is accessible |
| COPY fails with auth error | Verify IAM role has S3 read access to `${S3_BUCKET}` |
| Dimension dedup needed | Run dedup SQL from `v2_l3_star_fact_load.sql` Section 3 |
| Dashboard shows stale data | Manually invoke Lambda or check EventBridge rule status |
| VARCHAR cast errors | Use regex filter `column ~ '^[0-9]'` before `::DECIMAL` casts |
| Empty string issues | Filter with `column IS NOT NULL AND column <> ''` |
