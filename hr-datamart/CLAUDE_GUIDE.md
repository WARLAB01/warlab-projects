# CLAUDE_GUIDE.md
## HR Datamart Project - Claude Cowork Session Manual

This guide is designed for Claude agents picking up the HR Datamart project in Cowork sessions. It provides a complete overview of the project architecture, required capabilities, deployment workflow, and troubleshooting guidance.

---

## Project Overview

**What is this?**
The HR Datamart is a data pipeline that simulates Workday HRDP (Human Resources Data Platform) feeds through a multi-layer architecture:
- **Source**: AWS S3 (synthetic HR feed files in pipe-delimited format)
- **L1 (Raw Layer)**: Staging tables in Redshift that mirror the source files
- **L3 (Mart Layer)**: Star schema with dimensions (SCD2) and facts, optimized for HR analytics

**Repository Structure**
- GitHub: WARLAB01/warlab-projects
- Path: `hr-datamart/`
- Contains: DDL scripts, DML transformations, AWS Glue jobs, QA test suite, synthetic data generator, and deployment automation

**Key Purpose**
Provide a realistic HR data pipeline that demonstrates:
- S3 → Redshift ingestion patterns
- Multi-layer data warehouse design (staging → source → dimensional modeling)
- Data quality validation with 60+ automated tests
- Full deployment automation with deploy.sh

---

## Required Claude Capabilities

### Essential Tools & Systems

| Capability | Purpose | Installation/Setup |
|---|---|---|
| **Bash execution** | Run deploy.sh, AWS CLI commands, shell scripts | Native to Claude Bash tool |
| **File reading/editing** | Modify YAML configs, SQL files, scripts | Claude Read/Edit/Write tools |
| **AWS CLI** | Interact with S3, Glue, IAM, Redshift | `pip install awscli` |
| **psql client** | Direct Redshift SQL execution | `apt-get install postgresql-client` (Linux) or `brew install postgresql` (macOS) |
| **Python 3** | Run data generator and utility scripts | Usually pre-installed; verify with `python3 --version` |
| **Git** | Clone/update the repository | Pre-installed on most systems |

### No Special Skills Required
- **Document handling**: This project does NOT require Cowork document skills (docx, pptx, xlsx, pdf)
- **APIs/SDKs**: All operations use standard AWS CLI and psql; no custom SDKs needed
- **This is pure infrastructure/data engineering**: Bash, AWS, and SQL only

### Primary Tools for This Project
- **Bash tool**: For deployment scripts, AWS CLI calls, and shell commands
- **Read tool**: For viewing config files, SQL scripts, and documentation
- **Edit tool**: For modifying environment config, SQL files, and deployment scripts
- **Write tool**: For creating new files (configs, logs, etc.)
- **Glob/Grep tools**: For finding files and searching code (when user requests)

---

## Deployment Workflow

### Option 1: Automated Deployment (Recommended)
```bash
# 1. Clone the repository or navigate to user's workspace
cd /path/to/warlab-projects/hr-datamart

# 2. Configure AWS CLI (if not already done)
aws configure

# 3. Set environment variables in config/env_config.yaml
# Edit the template with user's actual AWS credentials and Redshift details

# 4. Run the one-script deployment
bash deploy.sh
```

The `deploy.sh` script orchestrates all steps:
1. Validate environment and prerequisites
2. Generate synthetic test data
3. Upload data to S3
4. Create L1 staging schemas
5. Run L1 COPY commands (S3 → Redshift)
6. Create L3 source staging tables
7. Create L3 dimensions and facts
8. Load L3 dimensions (with SCD2 handling)
9. Load L3 facts
10. Run QA validation suite
11. Generate completion report

### Option 2: Manual Step-by-Step Execution
If you need granular control or debugging:

```bash
# Step 1: Generate Data
python3 artifacts/data_gen/generate_all_feeds.py

# Step 2: Upload to S3
aws s3 cp data/output/ s3://${S3_BUCKET}/hr-datamart/ --recursive

# Step 3: Create L1 schemas & tables
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/ddl/l1/l1_schema_ddl.sql

# Step 4: Copy L1 data from S3
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/dml/l1_copy/l1_copy_statements.sql

# Step 5: Create L3 source staging
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/ddl/l3_source/l3_source_ddl.sql

# Step 6: Load L3 source
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/dml/l3_source_load/l3_source_load.sql

# Step 7: Create L3 dimensions
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/ddl/l3_star/l3_dim_ddl.sql

# Step 8: Load L3 dimensions (CRITICAL ORDER - see below)
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/dml/l3_dim_load/l3_dim_load.sql

# Step 9: Create L3 facts
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/ddl/l3_star/l3_fact_ddl.sql

# Step 10: Load L3 facts
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/dml/l3_fact_load/l3_fact_load.sql

# Step 11: Run QA validation
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/qa/qa_tests.sql

# Step 12: Generate completion report
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} \
  -f artifacts/qa/completion_report.sql
```

### Prerequisites Checklist
Before starting deployment:
- [ ] AWS credentials configured (`aws configure`)
- [ ] S3 bucket created and accessible
- [ ] Redshift cluster available and network accessible
- [ ] Redshift user account created with necessary permissions
- [ ] IAM role for Redshift S3 access created and ARN available
- [ ] Python 3 and required packages installed
- [ ] psql client installed
- [ ] Repository cloned locally

---

## Key Files Reference

Use this table to understand what each file does and when it's used in the pipeline:

### Configuration & Setup
| File | Purpose | When Used |
|---|---|---|
| `config/env_config.yaml` | Environment variables template (S3_BUCKET, REDSHIFT_HOST, etc.) | Before deploy.sh; edit with user's actual values |
| `QUICKSTART.md` | Human-readable deployment guide for users | Reference guide for manual steps |
| `artifacts/docs/README.md` | Full technical documentation and architecture | Deep dive reference; explains data model, schemas, FK relationships |
| `artifacts/docs/data_dictionary.json` | Complete metadata for all tables, columns, data types | Reference when building analytics queries |

### L1 (Raw/Staging Layer)
| File | Purpose | When Used | Notes |
|---|---|---|---|
| `artifacts/ddl/l1/l1_schema_ddl.sql` | Creates L1 schemas and staging tables | deploy.sh Step 5 | Mirrors source feed structure exactly (no transformations) |
| `artifacts/dml/l1_copy/l1_copy_statements.sql` | COPY commands for S3 → Redshift | deploy.sh Step 6 | Requires ${S3_BUCKET} and ${REDSHIFT_IAM_ROLE_ARN} substitution |

### L3 Source (Denormalized Staging)
| File | Purpose | When Used | Notes |
|---|---|---|---|
| `artifacts/ddl/l3_source/l3_source_ddl.sql` | Creates L3 source staging tables | deploy.sh Step 7 | Denormalized views/tables from L1 with business logic |
| `artifacts/dml/l3_source_load/l3_source_load.sql` | Transforms L1 data into L3 source | deploy.sh Step 8 | Applies transformations, joins, and business rules |

### L3 Star Schema (Dimensional Modeling)
| File | Purpose | When Used | Notes |
|---|---|---|---|
| `artifacts/ddl/l3_star/l3_dim_ddl.sql` | Creates L3 dimension tables (SCD2) | deploy.sh Step 9 | Tables: dim_worker_d, dim_job_d, dim_organization_d, dim_day_d |
| `artifacts/dml/l3_dim_load/l3_dim_load.sql` | Loads L3 dimensions with SCD2 logic | deploy.sh Step 10 | **MUST run before fact loads due to FK constraints** |
| `artifacts/ddl/l3_star/l3_fact_ddl.sql` | Creates L3 fact tables | deploy.sh Step 11 | Tables: fct_worker_movement_f, fct_worker_compensation_f, fct_worker_status_f |
| `artifacts/dml/l3_fact_load/l3_fact_load.sql` | Loads L3 fact tables | deploy.sh Step 12 | Joins to dimensions; FK constraints require dimension load first |

### Data Generation & Testing
| File | Purpose | When Used | Notes |
|---|---|---|---|
| `artifacts/data_gen/generate_all_feeds.py` | Generates synthetic HR data files | deploy.sh Step 1 or standalone | Creates pipe-delimited CSV files in data/output/ |
| `artifacts/qa/qa_tests.sql` | 60+ validation tests for data quality | deploy.sh Step 13 | Tests row counts, nulls, FK integrity, business logic |
| `artifacts/qa/completion_report.sql` | Summary report of pipeline execution | deploy.sh Step 14 | Shows load times, record counts, test results |

### AWS Glue Jobs (Optional - Alternative to Manual Steps)
| File | Purpose | When Used | Notes |
|---|---|---|---|
| `artifacts/glue/glue_s3_to_l1_job.py` | AWS Glue job for S3 → L1 ingestion | Alternative to manual COPY | Can be deployed to Glue service for scheduled runs |
| `artifacts/glue/glue_l1_to_l3_job.py` | AWS Glue job for L1 → L3 transformations | Alternative to manual SQL | Can be deployed to Glue service for scheduled runs |

### Runbook & Orchestration
| File | Purpose | When Used | Notes |
|---|---|---|---|
| `artifacts/runbook/runbook.sql` | SQL runbook with step-by-step comments | Reference during manual execution | Explains each transformation and validation step |

---

## Execution Order (Critical Dependency)

The L3 loads **MUST** run in this exact order due to foreign key dependencies:

### Correct Execution Order
1. **L1 DDL** → Create L1 staging schemas and tables
2. **L1 DML (COPY)** → Load data from S3 into L1
3. **L3 Source DDL** → Create denormalized L3 source tables
4. **L3 Source DML** → Transform and load L1 → L3 source
5. **L3 Dimension DDL** → Create all dimension tables (SCD2)
6. **L3 Dimension DML** → Load dimensions in order:
   - `dim_day_d` (no FK dependencies)
   - `dim_organization_d` (depends on dim_day_d only if parent-child orgs exist)
   - `dim_job_d` (depends on dim_organization_d)
   - `dim_worker_d` (depends on dim_organization_d, dim_job_d)
7. **L3 Fact DDL** → Create all fact tables (after dimensions exist)
8. **L3 Fact DML** → Load facts in order:
   - `fct_worker_status_f` (base worker status)
   - `fct_worker_movement_f` (depends on dim_worker_d, dim_job_d, dim_organization_d)
   - `fct_worker_compensation_f` (depends on dim_worker_d)
9. **QA Tests** → Run 60+ validation tests
10. **Completion Report** → Generate summary report

### Why This Order Matters
- Foreign keys reference dimensions; dimensions must exist before facts
- SCD2 dimension logic depends on historical data; run all dim_* before facts
- Data integrity tests depend on complete load; run QA at the end

---

## Common Issues & Troubleshooting

### Issue: "psql: command not found"
**Cause**: PostgreSQL client not installed
```bash
# Linux (Ubuntu/Debian)
sudo apt-get install postgresql-client

# macOS
brew install postgresql
```

### Issue: "COPY failed" or "No such S3 location"
**Cause**: S3 path incorrect, IAM role doesn't have permissions, or file format mismatch
```bash
# Check S3 bucket and path exist
aws s3 ls s3://${S3_BUCKET}/hr-datamart/

# Verify IAM role has S3:GetObject and S3:ListBucket permissions
aws iam get-role --role-name ${REDSHIFT_IAM_ROLE}

# Files must be pipe-delimited (|); check first file
aws s3 cp s3://${S3_BUCKET}/hr-datamart/worker.csv - | head -1
```

### Issue: "permission denied for schema" or "must be owner of schema"
**Cause**: Redshift user doesn't have schema ownership or privileges
```bash
# Run GRANT statements from README section 9.4
GRANT USAGE ON SCHEMA l1 TO ${REDSHIFT_USER};
GRANT USAGE ON SCHEMA l3_source TO ${REDSHIFT_USER};
GRANT USAGE ON SCHEMA l3_star TO ${REDSHIFT_USER};
GRANT CREATE ON SCHEMA l1, l3_source, l3_star TO ${REDSHIFT_USER};
```

### Issue: "relation does not exist" when running DML
**Cause**: DDL hasn't been executed yet; tables don't exist
**Fix**: Run DDL scripts in order before DML scripts

### Issue: "timeout waiting for connection" to Redshift
**Cause**: Network connectivity issue; Redshift cluster not accessible
```bash
# Check cluster status
aws redshift describe-clusters --cluster-identifier ${REDSHIFT_CLUSTER_ID}

# Verify security group allows inbound on port 5439
aws ec2 describe-security-groups --group-ids ${SECURITY_GROUP_ID}

# Test connection manually
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} -c "SELECT 1"
```

### Issue: "Variable substitution not working" (e.g., ${S3_BUCKET} appears in SQL)
**Cause**: Environment variables not exported or SQL file not processed for substitution
**Fix**: Use `envsubst` to replace variables before execution
```bash
envsubst < artifacts/dml/l1_copy/l1_copy_statements.sql | \
  psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB}
```

### Issue: Data integrity test failures (QA tests fail)
**Possible Causes**:
- Data not fully loaded (check row counts in completion report)
- SCD2 historical records expected but not found (check dimension load order)
- FK constraint violations (check dimension keys in fact tables)

**Debug Steps**:
```bash
# Check row counts in each layer
psql -h ${REDSHIFT_HOST} -U ${REDSHIFT_USER} -d ${REDSHIFT_DB} << EOF
SELECT COUNT(*) as l1_workers FROM l1.worker;
SELECT COUNT(*) as l3_source_workers FROM l3_source.worker_s;
SELECT COUNT(*) as dim_workers FROM l3_star.dim_worker_d;
SELECT COUNT(*) as fact_movements FROM l3_star.fct_worker_movement_f;
EOF

# Run individual test from qa_tests.sql manually to see error message
```

### Issue: "AWS credentials not configured"
**Cause**: AWS CLI not set up with credentials
```bash
# Configure AWS CLI
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_DEFAULT_REGION=us-east-1

# Verify configuration
aws sts get-caller-identity
```

### Issue: "Python module not found" when running data generator
**Cause**: Required Python dependencies not installed
```bash
# Install required packages
pip install boto3 pandas faker psycopg2-binary

# Or check requirements.txt if it exists
pip install -r requirements.txt
```

---

## Security Notes & Best Practices

### Credentials Management
- **Never commit AWS credentials** to the repository (add to .gitignore)
- **Use aws configure** for interactive credential setup, or set environment variables:
  ```bash
  export AWS_ACCESS_KEY_ID=<key>
  export AWS_SECRET_ACCESS_KEY=<secret>
  ```
- **Redshift password** should be entered interactively when prompted by psql, not in scripts
- If user provides a GitHub Personal Access Token (PAT), use it for git operations only, then **remind them to rotate it** after the session

### IAM Role Configuration
- The Redshift cluster must have an IAM role with:
  - `s3:GetObject` - Read files from S3
  - `s3:ListBucket` - List S3 bucket contents
  - `s3:GetBucketLocation` - Determine bucket region
- Store the IAM role ARN in `env_config.yaml` as `REDSHIFT_IAM_ROLE_ARN`

### S3 Bucket Security
- Enable versioning on S3 bucket to prevent accidental data loss
- Restrict bucket access to Redshift cluster IAM role only
- Consider enabling encryption at rest (SSE-S3 or SSE-KMS)

### Redshift User Permissions
- Create a dedicated Redshift user (not the master user) for this pipeline
- Grant schema-specific privileges rather than full GRANT ALL
- See QUICKSTART.md section 9.4 for GRANT statements

### Logging & Monitoring
- All deploy.sh operations log to stdout; capture logs for troubleshooting:
  ```bash
  bash deploy.sh > deploy.log 2>&1
  ```
- Check Redshift system tables for load errors:
  ```sql
  SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;
  ```

---

## Claude Execution Checklist

When starting a new Cowork session with this project:

- [ ] **Verify bash tool** works: `bash --version`
- [ ] **Verify AWS CLI installed**: `aws --version` (if not: `pip install awscli`)
- [ ] **Verify psql installed**: `psql --version` (if not: `apt-get install postgresql-client`)
- [ ] **Verify Python 3**: `python3 --version`
- [ ] **Read env_config.yaml** to understand required environment variables
- [ ] **Ask user for AWS credentials** if not already configured
- [ ] **Ask user for Redshift connection details** (host, user, database, password)
- [ ] **Ask user for S3 bucket name** and IAM role ARN
- [ ] **Offer to run deploy.sh** or guide through manual steps
- [ ] **Monitor for failures** and apply troubleshooting steps from this guide
- [ ] **Report completion** with QA test results and row count summary

---

## Quick Reference: Typical Cowork Session Flow

```
1. User asks: "Deploy the HR Datamart"
   ↓
2. Claude reads env_config.yaml and QUICKSTART.md
   ↓
3. Claude asks user for: AWS creds, Redshift host, S3 bucket, IAM role ARN
   ↓
4. Claude runs: bash deploy.sh (or guides through manual steps)
   ↓
5. If errors occur: Debug using troubleshooting section above
   ↓
6. Claude reports: QA test results, row counts, load times
   ↓
7. Session complete!
```

---

## Additional Resources

- **QUICKSTART.md**: Human-readable guide for non-technical users
- **artifacts/docs/README.md**: Full technical architecture and data dictionary
- **artifacts/runbook/runbook.sql**: Step-by-step SQL runbook with explanations
- **artifacts/docs/data_dictionary.json**: Complete metadata reference

---

**Last Updated**: February 2025
**For**: Claude Cowork Sessions
**Questions?**: Refer to README.md or contact the project maintainers
