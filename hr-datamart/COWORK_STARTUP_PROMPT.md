# Cowork Startup Prompt

Copy and paste the text below (between the --- markers) into a new Claude Cowork session on your other machine.

---

## Context: HR Datamart Deployment to AWS

I have an HR Datamart project in my GitHub repo that I need your help deploying and testing in my AWS environment. The project simulates Workday HRDP feeds flowing through an S3 → Redshift L1/L3 star-schema pipeline.

### What's Already Done
- All project code is complete and pushed to GitHub: https://github.com/WARLAB01/warlab-projects/tree/main/hr-datamart
- 17 source files (SQL DDL/DML, Python Glue jobs, YAML config, QA tests, data generator)
- 12 sample CSV data files already generated
- Deployment automation script (deploy.sh) and full documentation

### What I Need You To Do
Help me deploy this pipeline to my AWS environment and verify everything works. Specifically:

1. **Clone the repo** from https://github.com/WARLAB01/warlab-projects.git (I'll provide a GitHub PAT if needed)
2. **Help me configure AWS CLI** — I may need to create access keys in IAM. Walk me through it.
3. **Set up the AWS resources** — S3 bucket, IAM role for Redshift, and connect to my Redshift cluster
4. **Run the full deployment** using the deploy.sh script, or step-by-step if we hit issues
5. **Verify the results** by running QA tests and checking row counts across all layers

### Key Files You Should Read First
Before starting any work, read these files from the repo (in this order):
1. `hr-datamart/CLAUDE_GUIDE.md` — Written specifically for you (Claude). Contains capabilities needed, file reference, execution order, and troubleshooting tips.
2. `hr-datamart/QUICKSTART.md` — Step-by-step deployment guide with all commands
3. `hr-datamart/deploy.sh` — Automated deployment script (review the --help output)
4. `hr-datamart/config/env_config.yaml` — Environment config template

### Environment Variables We'll Need to Set
```bash
export S3_BUCKET="<my-bucket-name>"
export REDSHIFT_HOST="<my-cluster>.xxxxx.<region>.redshift.amazonaws.com"
export REDSHIFT_PORT="5439"
export REDSHIFT_DB="dev"
export REDSHIFT_USER="admin"
export REDSHIFT_IAM_ROLE_ARN="arn:aws:iam::<account-id>:role/<role-name>"
export PGPASSWORD="<my-redshift-password>"
export DATA_DATE="2026-02-05"
export ETL_BATCH_ID="BATCH_$(date +%Y%m%d)_001"
```
I'll fill in the actual values as we go. Help me figure out what I need if I don't have these resources yet.

### Architecture Overview
```
Workday HRDP Feeds (12 CSVs, pipe-delimited)
    ↓
S3 Landing Zone (s3://bucket/workday/hrdp/{feed}/dt=YYYY-MM-DD/)
    ↓
L1 Staging (Redshift schema: l1_workday) — Raw COPY, full refresh
    ↓
L3 Source (Redshift schema: l3_workday) — Denormalized staging
    ↓
L3 Star Schema (Redshift schema: l3_workday):
  • dim_worker_d (SCD2)
  • dim_job_d (SCD2)
  • dim_organization_d (SCD2)
  • dim_day_d (calendar)
  • fct_worker_movement_f
  • fct_worker_compensation_f
  • fct_worker_status_f
```

### CSV-to-S3 Feed Mapping (Critical)
The data generator outputs files like `workday.hrdp.dly_grade_profile.full.20260205060000.csv` but COPY statements expect S3 paths like `int6020_grade_profile`. The deploy.sh script handles this mapping automatically, but if running manually:

| CSV Name Pattern | S3 Feed Folder | L1 Table |
|---|---|---|
| dly_grade_profile | int6020_grade_profile | int6020_grade_profile |
| dly_job_profile | int6021_job_profile | int6021_job_profile |
| dly_job_classification | int6022_job_classification | int6022_job_classification |
| dly_location | int6023_location | int6023_location |
| dly_company | int6024_company | int6024_company |
| dly_cost_center | int6025_cost_center | int6025_cost_center |
| dly_department_hierarchy | int6028_department_hierarchy | int6028_department_hierarchy |
| dly_positions | int6032_positions | int6032_positions |
| dly_worker_job | int0095e_worker_job | int0095e_worker_job |
| dly_worker_organization | int0096_worker_organization | int0096_worker_organization |
| dly_worker_compensation | int0098_worker_compensation | int0098_worker_compensation |
| dly_rescinded | int270_rescinded | int270_rescinded |

### Tools You'll Need
- **Bash**: For running deploy.sh, AWS CLI, psql, git, python3
- **No special Cowork skills needed** — this is all infrastructure/data engineering work
- Install if missing: `pip install awscli` and `apt-get install postgresql-client`

### How to Approach This
1. Start by reading CLAUDE_GUIDE.md and QUICKSTART.md from the repo
2. Help me get AWS CLI configured (I may need guidance creating IAM access keys)
3. Verify/create AWS resources (S3 bucket, IAM role, Redshift connectivity)
4. Run deploy.sh with appropriate flags, troubleshooting any failures
5. Validate with QA tests — all 60+ tests should pass

Let's start by cloning the repo and reading the guide files. I'll provide credentials as needed along the way.

---
