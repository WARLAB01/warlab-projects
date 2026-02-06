# HR Datamart AWS Glue ETL - File Index

Complete guide to all files in this ETL solution.

## Directory Structure

```
/warlab-projects/hr-datamart/artifacts/glue/
├── glue_s3_to_l1_etl.py              (16 KB, 450+ lines)
├── deploy_glue_jobs.sh               (18 KB, executable)
├── manage_workflow.sh                (16 KB, executable)
├── glue_workflow_config.json         (10 KB, configuration)
│
├── README.md                         (13 KB, comprehensive docs)
├── QUICK_START.md                    (6.4 KB, getting started)
├── REQUIREMENTS.md                   (12 KB, prerequisites)
├── INDEX.md                          (This file)
│
├── glue_l1_to_l3_job.py              (Legacy, not used)
└── glue_s3_to_l1_job.py              (Legacy, not used)
```

## Core Files (Required)

### 1. glue_s3_to_l1_etl.py

**Purpose**: Parameterized AWS Glue ETL script that loads pipe-delimited CSV data from S3 into Redshift L1 staging tables.

**Key Features**:
- Reads pipe-delimited CSV files with headers
- Applies ResolveChoice transform for type safety
- Truncates Redshift target tables before load (idempotent)
- Writes data via Glue Redshift connector
- Comprehensive error handling and logging
- Supports parameterized execution for multiple tables

**Parameters Accepted**:
- `--source_table` (required): Source table name
- `--s3_path` (required): S3 path to CSV
- `--redshift_schema` (optional, default: l1_workday)
- `--redshift_table` (optional, default: source_table)
- `--redshift_connection` (optional, default: warlab-redshift-connection)
- `--redshift_database` (optional, default: dev)

**Usage**:
```bash
# Via Glue job
aws glue start-job-run --job-name warlab-hr-int6024-company

# Direct execution in Glue shell
python glue_s3_to_l1_etl.py \
    --source_table int6024_company \
    --s3_path s3://warlab-hr-datamart-dev/workday/hrdp/int6024_company/
```

**Lines of Code**: 450+
**Last Updated**: 2026-02-06

---

### 2. deploy_glue_jobs.sh

**Purpose**: Automated deployment script that creates all 12 Glue jobs, workflow, and triggers in AWS.

**What It Does**:
1. Validates prerequisites (AWS CLI, credentials, bucket access)
2. Uploads ETL script to S3
3. Creates 12 Glue jobs with appropriate configurations
4. Creates Glue workflow: `warlab-hr-l1-load`
5. Creates job actions and triggers
6. Verifies deployment success

**Key Functions**:
- `validate_prerequisites()`: Check all requirements are met
- `upload_etl_script()`: Push script to S3
- `create_glue_job()`: Create individual job
- `create_all_glue_jobs()`: Create all 12 jobs
- `create_glue_workflow()`: Create workflow
- `create_workflow_triggers()`: Create on-demand + scheduled triggers

**Usage**:
```bash
# Dry-run first (recommended)
./deploy_glue_jobs.sh --dry-run --region us-east-1

# Deploy for real
./deploy_glue_jobs.sh --region us-east-1 --profile default

# Full help
./deploy_glue_jobs.sh --help
```

**Options**:
- `--dry-run`: Show what would be deployed without making changes
- `--region`: AWS region (default: us-east-1)
- `--profile`: AWS CLI profile (default: default)

**Deployment Time**: 3-5 minutes
**Lines of Code**: 400+
**Last Updated**: 2026-02-06

---

### 3. manage_workflow.sh

**Purpose**: Workflow management and monitoring utility for running and troubleshooting Glue jobs.

**Commands Available**:

| Command | Purpose | Example |
|---------|---------|---------|
| `start` | Start workflow run | `./manage_workflow.sh start` |
| `status` | Get latest workflow status | `./manage_workflow.sh status` |
| `list-runs` | List recent workflow runs | `./manage_workflow.sh list-runs` |
| `list-jobs` | List all HR Datamart jobs | `./manage_workflow.sh list-jobs` |
| `job-status` | Get specific job status | `./manage_workflow.sh job-status warlab-hr-int6024-company` |
| `logs` | Tail CloudWatch logs | `./manage_workflow.sh logs warlab-hr-int6024-company` |
| `delete-job` | Delete a job | `./manage_workflow.sh delete-job warlab-hr-int6024-company` |
| `delete-workflow` | Delete entire workflow | `./manage_workflow.sh delete-workflow` |
| `help` | Show help | `./manage_workflow.sh help` |

**Key Features**:
- Real-time workflow status monitoring
- Job execution tracking
- CloudWatch log streaming
- Color-coded status output
- Batch job management

**Usage Examples**:
```bash
# Monitor workflow continuously
watch -n 10 './manage_workflow.sh status'

# View live logs
./manage_workflow.sh logs warlab-hr-int6024-company

# Set region
AWS_REGION=us-west-2 ./manage_workflow.sh list-runs
```

**Lines of Code**: 500+
**Last Updated**: 2026-02-06

---

### 4. glue_workflow_config.json

**Purpose**: Configuration reference for all 12 Glue jobs with their parameters and workflow details.

**Contents**:
- Workflow metadata and description
- Shared ETL script location
- Configuration for all 12 Glue jobs
- S3 path mappings for each source table
- Redshift target schema and database
- Workflow triggers (on-demand + scheduled)
- Execution order and parallelization strategy
- Monitoring and alerting guidance

**Structure**:
```json
{
  "workflow_name": "warlab-hr-l1-load",
  "glue_jobs": [
    {
      "job_name": "warlab-hr-int0095e-worker-job",
      "source_table": "int0095e_worker_job",
      "s3_path": "s3://warlab-hr-datamart-dev/workday/hrdp/int0095e_worker_job/",
      ...
    },
    // ... 11 more jobs
  ],
  "workflow_triggers": { ... },
  "job_execution_order": { ... }
}
```

**Usage**:
- Reference for understanding job configuration
- Template for adding new jobs
- Documentation for infrastructure teams
- Configuration source for tools and automation

**Size**: 10 KB (JSON)
**Last Updated**: 2026-02-06

---

## Documentation Files

### 5. README.md

**Comprehensive guide covering**:
- Architecture and data flow
- Complete source table reference (all 12 tables)
- Prerequisites and infrastructure requirements
- Installation and deployment steps
- Usage examples (individual jobs, workflow, scheduling)
- Configuration options and customization
- Monitoring, troubleshooting, and best practices
- Performance tuning recommendations
- Security considerations
- Maintenance procedures
- Cost estimation
- Version history

**When to Use**:
- Deep technical understanding needed
- Troubleshooting complex issues
- Performance optimization
- Security and compliance questions

**Size**: 13 KB (comprehensive)

---

### 6. QUICK_START.md

**Getting started guide for**:
- Prerequisites checklist
- 5-minute deployment walkthrough
- First test run verification
- Common commands reference
- Troubleshooting quick fixes
- Configuration changes
- Next steps

**When to Use**:
- First-time setup
- Quick reference during deployment
- Common command reminders
- Troubleshooting quick issues

**Size**: 6.4 KB (concise)

---

### 7. REQUIREMENTS.md

**Detailed requirements documentation**:
- AWS account requirements
- AWS service quotas needed
- Glue connection configuration
- Glue service role permissions (full IAM policy)
- S3 bucket structure and setup
- Redshift schema and table requirements
- Local software prerequisites
- AWS CLI configuration
- Data format requirements
- Network and security group configuration
- Testing requirements
- Scalability limits and cost implications
- Compliance and monitoring setup

**When to Use**:
- Planning infrastructure
- Setting up prerequisites
- Permission troubleshooting
- Compliance validation

**Size**: 12 KB (detailed)

---

### 8. INDEX.md

**This file**. Quick reference guide to all project files with descriptions, usage, and quick links.

---

## Legacy Files (Not Used)

### glue_l1_to_l3_job.py
- **Status**: Legacy/archived
- **Purpose**: Old L1→L3 transformation job
- **Note**: Not used in current S3→L1 pipeline

### glue_s3_to_l1_job.py
- **Status**: Legacy/archived
- **Purpose**: Previous version of S3→L1 ETL
- **Note**: Replaced by `glue_s3_to_l1_etl.py` (parameterized version)

---

## File Summary

### By Type

**Python Scripts** (ETL):
- `glue_s3_to_l1_etl.py` (450+ lines) - PRODUCTION

**Bash Scripts** (Deployment/Management):
- `deploy_glue_jobs.sh` (400+ lines) - PRODUCTION
- `manage_workflow.sh` (500+ lines) - PRODUCTION

**Configuration**:
- `glue_workflow_config.json` (370+ lines) - Reference

**Documentation**:
- `README.md` (13 KB)
- `QUICK_START.md` (6.4 KB)
- `REQUIREMENTS.md` (12 KB)
- `INDEX.md` (This file)

### Total Lines of Code

```
Python ETL Scripts:    450+ lines
Bash Scripts:          900+ lines
Configuration (JSON):  370+ lines
Documentation:         4,579+ lines
────────────────────
Total Project:         ~6,300 lines
```

### File Size Summary

| File | Size | Purpose |
|------|------|---------|
| glue_s3_to_l1_etl.py | 16 KB | Main ETL script |
| deploy_glue_jobs.sh | 18 KB | Deployment automation |
| manage_workflow.sh | 16 KB | Monitoring utility |
| glue_workflow_config.json | 10 KB | Configuration reference |
| README.md | 13 KB | Full documentation |
| QUICK_START.md | 6.4 KB | Getting started guide |
| REQUIREMENTS.md | 12 KB | Prerequisites checklist |
| **Total** | **~92 KB** | **Production-ready solution** |

---

## Usage Workflows

### Initial Deployment

1. **Read**: `QUICK_START.md`
2. **Verify**: `REQUIREMENTS.md` - prerequisites checklist
3. **Run**: `deploy_glue_jobs.sh --dry-run`
4. **Deploy**: `deploy_glue_jobs.sh`
5. **Test**: First workflow run with `manage_workflow.sh start`
6. **Reference**: `README.md` for detailed operations

### Day-to-Day Operations

```
Monitoring:
  ./manage_workflow.sh status
  ./manage_workflow.sh list-runs

View Logs:
  ./manage_workflow.sh logs warlab-hr-int6024-company

Manual Trigger:
  ./manage_workflow.sh start

Check Job Status:
  ./manage_workflow.sh job-status warlab-hr-int6024-company
```

### Troubleshooting

1. **Issue Appears**: Check `README.md` → Troubleshooting section
2. **Quick Fix**: Check `QUICK_START.md` → Troubleshooting section
3. **Requirements Issue**: Check `REQUIREMENTS.md` → Verify prerequisites
4. **Infrastructure Help**: Reference `README.md` → Architecture section

### Customization

1. **Change Job Parameters**: Edit `deploy_glue_jobs.sh` → Line 45-50
2. **Change Schedule**: See `README.md` → Scheduling section
3. **Add New Table**: See `QUICK_START.md` → Add a New Source Table
4. **Performance Tuning**: See `README.md` → Performance Tuning section

---

## Code Quality Notes

### Python Script (glue_s3_to_l1_etl.py)

- **Style**: PEP 8 compliant
- **Logging**: Comprehensive with class-based logger
- **Error Handling**: Try-catch with detailed error messages
- **Documentation**: Full docstrings and inline comments
- **Modularity**: Separate functions for each ETL step
- **Type Hints**: Where applicable
- **Testing**: Can be run locally with test data

### Bash Scripts (deploy_glue_jobs.sh, manage_workflow.sh)

- **Style**: Bash best practices (set -euo pipefail)
- **Error Handling**: Comprehensive exit code handling
- **Logging**: Color-coded output with levels
- **Documentation**: Full headers and inline comments
- **Modularity**: Separate functions for each command
- **Portability**: Works on macOS and Linux
- **Idempotency**: Safe to re-run (checks before creating)

### JSON Configuration (glue_workflow_config.json)

- **Format**: Valid JSON, well-structured
- **Comments**: Included as documentation fields
- **Consistency**: All 12 jobs follow same pattern
- **Maintainability**: Easy to add/modify jobs
- **Validation**: Can be validated with `jq` or Python

---

## Quick Reference Commands

### Deployment

```bash
# Validate setup
./deploy_glue_jobs.sh --dry-run

# Deploy
./deploy_glue_jobs.sh

# Redeploy (overwrites existing)
./deploy_glue_jobs.sh
```

### Operations

```bash
# Start workflow
./manage_workflow.sh start

# Check status
./manage_workflow.sh status

# View logs
./manage_workflow.sh logs warlab-hr-int6024-company

# List jobs
./manage_workflow.sh list-jobs
```

### AWS CLI

```bash
# List jobs
aws glue list-jobs --region us-east-1

# Get job details
aws glue get-job --name warlab-hr-int6024-company

# Start job
aws glue start-job-run --job-name warlab-hr-int6024-company

# List runs
aws glue get-workflow-runs --name warlab-hr-l1-load
```

---

## Support Resources

| Resource | Location | Purpose |
|----------|----------|---------|
| Quick Start | `QUICK_START.md` | Getting started (5 min read) |
| Full Docs | `README.md` | Complete reference (15 min read) |
| Prerequisites | `REQUIREMENTS.md` | Setup checklist (10 min read) |
| This Guide | `INDEX.md` | File reference (5 min read) |
| ETL Code | `glue_s3_to_l1_etl.py` | Production script (450+ lines) |
| Deployment | `deploy_glue_jobs.sh` | Automation (400+ lines) |
| Management | `manage_workflow.sh` | Operations (500+ lines) |

---

## Version Information

| Component | Version | Date | Status |
|-----------|---------|------|--------|
| ETL Script | 1.0 | 2026-02-06 | Production |
| Deployment Script | 1.0 | 2026-02-06 | Production |
| Management Script | 1.0 | 2026-02-06 | Production |
| Documentation | 1.0 | 2026-02-06 | Production |

---

## Getting Help

1. **Quick issue?** → Check `QUICK_START.md`
2. **Technical details?** → Check `README.md`
3. **Setup problem?** → Check `REQUIREMENTS.md`
4. **Not sure where?** → Start with this file (`INDEX.md`)

---

**Navigation**:
- Start here: `QUICK_START.md`
- Learn more: `README.md`
- Setup guide: `REQUIREMENTS.md`
- This index: `INDEX.md`

**Last Updated**: 2026-02-06
**Maintained By**: Data Engineering Team
