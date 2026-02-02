# HR Workday Data Pipeline

Simulated HR Workday data generator and AWS data pipeline for loading transactional HR data into Redshift.

[![Documentation](https://img.shields.io/badge/docs-complete-brightgreen)](docs/)
[![AWS](https://img.shields.io/badge/platform-AWS-orange)](aws_deployment/)
[![Python](https://img.shields.io/badge/python-3.8+-blue)](workday_hr_data_generator.py)

## Overview

This project provides:
1. **Python Data Generator** - Creates realistic Workday-style HR datasets for a 10,000-employee North American Financial Services organization
2. **AWS Deployment Scripts** - Complete infrastructure-as-code for S3, Glue, and Redshift Serverless

## Data Model

| Dataset | Records | Description |
|---------|---------|-------------|
| `core_hr_employees` | 10,000 | Non-transactional employee master data |
| `job_movement_transactions` | ~7,000 | Hires, promotions, terminations, lateral moves |
| `compensation_change_transactions` | ~20,000 | Merit increases, market adjustments, equity grants |
| `worker_movement_transactions` | ~13,000 | Transfers, relocations, org changes |

**Total: ~40,000+ transactions over 1 year**

## Key Features

### Workday-Style Data Structure
- **Keys**: Employee_ID (EIN) + Effective_Date for transactions
- **Org Hierarchy**: Business Unit → Division → Department → Team
- **Compensation**: Base salary, bonus targets, equity grants, allowances
- **Locations**: 12 North American financial centers

### AWS Pipeline
- S3 bucket with encryption and versioning
- Redshift Serverless with optimized table design
- Glue ETL jobs using native COPY command
- Daily automated scheduling
- Full IAM roles via CloudFormation

## Quick Start

### 1. Generate Data
```bash
pip install faker pandas numpy
python workday_hr_data_generator.py
```

### 2. Deploy to AWS
```bash
cd aws_deployment
chmod +x deploy.sh scripts/*.sh
./deploy.sh ../data
```

## Project Structure

```
hr-workday-pipeline/
├── README.md
├── workday_hr_data_generator.py    # Python data generator
├── data/                           # Generated CSV files
│   ├── core_hr_employees.csv
│   ├── job_movement_transactions.csv
│   ├── compensation_change_transactions.csv
│   └── worker_movement_transactions.csv
├── aws_deployment/
│   ├── README.md                   # Detailed AWS deployment guide
│   ├── deploy.sh                   # Master deployment script
│   ├── scripts/                    # Individual setup scripts
│   ├── cloudformation/             # IAM roles template
│   ├── glue_jobs/                  # ETL Python scripts
│   └── sql/                        # Redshift DDL
├── docs/
│   ├── project-management/
│   │   ├── PROJECT_CHARTER.md      # Executive summary & scope
│   │   └── REQUIREMENTS.md         # Functional requirements
│   ├── technical/
│   │   ├── ARCHITECTURE.md         # System architecture
│   │   └── DATA_DICTIONARY.md      # Field-level documentation
│   ├── operations/
│   │   └── RUNBOOK.md              # Operational procedures
│   ├── governance/
│   │   └── DATA_GOVERNANCE.md      # Security & compliance
│   └── user-guides/
│       ├── GETTING_STARTED.md      # New user guide
│       └── FAQ.md                  # Common questions
└── wiki/                           # GitHub Wiki pages
    ├── Home.md
    ├── Architecture-Overview.md
    ├── Data-Model.md
    ├── Sample-Queries.md
    └── Troubleshooting.md
```

## Documentation

### For New Users
- **[Getting Started Guide](docs/user-guides/GETTING_STARTED.md)** - Access setup, first queries
- **[FAQ](docs/user-guides/FAQ.md)** - Frequently asked questions
- **[Sample Queries](wiki/Sample-Queries.md)** - Common analytics patterns

### Technical Documentation
- **[Architecture](docs/technical/ARCHITECTURE.md)** - System design, data flow, AWS components
- **[Data Dictionary](docs/technical/DATA_DICTIONARY.md)** - Complete field-level documentation
- **[Data Model](wiki/Data-Model.md)** - Tables, relationships, joins

### Operations
- **[Runbook](docs/operations/RUNBOOK.md)** - Daily operations, incident response, maintenance
- **[Troubleshooting](wiki/Troubleshooting.md)** - Common issues and solutions

### Governance
- **[Data Governance](docs/governance/DATA_GOVERNANCE.md)** - Classification, access control, retention
- **[Project Charter](docs/project-management/PROJECT_CHARTER.md)** - Scope, budget, stakeholders
- **[Requirements](docs/project-management/REQUIREMENTS.md)** - Functional & non-functional specs

## Requirements

- Python 3.8+
- AWS CLI configured with appropriate permissions
- Faker, Pandas, NumPy libraries

## License

Internal use only.
