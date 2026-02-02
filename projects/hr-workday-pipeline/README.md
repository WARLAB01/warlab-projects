# HR Workday Data Pipeline

Simulated HR Workday data generator and AWS data pipeline for loading transactional HR data into Redshift.

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
└── aws_deployment/
    ├── README.md                   # Detailed AWS deployment guide
    ├── deploy.sh                   # Master deployment script
    ├── scripts/                    # Individual setup scripts
    ├── cloudformation/             # IAM roles template
    ├── glue_jobs/                  # ETL Python scripts
    └── sql/                        # Redshift DDL
```

## Requirements

- Python 3.8+
- AWS CLI configured with appropriate permissions
- Faker, Pandas, NumPy libraries

## License

Internal use only.
