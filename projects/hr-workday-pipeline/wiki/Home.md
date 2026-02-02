# HR Workday Data Pipeline Wiki

Welcome to the HR Workday Data Pipeline documentation wiki!

## Quick Links

| Topic | Description |
|-------|-------------|
| [Getting Started](Getting-Started) | New user onboarding guide |
| [Architecture Overview](Architecture-Overview) | System design and data flow |
| [Data Model](Data-Model) | Tables, fields, and relationships |
| [Sample Queries](Sample-Queries) | Common query patterns |
| [Troubleshooting](Troubleshooting) | Common issues and solutions |
| [FAQ](FAQ) | Frequently asked questions |

---

## What is this Pipeline?

The HR Workday Data Pipeline is an automated ETL (Extract, Transform, Load) solution that:

1. **Extracts** HR data from Workday HCM
2. **Transforms** it into analytics-ready format
3. **Loads** it into Amazon Redshift for querying

This enables self-service HR analytics, workforce planning, and operational reporting across the organization.

---

## Key Features

✅ **Daily Automated Updates** - Fresh data every morning by 6:30 AM UTC

✅ **Self-Service Analytics** - Query directly via Redshift Query Editor

✅ **Complete HR Picture** - Employees, jobs, compensation, and org structure

✅ **Historical Tracking** - Full transaction history for trend analysis

✅ **Secure Access** - Role-based access controls protecting sensitive data

---

## Data Available

| Dataset | Records | Description |
|---------|---------|-------------|
| Core HR Employees | ~10,000 | Current state snapshot |
| Job Movement | ~7,000 | Promotions, transfers, hires |
| Compensation Changes | ~20,000 | Salary and bonus changes |
| Worker Movement | ~13,000 | Location and org changes |

---

## Getting Help

- **Slack**: #hr-analytics
- **Email**: hr-analytics@company.com
- **Office Hours**: Wednesdays 2-3 PM ET

---

## Recent Updates

| Date | Update |
|------|--------|
| 2026-02-02 | Initial pipeline deployment |
| 2026-02-02 | Documentation complete |

---

*This wiki is maintained by the Data Engineering team.*
