#!/usr/bin/env python3
"""
Workday HR Data Generator
=========================
Generates simulated HR transactional and core data for a North American
Financial Services organization with 10,000 employees.

Outputs:
- core_hr_employees.csv: Non-transactional employee master data
- job_movement_transactions.csv: Hire, promotion, termination events
- compensation_change_transactions.csv: Salary, bonus, equity changes
- worker_movement_transactions.csv: Transfers, relocations, org changes

Author: Generated for HR Analytics
Date: 2026
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import random
import os

# Initialize
fake = Faker(['en_US', 'en_CA'])
np.random.seed(42)
random.seed(42)

# =============================================================================
# CONFIGURATION - Financial Services Organization Structure
# =============================================================================

# Date range for transactions (1 year lookback)
END_DATE = datetime(2026, 2, 2)
START_DATE = END_DATE - timedelta(days=365)

# Organization size
TOTAL_EMPLOYEES = 10000
TERMINATION_RATE = 0.12  # 12% annual turnover
NEW_HIRE_RATE = 0.15     # 15% growth/replacement

# Business Units (Financial Services specific)
BUSINESS_UNITS = {
    'Retail Banking': {
        'divisions': ['Consumer Lending', 'Deposits & Payments', 'Branch Operations', 'Digital Banking'],
        'weight': 0.30
    },
    'Investment Banking': {
        'divisions': ['Mergers & Acquisitions', 'Capital Markets', 'Structured Finance', 'Advisory Services'],
        'weight': 0.20
    },
    'Wealth Management': {
        'divisions': ['Private Banking', 'Asset Management', 'Trust Services', 'Financial Planning'],
        'weight': 0.15
    },
    'Commercial Banking': {
        'divisions': ['Corporate Lending', 'Treasury Services', 'Trade Finance', 'Real Estate Finance'],
        'weight': 0.15
    },
    'Risk & Compliance': {
        'divisions': ['Enterprise Risk', 'Regulatory Compliance', 'Audit', 'Financial Crimes'],
        'weight': 0.08
    },
    'Technology': {
        'divisions': ['Infrastructure', 'Application Development', 'Cybersecurity', 'Data & Analytics'],
        'weight': 0.07
    },
    'Corporate Functions': {
        'divisions': ['Human Resources', 'Finance', 'Legal', 'Marketing'],
        'weight': 0.05
    }
}

# Departments by Division
DEPARTMENTS = {
    'Consumer Lending': ['Mortgage Origination', 'Auto Loans', 'Personal Loans', 'Credit Cards', 'Collections'],
    'Deposits & Payments': ['Checking & Savings', 'Payment Processing', 'ACH Operations', 'Wire Transfers'],
    'Branch Operations': ['Branch Management', 'Teller Operations', 'Customer Service', 'Sales'],
    'Digital Banking': ['Mobile Banking', 'Online Banking', 'Digital Onboarding', 'UX Design'],
    'Mergers & Acquisitions': ['Deal Origination', 'Due Diligence', 'Valuation', 'Integration'],
    'Capital Markets': ['Equity Sales', 'Fixed Income', 'Derivatives', 'Research'],
    'Structured Finance': ['Securitization', 'Project Finance', 'Leveraged Finance', 'Syndications'],
    'Advisory Services': ['Strategic Advisory', 'Restructuring', 'Sponsor Coverage', 'Industry Coverage'],
    'Private Banking': ['Client Advisory', 'Portfolio Management', 'Client Onboarding', 'Relationship Management'],
    'Asset Management': ['Equity Strategies', 'Fixed Income Strategies', 'Alternative Investments', 'Fund Operations'],
    'Trust Services': ['Estate Planning', 'Trust Administration', 'Fiduciary Services', 'Charitable Services'],
    'Financial Planning': ['Retirement Planning', 'Tax Planning', 'Insurance Services', 'Education Planning'],
    'Corporate Lending': ['Underwriting', 'Portfolio Management', 'Loan Operations', 'Relationship Management'],
    'Treasury Services': ['Cash Management', 'Liquidity Management', 'FX Services', 'Trade Operations'],
    'Trade Finance': ['Letters of Credit', 'Documentary Collections', 'Supply Chain Finance', 'Export Finance'],
    'Real Estate Finance': ['Commercial Mortgage', 'Construction Lending', 'Property Management', 'REIT Coverage'],
    'Enterprise Risk': ['Credit Risk', 'Market Risk', 'Operational Risk', 'Model Risk'],
    'Regulatory Compliance': ['Bank Secrecy Act', 'Consumer Compliance', 'Securities Compliance', 'Policy Management'],
    'Audit': ['Internal Audit', 'SOX Compliance', 'IT Audit', 'Operational Audit'],
    'Financial Crimes': ['AML Operations', 'Fraud Detection', 'Sanctions Screening', 'Investigations'],
    'Infrastructure': ['Network Operations', 'Cloud Services', 'Database Administration', 'Systems Engineering'],
    'Application Development': ['Front-End Development', 'Back-End Development', 'DevOps', 'Quality Assurance'],
    'Cybersecurity': ['Security Operations', 'Identity Management', 'Threat Intelligence', 'Penetration Testing'],
    'Data & Analytics': ['Data Engineering', 'Business Intelligence', 'Machine Learning', 'Data Governance'],
    'Human Resources': ['Talent Acquisition', 'HR Business Partners', 'Compensation & Benefits', 'Learning & Development'],
    'Finance': ['Financial Planning & Analysis', 'Accounting', 'Treasury', 'Tax'],
    'Legal': ['Corporate Legal', 'Litigation', 'Regulatory Affairs', 'Contract Management'],
    'Marketing': ['Brand Marketing', 'Digital Marketing', 'Product Marketing', 'Communications']
}

# Job Profiles by Level (Financial Services)
JOB_PROFILES = {
    'Individual Contributor': {
        1: ['Analyst I', 'Associate I', 'Specialist I', 'Coordinator'],
        2: ['Analyst II', 'Associate II', 'Specialist II', 'Representative'],
        3: ['Senior Analyst', 'Senior Associate', 'Senior Specialist', 'Lead Representative'],
        4: ['Lead Analyst', 'Principal Associate', 'Lead Specialist', 'Team Lead'],
        5: ['Principal Analyst', 'Expert', 'Senior Principal', 'Staff Specialist']
    },
    'Management': {
        6: ['Supervisor', 'Assistant Manager', 'Team Manager'],
        7: ['Manager', 'Branch Manager', 'Operations Manager'],
        8: ['Senior Manager', 'Associate Director', 'Program Manager'],
        9: ['Director', 'Regional Director', 'Executive Director'],
        10: ['Senior Director', 'Group Director', 'Managing Director']
    },
    'Executive': {
        11: ['Vice President', 'Senior Vice President'],
        12: ['Executive Vice President', 'Chief Officer']
    }
}

# Job Families
JOB_FAMILIES = [
    'Banking Operations', 'Investment Management', 'Risk Management',
    'Technology', 'Finance', 'Human Resources', 'Sales', 'Client Services',
    'Compliance', 'Legal', 'Marketing', 'Analytics', 'Audit', 'Trading',
    'Wealth Advisory', 'Credit', 'Treasury', 'Product Management'
]

# Locations (North American Financial Centers)
LOCATIONS = {
    'New York, NY': {'weight': 0.25, 'country': 'USA', 'region': 'Northeast'},
    'Charlotte, NC': {'weight': 0.15, 'country': 'USA', 'region': 'Southeast'},
    'Chicago, IL': {'weight': 0.12, 'country': 'USA', 'region': 'Midwest'},
    'San Francisco, CA': {'weight': 0.10, 'country': 'USA', 'region': 'West'},
    'Boston, MA': {'weight': 0.08, 'country': 'USA', 'region': 'Northeast'},
    'Toronto, ON': {'weight': 0.08, 'country': 'Canada', 'region': 'Ontario'},
    'Dallas, TX': {'weight': 0.06, 'country': 'USA', 'region': 'Southwest'},
    'Los Angeles, CA': {'weight': 0.05, 'country': 'USA', 'region': 'West'},
    'Atlanta, GA': {'weight': 0.04, 'country': 'USA', 'region': 'Southeast'},
    'Denver, CO': {'weight': 0.03, 'country': 'USA', 'region': 'Mountain'},
    'Montreal, QC': {'weight': 0.02, 'country': 'Canada', 'region': 'Quebec'},
    'Phoenix, AZ': {'weight': 0.02, 'country': 'USA', 'region': 'Southwest'}
}

# Compensation Bands by Level (Financial Services - in USD)
COMPENSATION_BANDS = {
    1: {'base_min': 55000, 'base_max': 75000, 'bonus_pct': 0.05, 'equity': False},
    2: {'base_min': 70000, 'base_max': 95000, 'bonus_pct': 0.08, 'equity': False},
    3: {'base_min': 85000, 'base_max': 120000, 'bonus_pct': 0.12, 'equity': True, 'equity_range': (5000, 15000)},
    4: {'base_min': 105000, 'base_max': 145000, 'bonus_pct': 0.15, 'equity': True, 'equity_range': (10000, 30000)},
    5: {'base_min': 130000, 'base_max': 175000, 'bonus_pct': 0.20, 'equity': True, 'equity_range': (20000, 50000)},
    6: {'base_min': 120000, 'base_max': 160000, 'bonus_pct': 0.18, 'equity': True, 'equity_range': (15000, 40000)},
    7: {'base_min': 140000, 'base_max': 190000, 'bonus_pct': 0.25, 'equity': True, 'equity_range': (25000, 60000)},
    8: {'base_min': 165000, 'base_max': 230000, 'bonus_pct': 0.30, 'equity': True, 'equity_range': (40000, 100000)},
    9: {'base_min': 200000, 'base_max': 300000, 'bonus_pct': 0.40, 'equity': True, 'equity_range': (75000, 200000)},
    10: {'base_min': 250000, 'base_max': 400000, 'bonus_pct': 0.50, 'equity': True, 'equity_range': (150000, 400000)},
    11: {'base_min': 350000, 'base_max': 550000, 'bonus_pct': 0.75, 'equity': True, 'equity_range': (300000, 750000)},
    12: {'base_min': 500000, 'base_max': 900000, 'bonus_pct': 1.00, 'equity': True, 'equity_range': (500000, 1500000)}
}

# Transaction Reason Codes (Workday style)
HIRE_REASONS = [
    'New Position', 'Backfill', 'Expansion', 'Campus Hire',
    'Experienced Hire', 'Internal Referral', 'Executive Recruitment'
]

TERMINATION_REASONS = [
    'Voluntary - New Opportunity', 'Voluntary - Relocation', 'Voluntary - Career Change',
    'Voluntary - Retirement', 'Voluntary - Personal Reasons', 'Voluntary - Return to School',
    'Involuntary - Performance', 'Involuntary - Position Elimination',
    'Involuntary - Restructuring', 'Involuntary - End of Contract',
    'Death', 'Disability'
]

PROMOTION_REASONS = [
    'Performance Based', 'Market Adjustment', 'Expanded Role',
    'Succession Planning', 'Skill Development', 'Retention'
]

TRANSFER_REASONS = [
    'Business Need', 'Career Development', 'Relocation Request',
    'Restructuring', 'Project Assignment', 'Leadership Development'
]

COMP_CHANGE_REASONS = [
    'Annual Merit Increase', 'Promotion', 'Market Adjustment',
    'Equity Refresh', 'Bonus Adjustment', 'Role Change',
    'Retention', 'Cost of Living Adjustment'
]

# Allowance Types (Financial Services)
ALLOWANCE_TYPES = {
    'Car Allowance': {'eligible_levels': [8, 9, 10, 11, 12], 'amount_range': (6000, 18000)},
    'Phone Allowance': {'eligible_levels': [5, 6, 7, 8, 9, 10, 11, 12], 'amount_range': (1200, 2400)},
    'Executive Perquisite': {'eligible_levels': [11, 12], 'amount_range': (25000, 100000)},
    'Relocation Allowance': {'eligible_levels': list(range(1, 13)), 'amount_range': (10000, 75000)},
    'Sign-on Bonus': {'eligible_levels': list(range(3, 13)), 'amount_range': (10000, 250000)}
}

# Worker Types
WORKER_TYPES = ['Regular', 'Temporary', 'Contractor', 'Intern']
WORKER_TYPE_WEIGHTS = [0.85, 0.05, 0.08, 0.02]

# Pay Rate Types
PAY_RATE_TYPES = {'Regular': 'Salary', 'Temporary': 'Hourly', 'Contractor': 'Hourly', 'Intern': 'Hourly'}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def generate_ein():
    """Generate a unique Employee Identification Number (Workday format)"""
    return f"EMP{random.randint(100000, 999999)}"

def generate_worker_id():
    """Generate Worker ID (different from EIN in Workday)"""
    return f"WD{random.randint(10000000, 99999999)}"

def get_random_location():
    """Select a location based on weights"""
    locations = list(LOCATIONS.keys())
    weights = [LOCATIONS[loc]['weight'] for loc in locations]
    return random.choices(locations, weights=weights)[0]

def get_random_business_unit():
    """Select a business unit based on weights"""
    bus = list(BUSINESS_UNITS.keys())
    weights = [BUSINESS_UNITS[b]['weight'] for b in bus]
    return random.choices(bus, weights=weights)[0]

def get_org_structure(business_unit):
    """Generate full org hierarchy"""
    division = random.choice(BUSINESS_UNITS[business_unit]['divisions'])
    department = random.choice(DEPARTMENTS.get(division, ['General']))
    team = f"{department} - Team {random.randint(1, 5)}"
    return division, department, team

def get_job_info(level):
    """Get job profile and management level based on job level"""
    if level <= 5:
        mgmt_level = 'Individual Contributor'
        profiles = JOB_PROFILES['Individual Contributor'][level]
    elif level <= 10:
        mgmt_level = 'Management'
        profiles = JOB_PROFILES['Management'][level]
    else:
        mgmt_level = 'Executive'
        profiles = JOB_PROFILES['Executive'][level]

    return random.choice(profiles), mgmt_level

def generate_compensation(level, location):
    """Generate compensation package based on level and location"""
    band = COMPENSATION_BANDS[level]

    # Location multiplier
    loc_multipliers = {
        'New York, NY': 1.15, 'San Francisco, CA': 1.20, 'Boston, MA': 1.10,
        'Los Angeles, CA': 1.08, 'Chicago, IL': 1.0, 'Toronto, ON': 0.95,
        'Charlotte, NC': 0.92, 'Dallas, TX': 0.95, 'Atlanta, GA': 0.93,
        'Denver, CO': 0.97, 'Montreal, QC': 0.88, 'Phoenix, AZ': 0.90
    }
    multiplier = loc_multipliers.get(location, 1.0)

    base_salary = round(random.uniform(band['base_min'], band['base_max']) * multiplier, -3)
    bonus_target_pct = band['bonus_pct']
    bonus_target_amount = round(base_salary * bonus_target_pct, -2)

    # Equity
    if band['equity']:
        equity_grant = round(random.uniform(*band['equity_range']) * multiplier, -3)
    else:
        equity_grant = 0

    return {
        'base_salary': base_salary,
        'bonus_target_pct': bonus_target_pct,
        'bonus_target_amount': bonus_target_amount,
        'annual_equity_grant': equity_grant,
        'total_compensation': base_salary + bonus_target_amount + equity_grant
    }

def generate_allowances(level, is_new_hire=False, is_relocation=False):
    """Generate applicable allowances"""
    allowances = {}

    for allowance_type, config in ALLOWANCE_TYPES.items():
        if level in config['eligible_levels']:
            # Sign-on bonus only for new hires (probability based)
            if allowance_type == 'Sign-on Bonus':
                if is_new_hire and random.random() < 0.3:
                    allowances[allowance_type] = round(random.uniform(*config['amount_range']), -3)
            # Relocation only if relocating
            elif allowance_type == 'Relocation Allowance':
                if is_relocation and random.random() < 0.8:
                    allowances[allowance_type] = round(random.uniform(*config['amount_range']), -3)
            # Other allowances have probability
            elif random.random() < 0.4:
                allowances[allowance_type] = round(random.uniform(*config['amount_range']), -2)

    return allowances

def random_date_in_range(start, end):
    """Generate a random date within range (returns date object)"""
    delta = end - start
    random_days = random.randint(0, delta.days)
    result = start + timedelta(days=random_days)
    # Return as date object for consistency with faker
    if hasattr(result, 'date'):
        return result.date()
    return result

def generate_cost_center(business_unit, division):
    """Generate cost center code"""
    bu_codes = {'Retail Banking': '10', 'Investment Banking': '20', 'Wealth Management': '30',
                'Commercial Banking': '40', 'Risk & Compliance': '50', 'Technology': '60',
                'Corporate Functions': '70'}
    bu_code = bu_codes.get(business_unit, '99')
    div_code = str(random.randint(100, 999))
    return f"CC-{bu_code}-{div_code}"


# =============================================================================
# DATA GENERATION FUNCTIONS
# =============================================================================

def generate_base_employees(n_employees):
    """Generate the base employee population"""
    print(f"Generating {n_employees} base employees...")

    employees = []
    managers = {}  # Track managers by org

    for i in range(n_employees):
        if i % 1000 == 0:
            print(f"  Progress: {i}/{n_employees}")

        # Generate basic info
        ein = generate_ein()
        worker_id = generate_worker_id()

        # Demographics
        gender = random.choice(['M', 'F'])
        if gender == 'M':
            first_name = fake.first_name_male()
        else:
            first_name = fake.first_name_female()
        last_name = fake.last_name()
        preferred_name = first_name if random.random() > 0.15 else fake.first_name()

        # Worker type
        worker_type = random.choices(WORKER_TYPES, weights=WORKER_TYPE_WEIGHTS)[0]

        # Job level distribution (pyramid structure)
        level_weights = [0.20, 0.25, 0.20, 0.12, 0.08, 0.05, 0.04, 0.03, 0.015, 0.01, 0.004, 0.001]
        level = random.choices(range(1, 13), weights=level_weights)[0]

        # Org structure
        business_unit = get_random_business_unit()
        division, department, team = get_org_structure(business_unit)
        location = get_random_location()

        # Job info
        job_profile, mgmt_level = get_job_info(level)
        job_family = random.choice(JOB_FAMILIES)
        business_title = job_profile

        # Cost center
        cost_center = generate_cost_center(business_unit, division)

        # Dates
        # Most employees hired before the transaction window
        if random.random() < 0.85:
            original_hire_date = fake.date_between(start_date='-10y', end_date=START_DATE - timedelta(days=1))
        else:
            original_hire_date = random_date_in_range(START_DATE, END_DATE)

        hire_date = original_hire_date  # Could be different if rehire

        # Active/Terminated status
        if worker_type == 'Regular':
            if random.random() < 0.08:  # 8% currently terminated
                worker_status = 'Terminated'
                termination_date = random_date_in_range(START_DATE, END_DATE)
            else:
                worker_status = 'Active'
                termination_date = None
        else:
            worker_status = 'Active'
            termination_date = None

        # Compensation
        comp = generate_compensation(level, location)
        allowances = generate_allowances(level, is_new_hire=(original_hire_date >= START_DATE.date()))

        # FTE
        if worker_type == 'Intern':
            fte = random.choice([0.5, 1.0])
        elif worker_type in ['Temporary', 'Contractor']:
            fte = 1.0
        else:
            fte = random.choices([1.0, 0.8, 0.5], weights=[0.95, 0.03, 0.02])[0]

        # Email
        email = f"{first_name.lower()}.{last_name.lower()}@globalfinancial.com"

        # Supervisory org
        sup_org = f"{department} ({team})"

        employee = {
            'Employee_ID': ein,
            'Worker_ID': worker_id,
            'First_Name': first_name,
            'Last_Name': last_name,
            'Preferred_Name': preferred_name,
            'Legal_Full_Name': f"{first_name} {last_name}",
            'Email_Work': email,
            'Gender': gender,
            'Original_Hire_Date': original_hire_date,
            'Hire_Date': hire_date,
            'Termination_Date': termination_date,
            'Worker_Status': worker_status,
            'Worker_Type': worker_type,
            'Business_Title': business_title,
            'Job_Profile': job_profile,
            'Job_Family': job_family,
            'Job_Level': level,
            'Management_Level': mgmt_level,
            'Supervisory_Organization': sup_org,
            'Manager_Employee_ID': None,  # Will be assigned later
            'Business_Unit': business_unit,
            'Division': division,
            'Department': department,
            'Team': team,
            'Cost_Center': cost_center,
            'Location': location,
            'Country': LOCATIONS[location]['country'],
            'Region': LOCATIONS[location]['region'],
            'Pay_Rate_Type': PAY_RATE_TYPES[worker_type],
            'FTE': fte,
            'Base_Salary': comp['base_salary'],
            'Bonus_Target_Percent': comp['bonus_target_pct'],
            'Bonus_Target_Amount': comp['bonus_target_amount'],
            'Annual_Equity_Grant': comp['annual_equity_grant'],
            'Total_Compensation': comp['total_compensation'],
            'Currency': 'CAD' if LOCATIONS[location]['country'] == 'Canada' else 'USD',
            'Car_Allowance': allowances.get('Car Allowance', 0),
            'Phone_Allowance': allowances.get('Phone Allowance', 0),
            'Executive_Perquisite': allowances.get('Executive Perquisite', 0),
            'Last_Performance_Rating': random.choices(
                ['Exceptional', 'Exceeds Expectations', 'Meets Expectations', 'Needs Improvement', 'Unsatisfactory'],
                weights=[0.10, 0.25, 0.50, 0.12, 0.03]
            )[0],
            'Years_of_Service': max(0, (END_DATE.date() - original_hire_date).days // 365),
            'Time_in_Position': random.randint(0, 5),
            'Is_Manager': mgmt_level in ['Management', 'Executive']
        }

        employees.append(employee)

    # Assign managers
    df = pd.DataFrame(employees)
    managers_pool = df[df['Is_Manager'] == True]['Employee_ID'].tolist()

    for idx in range(len(employees)):
        if employees[idx]['Job_Level'] < 12:  # Everyone except top level has a manager
            # Try to assign a manager from same org with higher level
            potential_managers = df[
                (df['Business_Unit'] == employees[idx]['Business_Unit']) &
                (df['Job_Level'] > employees[idx]['Job_Level']) &
                (df['Is_Manager'] == True)
            ]['Employee_ID'].tolist()

            if potential_managers:
                employees[idx]['Manager_Employee_ID'] = random.choice(potential_managers)
            elif managers_pool:
                employees[idx]['Manager_Employee_ID'] = random.choice(managers_pool)

    print(f"  Completed: {n_employees} employees generated")
    return pd.DataFrame(employees)


def generate_job_movement_transactions(employees_df, start_date, end_date):
    """Generate job movement transactions (hire, promotion, termination, etc.)"""
    print("Generating job movement transactions...")

    transactions = []
    active_employees = employees_df[employees_df['Worker_Status'] == 'Active']

    # 1. Generate HIRE transactions for employees hired within the period
    new_hires = employees_df[
        (employees_df['Hire_Date'] >= start_date.date()) &
        (employees_df['Hire_Date'] <= end_date.date())
    ]

    for _, emp in new_hires.iterrows():
        transactions.append({
            'Transaction_ID': f"JM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': emp['Hire_Date'],
            'Transaction_Type': 'Hire',
            'Transaction_Status': 'Completed',
            'Reason_Code': random.choice(HIRE_REASONS),
            'Prior_Job_Profile': None,
            'New_Job_Profile': emp['Job_Profile'],
            'Prior_Job_Level': None,
            'New_Job_Level': emp['Job_Level'],
            'Prior_Business_Unit': None,
            'New_Business_Unit': emp['Business_Unit'],
            'Prior_Division': None,
            'New_Division': emp['Division'],
            'Prior_Department': None,
            'New_Department': emp['Department'],
            'Prior_Manager_ID': None,
            'New_Manager_ID': emp['Manager_Employee_ID'],
            'Prior_Location': None,
            'New_Location': emp['Location'],
            'Prior_Worker_Type': None,
            'New_Worker_Type': emp['Worker_Type'],
            'Initiated_By': random.choice(['HR System', 'Recruiter', 'Hiring Manager']),
            'Initiated_Date': emp['Hire_Date'] - timedelta(days=random.randint(7, 30)),
            'Completed_Date': emp['Hire_Date'],
            'Comments': f"New hire - {random.choice(HIRE_REASONS)}"
        })

    # 2. Generate TERMINATION transactions
    terminated = employees_df[
        (employees_df['Termination_Date'].notna()) &
        (employees_df['Termination_Date'] >= start_date.date()) &
        (employees_df['Termination_Date'] <= end_date.date())
    ]

    for _, emp in terminated.iterrows():
        reason = random.choice(TERMINATION_REASONS)
        transactions.append({
            'Transaction_ID': f"JM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': emp['Termination_Date'],
            'Transaction_Type': 'Termination',
            'Transaction_Status': 'Completed',
            'Reason_Code': reason,
            'Prior_Job_Profile': emp['Job_Profile'],
            'New_Job_Profile': None,
            'Prior_Job_Level': emp['Job_Level'],
            'New_Job_Level': None,
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': None,
            'Prior_Division': emp['Division'],
            'New_Division': None,
            'Prior_Department': emp['Department'],
            'New_Department': None,
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': None,
            'Prior_Location': emp['Location'],
            'New_Location': None,
            'Prior_Worker_Type': emp['Worker_Type'],
            'New_Worker_Type': None,
            'Initiated_By': 'HR' if 'Involuntary' in reason else 'Employee',
            'Initiated_Date': emp['Termination_Date'] - timedelta(days=random.randint(14, 60)),
            'Completed_Date': emp['Termination_Date'],
            'Comments': reason
        })

    # 3. Generate PROMOTIONS (high volume for stress testing)
    n_promotions = int(len(active_employees) * 0.30)
    promotion_pool = active_employees[active_employees['Job_Level'] < 12]
    promotion_candidates = promotion_pool.sample(n=min(n_promotions, len(promotion_pool)))

    for _, emp in promotion_candidates.iterrows():
        new_level = emp['Job_Level'] + 1
        new_profile, new_mgmt_level = get_job_info(new_level)
        effective_date = random_date_in_range(start_date, end_date)

        transactions.append({
            'Transaction_ID': f"JM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Transaction_Type': 'Promotion',
            'Transaction_Status': 'Completed',
            'Reason_Code': random.choice(PROMOTION_REASONS),
            'Prior_Job_Profile': emp['Job_Profile'],
            'New_Job_Profile': new_profile,
            'Prior_Job_Level': emp['Job_Level'],
            'New_Job_Level': new_level,
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': emp['Business_Unit'],
            'Prior_Division': emp['Division'],
            'New_Division': emp['Division'],
            'Prior_Department': emp['Department'],
            'New_Department': emp['Department'],
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': emp['Manager_Employee_ID'],
            'Prior_Location': emp['Location'],
            'New_Location': emp['Location'],
            'Prior_Worker_Type': emp['Worker_Type'],
            'New_Worker_Type': emp['Worker_Type'],
            'Initiated_By': 'Manager',
            'Initiated_Date': effective_date - timedelta(days=random.randint(30, 60)),
            'Completed_Date': effective_date,
            'Comments': f"Promotion from Level {emp['Job_Level']} to Level {new_level}"
        })

    # 4. Generate LATERAL MOVES (job changes without level change)
    n_lateral = int(len(active_employees) * 0.22)
    lateral_candidates = active_employees.sample(n=min(n_lateral, len(active_employees)))

    for _, emp in lateral_candidates.iterrows():
        new_bu = get_random_business_unit()
        new_div, new_dept, new_team = get_org_structure(new_bu)
        new_profile, _ = get_job_info(emp['Job_Level'])
        effective_date = random_date_in_range(start_date, end_date)

        transactions.append({
            'Transaction_ID': f"JM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Transaction_Type': 'Lateral Move',
            'Transaction_Status': 'Completed',
            'Reason_Code': random.choice(TRANSFER_REASONS),
            'Prior_Job_Profile': emp['Job_Profile'],
            'New_Job_Profile': new_profile,
            'Prior_Job_Level': emp['Job_Level'],
            'New_Job_Level': emp['Job_Level'],
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': new_bu,
            'Prior_Division': emp['Division'],
            'New_Division': new_div,
            'Prior_Department': emp['Department'],
            'New_Department': new_dept,
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': emp['Manager_Employee_ID'],  # May change
            'Prior_Location': emp['Location'],
            'New_Location': emp['Location'],
            'Prior_Worker_Type': emp['Worker_Type'],
            'New_Worker_Type': emp['Worker_Type'],
            'Initiated_By': random.choice(['Employee', 'Manager', 'HR']),
            'Initiated_Date': effective_date - timedelta(days=random.randint(14, 45)),
            'Completed_Date': effective_date,
            'Comments': f"Lateral move to {new_bu}"
        })

    # 5. Generate DEMOTIONS (rare, ~0.5%)
    n_demotions = int(len(active_employees) * 0.005)
    demotion_pool = active_employees[active_employees['Job_Level'] > 1]
    demotion_candidates = demotion_pool.sample(n=min(n_demotions, len(demotion_pool)))

    for _, emp in demotion_candidates.iterrows():
        new_level = emp['Job_Level'] - 1
        new_profile, _ = get_job_info(new_level)
        effective_date = random_date_in_range(start_date, end_date)

        transactions.append({
            'Transaction_ID': f"JM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Transaction_Type': 'Demotion',
            'Transaction_Status': 'Completed',
            'Reason_Code': random.choice(['Performance', 'Restructuring', 'Voluntary Step-Down']),
            'Prior_Job_Profile': emp['Job_Profile'],
            'New_Job_Profile': new_profile,
            'Prior_Job_Level': emp['Job_Level'],
            'New_Job_Level': new_level,
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': emp['Business_Unit'],
            'Prior_Division': emp['Division'],
            'New_Division': emp['Division'],
            'Prior_Department': emp['Department'],
            'New_Department': emp['Department'],
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': emp['Manager_Employee_ID'],
            'Prior_Location': emp['Location'],
            'New_Location': emp['Location'],
            'Prior_Worker_Type': emp['Worker_Type'],
            'New_Worker_Type': emp['Worker_Type'],
            'Initiated_By': 'HR',
            'Initiated_Date': effective_date - timedelta(days=random.randint(14, 30)),
            'Completed_Date': effective_date,
            'Comments': 'Demotion'
        })

    df = pd.DataFrame(transactions)
    print(f"  Generated {len(df)} job movement transactions")
    return df.sort_values(['Employee_ID', 'Effective_Date'])


def generate_compensation_transactions(employees_df, start_date, end_date):
    """Generate compensation change transactions"""
    print("Generating compensation change transactions...")

    transactions = []
    active_employees = employees_df[employees_df['Worker_Status'] == 'Active']

    # 1. Annual merit increases (typically in Q1)
    merit_date = datetime(start_date.year, 4, 1)  # April 1 merit cycle
    if start_date <= merit_date <= end_date:
        merit_eligible = active_employees[active_employees['Worker_Type'] == 'Regular']

        for _, emp in merit_eligible.iterrows():
            # Merit increase based on performance
            perf_multipliers = {
                'Exceptional': (0.06, 0.10),
                'Exceeds Expectations': (0.04, 0.06),
                'Meets Expectations': (0.02, 0.04),
                'Needs Improvement': (0.00, 0.01),
                'Unsatisfactory': (0.00, 0.00)
            }

            perf = emp['Last_Performance_Rating']
            min_inc, max_inc = perf_multipliers.get(perf, (0.02, 0.04))
            merit_pct = random.uniform(min_inc, max_inc)

            new_base = round(emp['Base_Salary'] * (1 + merit_pct), -2)
            new_bonus_amt = round(new_base * emp['Bonus_Target_Percent'], -2)

            transactions.append({
                'Transaction_ID': f"CC{random.randint(1000000, 9999999)}",
                'Employee_ID': emp['Employee_ID'],
                'Worker_ID': emp['Worker_ID'],
                'Effective_Date': merit_date.date(),
                'Transaction_Type': 'Merit Increase',
                'Transaction_Status': 'Completed',
                'Reason_Code': 'Annual Merit Increase',
                'Prior_Base_Salary': emp['Base_Salary'],
                'New_Base_Salary': new_base,
                'Base_Change_Amount': new_base - emp['Base_Salary'],
                'Base_Change_Percent': round(merit_pct * 100, 2),
                'Prior_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
                'New_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
                'Prior_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
                'New_Bonus_Target_Amount': new_bonus_amt,
                'Prior_Annual_Equity': emp['Annual_Equity_Grant'],
                'New_Annual_Equity': emp['Annual_Equity_Grant'],
                'Allowance_Type': None,
                'Allowance_Amount': None,
                'Currency': emp['Currency'],
                'Performance_Rating': perf,
                'Compa_Ratio_Prior': random.uniform(0.85, 1.15),
                'Compa_Ratio_New': random.uniform(0.90, 1.20),
                'Initiated_By': 'Annual Compensation Cycle',
                'Approved_By': 'Compensation Committee',
                'Initiated_Date': (merit_date - timedelta(days=60)).date(),
                'Completed_Date': merit_date.date(),
                'Comments': f"Annual merit increase - {perf}"
            })

    # 2. Promotion-related compensation changes
    n_promo_comp = int(len(active_employees) * 0.30)
    promo_comp_pool = active_employees[active_employees['Job_Level'] < 12]
    promo_comp_candidates = promo_comp_pool.sample(n=min(n_promo_comp, len(promo_comp_pool)))

    for _, emp in promo_comp_candidates.iterrows():
        new_level = emp['Job_Level'] + 1
        new_comp = generate_compensation(new_level, emp['Location'])
        effective_date = random_date_in_range(start_date, end_date)

        transactions.append({
            'Transaction_ID': f"CC{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Transaction_Type': 'Promotion Adjustment',
            'Transaction_Status': 'Completed',
            'Reason_Code': 'Promotion',
            'Prior_Base_Salary': emp['Base_Salary'],
            'New_Base_Salary': new_comp['base_salary'],
            'Base_Change_Amount': new_comp['base_salary'] - emp['Base_Salary'],
            'Base_Change_Percent': round((new_comp['base_salary'] - emp['Base_Salary']) / emp['Base_Salary'] * 100, 2),
            'Prior_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
            'New_Bonus_Target_Percent': new_comp['bonus_target_pct'],
            'Prior_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
            'New_Bonus_Target_Amount': new_comp['bonus_target_amount'],
            'Prior_Annual_Equity': emp['Annual_Equity_Grant'],
            'New_Annual_Equity': new_comp['annual_equity_grant'],
            'Allowance_Type': None,
            'Allowance_Amount': None,
            'Currency': emp['Currency'],
            'Performance_Rating': emp['Last_Performance_Rating'],
            'Compa_Ratio_Prior': random.uniform(0.95, 1.10),
            'Compa_Ratio_New': random.uniform(0.85, 1.00),
            'Initiated_By': 'Manager',
            'Approved_By': 'HR Business Partner',
            'Initiated_Date': effective_date - timedelta(days=random.randint(30, 45)),
            'Completed_Date': effective_date,
            'Comments': f"Promotion to Level {new_level}"
        })

    # 3. Market adjustments
    n_market_adj = int(len(active_employees) * 0.22)
    market_candidates = active_employees.sample(n=min(n_market_adj, len(active_employees)))

    for _, emp in market_candidates.iterrows():
        adj_pct = random.uniform(0.05, 0.15)
        new_base = round(emp['Base_Salary'] * (1 + adj_pct), -2)
        effective_date = random_date_in_range(start_date, end_date)

        transactions.append({
            'Transaction_ID': f"CC{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Transaction_Type': 'Market Adjustment',
            'Transaction_Status': 'Completed',
            'Reason_Code': 'Market Adjustment',
            'Prior_Base_Salary': emp['Base_Salary'],
            'New_Base_Salary': new_base,
            'Base_Change_Amount': new_base - emp['Base_Salary'],
            'Base_Change_Percent': round(adj_pct * 100, 2),
            'Prior_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
            'New_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
            'Prior_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
            'New_Bonus_Target_Amount': round(new_base * emp['Bonus_Target_Percent'], -2),
            'Prior_Annual_Equity': emp['Annual_Equity_Grant'],
            'New_Annual_Equity': emp['Annual_Equity_Grant'],
            'Allowance_Type': None,
            'Allowance_Amount': None,
            'Currency': emp['Currency'],
            'Performance_Rating': emp['Last_Performance_Rating'],
            'Compa_Ratio_Prior': random.uniform(0.75, 0.90),
            'Compa_Ratio_New': random.uniform(0.95, 1.05),
            'Initiated_By': 'Compensation Team',
            'Approved_By': 'HR Director',
            'Initiated_Date': effective_date - timedelta(days=random.randint(14, 30)),
            'Completed_Date': effective_date,
            'Comments': 'Market adjustment based on compensation survey'
        })

    # 4. Equity refresh grants
    n_equity_refresh = int(len(active_employees) * 0.45)
    equity_pool = active_employees[active_employees['Job_Level'] >= 3]
    equity_candidates = equity_pool.sample(n=min(n_equity_refresh, len(equity_pool)))

    for _, emp in equity_candidates.iterrows():
        if emp['Annual_Equity_Grant'] > 0:
            refresh_mult = random.uniform(0.8, 1.5)
            new_equity = round(emp['Annual_Equity_Grant'] * refresh_mult, -3)
            effective_date = random_date_in_range(start_date, end_date)

            transactions.append({
                'Transaction_ID': f"CC{random.randint(1000000, 9999999)}",
                'Employee_ID': emp['Employee_ID'],
                'Worker_ID': emp['Worker_ID'],
                'Effective_Date': effective_date,
                'Transaction_Type': 'Equity Refresh',
                'Transaction_Status': 'Completed',
                'Reason_Code': 'Equity Refresh',
                'Prior_Base_Salary': emp['Base_Salary'],
                'New_Base_Salary': emp['Base_Salary'],
                'Base_Change_Amount': 0,
                'Base_Change_Percent': 0,
                'Prior_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
                'New_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
                'Prior_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
                'New_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
                'Prior_Annual_Equity': emp['Annual_Equity_Grant'],
                'New_Annual_Equity': new_equity,
                'Allowance_Type': None,
                'Allowance_Amount': None,
                'Currency': emp['Currency'],
                'Performance_Rating': emp['Last_Performance_Rating'],
                'Compa_Ratio_Prior': None,
                'Compa_Ratio_New': None,
                'Initiated_By': 'Annual Equity Cycle',
                'Approved_By': 'Compensation Committee',
                'Initiated_Date': effective_date - timedelta(days=random.randint(30, 60)),
                'Completed_Date': effective_date,
                'Comments': 'Annual equity refresh grant'
            })

    # 5. Retention bonuses/adjustments
    n_retention = int(len(active_employees) * 0.18)
    retention_pool = active_employees[
        (active_employees['Job_Level'] >= 4) &
        (active_employees['Last_Performance_Rating'].isin(['Exceptional', 'Exceeds Expectations']))
    ]
    retention_candidates = retention_pool.sample(n=min(n_retention, len(retention_pool)))

    for _, emp in retention_candidates.iterrows():
        effective_date = random_date_in_range(start_date, end_date)
        retention_type = random.choice(['Base Increase', 'Equity Grant', 'Cash Bonus'])

        if retention_type == 'Base Increase':
            adj_pct = random.uniform(0.08, 0.15)
            new_base = round(emp['Base_Salary'] * (1 + adj_pct), -2)
            new_equity = emp['Annual_Equity_Grant']
        elif retention_type == 'Equity Grant':
            adj_pct = 0
            new_base = emp['Base_Salary']
            new_equity = emp['Annual_Equity_Grant'] + random.randint(25000, 100000)
        else:
            adj_pct = 0
            new_base = emp['Base_Salary']
            new_equity = emp['Annual_Equity_Grant']

        transactions.append({
            'Transaction_ID': f"CC{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Transaction_Type': 'Retention',
            'Transaction_Status': 'Completed',
            'Reason_Code': 'Retention',
            'Prior_Base_Salary': emp['Base_Salary'],
            'New_Base_Salary': new_base,
            'Base_Change_Amount': new_base - emp['Base_Salary'],
            'Base_Change_Percent': round(adj_pct * 100, 2),
            'Prior_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
            'New_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
            'Prior_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
            'New_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
            'Prior_Annual_Equity': emp['Annual_Equity_Grant'],
            'New_Annual_Equity': new_equity,
            'Allowance_Type': None,
            'Allowance_Amount': None,
            'Currency': emp['Currency'],
            'Performance_Rating': emp['Last_Performance_Rating'],
            'Compa_Ratio_Prior': None,
            'Compa_Ratio_New': None,
            'Initiated_By': 'Manager',
            'Approved_By': 'SVP HR',
            'Initiated_Date': effective_date - timedelta(days=random.randint(7, 21)),
            'Completed_Date': effective_date,
            'Comments': f'Retention - {retention_type}'
        })

    # 6. Allowance changes
    n_allowance = int(len(active_employees) * 0.25)
    allowance_pool = active_employees[active_employees['Job_Level'] >= 5]
    allowance_candidates = allowance_pool.sample(n=min(n_allowance, len(allowance_pool)))

    for _, emp in allowance_candidates.iterrows():
        effective_date = random_date_in_range(start_date, end_date)
        allowance_type = random.choice(list(ALLOWANCE_TYPES.keys())[:3])  # Exclude one-time allowances
        allowance_amount = round(random.uniform(*ALLOWANCE_TYPES[allowance_type]['amount_range']), -2)

        transactions.append({
            'Transaction_ID': f"CC{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Transaction_Type': 'Allowance Change',
            'Transaction_Status': 'Completed',
            'Reason_Code': 'Allowance Update',
            'Prior_Base_Salary': emp['Base_Salary'],
            'New_Base_Salary': emp['Base_Salary'],
            'Base_Change_Amount': 0,
            'Base_Change_Percent': 0,
            'Prior_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
            'New_Bonus_Target_Percent': emp['Bonus_Target_Percent'],
            'Prior_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
            'New_Bonus_Target_Amount': emp['Bonus_Target_Amount'],
            'Prior_Annual_Equity': emp['Annual_Equity_Grant'],
            'New_Annual_Equity': emp['Annual_Equity_Grant'],
            'Allowance_Type': allowance_type,
            'Allowance_Amount': allowance_amount,
            'Currency': emp['Currency'],
            'Performance_Rating': None,
            'Compa_Ratio_Prior': None,
            'Compa_Ratio_New': None,
            'Initiated_By': 'HR',
            'Approved_By': 'Manager',
            'Initiated_Date': effective_date - timedelta(days=random.randint(7, 14)),
            'Completed_Date': effective_date,
            'Comments': f'New/updated {allowance_type}'
        })

    df = pd.DataFrame(transactions)
    print(f"  Generated {len(df)} compensation transactions")
    return df.sort_values(['Employee_ID', 'Effective_Date'])


def generate_worker_movement_transactions(employees_df, start_date, end_date):
    """Generate worker movement transactions (transfers, relocations, org changes)"""
    print("Generating worker movement transactions...")

    transactions = []
    active_employees = employees_df[employees_df['Worker_Status'] == 'Active']

    # 1. Location transfers/relocations
    n_relocations = int(len(active_employees) * 0.10)
    relocation_candidates = active_employees.sample(n=min(n_relocations, len(active_employees)))

    for _, emp in relocation_candidates.iterrows():
        # Get a different location
        new_location = get_random_location()
        while new_location == emp['Location']:
            new_location = get_random_location()

        effective_date = random_date_in_range(start_date, end_date)

        transactions.append({
            'Transaction_ID': f"WM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Movement_Type': 'Relocation',
            'Movement_Status': 'Completed',
            'Reason_Code': random.choice(['Business Need', 'Employee Request', 'Cost Optimization', 'Office Consolidation']),
            'Prior_Location': emp['Location'],
            'New_Location': new_location,
            'Prior_Country': emp['Country'],
            'New_Country': LOCATIONS[new_location]['country'],
            'Prior_Region': emp['Region'],
            'New_Region': LOCATIONS[new_location]['region'],
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': emp['Business_Unit'],
            'Prior_Division': emp['Division'],
            'New_Division': emp['Division'],
            'Prior_Department': emp['Department'],
            'New_Department': emp['Department'],
            'Prior_Team': emp['Team'],
            'New_Team': emp['Team'],
            'Prior_Cost_Center': emp['Cost_Center'],
            'New_Cost_Center': emp['Cost_Center'],  # May change with relocation
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': emp['Manager_Employee_ID'],
            'Prior_Supervisory_Org': emp['Supervisory_Organization'],
            'New_Supervisory_Org': emp['Supervisory_Organization'],
            'Relocation_Package': random.choice(['Tier 1 - Full', 'Tier 2 - Partial', 'Tier 3 - Lump Sum', 'None']),
            'Remote_Work_Arrangement': random.choice(['Office', 'Hybrid', 'Remote']),
            'Initiated_By': random.choice(['Employee', 'Manager', 'HR']),
            'Approved_By': 'HR Director',
            'Initiated_Date': effective_date - timedelta(days=random.randint(30, 90)),
            'Completed_Date': effective_date,
            'Comments': f"Relocation from {emp['Location']} to {new_location}"
        })

    # 2. Internal transfers (department/team changes)
    n_transfers = int(len(active_employees) * 0.30)
    transfer_candidates = active_employees.sample(n=min(n_transfers, len(active_employees)))

    for _, emp in transfer_candidates.iterrows():
        # May or may not change business unit
        if random.random() < 0.3:
            new_bu = get_random_business_unit()
        else:
            new_bu = emp['Business_Unit']

        new_div, new_dept, new_team = get_org_structure(new_bu)
        new_cost_center = generate_cost_center(new_bu, new_div)
        effective_date = random_date_in_range(start_date, end_date)

        transactions.append({
            'Transaction_ID': f"WM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Movement_Type': 'Internal Transfer',
            'Movement_Status': 'Completed',
            'Reason_Code': random.choice(TRANSFER_REASONS),
            'Prior_Location': emp['Location'],
            'New_Location': emp['Location'],
            'Prior_Country': emp['Country'],
            'New_Country': emp['Country'],
            'Prior_Region': emp['Region'],
            'New_Region': emp['Region'],
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': new_bu,
            'Prior_Division': emp['Division'],
            'New_Division': new_div,
            'Prior_Department': emp['Department'],
            'New_Department': new_dept,
            'Prior_Team': emp['Team'],
            'New_Team': new_team,
            'Prior_Cost_Center': emp['Cost_Center'],
            'New_Cost_Center': new_cost_center,
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': emp['Manager_Employee_ID'],  # Could change
            'Prior_Supervisory_Org': emp['Supervisory_Organization'],
            'New_Supervisory_Org': f"{new_dept} ({new_team})",
            'Relocation_Package': None,
            'Remote_Work_Arrangement': random.choice(['Office', 'Hybrid', 'Remote']),
            'Initiated_By': random.choice(['Employee', 'Manager', 'Internal Job Posting']),
            'Approved_By': 'Hiring Manager',
            'Initiated_Date': effective_date - timedelta(days=random.randint(14, 45)),
            'Completed_Date': effective_date,
            'Comments': f"Transfer from {emp['Department']} to {new_dept}"
        })

    # 3. Manager/reporting line changes
    n_reporting_changes = int(len(active_employees) * 0.35)
    reporting_pool = active_employees[active_employees['Manager_Employee_ID'].notna()]
    reporting_candidates = reporting_pool.sample(n=min(n_reporting_changes, len(reporting_pool)))

    managers_list = employees_df[employees_df['Is_Manager'] == True]['Employee_ID'].tolist()

    for _, emp in reporting_candidates.iterrows():
        new_manager = random.choice(managers_list)
        while new_manager == emp['Manager_Employee_ID'] or new_manager == emp['Employee_ID']:
            new_manager = random.choice(managers_list)

        effective_date = random_date_in_range(start_date, end_date)

        transactions.append({
            'Transaction_ID': f"WM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Movement_Type': 'Reporting Change',
            'Movement_Status': 'Completed',
            'Reason_Code': random.choice(['Manager Change', 'Reorganization', 'Team Restructure', 'Manager Departure']),
            'Prior_Location': emp['Location'],
            'New_Location': emp['Location'],
            'Prior_Country': emp['Country'],
            'New_Country': emp['Country'],
            'Prior_Region': emp['Region'],
            'New_Region': emp['Region'],
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': emp['Business_Unit'],
            'Prior_Division': emp['Division'],
            'New_Division': emp['Division'],
            'Prior_Department': emp['Department'],
            'New_Department': emp['Department'],
            'Prior_Team': emp['Team'],
            'New_Team': emp['Team'],
            'Prior_Cost_Center': emp['Cost_Center'],
            'New_Cost_Center': emp['Cost_Center'],
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': new_manager,
            'Prior_Supervisory_Org': emp['Supervisory_Organization'],
            'New_Supervisory_Org': emp['Supervisory_Organization'],
            'Relocation_Package': None,
            'Remote_Work_Arrangement': None,
            'Initiated_By': 'HR System',
            'Approved_By': 'HR',
            'Initiated_Date': effective_date - timedelta(days=random.randint(1, 14)),
            'Completed_Date': effective_date,
            'Comments': 'Reporting line change'
        })

    # 4. Organization restructuring events
    n_reorg = int(len(active_employees) * 0.20)
    reorg_candidates = active_employees.sample(n=min(n_reorg, len(active_employees)))

    for _, emp in reorg_candidates.iterrows():
        effective_date = random_date_in_range(start_date, end_date)
        new_div, new_dept, new_team = get_org_structure(emp['Business_Unit'])

        transactions.append({
            'Transaction_ID': f"WM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Movement_Type': 'Org Restructure',
            'Movement_Status': 'Completed',
            'Reason_Code': random.choice(['Reorganization', 'Division Merger', 'Department Split', 'Functional Realignment']),
            'Prior_Location': emp['Location'],
            'New_Location': emp['Location'],
            'Prior_Country': emp['Country'],
            'New_Country': emp['Country'],
            'Prior_Region': emp['Region'],
            'New_Region': emp['Region'],
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': emp['Business_Unit'],
            'Prior_Division': emp['Division'],
            'New_Division': new_div,
            'Prior_Department': emp['Department'],
            'New_Department': new_dept,
            'Prior_Team': emp['Team'],
            'New_Team': new_team,
            'Prior_Cost_Center': emp['Cost_Center'],
            'New_Cost_Center': generate_cost_center(emp['Business_Unit'], new_div),
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': emp['Manager_Employee_ID'],
            'Prior_Supervisory_Org': emp['Supervisory_Organization'],
            'New_Supervisory_Org': f"{new_dept} ({new_team})",
            'Relocation_Package': None,
            'Remote_Work_Arrangement': None,
            'Initiated_By': 'Executive Leadership',
            'Approved_By': 'CEO',
            'Initiated_Date': effective_date - timedelta(days=random.randint(60, 120)),
            'Completed_Date': effective_date,
            'Comments': 'Organization restructuring'
        })

    # 5. Remote work arrangement changes
    n_remote = int(len(active_employees) * 0.45)
    remote_candidates = active_employees.sample(n=min(n_remote, len(active_employees)))

    for _, emp in remote_candidates.iterrows():
        effective_date = random_date_in_range(start_date, end_date)
        prior_arrangement = random.choice(['Office', 'Hybrid', 'Remote'])
        new_arrangement = random.choice(['Office', 'Hybrid', 'Remote'])
        while new_arrangement == prior_arrangement:
            new_arrangement = random.choice(['Office', 'Hybrid', 'Remote'])

        transactions.append({
            'Transaction_ID': f"WM{random.randint(1000000, 9999999)}",
            'Employee_ID': emp['Employee_ID'],
            'Worker_ID': emp['Worker_ID'],
            'Effective_Date': effective_date,
            'Movement_Type': 'Work Arrangement Change',
            'Movement_Status': 'Completed',
            'Reason_Code': random.choice(['Employee Request', 'Policy Change', 'Business Need', 'Accommodation']),
            'Prior_Location': emp['Location'],
            'New_Location': emp['Location'],
            'Prior_Country': emp['Country'],
            'New_Country': emp['Country'],
            'Prior_Region': emp['Region'],
            'New_Region': emp['Region'],
            'Prior_Business_Unit': emp['Business_Unit'],
            'New_Business_Unit': emp['Business_Unit'],
            'Prior_Division': emp['Division'],
            'New_Division': emp['Division'],
            'Prior_Department': emp['Department'],
            'New_Department': emp['Department'],
            'Prior_Team': emp['Team'],
            'New_Team': emp['Team'],
            'Prior_Cost_Center': emp['Cost_Center'],
            'New_Cost_Center': emp['Cost_Center'],
            'Prior_Manager_ID': emp['Manager_Employee_ID'],
            'New_Manager_ID': emp['Manager_Employee_ID'],
            'Prior_Supervisory_Org': emp['Supervisory_Organization'],
            'New_Supervisory_Org': emp['Supervisory_Organization'],
            'Relocation_Package': None,
            'Remote_Work_Arrangement': new_arrangement,
            'Initiated_By': random.choice(['Employee', 'Manager', 'HR Policy']),
            'Approved_By': 'Manager',
            'Initiated_Date': effective_date - timedelta(days=random.randint(7, 30)),
            'Completed_Date': effective_date,
            'Comments': f"Work arrangement: {prior_arrangement} → {new_arrangement}"
        })

    df = pd.DataFrame(transactions)
    print(f"  Generated {len(df)} worker movement transactions")
    return df.sort_values(['Employee_ID', 'Effective_Date'])


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main function to generate all HR datasets"""
    print("=" * 70)
    print("WORKDAY HR DATA GENERATOR")
    print("North American Financial Services Organization - 10,000 Employees")
    print("=" * 70)
    print(f"\nTransaction Period: {START_DATE.date()} to {END_DATE.date()}")
    print(f"Target Employees: {TOTAL_EMPLOYEES:,}")
    print()

    # Create output directory
    output_dir = '/sessions/funny-kind-lovelace/mnt/outputs'
    os.makedirs(output_dir, exist_ok=True)

    # 1. Generate base employee data
    print("\n[1/4] GENERATING CORE HR EMPLOYEE DATA")
    print("-" * 50)
    employees_df = generate_base_employees(TOTAL_EMPLOYEES)

    # 2. Generate job movement transactions
    print("\n[2/4] GENERATING JOB MOVEMENT TRANSACTIONS")
    print("-" * 50)
    job_movement_df = generate_job_movement_transactions(employees_df, START_DATE, END_DATE)

    # 3. Generate compensation transactions
    print("\n[3/4] GENERATING COMPENSATION TRANSACTIONS")
    print("-" * 50)
    compensation_df = generate_compensation_transactions(employees_df, START_DATE, END_DATE)

    # 4. Generate worker movement transactions
    print("\n[4/4] GENERATING WORKER MOVEMENT TRANSACTIONS")
    print("-" * 50)
    worker_movement_df = generate_worker_movement_transactions(employees_df, START_DATE, END_DATE)

    # Save all files
    print("\n" + "=" * 70)
    print("SAVING OUTPUT FILES")
    print("=" * 70)

    # Core HR Employees
    core_hr_path = os.path.join(output_dir, 'core_hr_employees.csv')
    employees_df.to_csv(core_hr_path, index=False)
    print(f"\n✓ Core HR Employees: {core_hr_path}")
    print(f"  Records: {len(employees_df):,}")

    # Job Movement Transactions
    job_path = os.path.join(output_dir, 'job_movement_transactions.csv')
    job_movement_df.to_csv(job_path, index=False)
    print(f"\n✓ Job Movement Transactions: {job_path}")
    print(f"  Records: {len(job_movement_df):,}")

    # Compensation Transactions
    comp_path = os.path.join(output_dir, 'compensation_change_transactions.csv')
    compensation_df.to_csv(comp_path, index=False)
    print(f"\n✓ Compensation Transactions: {comp_path}")
    print(f"  Records: {len(compensation_df):,}")

    # Worker Movement Transactions
    movement_path = os.path.join(output_dir, 'worker_movement_transactions.csv')
    worker_movement_df.to_csv(movement_path, index=False)
    print(f"\n✓ Worker Movement Transactions: {movement_path}")
    print(f"  Records: {len(worker_movement_df):,}")

    # Summary statistics
    print("\n" + "=" * 70)
    print("GENERATION SUMMARY")
    print("=" * 70)
    total_transactions = len(job_movement_df) + len(compensation_df) + len(worker_movement_df)
    print(f"\nTotal Employees: {len(employees_df):,}")
    print(f"Total Transactions: {total_transactions:,}")
    print(f"  - Job Movement: {len(job_movement_df):,}")
    print(f"  - Compensation: {len(compensation_df):,}")
    print(f"  - Worker Movement: {len(worker_movement_df):,}")
    print(f"\nAvg Transactions per Employee: {total_transactions/len(employees_df):.1f}")

    # Key field summary
    print("\n" + "-" * 50)
    print("KEY FIELDS (Workday-style)")
    print("-" * 50)
    print("\nCore HR Keys:")
    print("  - Employee_ID (EIN): Primary identifier")
    print("  - Worker_ID: Workday internal ID")

    print("\nTransaction Keys:")
    print("  - Employee_ID + Effective_Date: Composite key")
    print("  - Transaction_ID: Unique transaction identifier")

    print("\nOrg Hierarchy:")
    print("  - Business_Unit → Division → Department → Team")

    print("\n" + "=" * 70)
    print("GENERATION COMPLETE!")
    print("=" * 70)

    return {
        'employees': employees_df,
        'job_movement': job_movement_df,
        'compensation': compensation_df,
        'worker_movement': worker_movement_df
    }


if __name__ == "__main__":
    datasets = main()
