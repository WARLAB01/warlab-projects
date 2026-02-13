"""
config.py - Master configuration for WARLab HR Datamart V2 synthetic data generation.

All constants, enumerations, growth curves, salary bands, and organizational structure
definitions live here. Seeded RNG ensures full deterministic reproducibility.
"""

import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

# ============================================================
# GLOBAL SEED & DATES
# ============================================================
SEED = 20160213
COMPANY_NAME = "WARLab"
COMPANY_FOUNDED = datetime.date(2016, 2, 13)
DATA_END_DATE = datetime.date(2026, 2, 13)
CALENDAR_START = datetime.date(2020, 1, 1)  # dim_day range start
CALENDAR_END = datetime.date(2030, 12, 31)  # dim_day range end
FISCAL_YEAR_START_MONTH = 11  # Nov 1 = start of fiscal year (typical financial services)

# ============================================================
# HEADCOUNT GROWTH MODEL (monthly targets)
# ============================================================
# Aggressive ramp: reach ~8-9k by end of year 3, stabilize at 10k by year 4-5
# Format: (year, month) -> target_headcount at month end
# We interpolate between these anchor points

GROWTH_ANCHORS = [
    (datetime.date(2016, 2, 28), 150),
    (datetime.date(2016, 6, 30), 600),
    (datetime.date(2016, 12, 31), 2000),
    (datetime.date(2017, 6, 30), 3500),
    (datetime.date(2017, 12, 31), 5000),
    (datetime.date(2018, 6, 30), 6500),
    (datetime.date(2018, 12, 31), 8000),
    (datetime.date(2019, 6, 30), 9000),
    (datetime.date(2019, 12, 31), 9500),
    (datetime.date(2020, 6, 30), 10000),
    (datetime.date(2020, 12, 31), 10000),
    (datetime.date(2021, 12, 31), 10200),
    (datetime.date(2022, 12, 31), 9800),
    (datetime.date(2023, 12, 31), 10100),
    (datetime.date(2024, 12, 31), 10300),
    (datetime.date(2025, 12, 31), 10000),
    (datetime.date(2026, 2, 13), 10050),
]

# ============================================================
# ATTRITION MODEL
# ============================================================
# Annual attrition rates by calendar year (12-18% range)
# Lower early (employees are new/excited), higher as company matures

ANNUAL_ATTRITION_RATE = {
    2016: 0.06,   # Very low in founding year (short tenure, startup energy)
    2017: 0.08,
    2018: 0.10,
    2019: 0.12,
    2020: 0.14,   # Stabilizing - typical FS attrition begins
    2021: 0.15,
    2022: 0.16,   # Great resignation effect
    2023: 0.15,
    2024: 0.14,
    2025: 0.13,
    2026: 0.13,
}

# Termination reason mix (sums to 1.0)
TERM_REASON_MIX = {
    "Voluntary": 0.58,
    "Involuntary": 0.25,
    "Retirement": 0.10,
    "Death": 0.01,
    "End of Contract": 0.06,
}

# Voluntary sub-reasons
VOLUNTARY_REASONS = [
    ("VOL-BETTER_OPP", "Better Opportunity", 0.40),
    ("VOL-RELOCATION", "Relocation", 0.12),
    ("VOL-CAREER_CHG", "Career Change", 0.15),
    ("VOL-COMPENSATION", "Compensation", 0.13),
    ("VOL-PERSONAL", "Personal Reasons", 0.10),
    ("VOL-RETURN_SCHOOL", "Return to School", 0.05),
    ("VOL-OTHER", "Other Voluntary", 0.05),
]

# Involuntary sub-reasons
INVOLUNTARY_REASONS = [
    ("INV-PERFORMANCE", "Performance", 0.40),
    ("INV-RESTRUCTURE", "Restructuring", 0.25),
    ("INV-MISCONDUCT", "Misconduct", 0.15),
    ("INV-POSITION_ELIM", "Position Elimination", 0.12),
    ("INV-OTHER", "Other Involuntary", 0.08),
]

# Regrettable termination rate (% of voluntary terms that are regrettable)
REGRETTABLE_TERM_RATE = 0.35

# ============================================================
# ORGANIZATIONAL STRUCTURE
# ============================================================

# Companies / Legal Entities
COMPANIES = [
    {"id": "WARLAB_HOLD", "name": "WARLab Holdings Inc.", "code": "WLH", "currency": "CAD",
     "business_unit": "Corporate", "subtype": "Holding Company"},
    {"id": "WARLAB_CA", "name": "WARLab Canada Ltd.", "code": "WLC", "currency": "CAD",
     "business_unit": "Canada Operations", "subtype": "Operating Company"},
    {"id": "WARLAB_US", "name": "WARLab Financial Services LLC", "code": "WLU", "currency": "USD",
     "business_unit": "US Operations", "subtype": "Operating Company"},
    {"id": "WARLAB_WM", "name": "WARLab Wealth Management Inc.", "code": "WLW", "currency": "CAD",
     "business_unit": "Wealth Management", "subtype": "Operating Company"},
    {"id": "WARLAB_CAP", "name": "WARLab Capital Markets Inc.", "code": "WLK", "currency": "CAD",
     "business_unit": "Capital Markets", "subtype": "Operating Company"},
    {"id": "WARLAB_INS", "name": "WARLab Insurance Services Ltd.", "code": "WLI", "currency": "CAD",
     "business_unit": "Insurance", "subtype": "Operating Company"},
    {"id": "WARLAB_TECH", "name": "WARLab Technology Solutions Inc.", "code": "WLT", "currency": "CAD",
     "business_unit": "Technology", "subtype": "Shared Services"},
    {"id": "WARLAB_USADV", "name": "WARLab Advisory Services LLC", "code": "WLA", "currency": "USD",
     "business_unit": "US Advisory", "subtype": "Operating Company"},
]

# Company distribution weights (probability an employee is in each entity)
COMPANY_WEIGHTS = {
    "WARLAB_HOLD": 0.02,    # Small corporate HQ
    "WARLAB_CA": 0.25,      # Largest CA entity
    "WARLAB_US": 0.20,      # Largest US entity
    "WARLAB_WM": 0.12,
    "WARLAB_CAP": 0.10,
    "WARLAB_INS": 0.08,
    "WARLAB_TECH": 0.15,    # Shared services - tech is large
    "WARLAB_USADV": 0.08,
}

# Country split by company
COMPANY_COUNTRY = {
    "WARLAB_HOLD": "CA",
    "WARLAB_CA": "CA",
    "WARLAB_US": "US",
    "WARLAB_WM": "CA",
    "WARLAB_CAP": "CA",
    "WARLAB_INS": "CA",
    "WARLAB_TECH": "CA",
    "WARLAB_USADV": "US",
}

# ============================================================
# LOCATIONS
# ============================================================

LOCATIONS_CA = [
    {"id": "LOC_TOR_HQ", "name": "Toronto - Head Office", "city": "Toronto", "region": "ON", "region_name": "Ontario",
     "country": "CA", "country_name": "Canada", "postal": "M5J 2S1", "lat": 43.6426, "lng": -79.3871,
     "type": "Office", "usage": "Business Site", "weight": 0.25},
    {"id": "LOC_TOR_FP", "name": "Toronto - First Canadian Place", "city": "Toronto", "region": "ON", "region_name": "Ontario",
     "country": "CA", "country_name": "Canada", "postal": "M5X 1A1", "lat": 43.6490, "lng": -79.3815,
     "type": "Office", "usage": "Business Site", "weight": 0.08},
    {"id": "LOC_MTL", "name": "Montreal Office", "city": "Montreal", "region": "QC", "region_name": "Quebec",
     "country": "CA", "country_name": "Canada", "postal": "H3B 4W8", "lat": 45.5017, "lng": -73.5673,
     "type": "Office", "usage": "Business Site", "weight": 0.07},
    {"id": "LOC_VAN", "name": "Vancouver Office", "city": "Vancouver", "region": "BC", "region_name": "British Columbia",
     "country": "CA", "country_name": "Canada", "postal": "V6C 3L6", "lat": 49.2827, "lng": -123.1207,
     "type": "Office", "usage": "Business Site", "weight": 0.05},
    {"id": "LOC_CAL", "name": "Calgary Office", "city": "Calgary", "region": "AB", "region_name": "Alberta",
     "country": "CA", "country_name": "Canada", "postal": "T2P 3C5", "lat": 51.0447, "lng": -114.0719,
     "type": "Office", "usage": "Business Site", "weight": 0.04},
    {"id": "LOC_OTT", "name": "Ottawa Office", "city": "Ottawa", "region": "ON", "region_name": "Ontario",
     "country": "CA", "country_name": "Canada", "postal": "K1P 1J9", "lat": 45.4215, "lng": -75.6972,
     "type": "Office", "usage": "Business Site", "weight": 0.03},
    {"id": "LOC_WIN", "name": "Winnipeg Office", "city": "Winnipeg", "region": "MB", "region_name": "Manitoba",
     "country": "CA", "country_name": "Canada", "postal": "R3C 4A5", "lat": 49.8951, "lng": -97.1384,
     "type": "Office", "usage": "Business Site", "weight": 0.02},
    {"id": "LOC_HAL", "name": "Halifax Office", "city": "Halifax", "region": "NS", "region_name": "Nova Scotia",
     "country": "CA", "country_name": "Canada", "postal": "B3J 3N2", "lat": 44.6488, "lng": -63.5752,
     "type": "Office", "usage": "Business Site", "weight": 0.02},
    {"id": "LOC_CA_REM", "name": "Canada - Remote", "city": "Various", "region": "ON", "region_name": "Ontario",
     "country": "CA", "country_name": "Canada", "postal": "M5V 1J2", "lat": 43.6426, "lng": -79.3871,
     "type": "Virtual", "usage": "Remote", "weight": 0.04},
]

LOCATIONS_US = [
    {"id": "LOC_NYC", "name": "New York Office", "city": "New York", "region": "NY", "region_name": "New York",
     "country": "US", "country_name": "United States", "postal": "10005", "lat": 40.7061, "lng": -74.0089,
     "type": "Office", "usage": "Business Site", "weight": 0.12},
    {"id": "LOC_CHI", "name": "Chicago Office", "city": "Chicago", "region": "IL", "region_name": "Illinois",
     "country": "US", "country_name": "United States", "postal": "60606", "lat": 41.8781, "lng": -87.6298,
     "type": "Office", "usage": "Business Site", "weight": 0.06},
    {"id": "LOC_SF", "name": "San Francisco Office", "city": "San Francisco", "region": "CA", "region_name": "California",
     "country": "US", "country_name": "United States", "postal": "94105", "lat": 37.7749, "lng": -122.4194,
     "type": "Office", "usage": "Business Site", "weight": 0.04},
    {"id": "LOC_BOS", "name": "Boston Office", "city": "Boston", "region": "MA", "region_name": "Massachusetts",
     "country": "US", "country_name": "United States", "postal": "02110", "lat": 42.3601, "lng": -71.0589,
     "type": "Office", "usage": "Business Site", "weight": 0.03},
    {"id": "LOC_MIA", "name": "Miami Office", "city": "Miami", "region": "FL", "region_name": "Florida",
     "country": "US", "country_name": "United States", "postal": "33131", "lat": 25.7617, "lng": -80.1918,
     "type": "Office", "usage": "Business Site", "weight": 0.03},
    {"id": "LOC_DAL", "name": "Dallas Office", "city": "Dallas", "region": "TX", "region_name": "Texas",
     "country": "US", "country_name": "United States", "postal": "75201", "lat": 32.7767, "lng": -96.7970,
     "type": "Office", "usage": "Business Site", "weight": 0.02},
    {"id": "LOC_US_REM", "name": "United States - Remote", "city": "Various", "region": "NY", "region_name": "New York",
     "country": "US", "country_name": "United States", "postal": "10001", "lat": 40.7484, "lng": -73.9967,
     "type": "Virtual", "usage": "Remote", "weight": 0.05},
]

ALL_LOCATIONS = LOCATIONS_CA + LOCATIONS_US

# ============================================================
# GRADES & SALARY BANDS (Financial Services)
# ============================================================

GRADES = [
    {"id": "G01", "name": "Analyst I", "profile_id": "GP_01", "profile_name": "Analyst I Grade Profile",
     "currency": "CAD", "min": 42000, "mid": 50000, "max": 58000, "segments": 3},
    {"id": "G02", "name": "Analyst II", "profile_id": "GP_02", "profile_name": "Analyst II Grade Profile",
     "currency": "CAD", "min": 52000, "mid": 62000, "max": 72000, "segments": 3},
    {"id": "G03", "name": "Senior Analyst", "profile_id": "GP_03", "profile_name": "Senior Analyst Grade Profile",
     "currency": "CAD", "min": 65000, "mid": 78000, "max": 91000, "segments": 3},
    {"id": "G04", "name": "Associate", "profile_id": "GP_04", "profile_name": "Associate Grade Profile",
     "currency": "CAD", "min": 78000, "mid": 95000, "max": 112000, "segments": 4},
    {"id": "G05", "name": "Senior Associate", "profile_id": "GP_05", "profile_name": "Senior Associate Grade Profile",
     "currency": "CAD", "min": 92000, "mid": 112000, "max": 132000, "segments": 4},
    {"id": "G06", "name": "Manager", "profile_id": "GP_06", "profile_name": "Manager Grade Profile",
     "currency": "CAD", "min": 105000, "mid": 130000, "max": 155000, "segments": 4},
    {"id": "G07", "name": "Senior Manager", "profile_id": "GP_07", "profile_name": "Senior Manager Grade Profile",
     "currency": "CAD", "min": 125000, "mid": 155000, "max": 185000, "segments": 4},
    {"id": "G08", "name": "Associate Director", "profile_id": "GP_08", "profile_name": "Associate Director Grade Profile",
     "currency": "CAD", "min": 145000, "mid": 180000, "max": 215000, "segments": 5},
    {"id": "G09", "name": "Director", "profile_id": "GP_09", "profile_name": "Director Grade Profile",
     "currency": "CAD", "min": 170000, "mid": 215000, "max": 260000, "segments": 5},
    {"id": "G10", "name": "Senior Director", "profile_id": "GP_10", "profile_name": "Senior Director Grade Profile",
     "currency": "CAD", "min": 200000, "mid": 255000, "max": 310000, "segments": 5},
    {"id": "G11", "name": "Vice President", "profile_id": "GP_11", "profile_name": "Vice President Grade Profile",
     "currency": "CAD", "min": 240000, "mid": 310000, "max": 380000, "segments": 5},
    {"id": "G12", "name": "Senior Vice President", "profile_id": "GP_12", "profile_name": "SVP Grade Profile",
     "currency": "CAD", "min": 300000, "mid": 390000, "max": 480000, "segments": 5},
    {"id": "G13", "name": "Executive Vice President", "profile_id": "GP_13", "profile_name": "EVP Grade Profile",
     "currency": "CAD", "min": 380000, "mid": 480000, "max": 580000, "segments": 5},
    {"id": "G14", "name": "Managing Director", "profile_id": "GP_14", "profile_name": "MD Grade Profile",
     "currency": "CAD", "min": 450000, "mid": 575000, "max": 700000, "segments": 5},
    {"id": "G15", "name": "C-Suite", "profile_id": "GP_15", "profile_name": "C-Suite Grade Profile",
     "currency": "CAD", "min": 550000, "mid": 750000, "max": 950000, "segments": 5},
]

# Grade distribution weights (what % of employees are at each grade)
GRADE_WEIGHTS = {
    "G01": 0.12, "G02": 0.14, "G03": 0.15, "G04": 0.13, "G05": 0.12,
    "G06": 0.10, "G07": 0.08, "G08": 0.05, "G09": 0.04, "G10": 0.03,
    "G11": 0.02, "G12": 0.01, "G13": 0.005, "G14": 0.003, "G15": 0.002,
}

# ============================================================
# JOB PROFILES & FUNCTIONS
# ============================================================

MANAGEMENT_LEVELS = [
    {"code": "MLH_Professional", "name": "Professional", "is_manager": False},
    {"code": "MLH_Management_Hierarchy", "name": "Management Hierarchy", "is_manager": True},
    {"code": "MLH_Management", "name": "Management", "is_manager": True},
    {"code": "MLH_Senior_Management", "name": "Senior Management", "is_manager": True},
    {"code": "MLH_Senior_Leadership", "name": "Senior Leadership", "is_manager": True},
    {"code": "MLH_Executive", "name": "Executive", "is_manager": True},
    {"code": "MLH_Senior_Executive", "name": "Senior Executive", "is_manager": True},
    {"code": "MLH_Group_Head", "name": "Group Head", "is_manager": True},
    {"code": "MLH_CEO", "name": "CEO", "is_manager": True},
]

# Grade -> Management level mapping
GRADE_MGMT_LEVEL = {
    "G01": "MLH_Professional", "G02": "MLH_Professional", "G03": "MLH_Professional",
    "G04": "MLH_Professional", "G05": "MLH_Professional",
    "G06": "MLH_Management", "G07": "MLH_Senior_Management",
    "G08": "MLH_Senior_Management", "G09": "MLH_Senior_Leadership",
    "G10": "MLH_Senior_Leadership", "G11": "MLH_Executive",
    "G12": "MLH_Senior_Executive", "G13": "MLH_Senior_Executive",
    "G14": "MLH_Group_Head", "G15": "MLH_CEO",
}

# Job functions and their profile counts
JOB_FUNCTIONS = [
    {"code": "FN_TECH", "name": "Technology", "family_group": "JFG_TECH", "family_group_name": "Technology",
     "weight": 0.20, "profiles": [
         "Software Engineer", "Senior Software Engineer", "Staff Engineer", "Data Engineer",
         "Data Scientist", "Cloud Architect", "DevOps Engineer", "QA Engineer",
         "IT Support Specialist", "Cybersecurity Analyst", "Solutions Architect",
         "Product Manager - Tech", "Technical Project Manager", "Database Administrator",
         "Network Engineer", "Systems Administrator", "Business Intelligence Analyst",
     ]},
    {"code": "FN_FIN", "name": "Finance", "family_group": "JFG_FIN", "family_group_name": "Finance & Accounting",
     "weight": 0.12, "profiles": [
         "Financial Analyst", "Senior Financial Analyst", "Accountant", "Senior Accountant",
         "Controller", "Treasury Analyst", "Tax Specialist", "Audit Analyst",
         "Financial Planning Analyst", "Accounts Payable Specialist",
     ]},
    {"code": "FN_RISK", "name": "Risk Management", "family_group": "JFG_RISK", "family_group_name": "Risk & Compliance",
     "weight": 0.10, "profiles": [
         "Risk Analyst", "Senior Risk Analyst", "Credit Risk Analyst", "Market Risk Analyst",
         "Operational Risk Analyst", "Compliance Analyst", "AML Analyst",
         "Regulatory Affairs Specialist", "Risk Manager",
     ]},
    {"code": "FN_OPS", "name": "Operations", "family_group": "JFG_OPS", "family_group_name": "Operations",
     "weight": 0.15, "profiles": [
         "Operations Analyst", "Operations Specialist", "Trade Operations Analyst",
         "Settlement Specialist", "Client Services Representative", "Operations Manager",
         "Process Improvement Analyst", "Project Manager",
     ]},
    {"code": "FN_HR", "name": "Human Resources", "family_group": "JFG_HR", "family_group_name": "Human Resources",
     "weight": 0.05, "profiles": [
         "HR Business Partner", "Recruiter", "Senior Recruiter", "Compensation Analyst",
         "Benefits Specialist", "HRIS Analyst", "Learning & Development Specialist",
         "Employee Relations Specialist",
     ]},
    {"code": "FN_LEGAL", "name": "Legal", "family_group": "JFG_LEGAL", "family_group_name": "Legal & Compliance",
     "weight": 0.04, "profiles": [
         "Corporate Counsel", "Paralegal", "Legal Analyst", "Regulatory Counsel",
         "Contract Specialist",
     ]},
    {"code": "FN_SALES", "name": "Sales & Advisory", "family_group": "JFG_SALES", "family_group_name": "Sales & Client Relations",
     "weight": 0.14, "profiles": [
         "Relationship Manager", "Financial Advisor", "Investment Advisor",
         "Sales Associate", "Client Relationship Manager", "Business Development Manager",
         "Private Banker", "Mortgage Specialist", "Insurance Advisor",
         "Wealth Advisor",
     ]},
    {"code": "FN_MKT", "name": "Marketing & Communications", "family_group": "JFG_MKT", "family_group_name": "Marketing",
     "weight": 0.04, "profiles": [
         "Marketing Analyst", "Brand Manager", "Digital Marketing Specialist",
         "Communications Specialist", "Content Writer",
     ]},
    {"code": "FN_EXEC", "name": "Executive", "family_group": "JFG_EXEC", "family_group_name": "Executive Leadership",
     "weight": 0.01, "profiles": [
         "Chief Executive Officer", "Chief Financial Officer", "Chief Technology Officer",
         "Chief Risk Officer", "Chief Operating Officer", "Chief Human Resources Officer",
         "Chief Legal Officer", "Chief Marketing Officer",
     ]},
    {"code": "FN_ADMIN", "name": "Administration", "family_group": "JFG_ADMIN", "family_group_name": "Corporate Services",
     "weight": 0.06, "profiles": [
         "Executive Assistant", "Office Administrator", "Facilities Coordinator",
         "Receptionist", "Administrative Assistant", "Office Manager",
     ]},
    {"code": "FN_INVEST", "name": "Investments", "family_group": "JFG_INVEST", "family_group_name": "Investment Management",
     "weight": 0.09, "profiles": [
         "Portfolio Manager", "Investment Analyst", "Research Analyst",
         "Quantitative Analyst", "Trader", "Fund Accountant",
         "Performance Analyst", "ESG Analyst",
     ]},
]

# Job categories for INT6022
JOB_CATEGORIES = [
    {"code": "JC_People_Manager", "name": "People Manager"},
    {"code": "JC_Individual_Contributor", "name": "Individual Contributor"},
    {"code": "JC_Executive", "name": "Executive"},
]

# ============================================================
# WORKER TYPES
# ============================================================

WORKER_TYPES = [
    {"type": "Employee", "sub_type": "Regular", "weight": 0.88},
    {"type": "Employee", "sub_type": "Fixed Term", "weight": 0.07},
    {"type": "Contingent Worker", "sub_type": "Contractor", "weight": 0.05},
]

# Time types
TIME_TYPES = [
    {"name": "Full_Time", "weight": 0.92, "fte": 1.0, "hours": 37.5},
    {"name": "Part_Time", "weight": 0.08, "fte": 0.5, "hours": 18.75},
]

# ============================================================
# WORK MODEL
# ============================================================

WORK_MODELS = [
    {"type": "On-Site", "weight_pre_2020": 0.85, "weight_post_2020": 0.20},
    {"type": "Remote", "weight_pre_2020": 0.05, "weight_post_2020": 0.35},
    {"type": "Hybrid", "weight_pre_2020": 0.10, "weight_post_2020": 0.45},
]

# ============================================================
# ACTIONS / BUSINESS PROCESSES
# ============================================================

HIRE_ACTIONS = [
    {"action": "Hire", "code": "HIR", "reason": "Hire", "reason_code": "HIR_NEW"},
    {"action": "Hire", "code": "HIR", "reason": "Hire > Rehire", "reason_code": "HIR_REH"},
]

CAREER_ACTIONS = [
    {"action": "Change Job", "code": "CHG_JOB", "reason": "Promotion", "reason_code": "CHG_PROMO", "weight": 0.20},
    {"action": "Change Job", "code": "CHG_JOB", "reason": "Lateral Move", "reason_code": "CHG_LAT", "weight": 0.12},
    {"action": "Change Job", "code": "CHG_JOB", "reason": "Transfer", "reason_code": "CHG_XFER", "weight": 0.10},
    {"action": "Change Job", "code": "CHG_JOB", "reason": "Demotion", "reason_code": "CHG_DEMO", "weight": 0.03},
    {"action": "Data Change", "code": "DAT_CHG", "reason": "Compensation Change", "reason_code": "DAT_COMP", "weight": 0.30},
    {"action": "Data Change", "code": "DAT_CHG", "reason": "Location Change", "reason_code": "DAT_LOC", "weight": 0.05},
    {"action": "Data Change", "code": "DAT_CHG", "reason": "Org Change", "reason_code": "DAT_ORG", "weight": 0.05},
    {"action": "Leave of Absence", "code": "LOA", "reason": "Leave of Absence", "reason_code": "LOA_GEN", "weight": 0.08},
    {"action": "Return from Leave", "code": "RFL", "reason": "Return from Leave", "reason_code": "RFL_GEN", "weight": 0.07},
]

TERMINATION_ACTION = {"action": "Termination", "code": "TER", "reason": "Termination Event", "reason_code": "TER_EVT"}

# ============================================================
# DEMOGRAPHICS (for INT6031 Worker Profile)
# ============================================================

GENDER_DISTRIBUTION = [
    ("Male", 0.52),
    ("Female", 0.45),
    ("Non-Binary", 0.02),
    ("Not Disclosed", 0.01),
]

RACE_ETHNICITY_DISTRIBUTION = [
    ("White", 0.55),
    ("Asian", 0.15),
    ("Black or African American", 0.08),
    ("Hispanic or Latino", 0.07),
    ("South Asian", 0.06),
    ("Indigenous", 0.03),
    ("Two or More Races", 0.03),
    ("Not Disclosed", 0.03),
]

GENERATION_BANDS = [
    ("Baby Boomer", 1946, 1964, 0.12),
    ("Gen X", 1965, 1980, 0.28),
    ("Millennial", 1981, 1996, 0.42),
    ("Gen Z", 1997, 2012, 0.18),
]

# ============================================================
# DEPARTMENT HIERARCHY TEMPLATE
# ============================================================
# Level 1 = top-level divisions, Level 2 = departments, etc.
# Each entry: (dept_id_prefix, name, level, parent_prefix, weight)

TOP_LEVEL_DEPTS = [
    {"id": "DEPT_CEO", "name": "Office of the CEO", "level": 1, "parent": None, "weight": 0.01},
    {"id": "DEPT_FIN", "name": "Finance Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.12},
    {"id": "DEPT_TECH", "name": "Technology Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.20},
    {"id": "DEPT_RISK", "name": "Risk Management Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.10},
    {"id": "DEPT_OPS", "name": "Operations Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.15},
    {"id": "DEPT_HR", "name": "Human Resources Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.05},
    {"id": "DEPT_LEGAL", "name": "Legal Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.04},
    {"id": "DEPT_SALES", "name": "Sales & Advisory Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.14},
    {"id": "DEPT_MKT", "name": "Marketing Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.04},
    {"id": "DEPT_INVEST", "name": "Investment Management Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.09},
    {"id": "DEPT_ADMIN", "name": "Corporate Services Division", "level": 1, "parent": "DEPT_CEO", "weight": 0.06},
]

# Number of sub-departments to generate per top-level division (levels 2-4)
SUB_DEPT_COUNTS = {
    "DEPT_CEO": 3,
    "DEPT_FIN": 12,
    "DEPT_TECH": 25,
    "DEPT_RISK": 15,
    "DEPT_OPS": 20,
    "DEPT_HR": 8,
    "DEPT_LEGAL": 6,
    "DEPT_SALES": 18,
    "DEPT_MKT": 6,
    "DEPT_INVEST": 12,
    "DEPT_ADMIN": 8,
}

# ============================================================
# MATRIX ORGANIZATIONS
# ============================================================

MATRIX_ORGS = [
    {"id": "MORG_DIG_XFORM", "name": "Digital Transformation", "code": "DT", "type": "Project", "subtype": "Strategic", "status": "Active"},
    {"id": "MORG_ESG", "name": "ESG & Sustainability", "code": "ESG", "type": "Committee", "subtype": "Governance", "status": "Active"},
    {"id": "MORG_DEI", "name": "Diversity Equity & Inclusion", "code": "DEI", "type": "Committee", "subtype": "Cultural", "status": "Active"},
    {"id": "MORG_INNOV", "name": "Innovation Lab", "code": "INN", "type": "Project", "subtype": "Strategic", "status": "Active"},
    {"id": "MORG_DATA_GOV", "name": "Data Governance Council", "code": "DGC", "type": "Committee", "subtype": "Governance", "status": "Active"},
    {"id": "MORG_CX", "name": "Client Experience", "code": "CX", "type": "Project", "subtype": "Strategic", "status": "Active"},
    {"id": "MORG_CLOUD", "name": "Cloud Migration", "code": "CLD", "type": "Project", "subtype": "Technology", "status": "Active"},
    {"id": "MORG_REG_CHG", "name": "Regulatory Change", "code": "REG", "type": "Project", "subtype": "Compliance", "status": "Active"},
    {"id": "MORG_MA", "name": "M&A Integration", "code": "MA", "type": "Project", "subtype": "Strategic", "status": "Active"},
    {"id": "MORG_TALENT", "name": "Talent Strategy", "code": "TAL", "type": "Committee", "subtype": "HR", "status": "Active"},
    {"id": "MORG_AI_ML", "name": "AI & Machine Learning", "code": "AI", "type": "Project", "subtype": "Technology", "status": "Active"},
    {"id": "MORG_COST_OPT", "name": "Cost Optimization", "code": "COT", "type": "Project", "subtype": "Finance", "status": "Active"},
    {"id": "MORG_CYBER", "name": "Cybersecurity Task Force", "code": "CYB", "type": "Committee", "subtype": "Technology", "status": "Active"},
    {"id": "MORG_WM_STRAT", "name": "Wealth Management Strategy", "code": "WMS", "type": "Project", "subtype": "Business", "status": "Active"},
    {"id": "MORG_US_EXPAN", "name": "US Expansion", "code": "USE", "type": "Project", "subtype": "Strategic", "status": "Active"},
]

# ============================================================
# EVENT FREQUENCY / CAREER PROGRESSION
# ============================================================

# Average months between career events (promotions, transfers, etc.) per employee
AVG_MONTHS_BETWEEN_EVENTS = 18
# Promotion probability per career event (vs lateral, transfer, etc.)
PROMOTION_PROBABILITY = 0.25
# Annual compensation change probability (separate from promotions)
ANNUAL_COMP_CHANGE_RATE = 0.85  # 85% of employees get annual comp adjustments
# Average annual raise % (base pay)
AVG_ANNUAL_RAISE_PCT = 0.035  # 3.5%
RAISE_STD_DEV = 0.015  # Standard deviation

# Leave of absence probability per year
LOA_ANNUAL_RATE = 0.04  # 4% of employees per year
AVG_LOA_DAYS = 90  # Average leave duration

# Internal hire (job application) rate as % of all hires
INTERNAL_HIRE_RATE = 0.12

# Rescind rate
RESCIND_RATE = 0.015  # 1.5% of transactions get rescinded

# Rehire rate (% of terminated employees who return)
REHIRE_RATE = 0.04  # 4% of terms come back

# ============================================================
# CSV OUTPUT CONFIG
# ============================================================

OUTPUT_DIR = "data/feeds"
FEED_TIMESTAMP = "20260213120000"  # Fixed timestamp for file naming

FEED_FILE_MAP = {
    "INT6020": "workday.hrdp.dly_grade_profile.full",
    "INT6021": "workday.hrdp.dly_job_profile.full",
    "INT6022": "workday.hrdp.dly_job_classification.full",
    "INT6023": "workday.hrdp.dly_location.full",
    "INT6024": "workday.hrdp.dly_company.full",
    "INT6025": "workday.hrdp.dly_cost_center.full",
    "INT6027": "workday.hrdp.dly_matrix_organization.full",
    "INT6028": "workday.hrdp.dly_department_hierarchy.full",
    "INT6031": "workday.hrdp.dly_worker_profile.full",
    "INT6032": "workday.hrdp.dly_positions.full",
    "INT0095E": "workday.hrdp.dly_worker_job.full",
    "INT0096": "workday.hrdp.dly_worker_organization.full",
    "INT0098": "workday.hrdp.dly_worker_compensation.full",
    "INT270": "workday.hrdp.dly_rescinded_transactions.full",
}
