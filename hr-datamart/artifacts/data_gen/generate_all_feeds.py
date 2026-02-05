#!/usr/bin/env python3
"""
HR Datamart Synthetic Data Generation Script
Generates all INT feeds with referential integrity and realistic data
"""

import csv
import random
from datetime import datetime, timedelta
from decimal import Decimal
import json
import os

# Configuration
SEED = 42
DATA_DATE = datetime.strptime("2026-02-05", "%Y-%m-%d")
TIMESTAMP = "20260205060000"
OUTPUT_DIR = "/sessions/pensive-epic-lamport/mnt/WesBarlow/hr-datamart/output/csv/"
DELIMITER = "|"

# Set random seed for reproducibility
random.seed(SEED)

# Synthetic data pools
FIRST_NAMES = ["John", "Sarah", "Michael", "Emma", "James", "Jessica", "David", "Lisa", "Robert", "Jennifer",
               "William", "Mary", "Richard", "Patricia", "Joseph", "Barbara", "Thomas", "Susan", "Charles", "Jessica"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego",
          "Dallas", "San Jose", "Austin", "Jacksonville", "Denver", "Boston", "Seattle"]
REGIONS = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA", "TX", "FL", "CO", "MA", "WA"]
COUNTRIES = ["US", "CA", "MX", "UK", "DE", "FR", "AU"]
JOB_FAMILIES = ["ENG", "FIN", "HR", "SAL", "OPS", "MKTG", "LEGAL", "IT"]
JOB_LEVELS = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4", "M5"]
JOB_CATEGORIES = ["Individual Contributor", "Manager", "Senior Manager", "Director", "Executive"]
TERMINATION_REASONS = ["Voluntary Resignation", "Retirement", "RIF", "Termination for Cause", "Contract End"]
TERMINATION_CATEGORIES = ["Voluntary", "Involuntary", "Retirement", "Other"]
ORGANIZATION_TYPES = ["Cost Center", "Company", "Supervisory Organization"]
WORK_MODEL_TYPES = ["Office", "Hybrid", "Remote"]
TIME_TYPES = ["Regular", "Temporary", "Seasonal", "Contract"]

class DataGenerator:
    def __init__(self):
        self.grade_profiles = {}
        self.job_profiles = {}
        self.job_classifications = {}
        self.locations = {}
        self.companies = {}
        self.cost_centers = {}
        self.positions = {}
        self.departments = {}
        self.employees = {}
        self.employee_jobs = {}
        self.rescinded_wids = []

    def generate_grade_profiles(self):
        """Generate INT6020 Grade Profile"""
        grades = ["G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8", "G9", "G10"]

        for i, grade_id in enumerate(grades, 1):
            for segment in range(1, 6):
                grade_profile_id = f"GP{i:04d}{segment}"
                min_sal = Decimal(str(50000 + (i - 1) * 20000))
                mid_sal = Decimal(str(65000 + (i - 1) * 25000))
                max_sal = Decimal(str(85000 + (i - 1) * 30000))

                self.grade_profiles[grade_profile_id] = {
                    "Grade_ID": grade_id,
                    "Grade_Name": f"Grade {grade_id}",
                    "Grade_Profile_Currency_Code": "USD",
                    "Grade_Profile_ID": grade_profile_id,
                    "Effective_Date": DATA_DATE.strftime("%Y-%m-%d"),
                    "Grade_Profile_Name": f"Grade {grade_id} - Segment {segment}",
                    "Grade_Profile_Number_of_Segements": segment,
                    "Grade_Profile_Salary_Range_Maximum": max_sal,
                    "Grade_Profile_Salary_Range_Midpoint": mid_sal,
                    "Grade_Profile_Salary_Range_Minimjum": min_sal,
                    "Grade_Profile_Segement_1_Top": Decimal("20000"),
                    "Grade_Profile_Segement_2_Top": Decimal("40000"),
                    "Grade_Profile_Segement_3_Top": Decimal("60000"),
                    "Grade_Profile_Segement_4_Top": Decimal("80000"),
                    "Grade_Profile_Segement_5_Top": Decimal("100000"),
                }

        return list(self.grade_profiles.values())

    def generate_job_profiles(self):
        """Generate INT6021 Job Profile"""
        job_titles = ["Software Engineer", "Data Analyst", "Project Manager", "Product Manager", "Business Analyst",
                     "Accountant", "Finance Manager", "HR Manager", "Sales Executive", "Marketing Manager",
                     "Operations Manager", "Quality Assurance", "System Administrator", "Network Engineer", "Security Analyst"]

        for i in range(200):
            job_family = random.choice(JOB_FAMILIES)
            job_level = random.choice(JOB_LEVELS)
            job_title = random.choice(job_titles)
            is_manager = "M" in job_level
            is_people_manager = is_manager and random.random() > 0.4

            job_profile_id = f"JP{i+1:05d}"

            self.job_profiles[job_profile_id] = {
                "Compensation_Grade": random.choice(list(self.grade_profiles.keys())).split("GP")[1][:2],
                "Critical_Job_Flag": random.choice(["Y", "N"]),
                "Difficult_to_Fill_Flag": random.choice(["Y", "N"]),
                "Inactive_Flag": False,
                "Job_Category_Code": f"CAT{i % 5 + 1}",
                "Job_Category_Name": random.choice(JOB_CATEGORIES),
                "Job_Exempt_Canada": random.choice(["Exempt", "Non-Exempt"]),
                "Job_Exempt_US": random.choice(["Exempt", "Non-Exempt"]),
                "Job_Family": job_family,
                "Job_Family_Group": f"JFG_{job_family}",
                "Job_Family_Group_Name": f"Job Family Group {job_family}",
                "Job_Family_Name": f"Family {job_family}",
                "Job_Level_Code": job_level,
                "Job_Level_Name": f"Level {job_level}",
                "Job_Profile_Code": f"JPC{i+1:04d}",
                "Job_Profile_Description": f"This is the job description for {job_title}. Responsible for key duties and responsibilities.",
                "Job_Profile_ID": job_profile_id,
                "Job_Profile_Name": job_title,
                "Job_Profile_Summary": f"Summary: {job_title} position",
                "Job_Profile_WID": f"WID{i+1:08d}",
                "Job_Title": job_title,
                "Management_Level_Code": f"ML{job_level[0]}",
                "Management_Level_Name": "Manager" if is_manager else "Individual Contributor",
                "Pay_Rate_Type": random.choice(["Salary", "Hourly"]),
                "Public_Job": random.choice([True, False]),
                "Work_Shift_Required": random.choice([True, False]),
                "JOB_MATRIX": f"MATRIX_{job_family}_{job_level}",
                "IS_PEOPLE_MANAGER": is_people_manager,
                "IS_MANAGER": is_manager,
                "FREQUENCY": "Biweekly",
            }

        return list(self.job_profiles.values())

    def generate_job_classifications(self):
        """Generate INT6022 Job Classification"""
        job_class_list = []

        for job_profile_id, job_profile in self.job_profiles.items():
            job_class_list.append({
                "Job_Profile_ID": job_profile_id,
                "Job_Profile_WID": job_profile["Job_Profile_WID"],
                "AAP_Job_Group": random.choice(["Officials and Managers", "Professionals", "Technicians", "Sales", "Administrative", "Service"]),
                "Bonus_Eligibility": random.choice(["Eligible", "Not Eligible", "Partial"]),
                "Customer_Facing": random.choice(["Yes", "No"]),
                "EEO1_Code": f"EEO{random.randint(1, 9)}",
                "Job_Collection": f"Collection_{random.choice(JOB_FAMILIES)}",
                "Loan_Originator_Code": random.choice(["LO001", "LO002", "LO003", ""]),
                "National_Occupation_Code": f"NOC{random.randint(1000, 9999)}",
                "Occupation_Code": f"OCC{random.randint(100, 999)}",
                "Recruitment_Channel": random.choice(["Internal", "External", "Referral", "University"]),
                "Standard_Occupation_Code": f"SOC{random.randint(10, 99)}-{random.randint(1000, 9999)}",
                "Stock": random.choice(["Eligible", "Not Eligible", ""]),
            })

        self.job_classifications = {item["Job_Profile_ID"]: item for item in job_class_list}
        return job_class_list

    def generate_locations(self):
        """Generate INT6023 Location"""
        for i in range(100):
            location_id = f"LOC{i+1:05d}"
            city_idx = i % len(CITIES)
            city = CITIES[city_idx]
            region = REGIONS[city_idx]
            country = "US" if region else random.choice(COUNTRIES)

            self.locations[location_id] = {
                "Location_ID": location_id,
                "Location_WID": f"LWID{i+1:08d}",
                "Location_Name": f"{city} Office",
                "Inactive": "N",
                "Address_Line_1": f"{random.randint(100, 999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm', 'Maple'])} Street",
                "Address_Line_2": f"Suite {random.randint(100, 999)}" if random.random() > 0.5 else "",
                "City": city,
                "Region": region,
                "REGION_NAME": f"{region} Region",
                "Country": country,
                "COUNTRY_NAME": "United States" if country == "US" else f"Country {country}",
                "Location_Postal_Code": f"{random.randint(10000, 99999)}",
                "Location_Identifier": f"LOCID{i+1:04d}",
                "Latitude": Decimal(str(round(random.uniform(25.0, 50.0), 8))),
                "Longitude": Decimal(str(round(random.uniform(-130.0, -65.0), 8))),
                "Location_Type": random.choice(["Office", "Warehouse", "Retail", "Factory"]),
                "Location_Usage_Type": random.choice(["Administrative", "Manufacturing", "Distribution", "Retail"]),
                "Trade_Name": f"Trade {i+1}" if random.random() > 0.7 else "",
                "Worksite_ID_Code": f"WSI{i+1:05d}",
            }

        return list(self.locations.values())

    def generate_companies(self):
        """Generate INT6024 Company"""
        company_names = ["TechCorp Inc", "FinServe LLC", "Global Solutions", "Digital Innovations", "Enterprise Systems",
                        "Capital Management", "Strategic Partners", "Data Systems", "Cloud Services", "Tech Solutions"]

        for i in range(20):
            company_id = f"CMP{i+1:04d}"

            self.companies[company_id] = {
                "Company_ID": company_id,
                "Company_WID": f"CWID{i+1:08d}",
                "Company_Name": company_names[i % len(company_names)],
                "Company_Code": f"CC{i+1:04d}",
                "Business_Unit": f"BU{random.randint(1, 5)}",
                "Company_Subtype": random.choice(["Subsidiary", "Division", "Branch", "Operating Unit"]),
                "Company_Currency": random.choice(["USD", "CAD", "GBP", "EUR"]),
            }

        return list(self.companies.values())

    def generate_cost_centers(self):
        """Generate INT6025 Cost Center"""
        cost_center_names = ["Engineering", "Sales", "Marketing", "Finance", "HR", "Operations", "IT", "Legal",
                            "Facilities", "Procurement", "Quality", "Supply Chain", "Research", "Development"]

        for i in range(100):
            cost_center_id = f"CC{i+1:05d}"

            self.cost_centers[cost_center_id] = {
                "Cost_Center_ID": cost_center_id,
                "Cost_Center_WID": f"CCWID{i+1:08d}",
                "Cost_Center_Code": f"CCC{i+1:04d}",
                "Cost_Center_Name": cost_center_names[i % len(cost_center_names)],
                "Hierarchy": f"Company > Division > Department > {cost_center_names[i % len(cost_center_names)]}",
                "Subtype": random.choice(["Primary", "Secondary", "Cost", "Revenue"]),
            }

        return list(self.cost_centers.values())

    def generate_positions(self):
        """Generate INT6032 Positions"""
        for i in range(600):
            position_id = f"POS{i+1:05d}"
            job_profile_id = random.choice(list(self.job_profiles.keys()))
            job_profile = self.job_profiles[job_profile_id]

            self.positions[position_id] = {
                "Position_ID": position_id,
                "Supervisory_Organization": random.choice(list(self.cost_centers.keys())),
                "Effective_Date": DATA_DATE.strftime("%Y-%m-%d"),
                "Reason": random.choice(["New Position", "Replacement", "Expansion", "Reorganization", "Reclass"]),
                "Worker_Type": random.choice(["Employee", "Contractor", "Intern"]),
                "Worker_Sub_Type": random.choice(TIME_TYPES),
                "Job_Profile": job_profile_id,
                "Job_Title": job_profile["Job_Title"],
                "Business_Title": job_profile["Job_Title"],
                "Time_Type": random.choice(TIME_TYPES),
                "Location": random.choice(list(self.locations.keys())),
            }

        return list(self.positions.values())

    def generate_departments(self):
        """Generate INT6028 Department Hierarchy"""
        dept_names = ["Engineering", "Sales", "Marketing", "Finance", "Human Resources", "Operations", "IT", "Legal",
                     "Facilities", "Procurement", "Quality Assurance", "Supply Chain", "Research & Development"]

        parent_depts = {}
        dept_list = []

        # Create top-level departments
        for i in range(5):
            dept_id = f"DPT{i+1:05d}"
            parent_depts[dept_id] = None

            dept_list.append({
                "Department_ID": dept_id,
                "Department_WID": f"DWID{i+1:08d}",
                "Department_Name": dept_names[i],
                "Dept_Name_with_Manager_Name": f"{dept_names[i]}",
                "Active": True,
                "Parent_Dept_ID": None,
                "Owner_EIN": f"EMP{random.randint(1, 500):05d}",
                "Department_Level": 1,
                "PRIMARY_LOCATION_CODE": random.choice(list(self.locations.keys())),
                "Type": "Department",
                "Subtype": "Business Unit",
            })

        # Create sub-departments
        for i in range(5, len(dept_names)):
            dept_id = f"DPT{i+1:05d}"
            parent_dept_id = random.choice(list(parent_depts.keys()))

            dept_list.append({
                "Department_ID": dept_id,
                "Department_WID": f"DWID{i+1:08d}",
                "Department_Name": dept_names[i],
                "Dept_Name_with_Manager_Name": f"{dept_names[i]}",
                "Active": True,
                "Parent_Dept_ID": parent_dept_id,
                "Owner_EIN": f"EMP{random.randint(1, 500):05d}",
                "Department_Level": 2,
                "PRIMARY_LOCATION_CODE": random.choice(list(self.locations.keys())),
                "Type": "Department",
                "Subtype": "Function",
            })

        # Additional sub-departments
        for i in range(len(dept_list), 200):
            dept_id = f"DPT{i+1:05d}"
            parent_dept_id = random.choice([d["Department_ID"] for d in dept_list])

            dept_list.append({
                "Department_ID": dept_id,
                "Department_WID": f"DWID{i+1:08d}",
                "Department_Name": f"{random.choice(dept_names)} Team {i}",
                "Dept_Name_with_Manager_Name": f"{random.choice(dept_names)} Team {i}",
                "Active": random.random() > 0.1,
                "Parent_Dept_ID": parent_dept_id,
                "Owner_EIN": f"EMP{random.randint(1, 500):05d}",
                "Department_Level": random.randint(2, 3),
                "PRIMARY_LOCATION_CODE": random.choice(list(self.locations.keys())),
                "Type": "Department",
                "Subtype": random.choice(["Function", "Team", "Section"]),
            })

        self.departments = {item["Department_ID"]: item for item in dept_list}
        return dept_list

    def generate_employees(self):
        """Generate synthetic employees"""
        employees = []

        for i in range(1, 501):
            emp_id = f"EMP{i:05d}"
            hire_date = DATA_DATE - timedelta(days=random.randint(30, 3650))

            # Status distribution: 80% active, 5% on leave, 15% terminated
            status_rand = random.random()
            if status_rand < 0.80:
                worker_status = "Active"
                terminated = False
                termination_date = None
                active_status_date = hire_date
            elif status_rand < 0.85:
                worker_status = "On Leave"
                terminated = False
                termination_date = None
                active_status_date = DATA_DATE
            else:
                worker_status = "Terminated"
                terminated = True
                termination_date = DATA_DATE - timedelta(days=random.randint(1, 365))
                active_status_date = termination_date

            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)

            employees.append({
                "emp_id": emp_id,
                "first_name": first_name,
                "last_name": last_name,
                "hire_date": hire_date,
                "worker_status": worker_status,
                "terminated": terminated,
                "termination_date": termination_date,
                "active_status_date": active_status_date,
            })

            self.employees[emp_id] = employees[-1]

        return employees

    def generate_worker_job(self):
        """Generate INT0095E Worker Job"""
        worker_job_list = []

        for emp_id, emp_data in self.employees.items():
            # Generate 2-6 transaction records per employee
            num_transactions = random.randint(2, 6)

            for trans_idx in range(num_transactions):
                transaction_wid = f"TXN{random.randint(100000000, 999999999):09d}"

                if trans_idx == 0:
                    # First transaction is hire
                    trans_eff_date = emp_data["hire_date"]
                    trans_type = "Hire"
                else:
                    # Subsequent transactions are job changes
                    trans_eff_date = emp_data["hire_date"] + timedelta(days=random.randint(90, 1095))
                    trans_type = random.choice(["Transfer", "Promotion", "Demotion", "Lateralove"])

                # Make sure transaction date doesn't exceed termination date
                if emp_data["terminated"] and trans_eff_date > emp_data["termination_date"]:
                    trans_eff_date = emp_data["termination_date"] - timedelta(days=random.randint(1, 90))

                trans_entry_date = trans_eff_date + timedelta(days=random.randint(0, 7))

                job_profile_id = random.choice(list(self.job_profiles.keys()))
                position_id = random.choice(list(self.positions.keys()))
                location = random.choice(list(self.locations.keys()))
                manager_id = random.choice([e for e in self.employees.keys() if e != emp_id])

                # Get job profile info
                job_profile = self.job_profiles[job_profile_id]
                job_title = job_profile["Job_Title"]

                record = {
                    "Employee_ID": emp_id,
                    "Transaction_WID": transaction_wid,
                    "Transaction_Effective_Date": trans_eff_date.strftime("%Y-%m-%d"),
                    "Transaction_Entry_Date": trans_entry_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "Transaction_Type": trans_type,
                    "Position_ID": position_id,
                    "Effective_Date": trans_eff_date.strftime("%Y-%m-%d"),
                    "Worker_Type": "Employee",
                    "Worker_Sub-Type": random.choice(TIME_TYPES),
                    "Business_Title": job_title,
                    "Business_Site_ID": location,
                    "Mailstop_Floor": f"FL{random.randint(1, 5)}" if random.random() > 0.7 else "",
                    "Worker_Status": emp_data["worker_status"],
                    "Active": emp_data["worker_status"] == "Active",
                    "Active_Status_Date": emp_data["active_status_date"].strftime("%Y-%m-%d"),
                    "Hire_Date": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "Original_Hire_Date": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "Hire_Reason": "New Hire" if trans_type == "Hire" else "Transfer",
                    "Employment_End_Date": emp_data["termination_date"].strftime("%Y-%m-%d") if emp_data["terminated"] else "",
                    "Continuous_Service_Date": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "First_Day_of_Work": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "Expected_Retirement_Date": "",
                    "Retirement_Eligibility_Date": "",
                    "Retired": False,
                    "Seniority_Date": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "Severance_Date": "",
                    "Benefits_Service_Date": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "Company_Service_Date": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "Time_Off_Service_Date": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "Vesting_Date": (emp_data["hire_date"] + timedelta(days=365)).strftime("%Y-%m-%d"),
                    "Terminated": emp_data["terminated"],
                    "Termination_Date": emp_data["termination_date"].strftime("%Y-%m-%d") if emp_data["terminated"] else "",
                    "Pay_Through_Date": emp_data["termination_date"].strftime("%Y-%m-%d") if emp_data["terminated"] else "",
                    "Primary_Termination_Reason": random.choice(TERMINATION_REASONS) if emp_data["terminated"] else "",
                    "Primary_Termination_Category": random.choice(TERMINATION_CATEGORIES) if emp_data["terminated"] else "",
                    "Termination_Involuntary": emp_data["terminated"] and random.random() > 0.6,
                    "Secondary_Termination_Reason": "",
                    "Local_Termination_Reason": "",
                    "Not_Eligible_for_Hire": False,
                    "Regrettable_Termination": emp_data["terminated"] and random.random() > 0.7,
                    "Hire_Rescinded": False,
                    "Resignation_Date": emp_data["termination_date"].strftime("%Y-%m-%d") if emp_data["terminated"] and random.random() > 0.6 else "",
                    "Last_Day_of_Work": emp_data["termination_date"].strftime("%Y-%m-%d") if emp_data["terminated"] else "",
                    "Last_Date_for_Which_Paid": emp_data["termination_date"].strftime("%Y-%m-%d") if emp_data["terminated"] else "",
                    "Expected_Date_of_Return": "",
                    "Not_Returning": emp_data["terminated"],
                    "Return_Unknown": "false",
                    "Probation_Start_Date": emp_data["hire_date"].strftime("%Y-%m-%d"),
                    "Probation_End_Date": (emp_data["hire_date"] + timedelta(days=90)).strftime("%Y-%m-%d"),
                    "Academic_Tenure_Date": "",
                    "Has_International_Assignment": random.choice([True, False]),
                    "Home_Country": "US",
                    "Host_Country": random.choice(COUNTRIES),
                    "International_Assignment_Type": random.choice(["Inpatriate", "Expatriate"]) if random.random() > 0.85 else "",
                    "Start_Date_of_International_Assignment": "",
                    "End_Date_of_International_Assignment": "",
                    "Rehire": False,
                    "Eligible_For_Rehire": "Y" if emp_data["terminated"] and random.random() > 0.7 else "N",
                    "Action": trans_type,
                    "Action_Code": f"ACT{random.randint(100, 999)}",
                    "Action_Reason": random.choice(["Business Need", "Employee Request", "Organizational Change"]),
                    "Action_Reason_Code": f"ARC{random.randint(100, 999)}",
                    "Manager_ID": manager_id,
                    "Soft_Retirement_Indicator": False,
                    "Job_Profile_ID": job_profile_id,
                    "Sequence_Number": trans_idx + 1,
                    "Planned_End_Contract_Date": "",
                    "Job_Entry_Dt": trans_eff_date.strftime("%Y-%m-%d"),
                    "Stock_Grants": "",
                    "Time_Type": random.choice(TIME_TYPES),
                    "Supervisory_Organization": random.choice(list(self.cost_centers.keys())),
                    "Location": location,
                    "Job_Title": job_title,
                    "French_Job_Title": f"Titre Français: {job_title}",
                    "Shift_Number": random.randint(1, 3) if random.random() > 0.85 else 1,
                    "Scheduled_Weekly_Hours": Decimal(str(40)) if random.random() > 0.2 else Decimal(str(random.choice([30, 35, 37.5]))),
                    "Default_Weekly_Hours": Decimal(str(40)),
                    "Scheduled_FTE": Decimal(str(1)) if random.random() > 0.15 else Decimal(str(round(random.uniform(0.5, 0.99), 2))),
                    "Work_Model_Start_Date": trans_eff_date.strftime("%Y-%m-%d"),
                    "Work_Model_Type": random.choice(WORK_MODEL_TYPES),
                    "Worker_Workday_ID": f"WID{emp_id[3:]}{trans_idx:02d}",
                }

                worker_job_list.append(record)
                self.employee_jobs[transaction_wid] = record

                # Add some to rescinded list
                if random.random() > 0.98:
                    self.rescinded_wids.append({
                        "workday_id": transaction_wid,
                        "idp_table": "INT095E"
                    })

        return worker_job_list

    def generate_worker_organization(self):
        """Generate INT0096 Worker Organization"""
        worker_org_list = []

        for emp_id in self.employees.keys():
            # Each employee has assignments for Company, Cost Center, and Supervisory Organization
            for org_type in ["Company", "Cost Center", "Supervisory Organization"]:
                transaction_wid = f"TXNO{random.randint(100000000, 999999999):08d}"

                if org_type == "Company":
                    org_id = random.choice(list(self.companies.keys()))
                elif org_type == "Cost Center":
                    org_id = random.choice(list(self.cost_centers.keys()))
                else:  # Supervisory Organization
                    org_id = random.choice(list(self.departments.keys()))

                # Use hire date from employee
                trans_eff_date = self.employees[emp_id]["hire_date"]
                trans_entry_date = trans_eff_date + timedelta(days=random.randint(0, 7))

                record = {
                    "Employee_ID": emp_id,
                    "Transaction_WID": transaction_wid,
                    "Transaction_Effective_Date": trans_eff_date.strftime("%Y-%m-%d"),
                    "Transaction_Entry_Date": trans_entry_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "Transaction_Type": "Org Assignment",
                    "Organization_ID": org_id,
                    "Organization_Type": org_type,
                    "Sequence_Number": 1,
                    "Worker_Workday_ID": f"WID{emp_id[3:]}O{ord(org_type[0])}",
                }

                worker_org_list.append(record)

                # Add some to rescinded list
                if random.random() > 0.98:
                    self.rescinded_wids.append({
                        "workday_id": transaction_wid,
                        "idp_table": "INT096"
                    })

        return worker_org_list

    def generate_worker_compensation(self):
        """Generate INT0098 Worker Compensation"""
        worker_comp_list = []

        for emp_id in self.employees.keys():
            # Generate 2-3 compensation records per employee
            num_comp_records = random.randint(2, 3)

            for comp_idx in range(num_comp_records):
                transaction_wid = f"TXNC{random.randint(100000000, 999999999):08d}"

                # Compensation records are spaced out
                trans_eff_date = self.employees[emp_id]["hire_date"] + timedelta(days=comp_idx * 365)
                trans_entry_date = trans_eff_date + timedelta(days=random.randint(0, 7))

                # Make sure compensation date doesn't exceed current date
                if trans_eff_date > DATA_DATE:
                    trans_eff_date = DATA_DATE

                grade_profile_id = random.choice(list(self.grade_profiles.keys()))
                grade_profile = self.grade_profiles[grade_profile_id]
                grade_id = grade_profile["Grade_ID"]

                base_pay = grade_profile["Grade_Profile_Salary_Range_Midpoint"]
                pay_range_min = grade_profile["Grade_Profile_Salary_Range_Minimjum"]
                pay_range_max = grade_profile["Grade_Profile_Salary_Range_Maximum"]

                record = {
                    "Employee_ID": emp_id,
                    "Transaction_WID": transaction_wid,
                    "Transaction_Effective_Date": trans_eff_date.strftime("%Y-%m-%d"),
                    "Transaction_Entry_Moment": trans_entry_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "Transaction_Type": "Compensation Update" if comp_idx > 0 else "Hire",
                    "Compensation_Package_Proposed": f"PKG{random.randint(1, 10):02d}",
                    "Compensation_Grade_Proposed": grade_id,
                    "Comp_Grade_Profile_Proposed": grade_profile_id,
                    "Compensation_Step_Proposed": f"Step {random.randint(1, 5)}",
                    "Pay_Range_Minimum": int(pay_range_min),
                    "Pay_Range_Midpoint": int(grade_profile["Grade_Profile_Salary_Range_Midpoint"]),
                    "Pay_Range_Maximum": int(pay_range_max),
                    "Base_Pay_Proposed_Amount": int(base_pay) + random.randint(-5000, 15000),
                    "Base_Pay_Proposed_Currency": "USD",
                    "Base_Pay_Proposed_Frequency": "Annual",
                    "Benefits_Annual_Rate_ABBR": int(base_pay) * Decimal("0.08") + random.randint(1000, 5000),
                    "Pay_Rate_Type": "Salary",
                    "Compensation": int(base_pay),
                    "Worker_Workday_ID": f"WID{emp_id[3:]}C{comp_idx:02d}",
                }

                worker_comp_list.append(record)

                # Add some to rescinded list
                if random.random() > 0.98:
                    self.rescinded_wids.append({
                        "workday_id": transaction_wid,
                        "idp_table": "INT098"
                    })

        return worker_comp_list

    def generate_rescinded(self):
        """Generate INT270 Rescinded"""
        rescinded_list = []

        for item in self.rescinded_wids:
            rescinded_list.append({
                "workday_id": item["workday_id"],
                "idp_table": item["idp_table"],
                "rescinded_moment": (DATA_DATE + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S"),
            })

        return rescinded_list

    def write_csv(self, filename, data, headers):
        """Write data to CSV file"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filepath = os.path.join(OUTPUT_DIR, filename)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=DELIMITER, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(data)

        row_count = len(data)
        return filepath, row_count

def main():
    print("Starting HR Datamart synthetic data generation...")
    print(f"Seed: {SEED}")
    print(f"Data Date: {DATA_DATE.strftime('%Y-%m-%d')}")
    print(f"Output Directory: {OUTPUT_DIR}")
    print()

    gen = DataGenerator()

    # Generate all feeds in order
    print("Generating Grade Profiles (INT6020)...")
    grade_profiles = gen.generate_grade_profiles()

    print("Generating Job Profiles (INT6021)...")
    job_profiles = gen.generate_job_profiles()

    print("Generating Job Classifications (INT6022)...")
    job_classifications = gen.generate_job_classifications()

    print("Generating Locations (INT6023)...")
    locations = gen.generate_locations()

    print("Generating Companies (INT6024)...")
    companies = gen.generate_companies()

    print("Generating Cost Centers (INT6025)...")
    cost_centers = gen.generate_cost_centers()

    print("Generating Positions (INT6032)...")
    positions = gen.generate_positions()

    print("Generating Departments (INT6028)...")
    departments = gen.generate_departments()

    print("Generating Employees...")
    employees = gen.generate_employees()

    print("Generating Worker Job (INT0095E)...")
    worker_job = gen.generate_worker_job()

    print("Generating Worker Organization (INT0096)...")
    worker_org = gen.generate_worker_organization()

    print("Generating Worker Compensation (INT0098)...")
    worker_comp = gen.generate_worker_compensation()

    print("Generating Rescinded (INT270)...")
    rescinded = gen.generate_rescinded()

    # Write CSVs
    print()
    print("Writing CSV files...")

    results = {}

    # INT6020
    headers = ["Grade_ID", "Grade_Name", "Grade_Profile_Currency_Code", "Grade_Profile_ID", "Effective_Date",
               "Grade_Profile_Name", "Grade_Profile_Number_of_Segements", "Grade_Profile_Salary_Range_Maximum",
               "Grade_Profile_Salary_Range_Midpoint", "Grade_Profile_Salary_Range_Minimjum",
               "Grade_Profile_Segement_1_Top", "Grade_Profile_Segement_2_Top", "Grade_Profile_Segement_3_Top",
               "Grade_Profile_Segement_4_Top", "Grade_Profile_Segement_5_Top"]
    filepath, count = gen.write_csv("workday.hrdp.dly_grade_profile.full.20260205060000.csv", grade_profiles, headers)
    results["INT6020 Grade Profile"] = (count, filepath)

    # INT6021
    headers = ["Compensation_Grade", "Critical_Job_Flag", "Difficult_to_Fill_Flag", "Inactive_Flag",
               "Job_Category_Code", "Job_Category_Name", "Job_Exempt_Canada", "Job_Exempt_US",
               "Job_Family", "Job_Family_Group", "Job_Family_Group_Name", "Job_Family_Name",
               "Job_Level_Code", "Job_Level_Name", "Job_Profile_Code", "Job_Profile_Description",
               "Job_Profile_ID", "Job_Profile_Name", "Job_Profile_Summary", "Job_Profile_WID",
               "Job_Title", "Management_Level_Code", "Management_Level_Name", "Pay_Rate_Type",
               "Public_Job", "Work_Shift_Required", "JOB_MATRIX", "IS_PEOPLE_MANAGER", "IS_MANAGER", "FREQUENCY"]
    filepath, count = gen.write_csv("workday.hrdp.dly_job_profile.full.20260205060000.csv", job_profiles, headers)
    results["INT6021 Job Profile"] = (count, filepath)

    # INT6022
    headers = ["Job_Profile_ID", "Job_Profile_WID", "AAP_Job_Group", "Bonus_Eligibility", "Customer_Facing",
               "EEO1_Code", "Job_Collection", "Loan_Originator_Code", "National_Occupation_Code",
               "Occupation_Code", "Recruitment_Channel", "Standard_Occupation_Code", "Stock"]
    filepath, count = gen.write_csv("workday.hrdp.dly_job_classification.full.20260205060000.csv", job_classifications, headers)
    results["INT6022 Job Classification"] = (count, filepath)

    # INT6023
    headers = ["Location_ID", "Location_WID", "Location_Name", "Inactive", "Address_Line_1", "Address_Line_2",
               "City", "Region", "REGION_NAME", "Country", "COUNTRY_NAME", "Location_Postal_Code",
               "Location_Identifier", "Latitude", "Longitude", "Location_Type", "Location_Usage_Type",
               "Trade_Name", "Worksite_ID_Code"]
    filepath, count = gen.write_csv("workday.hrdp.dly_location.full.20260205060000.csv", locations, headers)
    results["INT6023 Location"] = (count, filepath)

    # INT6024
    headers = ["Company_ID", "Company_WID", "Company_Name", "Company_Code", "Business_Unit", "Company_Subtype", "Company_Currency"]
    filepath, count = gen.write_csv("workday.hrdp.dly_company.full.20260205060000.csv", companies, headers)
    results["INT6024 Company"] = (count, filepath)

    # INT6025
    headers = ["Cost_Center_ID", "Cost_Center_WID", "Cost_Center_Code", "Cost_Center_Name", "Hierarchy", "Subtype"]
    filepath, count = gen.write_csv("workday.hrdp.dly_cost_center.full.20260205060000.csv", cost_centers, headers)
    results["INT6025 Cost Center"] = (count, filepath)

    # INT6032
    headers = ["Position_ID", "Supervisory_Organization", "Effective_Date", "Reason", "Worker_Type",
               "Worker_Sub_Type", "Job_Profile", "Job_Title", "Business_Title", "Time_Type", "Location"]
    filepath, count = gen.write_csv("workday.hrdp.dly_positions.full.20260205060000.csv", positions, headers)
    results["INT6032 Positions"] = (count, filepath)

    # INT6028
    headers = ["Department_ID", "Department_WID", "Department_Name", "Dept_Name_with_Manager_Name", "Active",
               "Parent_Dept_ID", "Owner_EIN", "Department_Level", "PRIMARY_LOCATION_CODE", "Type", "Subtype"]
    filepath, count = gen.write_csv("workday.hrdp.dly_department_hierarchy.full.20260205060000.csv", departments, headers)
    results["INT6028 Department Hierarchy"] = (count, filepath)

    # INT0095E
    headers = ["Employee_ID", "Transaction_WID", "Transaction_Effective_Date", "Transaction_Entry_Date",
               "Transaction_Type", "Position_ID", "Effective_Date", "Worker_Type", "Worker_Sub-Type",
               "Business_Title", "Business_Site_ID", "Mailstop_Floor", "Worker_Status", "Active",
               "Active_Status_Date", "Hire_Date", "Original_Hire_Date", "Hire_Reason", "Employment_End_Date",
               "Continuous_Service_Date", "First_Day_of_Work", "Expected_Retirement_Date",
               "Retirement_Eligibility_Date", "Retired", "Seniority_Date", "Severance_Date",
               "Benefits_Service_Date", "Company_Service_Date", "Time_Off_Service_Date", "Vesting_Date",
               "Terminated", "Termination_Date", "Pay_Through_Date", "Primary_Termination_Reason",
               "Primary_Termination_Category", "Termination_Involuntary", "Secondary_Termination_Reason",
               "Local_Termination_Reason", "Not_Eligible_for_Hire", "Regrettable_Termination",
               "Hire_Rescinded", "Resignation_Date", "Last_Day_of_Work", "Last_Date_for_Which_Paid",
               "Expected_Date_of_Return", "Not_Returning", "Return_Unknown", "Probation_Start_Date",
               "Probation_End_Date", "Academic_Tenure_Date", "Has_International_Assignment", "Home_Country",
               "Host_Country", "International_Assignment_Type", "Start_Date_of_International_Assignment",
               "End_Date_of_International_Assignment", "Rehire", "Eligible_For_Rehire", "Action",
               "Action_Code", "Action_Reason", "Action_Reason_Code", "Manager_ID", "Soft_Retirement_Indicator",
               "Job_Profile_ID", "Sequence_Number", "Planned_End_Contract_Date", "Job_Entry_Dt",
               "Stock_Grants", "Time_Type", "Supervisory_Organization", "Location", "Job_Title",
               "French_Job_Title", "Shift_Number", "Scheduled_Weekly_Hours", "Default_Weekly_Hours",
               "Scheduled_FTE", "Work_Model_Start_Date", "Work_Model_Type", "Worker_Workday_ID"]
    filepath, count = gen.write_csv("workday.hrdp.dly_worker_job.full.20260205060000.csv", worker_job, headers)
    results["INT0095E Worker Job"] = (count, filepath)

    # INT0096
    headers = ["Employee_ID", "Transaction_WID", "Transaction_Effective_Date", "Transaction_Entry_Date",
               "Transaction_Type", "Organization_ID", "Organization_Type", "Sequence_Number", "Worker_Workday_ID"]
    filepath, count = gen.write_csv("workday.hrdp.dly_worker_organization.full.20260205060000.csv", worker_org, headers)
    results["INT0096 Worker Organization"] = (count, filepath)

    # INT0098
    headers = ["Employee_ID", "Transaction_WID", "Transaction_Effective_Date", "Transaction_Entry_Moment",
               "Transaction_Type", "Compensation_Package_Proposed", "Compensation_Grade_Proposed",
               "Comp_Grade_Profile_Proposed", "Compensation_Step_Proposed", "Pay_Range_Minimum",
               "Pay_Range_Midpoint", "Pay_Range_Maximum", "Base_Pay_Proposed_Amount",
               "Base_Pay_Proposed_Currency", "Base_Pay_Proposed_Frequency", "Benefits_Annual_Rate_ABBR",
               "Pay_Rate_Type", "Compensation", "Worker_Workday_ID"]
    filepath, count = gen.write_csv("workday.hrdp.dly_worker_compensation.full.20260205060000.csv", worker_comp, headers)
    results["INT0098 Worker Compensation"] = (count, filepath)

    # INT270
    headers = ["workday_id", "idp_table", "rescinded_moment"]
    filepath, count = gen.write_csv("workday.hrdp.dly_rescinded.full.20260205060000.csv", rescinded, headers)
    results["INT270 Rescinded"] = (count, filepath)

    # Print results
    print()
    print("=" * 80)
    print("DATA GENERATION COMPLETE")
    print("=" * 80)
    print()
    print("Row counts by feed:")
    print()

    for feed_name, (count, filepath) in sorted(results.items()):
        print(f"{feed_name:<40} {count:>6} rows")

    print()
    print(f"Total rows generated: {sum(count for count, _ in results.values())}")
    print()

if __name__ == "__main__":
    main()
