"""
reference_data.py - Generate all reference/dimension source feeds.

Produces: INT6020 (Grade), INT6021 (Job Profile), INT6022 (Job Classification),
          INT6023 (Location), INT6024 (Company), INT6025 (Cost Center),
          INT6027 (Matrix Org), INT6028 (Dept Hierarchy), INT6032 (Positions)

These are generated first since transactional data references them.
"""

import datetime
from typing import List, Dict, Tuple
from . import config
from . import utils


class ReferenceDataGenerator:
    """Generates all reference/dimension source feeds."""

    def __init__(self, rng=None):
        self.rng = rng or utils.get_rng()
        # These get populated during generation and shared with transactional generator
        self.grades = []
        self.job_profiles = []
        self.job_classifications = []
        self.locations = []
        self.companies = []
        self.cost_centers = []
        self.matrix_orgs = []
        self.departments = []   # flat list of all departments
        self.dept_tree = {}     # dept_id -> dept record (with parent, children)
        self.positions = []

    def generate_all(self) -> Dict[str, List[Dict]]:
        """Generate all reference feeds. Returns dict of feed_key -> rows."""
        print("  Generating reference data...")
        self.grades = self._gen_grades()
        print(f"    INT6020 Grade Profiles: {len(self.grades)} rows")

        self.job_profiles = self._gen_job_profiles()
        print(f"    INT6021 Job Profiles: {len(self.job_profiles)} rows")

        self.job_classifications = self._gen_job_classifications()
        print(f"    INT6022 Job Classifications: {len(self.job_classifications)} rows")

        self.locations = self._gen_locations()
        print(f"    INT6023 Locations: {len(self.locations)} rows")

        self.companies = self._gen_companies()
        print(f"    INT6024 Companies: {len(self.companies)} rows")

        self.cost_centers = self._gen_cost_centers()
        print(f"    INT6025 Cost Centers: {len(self.cost_centers)} rows")

        self.matrix_orgs = self._gen_matrix_orgs()
        print(f"    INT6027 Matrix Organizations: {len(self.matrix_orgs)} rows")

        self.departments = self._gen_departments()
        print(f"    INT6028 Department Hierarchy: {len(self.departments)} rows")

        return {
            "INT6020": self.grades,
            "INT6021": self.job_profiles,
            "INT6022": self.job_classifications,
            "INT6023": self.locations,
            "INT6024": self.companies,
            "INT6025": self.cost_centers,
            "INT6027": self.matrix_orgs,
            "INT6028": self.departments,
        }

    # -------------------------------------------------------
    # INT6020 - Grade Profile
    # -------------------------------------------------------
    def _gen_grades(self) -> List[Dict]:
        rows = []
        for g in config.GRADES:
            seg_range = g["max"] - g["min"]
            seg_step = seg_range / g["segments"] if g["segments"] > 0 else seg_range
            row = {
                "Grade_ID": g["id"],
                "Grade_Name": g["name"],
                "Grade_Profile_Currency_Code": g["currency"],
                "Grade_Profile_ID": g["profile_id"],
                "Effective_Date": config.COMPANY_FOUNDED.isoformat(),
                "Grade_Profile_Name": g["profile_name"],
                "Grade_Profile_Number_of_Segements": g["segments"],
                "Grade_Profile_Salary_Range_Maximum": f"{g['max']:.4f}",
                "Grade_Profile_Salary_Range_Midpoint": f"{g['mid']:.4f}",
                "Grade_Profile_Salary_Range_Minimjum": f"{g['min']:.4f}",
            }
            # Add segment tops
            for s in range(1, 6):
                if s <= g["segments"]:
                    seg_top = g["min"] + seg_step * s
                    row[f"Grade_Profile_Segement_{s}_Top"] = f"{seg_top:.4f}"
                else:
                    row[f"Grade_Profile_Segement_{s}_Top"] = ""
            rows.append(row)
        return rows

    # -------------------------------------------------------
    # INT6021 - Job Profile
    # -------------------------------------------------------
    def _gen_job_profiles(self) -> List[Dict]:
        rows = []
        jp_seq = 1

        for func in config.JOB_FUNCTIONS:
            for profile_name in func["profiles"]:
                jp_id = f"JP_{jp_seq:04d}"
                jp_code = f"JPC_{jp_seq:04d}"
                jp_wid = utils.generate_wid(self.rng)

                # Determine grade range for this profile based on function
                if func["code"] == "FN_EXEC":
                    grade_idx = self.rng.randint(12, 14)  # G13-G15
                    mgmt_level = config.MANAGEMENT_LEVELS[-1 if "CEO" in profile_name else -2]
                    is_manager = True
                    is_people_manager = True
                    job_cat_code = "JC_Executive"
                elif "Senior" in profile_name or "Manager" in profile_name or "Director" in profile_name:
                    grade_idx = self.rng.randint(5, 10)  # G06-G11
                    mgmt_level_code = config.GRADE_MGMT_LEVEL.get(f"G{grade_idx+1:02d}", "MLH_Professional")
                    mgmt_level = next((m for m in config.MANAGEMENT_LEVELS if m["code"] == mgmt_level_code), config.MANAGEMENT_LEVELS[0])
                    is_manager = mgmt_level["is_manager"]
                    is_people_manager = "Manager" in profile_name
                    job_cat_code = "JC_People_Manager" if is_people_manager else "JC_Individual_Contributor"
                else:
                    grade_idx = self.rng.randint(0, 5)  # G01-G06
                    mgmt_level = config.MANAGEMENT_LEVELS[0]  # Professional
                    is_manager = False
                    is_people_manager = False
                    job_cat_code = "JC_Individual_Contributor"

                comp_grade = config.GRADES[grade_idx]["id"]

                row = {
                    "Compensation_Grade": comp_grade,
                    "Critical_Job_Flag": self.rng.choice(["Y", "N", "N", "N"]),
                    "Difficult_to_Fill_Flag": self.rng.choice(["Y", "N", "N", "N", "N"]),
                    "Inactive_Flag": "0",
                    "Job_Category_Code": job_cat_code,
                    "Job_Category_Name": next((c["name"] for c in config.JOB_CATEGORIES if c["code"] == job_cat_code), ""),
                    "Job_Exempt_Canada": self.rng.choice(["Exempt", "Non-Exempt"]),
                    "Job_Exempt_US": self.rng.choice(["Exempt", "Non-Exempt"]),
                    "Job_Family": func["code"],
                    "Job_Family_Group": func["family_group"],
                    "Job_Family_Group_Name": func["family_group_name"],
                    "Job_Family_Name": func["name"],
                    "Job_Level_Code": comp_grade,
                    "Job_Level_Name": config.GRADES[grade_idx]["name"],
                    "Job_Profile_Code": jp_code,
                    "Job_Profile_Description": f"Responsible for {profile_name.lower()} functions within {func['name']}.",
                    "Job_Profile_ID": jp_id,
                    "Job_Profile_Name": profile_name,
                    "Job_Profile_Summary": f"{profile_name} - {func['name']}",
                    "Job_Profile_WID": jp_wid,
                    "Job_Title": profile_name,
                    "Management_Level_Code": mgmt_level["code"],
                    "Management_Level_Name": mgmt_level["name"],
                    "Pay_Rate_Type": "Salary",
                    "Public_Job": utils.bool_to_str(True),
                    "Work_Shift_Required": utils.bool_to_str(False),
                    "JOB_MATRIX": func["family_group"],
                    "IS_PEOPLE_MANAGER": utils.bool_to_str(is_people_manager),
                    "IS_MANAGER": utils.bool_to_str(is_manager),
                    "FREQUENCY": "Annual",
                }
                rows.append(row)
                jp_seq += 1

        return rows

    # -------------------------------------------------------
    # INT6022 - Job Classification
    # -------------------------------------------------------
    def _gen_job_classifications(self) -> List[Dict]:
        rows = []
        noc_codes = ["11100", "11101", "11102", "11200", "11201", "12100", "12101",
                      "21110", "21211", "21220", "21230", "21231", "21232", "21310",
                      "41200", "41201", "41400", "51100", "51101", "62100", "64100"]
        eeo_codes = ["1.1", "1.2", "2.1", "2.2", "3.1", "4.1", "5.1", "6.1", "7.1", "8.1", "9.1"]
        soc_codes = ["11-1011", "11-1021", "13-1111", "13-2011", "13-2051", "15-1252",
                      "15-1256", "15-2051", "41-3021", "43-3031", "43-4051"]

        for jp in self.job_profiles:
            row = {
                "Job_Profile_ID": jp["Job_Profile_ID"],
                "Job_Profile_WID": jp["Job_Profile_WID"],
                "AAP_Job_Group": f"AAP_{self.rng.randint(1, 20):02d}",
                "Bonus_Eligibility": self.rng.choice(["Eligible", "Not Eligible", "Eligible", "Eligible"]),
                "Customer_Facing": self.rng.choice(["Y", "N"]),
                "EEO1_Code": self.rng.choice(eeo_codes),
                "Job_Collection": jp["Job_Family_Group_Name"],
                "Loan_Originator_Code": "" if self.rng.random() > 0.05 else f"LO{self.rng.randint(100, 999)}",
                "National_Occupation_Code": self.rng.choice(noc_codes),
                "Occupation_Code": f"OCC_{self.rng.randint(1000, 9999)}",
                "Recruitment_Channel": self.rng.choice(["Internal", "External", "Agency", "Referral"]),
                "Standard_Occupation_Code": self.rng.choice(soc_codes),
                "Stock": self.rng.choice(["Eligible", "Not Eligible", "Eligible"]),
            }
            rows.append(row)
        return rows

    # -------------------------------------------------------
    # INT6023 - Location
    # -------------------------------------------------------
    def _gen_locations(self) -> List[Dict]:
        rows = []
        for loc in config.ALL_LOCATIONS:
            row = {
                "Location_ID": loc["id"],
                "Location_WID": utils.generate_wid(self.rng),
                "Location_Name": loc["name"],
                "Inactive": "0",
                "Address_Line_1": f"{self.rng.randint(1, 999)} {self.rng.choice(['Bay', 'King', 'Queen', 'Front', 'Adelaide', 'Wall', 'Broad', 'State'])} Street",
                "Address_Line_2": self.rng.choice(["", "", "", f"Suite {self.rng.randint(100, 9999)}"]),
                "City": loc["city"],
                "Region": loc["region"],
                "REGION_NAME": loc["region_name"],
                "Country": loc["country"],
                "COUNTRY_NAME": loc["country_name"],
                "Location_Postal_Code": loc["postal"],
                "Location_Identifier": loc["id"],
                "Latitude": f"{loc['lat']:.8f}",
                "Longitude": f"{loc['lng']:.8f}",
                "Location_Type": loc["type"],
                "Location_Usage_Type": loc["usage"],
                "Trade_Name": config.COMPANY_NAME,
                "Worksite_ID_Code": f"WS_{loc['id']}",
            }
            rows.append(row)
        return rows

    # -------------------------------------------------------
    # INT6024 - Company
    # -------------------------------------------------------
    def _gen_companies(self) -> List[Dict]:
        rows = []
        for comp in config.COMPANIES:
            row = {
                "Company_ID": comp["id"],
                "Company_WID": utils.generate_wid(self.rng),
                "Company_Name": comp["name"],
                "Company_Code": comp["code"],
                "Business_Unit": comp["business_unit"],
                "Company_Subtype": comp["subtype"],
                "Company_Currency": comp["currency"],
            }
            rows.append(row)
        return rows

    # -------------------------------------------------------
    # INT6025 - Cost Center
    # -------------------------------------------------------
    def _gen_cost_centers(self) -> List[Dict]:
        rows = []
        cc_seq = 1

        for dept_def in config.TOP_LEVEL_DEPTS:
            # Create cost centers aligned to top-level departments
            n_cc = max(2, int(config.SUB_DEPT_COUNTS.get(dept_def["id"], 5) * 1.5))
            dept_name = dept_def["name"].replace(" Division", "")

            for i in range(n_cc):
                cc_id = f"CC_{cc_seq:04d}"
                cc_code = f"{cc_seq:04d}"
                suffix = self._cc_suffix(dept_name, i)
                row = {
                    "Cost_Center_ID": cc_id,
                    "Cost_Center_WID": utils.generate_wid(self.rng),
                    "Cost_Center_Code": cc_code,
                    "Cost_Center_Name": f"{dept_name} - {suffix}",
                    "Hierarchy": f"{config.COMPANY_NAME} > {dept_name} > {suffix}",
                    "Subtype": "Cost Center",
                }
                rows.append(row)
                cc_seq += 1

        return rows

    def _cc_suffix(self, dept_name: str, idx: int) -> str:
        """Generate a cost center suffix name."""
        suffixes = ["General", "Projects", "Operations", "Support", "Strategy",
                     "Analytics", "Delivery", "Infrastructure", "Advisory",
                     "Compliance", "Trading", "Processing", "Client Services",
                     "Research", "Development", "Planning", "Governance",
                     "Reporting", "Integration", "Transformation"]
        return suffixes[idx % len(suffixes)]

    # -------------------------------------------------------
    # INT6027 - Matrix Organization
    # -------------------------------------------------------
    def _gen_matrix_orgs(self) -> List[Dict]:
        rows = []
        for mo in config.MATRIX_ORGS:
            row = {
                "Matrix_Organization_ID": mo["id"],
                "Matrix_Organization_Status": mo["status"],
                "Maxtrix_Organization_Name": mo["name"],  # Note: typo matches source schema
                "Maxtrix_Organization_Code": mo["code"],
                "Matrix_Organization_Type": mo["type"],
                "Matrix_Organization_SubType": mo["subtype"],
            }
            rows.append(row)
        return rows

    # -------------------------------------------------------
    # INT6028 - Department Hierarchy
    # -------------------------------------------------------
    def _gen_departments(self) -> List[Dict]:
        rows = []
        dept_seq = 1

        # Level 1: Top-level departments
        for dept_def in config.TOP_LEVEL_DEPTS:
            dept_id = dept_def["id"]
            parent_id = dept_def["parent"] if dept_def["parent"] else ""
            wid = utils.generate_wid(self.rng)

            row = {
                "Department_ID": dept_id,
                "Department_WID": wid,
                "Department_Name": dept_def["name"],
                "Dept_Name_with_Manager_Name": dept_def["name"],
                "Active": "1",
                "Parent_Dept_ID": parent_id,
                "Owner_EIN": "",  # Will be assigned when employees are created
                "Department_Level": 1,
                "PRIMARY_LOCATION_CODE": "LOC_TOR_HQ",
                "Type": "Supervisory",
                "Subtype": "Division",
            }
            rows.append(row)
            self.dept_tree[dept_id] = {**row, "children": [], "level": 1}

            # Level 2-4 sub-departments
            n_sub = config.SUB_DEPT_COUNTS.get(dept_id, 5)
            sub_names = self._gen_sub_dept_names(dept_def["name"], n_sub)

            level_2_depts = []
            for i, sub_name in enumerate(sub_names):
                sub_id = f"{dept_id}_{dept_seq:03d}"
                sub_wid = utils.generate_wid(self.rng)

                # ~60% are level 2, ~30% are level 3, ~10% are level 4
                if i < int(n_sub * 0.6):
                    level = 2
                    parent = dept_id
                elif i < int(n_sub * 0.9):
                    level = 3
                    parent = level_2_depts[i % max(1, len(level_2_depts))]["Department_ID"] if level_2_depts else dept_id
                else:
                    level = 4
                    parent = level_2_depts[i % max(1, len(level_2_depts))]["Department_ID"] if level_2_depts else dept_id

                loc = utils.location_for_company(self.rng,
                    self.rng.choice(list(config.COMPANY_COUNTRY.keys())))

                sub_row = {
                    "Department_ID": sub_id,
                    "Department_WID": sub_wid,
                    "Department_Name": sub_name,
                    "Dept_Name_with_Manager_Name": sub_name,
                    "Active": "1",
                    "Parent_Dept_ID": parent,
                    "Owner_EIN": "",
                    "Department_Level": level,
                    "PRIMARY_LOCATION_CODE": loc["id"],
                    "Type": "Supervisory",
                    "Subtype": "Department" if level == 2 else "Team",
                }
                rows.append(sub_row)
                self.dept_tree[sub_id] = {**sub_row, "children": [], "level": level}

                if level == 2:
                    level_2_depts.append(sub_row)

                # Link parent
                if parent in self.dept_tree:
                    self.dept_tree[parent]["children"].append(sub_id)

                dept_seq += 1

        return rows

    def _gen_sub_dept_names(self, division_name: str, count: int) -> List[str]:
        """Generate sub-department names for a division."""
        div_short = division_name.replace(" Division", "").strip()
        suffixes = [
            "Strategy", "Operations", "Analytics", "Delivery", "Support",
            "Advisory", "Governance", "Projects", "Infrastructure", "Client Services",
            "Research", "Development", "Planning", "Integration", "Compliance",
            "Reporting", "Processing", "Trading", "Risk", "Innovation",
            "Quality Assurance", "Architecture", "Engineering", "Data",
            "Transformation",
        ]
        names = []
        for i in range(count):
            suffix = suffixes[i % len(suffixes)]
            names.append(f"{div_short} - {suffix}")
        return names

    # -------------------------------------------------------
    # INT6032 - Positions (generated later, needs employee data)
    # -------------------------------------------------------
    def generate_positions(self, employee_positions: List[Dict]) -> List[Dict]:
        """Generate positions feed from employee assignment data.
        Called after employee timeline is built.
        """
        rows = []
        seen_positions = set()

        for ep in employee_positions:
            pos_id = ep["position_id"]
            if pos_id in seen_positions:
                continue
            seen_positions.add(pos_id)

            row = {
                "Position_ID": pos_id,
                "Supervisory_Organization": ep.get("sup_org_id", ""),
                "Effective_Date": ep.get("effective_date", config.COMPANY_FOUNDED.isoformat()),
                "Reason": ep.get("reason", "New Position"),
                "Worker_Type": ep.get("worker_type", "Employee"),
                "Worker_Sub_Type": ep.get("worker_sub_type", "Regular"),
                "Job_Profile": ep.get("job_profile_id", ""),
                "Job_Title": ep.get("job_title", ""),
                "Business_Title": ep.get("business_title", ""),
                "Time_Type": ep.get("time_type", "Full_Time"),
                "Location": ep.get("location_id", ""),
            }
            rows.append(row)

        self.positions = rows
        print(f"    INT6032 Positions: {len(rows)} rows")
        return rows


# ============================================================
# CSV Field Orders (matching source schema exactly)
# ============================================================

FIELD_ORDERS = {
    "INT6020": [
        "Grade_ID", "Grade_Name", "Grade_Profile_Currency_Code", "Grade_Profile_ID",
        "Effective_Date", "Grade_Profile_Name", "Grade_Profile_Number_of_Segements",
        "Grade_Profile_Salary_Range_Maximum", "Grade_Profile_Salary_Range_Midpoint",
        "Grade_Profile_Salary_Range_Minimjum",
        "Grade_Profile_Segement_1_Top", "Grade_Profile_Segement_2_Top",
        "Grade_Profile_Segement_3_Top", "Grade_Profile_Segement_4_Top",
        "Grade_Profile_Segement_5_Top",
    ],
    "INT6021": [
        "Compensation_Grade", "Critical_Job_Flag", "Difficult_to_Fill_Flag",
        "Inactive_Flag", "Job_Category_Code", "Job_Category_Name",
        "Job_Exempt_Canada", "Job_Exempt_US", "Job_Family", "Job_Family_Group",
        "Job_Family_Group_Name", "Job_Family_Name", "Job_Level_Code", "Job_Level_Name",
        "Job_Profile_Code", "Job_Profile_Description", "Job_Profile_ID",
        "Job_Profile_Name", "Job_Profile_Summary", "Job_Profile_WID",
        "Job_Title", "Management_Level_Code", "Management_Level_Name",
        "Pay_Rate_Type", "Public_Job", "Work_Shift_Required",
        "JOB_MATRIX", "IS_PEOPLE_MANAGER", "IS_MANAGER", "FREQUENCY",
    ],
    "INT6022": [
        "Job_Profile_ID", "Job_Profile_WID", "AAP_Job_Group", "Bonus_Eligibility",
        "Customer_Facing", "EEO1_Code", "Job_Collection", "Loan_Originator_Code",
        "National_Occupation_Code", "Occupation_Code", "Recruitment_Channel",
        "Standard_Occupation_Code", "Stock",
    ],
    "INT6023": [
        "Location_ID", "Location_WID", "Location_Name", "Inactive",
        "Address_Line_1", "Address_Line_2", "City", "Region", "REGION_NAME",
        "Country", "COUNTRY_NAME", "Location_Postal_Code", "Location_Identifier",
        "Latitude", "Longitude", "Location_Type", "Location_Usage_Type",
        "Trade_Name", "Worksite_ID_Code",
    ],
    "INT6024": [
        "Company_ID", "Company_WID", "Company_Name", "Company_Code",
        "Business_Unit", "Company_Subtype", "Company_Currency",
    ],
    "INT6025": [
        "Cost_Center_ID", "Cost_Center_WID", "Cost_Center_Code",
        "Cost_Center_Name", "Hierarchy", "Subtype",
    ],
    "INT6027": [
        "Matrix_Organization_ID", "Matrix_Organization_Status",
        "Maxtrix_Organization_Name", "Maxtrix_Organization_Code",
        "Matrix_Organization_Type", "Matrix_Organization_SubType",
    ],
    "INT6028": [
        "Department_ID", "Department_WID", "Department_Name",
        "Dept_Name_with_Manager_Name", "Active", "Parent_Dept_ID",
        "Owner_EIN", "Department_Level", "PRIMARY_LOCATION_CODE",
        "Type", "Subtype",
    ],
    "INT6032": [
        "Position_ID", "Supervisory_Organization", "Effective_Date",
        "Reason", "Worker_Type", "Worker_Sub_Type", "Job_Profile",
        "Job_Title", "Business_Title", "Time_Type", "Location",
    ],
}
