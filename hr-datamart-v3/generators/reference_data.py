"""
reference_data.py - Generate all reference/dimension source feeds.

V3 changes:
  CR1  - INT6021: Field order resequenced (Job_Profile_ID, Job_Profile_WID first)
  CR2  - INT6022: Normalized schema — parent/child structure with Job_Classification_ID PK
  CR3  - INT6027: Added Matrix_Organization_Description field
  CR4  - INT6028: Added Owner_EIN_WID field (varchar 32, references Worker_Workday_ID)
  INT6022/CR: JOB_CLASSIFICATION_GROUPS replaced with 11 Workday-standard groups;
              _gen_job_classifications() now emits N×11 rows (one per job profile per group)
  INT6032/CR: generate_positions() adds Work_Space, Pay_Rate_Type, Schedule_Weekly_Hours,
              Scheduled_FTE, Default_Weekly_Hours, Employee_Type, shift_number,
              Exclude_From_Headcount
"""

import datetime
from typing import List, Dict, Tuple
from . import config
from . import utils


class ReferenceDataGenerator:
    """Generates all reference/dimension source feeds."""

    def __init__(self, rng=None):
        self.rng = rng or utils.get_rng()
        self.grades = []
        self.job_profiles = []
        self.job_classifications = []
        self.locations = []
        self.companies = []
        self.cost_centers = []
        self.matrix_orgs = []
        self.departments = []
        self.dept_tree = {}
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
    # INT6020 - Grade Profile  (unchanged from v2)
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
    # CR1: Field order resequenced — Job_Profile_ID, Job_Profile_WID now lead
    #      (data content unchanged; FIELD_ORDERS drives column sequence)
    # -------------------------------------------------------
    def _gen_job_profiles(self) -> List[Dict]:
        rows = []
        jp_seq = 1

        for func in config.JOB_FUNCTIONS:
            for profile_name in func["profiles"]:
                jp_id = f"JP_{jp_seq:04d}"
                jp_code = f"JPC_{jp_seq:04d}"
                jp_wid = utils.generate_wid(self.rng)

                if func["code"] == "FN_EXEC":
                    grade_idx = self.rng.randint(12, 14)
                    mgmt_level = config.MANAGEMENT_LEVELS[-1 if "CEO" in profile_name else -2]
                    is_manager = True
                    is_people_manager = True
                    job_cat_code = "JC_Executive"
                elif "Senior" in profile_name or "Manager" in profile_name or "Director" in profile_name:
                    grade_idx = self.rng.randint(5, 10)
                    mgmt_level_code = config.GRADE_MGMT_LEVEL.get(f"G{grade_idx+1:02d}", "MLH_Professional")
                    mgmt_level = next((m for m in config.MANAGEMENT_LEVELS if m["code"] == mgmt_level_code), config.MANAGEMENT_LEVELS[0])
                    is_manager = mgmt_level["is_manager"]
                    is_people_manager = "Manager" in profile_name
                    job_cat_code = "JC_People_Manager" if is_people_manager else "JC_Individual_Contributor"
                else:
                    grade_idx = self.rng.randint(0, 5)
                    mgmt_level = config.MANAGEMENT_LEVELS[0]
                    is_manager = False
                    is_people_manager = False
                    job_cat_code = "JC_Individual_Contributor"

                comp_grade = config.GRADES[grade_idx]["id"]

                row = {
                    "Job_Profile_ID": jp_id,
                    "Job_Profile_WID": jp_wid,
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
                    "Job_Profile_Name": profile_name,
                    "Job_Profile_Summary": f"{profile_name} - {func['name']}",
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
    # CR2: Normalized schema — parent/child structure
    #      New fields: Job_Classification_ID (PK), Job_Classification_WID,
    #                  Job_Classification_Name, Job_Classification_Group_ID,
    #                  Job_Classification_Group_Name, Job_Profile_ID
    # INT6022/CR refinement: Each group has 2–6 domain values; each job
    #   profile is assigned to exactly ONE domain value per group.
    #   Produces N_profiles × 11 rows (same count as before).
    # -------------------------------------------------------
    def _gen_job_classifications(self) -> List[Dict]:
        rows = []
        jcl_seq = 1

        for jp in self.job_profiles:
            for grp in config.JOB_CLASSIFICATION_GROUPS:
                jcl_id = f"JCL_{jcl_seq:04d}"
                jcl_wid = utils.generate_wid(self.rng)
                jcl_name = self._pick_classification(jp, grp["id"])

                row = {
                    "Job_Classification_ID": jcl_id,
                    "Job_Classification_WID": jcl_wid,
                    "Job_Classification_Name": jcl_name,
                    "Job_Classification_Group_ID": grp["id"],
                    "Job_Classification_Group_Name": grp["name"],
                    "Job_Profile_ID": jp["Job_Profile_ID"],
                }
                rows.append(row)
                jcl_seq += 1

        return rows

    def _pick_classification(self, jp: Dict, grp_id: str) -> str:
        """Return the single domain value for a (job_profile, group) pair.

        Uses deterministic sequence-based selection (no rng state consumed)
        so downstream generators produce identical output to earlier runs.
        """
        # Profile sequence number drives variation (JP_0003 → seq=3)
        seq = int(jp["Job_Profile_ID"].split("_")[1])

        fn = jp["Job_Family"]
        name = jp["Job_Profile_Name"]
        is_exec  = fn == "FN_EXEC"
        is_mgr   = jp.get("IS_MANAGER", "0") == "1"
        is_sales = fn == "FN_SALES"
        is_admin = fn == "FN_ADMIN"
        is_tech  = fn == "FN_TECH"
        is_fin   = fn in ("FN_FIN", "FN_INVEST")
        is_risk  = fn in ("FN_RISK", "FN_LEGAL")
        client_kw = ("Advisor", "Relationship", "Client", "Banker",
                     "Mortgage", "Insurance", "Wealth")

        if grp_id == "JCL_GRP_AAP":
            if is_exec or (is_mgr and fn not in ("FN_TECH", "FN_FIN")):
                return "Officials and Managers"
            if is_sales:
                return "Sales Workers"
            if is_admin:
                return "Administrative Support"
            if is_tech:
                return ["Professionals", "Technicians"][seq % 2]
            return "Professionals"

        if grp_id == "JCL_GRP_BONUS":
            if is_exec or is_mgr:
                return "Eligible"
            if is_admin:
                return ["Not Eligible", "Not Eligible", "Eligible"][seq % 3]
            return ["Eligible", "Eligible", "Not Eligible", "Discretionary"][seq % 4]

        if grp_id == "JCL_GRP_CUST":
            if is_sales or any(k in name for k in client_kw):
                return "Yes"
            return "No"

        if grp_id == "JCL_GRP_EEO1":
            if is_exec:
                return "Exec/Sr Officials & Mgrs"
            if is_mgr:
                return "First/Mid Officials & Mgrs"
            if is_sales:
                return "Sales Workers"
            if is_admin:
                return "Administrative Support"
            if is_tech:
                return ["Professionals", "Technicians"][seq % 2]
            return "Professionals"

        if grp_id == "JCL_GRP_COLL":
            mapping = {
                "FN_TECH":   "Technology",
                "FN_FIN":    "Finance",
                "FN_INVEST": "Finance",
                "FN_RISK":   "Risk & Compliance",
                "FN_LEGAL":  "Risk & Compliance",
                "FN_SALES":  "Commercial",
                "FN_MKT":    "Commercial",
                "FN_OPS":    "Corporate",
                "FN_HR":     "Corporate",
                "FN_EXEC":   "Corporate",
                "FN_ADMIN":  "Corporate",
            }
            return mapping.get(fn, "Corporate")

        if grp_id == "JCL_GRP_LOAN":
            loan_kw = ("Mortgage", "Banker", "Financial Advisor",
                       "Investment Advisor", "Wealth Advisor")
            if fn == "FN_SALES" and any(k in name for k in loan_kw):
                return "Registered"
            if is_exec:
                return "Exempt"
            return "Not Registered"

        if grp_id == "JCL_GRP_NOC":
            if is_exec or (is_mgr and fn not in ("FN_TECH", "FN_FIN")):
                return "NOC 0 - Management"
            if is_fin:
                return "NOC 1 - Business, Finance & Administration"
            if is_tech:
                return "NOC 2 - Natural & Applied Sciences"
            if is_risk or fn == "FN_HR":
                return "NOC 4 - Education, Law & Social Services"
            if is_sales:
                return "NOC 6 - Sales & Service"
            return "NOC 1 - Business, Finance & Administration"

        if grp_id == "JCL_GRP_OCC":
            mapping = {
                "FN_TECH":   "Technology & Engineering",
                "FN_FIN":    "Finance & Accounting",
                "FN_INVEST": "Finance & Accounting",
                "FN_RISK":   "Risk & Compliance",
                "FN_LEGAL":  "Risk & Compliance",
                "FN_OPS":    "Operations & Support",
                "FN_SALES":  "Commercial & Sales",
                "FN_MKT":    "Commercial & Sales",
                "FN_HR":     "Corporate Functions",
                "FN_EXEC":   "Corporate Functions",
                "FN_ADMIN":  "Corporate Functions",
            }
            return mapping.get(fn, "Corporate Functions")

        if grp_id == "JCL_GRP_RECR":
            options = [
                "Internal Transfer",
                "External Job Site", "External Job Site",
                "Employee Referral",
                "Recruiting Fair",
                "Direct Sourcing",
            ]
            return options[seq % len(options)]

        if grp_id == "JCL_GRP_SOC":
            if is_exec or (is_mgr and fn == "FN_EXEC"):
                return "11-0000 Management"
            if is_fin or is_risk:
                return "13-0000 Business & Financial Operations"
            if is_tech:
                return "15-0000 Computer & Mathematical"
            if is_sales:
                return "41-0000 Sales & Related"
            return "13-0000 Business & Financial Operations"

        if grp_id == "JCL_GRP_STOCK":
            if is_exec:
                return "Restricted"
            if is_mgr or fn in ("FN_TECH", "FN_FIN", "FN_INVEST", "FN_SALES"):
                return "Eligible"
            return ["Eligible", "Not Eligible", "Not Eligible"][seq % 3]

        # Fallback: sequence-based pick from domain values
        vals = config.JOB_CLASSIFICATION_DOMAIN_VALUES[grp_id]
        return vals[seq % len(vals)]

    # -------------------------------------------------------
    # INT6023 - Location  (unchanged from v2)
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
    # INT6024 - Company  (unchanged from v2)
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
    # INT6025 - Cost Center  (unchanged from v2)
    # -------------------------------------------------------
    def _gen_cost_centers(self) -> List[Dict]:
        rows = []
        cc_seq = 1

        for dept_def in config.TOP_LEVEL_DEPTS:
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
        suffixes = ["General", "Projects", "Operations", "Support", "Strategy",
                     "Analytics", "Delivery", "Infrastructure", "Advisory",
                     "Compliance", "Trading", "Processing", "Client Services",
                     "Research", "Development", "Planning", "Governance",
                     "Reporting", "Integration", "Transformation"]
        return suffixes[idx % len(suffixes)]

    # -------------------------------------------------------
    # INT6027 - Matrix Organization
    # CR3: Added Matrix_Organization_Description (varchar 200)
    # -------------------------------------------------------
    def _gen_matrix_orgs(self) -> List[Dict]:
        rows = []
        for mo in config.MATRIX_ORGS:
            row = {
                "Matrix_Organization_ID": mo["id"],
                "Matrix_Organization_Status": mo["status"],
                "Maxtrix_Organization_Name": mo["name"],   # typo preserved from source schema
                "Maxtrix_Organization_Code": mo["code"],   # typo preserved from source schema
                "Matrix_Organization_Type": mo["type"],
                "Matrix_Organization_SubType": mo["subtype"],
                "Matrix_Organization_Description": mo.get("description", ""),  # CR3 new field
            }
            rows.append(row)
        return rows

    # -------------------------------------------------------
    # INT6028 - Department Hierarchy
    # CR4: Added Owner_EIN_WID (varchar 32, references Worker_Workday_ID)
    #      Full field order: Dept_ID, Dept_WID, Dept_Name, Dept_Name_with_Mgr,
    #      Active, Parent_Dept_ID, Owner_EIN, Owner_EIN_WID, Dept_Level,
    #      PRIMARY_LOCATION_CODE, Type, Subtype
    # -------------------------------------------------------
    def _gen_departments(self) -> List[Dict]:
        rows = []
        dept_seq = 1

        for dept_def in config.TOP_LEVEL_DEPTS:
            dept_id = dept_def["id"]
            parent_id = dept_def["parent"] if dept_def["parent"] else ""
            wid = utils.generate_wid(self.rng)
            # Owner_EIN_WID: generate a placeholder WID (will reference a Worker_Workday_ID
            # once employee data is loaded; left as generated synthetic ID for reference feeds)
            owner_ein_wid = utils.generate_wid(self.rng)

            row = {
                "Department_ID": dept_id,
                "Department_WID": wid,
                "Department_Name": dept_def["name"],
                "Dept_Name_with_Manager_Name": dept_def["name"],
                "Active": "1",
                "Parent_Dept_ID": parent_id,
                "Owner_EIN": "",
                "Owner_EIN_WID": owner_ein_wid,   # CR4 new field
                "Department_Level": 1,
                "PRIMARY_LOCATION_CODE": "LOC_TOR_HQ",
                "Type": "Supervisory",
                "Subtype": "Division",
            }
            rows.append(row)
            self.dept_tree[dept_id] = {**row, "children": [], "level": 1}

            n_sub = config.SUB_DEPT_COUNTS.get(dept_id, 5)
            sub_names = self._gen_sub_dept_names(dept_def["name"], n_sub)

            level_2_depts = []
            for i, sub_name in enumerate(sub_names):
                sub_id = f"{dept_id}_{dept_seq:03d}"
                sub_wid = utils.generate_wid(self.rng)
                sub_owner_ein_wid = utils.generate_wid(self.rng)

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
                    "Owner_EIN_WID": sub_owner_ein_wid,   # CR4 new field
                    "Department_Level": level,
                    "PRIMARY_LOCATION_CODE": loc["id"],
                    "Type": "Supervisory",
                    "Subtype": "Department" if level == 2 else "Team",
                }
                rows.append(sub_row)
                self.dept_tree[sub_id] = {**sub_row, "children": [], "level": level}

                if level == 2:
                    level_2_depts.append(sub_row)

                if parent in self.dept_tree:
                    self.dept_tree[parent]["children"].append(sub_id)

                dept_seq += 1

        return rows

    def _gen_sub_dept_names(self, division_name: str, count: int) -> List[str]:
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
    # INT6032/CR: Added Work_Space (varchar, nullable), Pay_Rate_Type (varchar),
    #             Schedule_Weekly_Hours (numeric), Scheduled_FTE (numeric),
    #             Default_Weekly_Hours (numeric), Employee_Type (varchar),
    #             shift_number (integer), Exclude_From_Headcount (boolean/varchar, nullable)
    # -------------------------------------------------------
    def generate_positions(self, employee_positions: List[Dict]) -> List[Dict]:
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
                "Work_Space": ep.get("work_space", ""),
                "Pay_Rate_Type": ep.get("pay_rate_type", "Salary"),
                "Schedule_Weekly_Hours": ep.get("scheduled_weekly_hours", 37.5),
                "Scheduled_FTE": ep.get("scheduled_fte", 1.0),
                "Default_Weekly_Hours": ep.get("default_weekly_hours", 37.5),
                "Employee_Type": ep.get("employee_type", "Regular"),
                "shift_number": ep.get("shift_number", 0),
                "Exclude_From_Headcount": ep.get("exclude_from_headcount", ""),
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
    # CR1: Job_Profile_ID and Job_Profile_WID moved to first two positions
    "INT6021": [
        "Job_Profile_ID", "Job_Profile_WID",
        "Compensation_Grade", "Critical_Job_Flag", "Difficult_to_Fill_Flag",
        "Inactive_Flag", "Job_Category_Code", "Job_Category_Name",
        "Job_Exempt_Canada", "Job_Exempt_US", "Job_Family", "Job_Family_Group",
        "Job_Family_Group_Name", "Job_Family_Name", "Job_Level_Code", "Job_Level_Name",
        "Job_Profile_Code", "Job_Profile_Description", "Job_Profile_Name",
        "Job_Profile_Summary", "Job_Title", "Management_Level_Code", "Management_Level_Name",
        "Pay_Rate_Type", "Public_Job", "Work_Shift_Required",
        "JOB_MATRIX", "IS_PEOPLE_MANAGER", "IS_MANAGER", "FREQUENCY",
    ],
    # CR2: Normalized schema — six fields replacing prior flat classification fields
    "INT6022": [
        "Job_Classification_ID",
        "Job_Classification_WID",
        "Job_Classification_Name",
        "Job_Classification_Group_ID",
        "Job_Classification_Group_Name",
        "Job_Profile_ID",
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
    # CR3: Matrix_Organization_Description appended
    "INT6027": [
        "Matrix_Organization_ID", "Matrix_Organization_Status",
        "Maxtrix_Organization_Name", "Maxtrix_Organization_Code",
        "Matrix_Organization_Type", "Matrix_Organization_SubType",
        "Matrix_Organization_Description",
    ],
    # CR4: Owner_EIN_WID inserted after Owner_EIN
    "INT6028": [
        "Department_ID", "Department_WID", "Department_Name",
        "Dept_Name_with_Manager_Name", "Active", "Parent_Dept_ID",
        "Owner_EIN", "Owner_EIN_WID",
        "Department_Level", "PRIMARY_LOCATION_CODE", "Type", "Subtype",
    ],
    # INT6032/CR: Added 8 new fields; final column order per spec
    "INT6032": [
        "Position_ID", "Supervisory_Organization", "Effective_Date",
        "Reason", "Worker_Type", "Worker_Sub_Type", "Job_Profile",
        "Job_Title", "Business_Title", "Time_Type", "Location",
        "Work_Space", "Pay_Rate_Type", "Schedule_Weekly_Hours",
        "Scheduled_FTE", "Default_Weekly_Hours", "Employee_Type",
        "shift_number", "Exclude_From_Headcount",
    ],
}
