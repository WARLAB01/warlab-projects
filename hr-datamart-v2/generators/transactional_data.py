"""
transactional_data.py - Convert employee timeline events into CSV-format rows.

Takes EmployeeEvent and EmployeeProfile objects from employee_timeline.py and
produces properly formatted rows for the 5 transactional feeds:
  - INT0095E  Worker Job
  - INT0096   Worker Organization  (3 rows per event: Cost Centre, Company, Supervisory)
  - INT0098   Worker Compensation
  - INT270    Rescinded Transactions
  - INT6031   Worker Profile
"""

import os
from typing import List, Dict

from . import config
from . import utils
from .employee_timeline import EmployeeEvent, EmployeeProfile


# ============================================================
# FIELD ORDERS - exact CSV column ordering per source schema
# ============================================================

FIELD_ORDERS = {
    "INT0095E": [
        "Employee_ID", "Transaction_WID", "Transaction_Effective_Date",
        "Transaction_Entry_Date", "Transaction_Type", "Position_ID",
        "Effective_Date", "Worker_Type", "Worker_Sub-Type", "Business_Title",
        "Business_Site_ID", "Mailstop_Floor", "Worker_Status", "Active",
        "Active_Status_Date", "Hire_Date", "Original_Hire_Date", "Hire_Reason",
        "Employment_End_Date", "Continuous_Service_Date", "First_Day_of_Work",
        "Expected_Retirement_Date", "Retirement_Eligibility_Date", "Retired",
        "Seniority_Date", "Severance_Date", "Benefits_Service_Date",
        "Company_Service_Date", "Time_Off_Service_Date", "Vesting_Date",
        "Terminated", "Termination_Date", "Pay_Through_Date",
        "Primary_Termination_Reason", "Primary_Termination_Category",
        "Termination_Involuntary", "Secondary_Termination_Reason",
        "Local_Termination_Reason", "Not_Eligible_for_Hire",
        "Regrettable_Termination", "Hire_Rescinded", "Resignation_Date",
        "Last_Day_of_Work", "Last_Date_for_Which_Paid",
        "Expected_Date_of_Return", "Not_Returning", "Return_Unknown",
        "Probation_Start_Date", "Probation_End_Date", "Academic_Tenure_Date",
        "Has_International_Assignment", "Home_Country", "Host_Country",
        "International_Assignment_Type",
        "Start_Date_of_International_Assignment",
        "End_Date_of_International_Assignment",
        "Rehire", "Eligible_For_Rehire", "Action", "Action_Code",
        "Action_Reason", "Action_Reason_Code", "Manager_ID",
        "Soft_Retirement_Indicator", "Job_Profile_ID", "Sequence_Number",
        "Planned_End_Contract_Date", "Job_Entry_Dt", "Stock_Grants",
        "Time_Type", "Supervisory_Organization", "Location", "Job_Title",
        "French_Job_Title", "Shift_Number", "Scheduled_Weekly_Hours",
        "Default_Weekly_Hours", "Scheduled_FTE", "Work_Model_Start_Date",
        "Work_Model_Type", "Worker_Workday_ID",
    ],
    "INT0096": [
        "Employee_ID", "Transaction_WID", "Transaction_Effective_Date",
        "Transaction_Entry_Date", "Transaction_Type", "Organization_ID",
        "Organization_Type", "Sequence_Number", "Worker_Workday_ID",
    ],
    "INT0098": [
        "Employee_ID", "Transaction_WID", "Transaction_Effective_Date",
        "Transaction_Entry_Moment", "Transaction_Type",
        "Compensation_Package_Proposed", "Compensation_Grade_Proposed",
        "Comp_Grade_Profile_Proposed", "Compensation_Step_Proposed",
        "Pay_Range_Minimum", "Pay_Range_Midpoint", "Pay_Range_Maximum",
        "Base_Pay_Proposed_Amount", "Base_Pay_Proposed_Currency",
        "Base_Pay_Proposed_Frequency", "Benefits_Annual_Rate_ABBR",
        "Pay_Rate_Type", "Compensation", "Worker_Workday_ID",
    ],
    "INT270": [
        "workday_id", "idp_table", "rescinded_moment",
    ],
    "INT6031": [
        "Bank_of_the_West_Employee_ID", "Date_of_Birth", "Enterprise_ID",
        "Race_Ethnicity", "Gender", "Gender_Identity", "Indigenous",
        "Home_Addres_Postal_Code",  # Note: typo matches source schema
        "Home_Address_City", "Home_Address_Country", "Home_Address_Region",
        "Last_Name", "Legal_First_Name", "Legal_Full_Name",
        "Legal_Full_Name_Formatted", "Military_Status",
        "Preferred_First_Name", "Preferred_Full_Name",
        "Preferred_Full_Name_Formatted", "Primary_Work_Email_Address",
        "Secondary_Work_Email_Address", "Sexual_Orientation", "Worker_ID",
        "Junior_Senior", "Product_Sector_Group", "Preferred_Language",
        "Bonus/Equity_Earliest_Retirement_Date", "Class_Year", "Admin_FTE",
        "CONSOLIDATED_TITLE", "GENERATION", "HOME_ADDRESS_COUNTRY_NAME",
        "HOME_ADDRESS_REGION_NAME", "INDIGENOUS",
        "PENSIONABLE_YRS_OF_SERVICE", "Worker_Workday_ID",
    ],
}


class TransactionalDataWriter:
    """Converts timeline events and profiles into CSV feed files."""

    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), config.OUTPUT_DIR
        )

    def write_all(self, events: List[EmployeeEvent],
                  profiles: List[EmployeeProfile],
                  rescinded: List[Dict],
                  ref_feeds: Dict[str, List[Dict]],
                  ref_positions: List[Dict]):
        """Write all transactional and reference feeds to CSV files."""
        print("\n  Writing CSV feeds...")

        # --- Reference feeds (from reference_data.py) ---
        from .reference_data import FIELD_ORDERS as REF_FIELD_ORDERS
        for feed_key, rows in ref_feeds.items():
            self._write_feed(feed_key, rows, REF_FIELD_ORDERS[feed_key])

        # --- INT6032 Positions ---
        if ref_positions:
            self._write_feed("INT6032", ref_positions, REF_FIELD_ORDERS["INT6032"])

        # --- INT0095E Worker Job ---
        rows_095e = [self._event_to_095e(e) for e in events]
        self._write_feed("INT0095E", rows_095e, FIELD_ORDERS["INT0095E"])

        # --- INT0096 Worker Organization (3 rows per event) ---
        rows_096 = []
        for e in events:
            rows_096.extend(self._event_to_096(e))
        self._write_feed("INT0096", rows_096, FIELD_ORDERS["INT0096"])

        # --- INT0098 Worker Compensation ---
        rows_098 = [self._event_to_098(e) for e in events]
        self._write_feed("INT0098", rows_098, FIELD_ORDERS["INT0098"])

        # --- INT270 Rescinded ---
        self._write_feed("INT270", rescinded, FIELD_ORDERS["INT270"])

        # --- INT6031 Worker Profile ---
        rows_6031 = [self._profile_to_6031(p) for p in profiles]
        self._write_feed("INT6031", rows_6031, FIELD_ORDERS["INT6031"])

        print("  All feeds written.\n")

    def _write_feed(self, feed_key: str, rows: List[Dict], fieldnames: List[str]):
        """Write a single feed to CSV."""
        filename = utils.feed_filename(feed_key)
        filepath = os.path.join(self.output_dir, filename)
        n = utils.write_csv(filepath, rows, fieldnames)
        print(f"    {feed_key}: {n:,} rows -> {filename}")

    # ---------------------------------------------------------
    # INT0095E - Worker Job
    # ---------------------------------------------------------
    def _event_to_095e(self, e: EmployeeEvent) -> Dict:
        """Convert an EmployeeEvent to an INT0095E row."""
        return {
            "Employee_ID": e.employee_id,
            "Transaction_WID": e.transaction_wid,
            "Transaction_Effective_Date": e.effective_date.isoformat(),
            "Transaction_Entry_Date": e.entry_datetime.isoformat(sep=" "),
            "Transaction_Type": e.transaction_type,
            "Position_ID": e.position_id,
            "Effective_Date": e.effective_date.isoformat(),
            "Worker_Type": e.worker_type,
            "Worker_Sub-Type": e.worker_sub_type,
            "Business_Title": e.business_title,
            "Business_Site_ID": e.business_site_id,
            "Mailstop_Floor": e.mailstop_floor,
            "Worker_Status": e.worker_status,
            "Active": utils.bool_to_str(e.active),
            "Active_Status_Date": e.active_status_date,
            "Hire_Date": e.hire_date,
            "Original_Hire_Date": e.original_hire_date,
            "Hire_Reason": e.hire_reason,
            "Employment_End_Date": e.employment_end_date,
            "Continuous_Service_Date": e.continuous_service_date,
            "First_Day_of_Work": e.first_day_of_work,
            "Expected_Retirement_Date": e.expected_retirement_date,
            "Retirement_Eligibility_Date": e.retirement_eligibility_date,
            "Retired": utils.bool_to_str(e.retired),
            "Seniority_Date": e.seniority_date,
            "Severance_Date": e.severance_date,
            "Benefits_Service_Date": e.benefits_service_date,
            "Company_Service_Date": e.company_service_date,
            "Time_Off_Service_Date": e.time_off_service_date,
            "Vesting_Date": e.vesting_date,
            "Terminated": utils.bool_to_str(e.terminated),
            "Termination_Date": e.termination_date,
            "Pay_Through_Date": e.pay_through_date,
            "Primary_Termination_Reason": e.primary_termination_reason,
            "Primary_Termination_Category": e.primary_termination_category,
            "Termination_Involuntary": utils.bool_to_str(e.termination_involuntary),
            "Secondary_Termination_Reason": e.secondary_termination_reason,
            "Local_Termination_Reason": e.local_termination_reason,
            "Not_Eligible_for_Hire": utils.bool_to_str(e.not_eligible_for_hire),
            "Regrettable_Termination": utils.bool_to_str(e.regrettable_termination),
            "Hire_Rescinded": utils.bool_to_str(e.hire_rescinded),
            "Resignation_Date": e.resignation_date,
            "Last_Day_of_Work": e.last_day_of_work,
            "Last_Date_for_Which_Paid": e.last_date_for_which_paid,
            "Expected_Date_of_Return": e.expected_date_of_return,
            "Not_Returning": utils.bool_to_str(e.not_returning),
            "Return_Unknown": e.return_unknown,
            "Probation_Start_Date": e.probation_start_date,
            "Probation_End_Date": e.probation_end_date,
            "Academic_Tenure_Date": e.academic_tenure_date,
            "Has_International_Assignment": utils.bool_to_str(e.has_international_assignment),
            "Home_Country": e.home_country,
            "Host_Country": e.host_country,
            "International_Assignment_Type": e.international_assignment_type,
            "Start_Date_of_International_Assignment": e.start_intl_assignment,
            "End_Date_of_International_Assignment": e.end_intl_assignment,
            "Rehire": utils.bool_to_str(e.rehire),
            "Eligible_For_Rehire": e.eligible_for_rehire,
            "Action": e.action,
            "Action_Code": e.action_code,
            "Action_Reason": e.action_reason,
            "Action_Reason_Code": e.action_reason_code,
            "Manager_ID": e.manager_id,
            "Soft_Retirement_Indicator": utils.bool_to_str(e.soft_retirement_indicator),
            "Job_Profile_ID": e.job_profile_id,
            "Sequence_Number": str(e.sequence_number),
            "Planned_End_Contract_Date": e.planned_end_contract_date,
            "Job_Entry_Dt": e.job_entry_dt,
            "Stock_Grants": e.stock_grants,
            "Time_Type": e.time_type,
            "Supervisory_Organization": e.supervisory_organization,
            "Location": e.location_name,
            "Job_Title": e.job_title,
            "French_Job_Title": e.french_job_title,
            "Shift_Number": str(e.shift_number),
            "Scheduled_Weekly_Hours": f"{e.scheduled_weekly_hours:.1f}",
            "Default_Weekly_Hours": f"{e.default_weekly_hours:.1f}",
            "Scheduled_FTE": f"{e.scheduled_fte:.2f}",
            "Work_Model_Start_Date": e.work_model_start_date,
            "Work_Model_Type": e.work_model_type,
            "Worker_Workday_ID": e.worker_workday_id,
        }

    # ---------------------------------------------------------
    # INT0096 - Worker Organization (3 rows per event)
    # ---------------------------------------------------------
    def _event_to_096(self, e: EmployeeEvent) -> List[Dict]:
        """Convert an EmployeeEvent to 3 INT0096 rows (CC, Company, SupOrg)."""
        base = {
            "Employee_ID": e.employee_id,
            "Transaction_WID": e.transaction_wid,
            "Transaction_Effective_Date": e.effective_date.isoformat(),
            "Transaction_Entry_Date": e.entry_datetime.isoformat(sep=" "),
            "Transaction_Type": e.transaction_type,
            "Sequence_Number": str(e.sequence_number),
            "Worker_Workday_ID": e.worker_workday_id,
        }

        rows = []

        # Row 1: Cost Centre
        r1 = {**base}
        r1["Organization_ID"] = e.cost_center_id
        r1["Organization_Type"] = "Cost_Center"
        rows.append(r1)

        # Row 2: Company
        r2 = {**base}
        r2["Organization_ID"] = e.company_id
        r2["Organization_Type"] = "Company"
        rows.append(r2)

        # Row 3: Supervisory Organization
        r3 = {**base}
        r3["Organization_ID"] = e.sup_org_id
        r3["Organization_Type"] = "Supervisory"
        rows.append(r3)

        return rows

    # ---------------------------------------------------------
    # INT0098 - Worker Compensation
    # ---------------------------------------------------------
    def _event_to_098(self, e: EmployeeEvent) -> Dict:
        """Convert an EmployeeEvent to an INT0098 row."""
        return {
            "Employee_ID": e.employee_id,
            "Transaction_WID": e.transaction_wid,
            "Transaction_Effective_Date": e.effective_date.isoformat(),
            "Transaction_Entry_Moment": e.entry_datetime.isoformat(sep=" "),
            "Transaction_Type": e.transaction_type,
            "Compensation_Package_Proposed": e.comp_package,
            "Compensation_Grade_Proposed": e.comp_grade,
            "Comp_Grade_Profile_Proposed": e.comp_grade_profile,
            "Compensation_Step_Proposed": e.comp_step,
            "Pay_Range_Minimum": f"{e.pay_range_min:.2f}" if e.pay_range_min else "",
            "Pay_Range_Midpoint": f"{e.pay_range_mid:.2f}" if e.pay_range_mid else "",
            "Pay_Range_Maximum": f"{e.pay_range_max:.2f}" if e.pay_range_max else "",
            "Base_Pay_Proposed_Amount": f"{e.base_pay:.2f}" if e.base_pay else "",
            "Base_Pay_Proposed_Currency": e.base_pay_currency,
            "Base_Pay_Proposed_Frequency": e.base_pay_frequency,
            "Benefits_Annual_Rate_ABBR": f"{e.benefits_annual_rate:.2f}" if e.benefits_annual_rate else "",
            "Pay_Rate_Type": e.pay_rate_type,
            "Compensation": f"{e.compensation:.2f}" if e.compensation else "",
            "Worker_Workday_ID": e.worker_workday_id,
        }

    # ---------------------------------------------------------
    # INT6031 - Worker Profile
    # ---------------------------------------------------------
    def _profile_to_6031(self, p: EmployeeProfile) -> Dict:
        """Convert an EmployeeProfile to an INT6031 row."""
        return {
            "Bank_of_the_West_Employee_ID": p.bank_of_west_id,
            "Date_of_Birth": p.date_of_birth.isoformat(),
            "Enterprise_ID": p.enterprise_id,
            "Race_Ethnicity": p.race_ethnicity,
            "Gender": p.gender,
            "Gender_Identity": p.gender_identity,
            "Indigenous": p.indigenous,
            "Home_Addres_Postal_Code": p.home_address_postal_code,  # typo matches schema
            "Home_Address_City": p.home_address_city,
            "Home_Address_Country": p.home_address_country,
            "Home_Address_Region": p.home_address_region,
            "Last_Name": p.last_name,
            "Legal_First_Name": p.legal_first_name,
            "Legal_Full_Name": p.legal_full_name,
            "Legal_Full_Name_Formatted": p.legal_full_name_formatted,
            "Military_Status": p.military_status,
            "Preferred_First_Name": p.preferred_first_name,
            "Preferred_Full_Name": p.preferred_full_name,
            "Preferred_Full_Name_Formatted": p.preferred_full_name_formatted,
            "Primary_Work_Email_Address": p.primary_work_email,
            "Secondary_Work_Email_Address": p.secondary_work_email,
            "Sexual_Orientation": p.sexual_orientation,
            "Worker_ID": p.employee_id,
            "Junior_Senior": p.junior_senior,
            "Product_Sector_Group": p.product_sector_group,
            "Preferred_Language": p.preferred_language,
            "Bonus/Equity_Earliest_Retirement_Date": p.bonus_equity_retirement_date,
            "Class_Year": p.class_year,
            "Admin_FTE": f"{p.admin_fte:.2f}",
            "CONSOLIDATED_TITLE": p.consolidated_title,
            "GENERATION": p.generation,
            "HOME_ADDRESS_COUNTRY_NAME": p.home_address_country_name,
            "HOME_ADDRESS_REGION_NAME": p.home_address_region_name,
            "INDIGENOUS": p.indigenous,
            "PENSIONABLE_YRS_OF_SERVICE": f"{p.pensionable_yrs_of_service:.3f}",
            "Worker_Workday_ID": p.worker_workday_id,
        }
