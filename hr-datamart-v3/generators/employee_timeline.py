"""
employee_timeline.py - Master employee lifecycle simulation engine.

V3 changes:
  CR5 - INT6031: Added address_line_1 and address_line_2 fields to EmployeeProfile.
                 These are populated via Faker in _gen_profile().
"""

import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from faker import Faker

from . import config
from . import utils


@dataclass
class EmployeeEvent:
    """A single event in an employee's timeline."""
    employee_id: str
    effective_date: datetime.date
    entry_datetime: datetime.datetime
    sequence_number: int
    transaction_wid: str
    transaction_type: str

    # Action fields
    action: str = ""
    action_code: str = ""
    action_reason: str = ""
    action_reason_code: str = ""

    # Job assignment
    position_id: str = ""
    job_profile_id: str = ""
    job_title: str = ""
    business_title: str = ""
    grade_id: str = ""
    location_id: str = ""
    business_site_id: str = ""
    worker_type: str = "Employee"
    worker_sub_type: str = "Regular"
    time_type: str = "Full_Time"
    scheduled_weekly_hours: float = 37.5
    default_weekly_hours: float = 37.5
    scheduled_fte: float = 1.0
    work_model_type: str = "On-Site"
    work_model_start_date: str = ""
    shift_number: int = 0

    # Organization
    company_id: str = ""
    cost_center_id: str = ""
    sup_org_id: str = ""
    matrix_org_id: str = ""

    # Compensation
    comp_package: str = ""
    comp_grade: str = ""
    comp_grade_profile: str = ""
    comp_step: str = ""
    pay_range_min: float = 0.0
    pay_range_mid: float = 0.0
    pay_range_max: float = 0.0
    base_pay: float = 0.0
    base_pay_currency: str = "CAD"
    base_pay_frequency: str = "Annual"
    benefits_annual_rate: float = 0.0
    pay_rate_type: str = "Salary"
    compensation: float = 0.0

    # Status fields
    worker_status: str = "Active"
    active: bool = True
    hire_date: str = ""
    original_hire_date: str = ""
    terminated: bool = False
    termination_date: str = ""
    pay_through_date: str = ""
    primary_termination_reason: str = ""
    primary_termination_category: str = ""
    termination_involuntary: bool = False
    secondary_termination_reason: str = ""
    local_termination_reason: str = ""
    regrettable_termination: bool = False
    not_eligible_for_hire: bool = False
    hire_rescinded: bool = False
    rehire: bool = False
    eligible_for_rehire: str = "Y"
    resignation_date: str = ""
    last_day_of_work: str = ""
    last_date_for_which_paid: str = ""
    retired: bool = False

    # Leave fields
    expected_date_of_return: str = ""
    not_returning: bool = False
    return_unknown: str = ""

    # Manager
    manager_id: str = ""

    # Additional fields
    worker_workday_id: str = ""
    mailstop_floor: str = ""
    french_job_title: str = ""
    supervisory_organization: str = ""
    location_name: str = ""

    # Dates
    employment_end_date: str = ""
    continuous_service_date: str = ""
    first_day_of_work: str = ""
    active_status_date: str = ""
    hire_reason: str = ""
    expected_retirement_date: str = ""
    retirement_eligibility_date: str = ""
    seniority_date: str = ""
    severance_date: str = ""
    benefits_service_date: str = ""
    company_service_date: str = ""
    time_off_service_date: str = ""
    vesting_date: str = ""
    probation_start_date: str = ""
    probation_end_date: str = ""
    academic_tenure_date: str = ""
    has_international_assignment: bool = False
    home_country: str = ""
    host_country: str = ""
    international_assignment_type: str = ""
    start_intl_assignment: str = ""
    end_intl_assignment: str = ""
    soft_retirement_indicator: bool = False
    planned_end_contract_date: str = ""
    job_entry_dt: str = ""
    stock_grants: str = ""


@dataclass
class EmployeeProfile:
    """Demographic/profile data for INT6031."""
    employee_id: str
    worker_workday_id: str
    date_of_birth: datetime.date
    gender: str
    gender_identity: str
    race_ethnicity: str
    indigenous: str
    legal_first_name: str
    legal_full_name: str
    legal_full_name_formatted: str
    last_name: str
    preferred_first_name: str
    preferred_full_name: str
    preferred_full_name_formatted: str
    primary_work_email: str
    secondary_work_email: str
    home_address_city: str
    home_address_region: str
    home_address_region_name: str
    home_address_country: str
    home_address_country_name: str
    home_address_postal_code: str
    # CR5: new address line fields
    address_line_1: str
    address_line_2: str
    military_status: str
    sexual_orientation: str
    bank_of_west_id: str
    enterprise_id: str
    junior_senior: str
    product_sector_group: str
    preferred_language: str
    bonus_equity_retirement_date: str
    class_year: str
    admin_fte: float
    consolidated_title: str
    generation: str
    pensionable_yrs_of_service: float


class EmployeeTimelineGenerator:
    """Generates complete employee lifecycles and all transactional feeds."""

    def __init__(self, ref_data, rng=None):
        self.rng = rng or utils.get_rng()
        self.faker_ca = Faker('en_CA')
        self.faker_ca.seed_instance(config.SEED)
        self.faker_us = Faker('en_US')
        self.faker_us.seed_instance(config.SEED + 1)
        self.ref = ref_data

        self.employee_seq = 0
        self.position_seq = 0
        self.active_employees: Dict[str, EmployeeEvent] = {}
        self.all_events: List[EmployeeEvent] = []
        self.profiles: List[EmployeeProfile] = []
        self.rescinded_wids: List[Dict] = []
        self.position_assignments: List[Dict] = []

        self.dept_ids = [d["Department_ID"] for d in ref_data.departments if d.get("Department_Level", 0) >= 2]
        self.cc_ids = [c["Cost_Center_ID"] for c in ref_data.cost_centers]
        self.company_list = ref_data.companies
        self.loc_list = ref_data.locations
        self.jp_list = ref_data.job_profiles
        self.grade_list = config.GRADES
        self.matrix_org_ids = [m["Matrix_Organization_ID"] for m in ref_data.matrix_orgs]

        self._build_jp_by_grade()

    def _build_jp_by_grade(self):
        self.jp_by_grade = {}
        for jp in self.jp_list:
            grade = jp["Compensation_Grade"]
            if grade not in self.jp_by_grade:
                self.jp_by_grade[grade] = []
            self.jp_by_grade[grade].append(jp)

    def generate_all(self):
        """Run the full monthly simulation from founding to data end date."""
        print("  Generating employee timelines...")
        current = datetime.date(config.COMPANY_FOUNDED.year, config.COMPANY_FOUNDED.month, 1)
        end = config.DATA_END_DATE

        month_count = 0
        while current <= end:
            year = current.year
            month = current.month
            month_end_dt = utils.month_end(year, month)

            target_hc = utils.interpolate_headcount(month_end_dt)
            current_hc = len(self.active_employees)

            n_terms = self._calc_monthly_terms(year, current_hc)
            if current_hc > 0 and n_terms > 0:
                self._process_terminations(year, month, n_terms)

            self._process_career_events(year, month)

            current_hc = len(self.active_employees)
            n_hires = max(0, target_hc - current_hc)
            n_hires = int(n_hires * 1.05)
            if n_hires > 0:
                self._process_hires(year, month, n_hires)

            month_count += 1
            if month_count % 12 == 0:
                print(f"    Year {year}: Active={len(self.active_employees)}, "
                      f"Total events={len(self.all_events)}, "
                      f"Total employees={self.employee_seq}")

            if month == 12:
                current = datetime.date(year + 1, 1, 1)
            else:
                current = datetime.date(year, month + 1, 1)

        print(f"  Timeline complete: {self.employee_seq} unique employees, "
              f"{len(self.all_events)} events, "
              f"{len(self.active_employees)} active at end")

    def _calc_monthly_terms(self, year: int, current_hc: int) -> int:
        annual_rate = config.ANNUAL_ATTRITION_RATE.get(year, 0.14)
        monthly_rate = annual_rate / 12
        monthly_rate *= self.rng.uniform(0.7, 1.3)
        return int(current_hc * monthly_rate)

    def _process_terminations(self, year: int, month: int, count: int):
        if not self.active_employees:
            return

        emp_ids = list(self.active_employees.keys())
        self.rng.shuffle(emp_ids)
        count = min(count, len(emp_ids))

        for i in range(count):
            emp_id = emp_ids[i]
            last_event = self.active_employees[emp_id]

            if last_event.effective_date.year == year and last_event.effective_date.month == month:
                continue

            term_date = utils.random_date_in_month(self.rng, year, month,
                                                     earliest=config.COMPANY_FOUNDED)

            cat = utils.weighted_choice_from_tuples(self.rng, list(config.TERM_REASON_MIX.items()))

            if cat == "Voluntary":
                reason_tuple = utils.weighted_choice_from_named_tuples(
                    self.rng, config.VOLUNTARY_REASONS, weight_idx=2)
                reason_code, reason_name = reason_tuple[0], reason_tuple[1]
                involuntary = False
                regrettable = self.rng.random() < config.REGRETTABLE_TERM_RATE
            elif cat == "Involuntary":
                reason_tuple = utils.weighted_choice_from_named_tuples(
                    self.rng, config.INVOLUNTARY_REASONS, weight_idx=2)
                reason_code, reason_name = reason_tuple[0], reason_tuple[1]
                involuntary = True
                regrettable = False
            elif cat == "Retirement":
                reason_code, reason_name = "TER-RET", "Retirement"
                involuntary = False
                regrettable = False
            elif cat == "Death":
                reason_code, reason_name = "TER-DEA", "Death"
                involuntary = False
                regrettable = False
            else:
                reason_code, reason_name = "TER-EOC", "End of Contract"
                involuntary = False
                regrettable = False

            event = self._clone_event(last_event, term_date)
            event.transaction_type = "Termination"
            event.action = "Termination"
            event.action_code = "TER"
            event.action_reason = reason_name
            event.action_reason_code = reason_code
            event.worker_status = "Terminated"
            event.active = False
            event.terminated = True
            event.termination_date = term_date.isoformat()
            event.pay_through_date = (term_date + datetime.timedelta(days=self.rng.randint(0, 14))).isoformat()
            event.primary_termination_reason = reason_code
            event.primary_termination_category = cat
            event.termination_involuntary = involuntary
            event.regrettable_termination = regrettable
            event.not_eligible_for_hire = involuntary and self.rng.random() < 0.3
            event.eligible_for_rehire = "N" if event.not_eligible_for_hire else "Y"
            event.last_day_of_work = term_date.isoformat()
            event.last_date_for_which_paid = event.pay_through_date
            event.employment_end_date = term_date.isoformat()
            event.retired = cat == "Retirement"

            if cat == "Voluntary":
                resign_date = term_date - datetime.timedelta(days=self.rng.randint(14, 30))
                event.resignation_date = max(resign_date, config.COMPANY_FOUNDED).isoformat()

            self.all_events.append(event)
            del self.active_employees[emp_id]

    def _process_career_events(self, year: int, month: int):
        emp_ids = list(self.active_employees.keys())

        for emp_id in emp_ids:
            last_event = self.active_employees[emp_id]

            months_since_hire = ((year - last_event.effective_date.year) * 12
                                  + month - last_event.effective_date.month)
            if months_since_hire < 3:
                continue

            if month in (1, 2, 3) and self.rng.random() < config.ANNUAL_COMP_CHANGE_RATE / 3:
                self._apply_comp_change(emp_id, year, month)
                continue

            monthly_event_prob = 1.0 / config.AVG_MONTHS_BETWEEN_EVENTS
            if self.rng.random() < monthly_event_prob:
                action = utils.weighted_choice(self.rng, config.CAREER_ACTIONS)
                self._apply_career_action(emp_id, year, month, action)

    def _apply_comp_change(self, emp_id: str, year: int, month: int):
        last_event = self.active_employees[emp_id]
        eff_date = utils.random_date_in_month(self.rng, year, month,
                                                earliest=config.COMPANY_FOUNDED)

        event = self._clone_event(last_event, eff_date)
        event.transaction_type = "Data Change"
        event.action = "Data Change"
        event.action_code = "DAT_CHG"
        event.action_reason = "Compensation Change"
        event.action_reason_code = "DAT_COMP"

        raise_pct = max(0, self.rng.gauss(config.AVG_ANNUAL_RAISE_PCT, config.RAISE_STD_DEV))
        new_base = round(last_event.base_pay * (1 + raise_pct), 2)

        grade = next((g for g in config.GRADES if g["id"] == event.grade_id), None)
        if grade:
            new_base = max(grade["min"], min(grade["max"], new_base))

        event.base_pay = new_base
        event.compensation = new_base
        event.benefits_annual_rate = new_base

        self.all_events.append(event)
        self.active_employees[emp_id] = event

    def _apply_career_action(self, emp_id: str, year: int, month: int, action: Dict):
        last_event = self.active_employees[emp_id]
        eff_date = utils.random_date_in_month(self.rng, year, month,
                                                earliest=config.COMPANY_FOUNDED)

        event = self._clone_event(last_event, eff_date)
        event.transaction_type = action["action"]
        event.action = action["action"]
        event.action_code = action["code"]
        event.action_reason = action["reason"]
        event.action_reason_code = action["reason_code"]

        code = action["reason_code"]

        if code == "CHG_PROMO":
            new_grade = utils.next_grade(last_event.grade_id, "up")
            if new_grade:
                event.grade_id = new_grade
                event.comp_grade = new_grade
                jp = self._pick_job_profile_for_grade(new_grade)
                if jp:
                    event.job_profile_id = jp["Job_Profile_ID"]
                    event.job_title = jp["Job_Profile_Name"]
                    event.business_title = jp["Job_Profile_Name"]
                event.base_pay = utils.salary_for_grade(self.rng, new_grade)
                event.compensation = event.base_pay
                event.benefits_annual_rate = event.base_pay
                grade_obj = next((g for g in config.GRADES if g["id"] == new_grade), None)
                if grade_obj:
                    event.pay_range_min = grade_obj["min"]
                    event.pay_range_mid = grade_obj["mid"]
                    event.pay_range_max = grade_obj["max"]
                    event.comp_grade_profile = grade_obj["profile_id"]

        elif code == "CHG_DEMO":
            new_grade = utils.next_grade(last_event.grade_id, "down")
            if new_grade:
                event.grade_id = new_grade
                event.comp_grade = new_grade
                # Issue 3 fix: keep comp_grade_profile in sync with grade on demotion
                demo_grade_obj = next((g for g in config.GRADES if g["id"] == new_grade), None)
                if demo_grade_obj:
                    event.comp_grade_profile = demo_grade_obj["profile_id"]
                    event.pay_range_min = demo_grade_obj["min"]
                    event.pay_range_mid = demo_grade_obj["mid"]
                    event.pay_range_max = demo_grade_obj["max"]
                event.base_pay = utils.salary_for_grade(self.rng, new_grade)
                event.compensation = event.base_pay
                event.benefits_annual_rate = event.base_pay

        elif code == "CHG_LAT":
            jp = self._pick_job_profile_for_grade(event.grade_id)
            if jp:
                event.job_profile_id = jp["Job_Profile_ID"]
                event.job_title = jp["Job_Profile_Name"]
                event.business_title = jp["Job_Profile_Name"]
            if self.rng.random() < 0.5 and self.dept_ids:
                event.sup_org_id = self.rng.choice(self.dept_ids)

        elif code == "CHG_XFER":
            company_id = self.rng.choice([c["id"] for c in config.COMPANIES])
            loc = utils.location_for_company(self.rng, company_id)
            event.company_id = company_id
            event.location_id = loc["id"]
            event.business_site_id = loc["id"]
            if self.dept_ids:
                event.sup_org_id = self.rng.choice(self.dept_ids)
            if self.cc_ids:
                event.cost_center_id = self.rng.choice(self.cc_ids)

        elif code == "DAT_LOC":
            loc = utils.location_for_company(self.rng, event.company_id)
            event.location_id = loc["id"]
            event.business_site_id = loc["id"]

            if year >= 2020:
                wm_weights = [{"type": wm["type"],
                              "weight": wm["weight_post_2020"]} for wm in config.WORK_MODELS]
            else:
                wm_weights = [{"type": wm["type"],
                              "weight": wm["weight_pre_2020"]} for wm in config.WORK_MODELS]
            wm = utils.weighted_choice(self.rng, wm_weights)
            event.work_model_type = wm["type"]
            event.work_model_start_date = eff_date.isoformat()

        elif code == "DAT_ORG":
            if self.dept_ids:
                event.sup_org_id = self.rng.choice(self.dept_ids)
            if self.cc_ids:
                event.cost_center_id = self.rng.choice(self.cc_ids)

        elif code == "LOA_GEN":
            event.worker_status = "On Leave"
            event.expected_date_of_return = (
                eff_date + datetime.timedelta(days=config.AVG_LOA_DAYS)
            ).isoformat()

        elif code == "RFL_GEN":
            event.worker_status = "Active"
            event.active = True
            event.expected_date_of_return = ""

        self.all_events.append(event)
        self.active_employees[emp_id] = event

    def _process_hires(self, year: int, month: int, count: int):
        for _ in range(count):
            self.employee_seq += 1
            self.position_seq += 1

            emp_id = utils.generate_employee_id(self.employee_seq)
            pos_id = utils.generate_position_id(self.position_seq)
            wid = utils.generate_wid(self.rng)
            worker_wid = utils.generate_wid(self.rng)

            hire_date = utils.random_date_in_month(self.rng, year, month,
                                                     earliest=config.COMPANY_FOUNDED)

            comp_weights = [{"id": c["id"], "weight": config.COMPANY_WEIGHTS.get(c["id"], 0.1)}
                           for c in config.COMPANIES]
            company = utils.weighted_choice(self.rng, comp_weights)
            company_id = company["id"]
            country = config.COMPANY_COUNTRY[company_id]

            loc = utils.location_for_company(self.rng, company_id)

            grade_weights = [{"id": g, "weight": w} for g, w in config.GRADE_WEIGHTS.items()]
            grade = utils.weighted_choice(self.rng, grade_weights)
            grade_id = grade["id"]
            grade_obj = next((g for g in config.GRADES if g["id"] == grade_id), config.GRADES[0])

            jp = self._pick_job_profile_for_grade(grade_id)
            if not jp:
                jp = self.rng.choice(self.jp_list)

            dept_id = self.rng.choice(self.dept_ids) if self.dept_ids else ""
            cc_id = self.rng.choice(self.cc_ids) if self.cc_ids else ""
            matrix_id = self.rng.choice(self.matrix_org_ids) if self.rng.random() < 0.3 else ""

            wt = utils.weighted_choice(self.rng, config.WORKER_TYPES)
            tt = utils.weighted_choice(self.rng, config.TIME_TYPES)

            if year >= 2020:
                wm_weights = [{"type": wm["type"], "weight": wm["weight_post_2020"]}
                             for wm in config.WORK_MODELS]
            else:
                wm_weights = [{"type": wm["type"], "weight": wm["weight_pre_2020"]}
                             for wm in config.WORK_MODELS]
            wm = utils.weighted_choice(self.rng, wm_weights)

            base_pay = utils.salary_for_grade(self.rng, grade_id)
            if country == "US":
                base_pay = round(base_pay * 1.05, 2)

            is_rehire = False
            action_reason = "Hire"
            action_reason_code = "HIR_NEW"

            manager_id = self._pick_manager(grade_id)
            entry_dt = utils.entry_date_from_effective(self.rng, hire_date)

            event = EmployeeEvent(
                employee_id=emp_id,
                effective_date=hire_date,
                entry_datetime=entry_dt,
                sequence_number=1,
                transaction_wid=wid,
                transaction_type="Hire",
                action="Hire",
                action_code="HIR",
                action_reason=action_reason,
                action_reason_code=action_reason_code,
                position_id=pos_id,
                job_profile_id=jp["Job_Profile_ID"],
                job_title=jp["Job_Profile_Name"],
                business_title=jp["Job_Profile_Name"],
                grade_id=grade_id,
                location_id=loc["id"],
                business_site_id=loc["id"],
                worker_type=wt["type"],
                worker_sub_type=wt["sub_type"],
                time_type=tt["name"],
                scheduled_weekly_hours=tt["hours"],
                default_weekly_hours=37.5,
                scheduled_fte=tt["fte"],
                work_model_type=wm["type"],
                work_model_start_date=hire_date.isoformat(),
                company_id=company_id,
                cost_center_id=cc_id,
                sup_org_id=dept_id,
                matrix_org_id=matrix_id,
                comp_package="STD_COMP_PKG",
                comp_grade=grade_id,
                comp_grade_profile=grade_obj["profile_id"],
                comp_step="",
                pay_range_min=grade_obj["min"],
                pay_range_mid=grade_obj["mid"],
                pay_range_max=grade_obj["max"],
                base_pay=base_pay,
                base_pay_currency="USD" if country == "US" else "CAD",
                base_pay_frequency="Annual",
                benefits_annual_rate=base_pay,
                pay_rate_type="Salary",
                compensation=base_pay,
                worker_status="Active",
                active=True,
                hire_date=hire_date.isoformat(),
                original_hire_date=hire_date.isoformat(),
                hire_reason="New Hire",
                continuous_service_date=hire_date.isoformat(),
                first_day_of_work=hire_date.isoformat(),
                active_status_date=hire_date.isoformat(),
                benefits_service_date=hire_date.isoformat(),
                company_service_date=hire_date.isoformat(),
                time_off_service_date=hire_date.isoformat(),
                seniority_date=hire_date.isoformat(),
                probation_start_date=hire_date.isoformat(),
                probation_end_date=(hire_date + datetime.timedelta(days=90)).isoformat(),
                manager_id=manager_id,
                worker_workday_id=worker_wid,
                home_country=country,
                rehire=is_rehire,
                job_entry_dt=hire_date.isoformat(),
                supervisory_organization=dept_id,
                location_name=loc["name"],
            )

            self.all_events.append(event)
            self.active_employees[emp_id] = event

            self.position_assignments.append({
                "position_id": pos_id,
                "effective_date": hire_date.isoformat(),
                "sup_org_id": dept_id,
                "reason": "New Position",
                "worker_type": wt["type"],
                "worker_sub_type": wt["sub_type"],
                "job_profile_id": jp["Job_Profile_ID"],
                "job_title": jp["Job_Profile_Name"],
                "business_title": jp["Job_Profile_Name"],
                "time_type": tt["name"],
                "location_id": loc["id"],
                # INT6032/CR: new fields
                "pay_rate_type": "Salary",
                "scheduled_weekly_hours": tt["hours"],
                "default_weekly_hours": 37.5,
                "scheduled_fte": tt["fte"],
                "employee_type": wt["sub_type"],
                "shift_number": 0,
                "work_space": "",
                "exclude_from_headcount": "1" if wt["type"] == "Contingent Worker" else "0",
            })

            self._gen_profile(emp_id, worker_wid, hire_date, country, loc, grade_id)

    def _gen_profile(self, emp_id: str, worker_wid: str, hire_date: datetime.date,
                     country: str, location: Dict, grade_id: str):
        """Generate an employee profile record (INT6031)."""
        faker = self.faker_ca if country == "CA" else self.faker_us

        gender = utils.weighted_choice_from_tuples(self.rng, config.GENDER_DISTRIBUTION)
        if gender == "Male":
            first_name = faker.first_name_male()
        elif gender == "Female":
            first_name = faker.first_name_female()
        else:
            first_name = faker.first_name()
        last_name = faker.last_name()

        dob = utils.generate_dob(self.rng, hire_date)
        age = utils.compute_age(dob, hire_date)
        generation = self._generation_from_birth_year(dob.year)
        race = utils.weighted_choice_from_tuples(self.rng, config.RACE_ETHNICITY_DISTRIBUTION)

        preferred_first = first_name if self.rng.random() > 0.1 else faker.first_name()
        legal_full = f"{first_name} {last_name}"
        preferred_full = f"{preferred_first} {last_name}"

        # CR5: Generate address_line_1 and address_line_2
        street_num = self.rng.randint(1, 9999)
        street_names = ["Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Park", "Lake",
                        "Hill", "River", "Bay", "King", "Queen", "Front", "Market"]
        street_types = ["St", "Ave", "Blvd", "Dr", "Rd", "Way", "Ln", "Cres"]
        address_line_1 = f"{street_num} {self.rng.choice(street_names)} {self.rng.choice(street_types)}"
        address_line_2 = self.rng.choice(["", "", "", f"Apt {self.rng.randint(1, 999)}", f"Unit {self.rng.randint(1, 50)}"])

        profile = EmployeeProfile(
            employee_id=emp_id,
            worker_workday_id=worker_wid,
            date_of_birth=dob,
            gender=gender,
            gender_identity=gender if gender in ("Male", "Female") else "Not Disclosed",
            race_ethnicity=race,
            indigenous="Yes" if race == "Indigenous" else "No",
            legal_first_name=first_name,
            legal_full_name=legal_full,
            legal_full_name_formatted=f"{last_name}, {first_name}",
            last_name=last_name,
            preferred_first_name=preferred_first,
            preferred_full_name=preferred_full,
            preferred_full_name_formatted=f"{last_name}, {preferred_first}",
            primary_work_email=f"{first_name.lower()}.{last_name.lower()}@warlab.com",
            secondary_work_email="",
            home_address_city=location.get("city", ""),
            home_address_region=location.get("region", ""),
            home_address_region_name=location.get("region_name", ""),
            home_address_country=country,
            home_address_country_name="Canada" if country == "CA" else "United States",
            home_address_postal_code=location.get("postal", ""),
            address_line_1=address_line_1,    # CR5
            address_line_2=address_line_2,    # CR5
            military_status=self.rng.choice(["Not Applicable", "Not Applicable", "Veteran", "Active Duty"]),
            sexual_orientation="Not Disclosed",
            bank_of_west_id="",
            enterprise_id=f"E{emp_id}",
            junior_senior=self.rng.choice(["", "Jr", "Sr", ""]),
            product_sector_group=self.rng.choice(["", "Retail", "Institutional", "Wealth", ""]),
            preferred_language="French" if location.get("region") == "QC" else "English",
            bonus_equity_retirement_date="",
            class_year=str(hire_date.year)[2:4],
            admin_fte=1.0,
            consolidated_title=config.GRADES[min(int(grade_id[1:]) - 1, len(config.GRADES) - 1)]["name"],
            generation=generation,
            pensionable_yrs_of_service=0.0,
        )
        self.profiles.append(profile)

    def _generation_from_birth_year(self, birth_year: int) -> str:
        for gen_name, start, end, _ in config.GENERATION_BANDS:
            if start <= birth_year <= end:
                return gen_name
        return "Unknown"

    def _pick_job_profile_for_grade(self, grade_id: str) -> Optional[Dict]:
        profiles = self.jp_by_grade.get(grade_id, [])
        if profiles:
            return self.rng.choice(profiles)
        return self.rng.choice(self.jp_list) if self.jp_list else None

    def _pick_manager(self, employee_grade_id: str) -> str:
        grade_idx = int(employee_grade_id[1:]) - 1
        min_mgr_grade_idx = grade_idx + 1

        candidates = []
        for emp_id, evt in self.active_employees.items():
            mgr_grade_idx = int(evt.grade_id[1:]) - 1
            if mgr_grade_idx >= min_mgr_grade_idx:
                candidates.append(emp_id)

        if candidates:
            return self.rng.choice(candidates[:100])
        return ""

    def _clone_event(self, source: EmployeeEvent, new_date: datetime.date) -> EmployeeEvent:
        import copy
        event = copy.deepcopy(source)
        event.effective_date = new_date
        event.entry_datetime = utils.entry_date_from_effective(self.rng, new_date)
        event.transaction_wid = utils.generate_wid(self.rng)
        event.sequence_number = source.sequence_number + 1
        return event

    def generate_rescinded(self):
        """Mark ~1.5% of transactions as rescinded."""
        print("  Generating rescinded transactions...")
        n_rescind = int(len(self.all_events) * config.RESCIND_RATE)
        candidates = [e for e in self.all_events if e.transaction_type not in ("Hire",)]
        self.rng.shuffle(candidates)

        for event in candidates[:n_rescind]:
            self.rescinded_wids.append({
                "workday_id": event.transaction_wid,
                "idp_table": self._table_code_for_type(event.transaction_type),
                "rescinded_moment": event.entry_datetime.isoformat(),
            })

        print(f"    INT270 Rescinded: {len(self.rescinded_wids)} rows")

    def _table_code_for_type(self, txn_type: str) -> str:
        if txn_type in ("Hire", "Termination", "Change Job", "Data Change",
                        "Leave of Absence", "Return from Leave"):
            return self.rng.choice(["INT095E", "INT096", "INT098"])
        return "INT095E"
