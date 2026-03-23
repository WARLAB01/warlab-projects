"""
utils.py - Shared utility functions for synthetic data generation.

Unchanged from V2.
"""

import csv
import hashlib
import os
import datetime
import random
from typing import List, Dict, Any, Optional, Tuple

from . import config


def get_rng(seed: int = None) -> random.Random:
    return random.Random(seed or config.SEED)


def generate_wid(rng: random.Random) -> str:
    return hashlib.md5(str(rng.random()).encode()).hexdigest()


def generate_employee_id(seq_num: int) -> str:
    return f"3{seq_num:07d}"


def generate_position_id(seq_num: int) -> str:
    return f"P-{seq_num:06d}"


def weighted_choice(rng: random.Random, items: List[Dict], weight_key: str = "weight") -> Dict:
    weights = [item[weight_key] for item in items]
    return rng.choices(items, weights=weights, k=1)[0]


def weighted_choice_from_tuples(rng: random.Random, items: List[Tuple]) -> Any:
    values = [item[0] for item in items]
    weights = [item[1] for item in items]
    return rng.choices(values, weights=weights, k=1)[0]


def weighted_choice_from_named_tuples(rng: random.Random, items: List[Tuple], weight_idx: int = -1) -> Tuple:
    weights = [item[weight_idx] for item in items]
    return rng.choices(items, weights=weights, k=1)[0]


def interpolate_headcount(target_date: datetime.date) -> int:
    anchors = config.GROWTH_ANCHORS

    if target_date <= anchors[0][0]:
        return anchors[0][1]
    if target_date >= anchors[-1][0]:
        return anchors[-1][1]

    for i in range(len(anchors) - 1):
        d1, h1 = anchors[i]
        d2, h2 = anchors[i + 1]
        if d1 <= target_date <= d2:
            days_total = (d2 - d1).days
            days_elapsed = (target_date - d1).days
            if days_total == 0:
                return h1
            ratio = days_elapsed / days_total
            return int(h1 + (h2 - h1) * ratio)

    return anchors[-1][1]


def month_end(year: int, month: int) -> datetime.date:
    if month == 12:
        return datetime.date(year, 12, 31)
    return datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)


def random_date_in_month(rng: random.Random, year: int, month: int,
                          earliest: datetime.date = None) -> datetime.date:
    first = datetime.date(year, month, 1)
    last = month_end(year, month)

    if earliest and earliest > first:
        first = earliest
    if first > last:
        return last

    days_range = (last - first).days
    return first + datetime.timedelta(days=rng.randint(0, max(0, days_range)))


def random_date_in_range(rng: random.Random, start: datetime.date,
                          end: datetime.date) -> datetime.date:
    if start >= end:
        return start
    days = (end - start).days
    return start + datetime.timedelta(days=rng.randint(0, days))


def entry_date_from_effective(rng: random.Random, effective_date: datetime.date) -> datetime.datetime:
    lag_days = rng.randint(0, 5)
    lag_hours = rng.randint(8, 17)
    lag_minutes = rng.randint(0, 59)
    lag_seconds = rng.randint(0, 59)

    entry_dt = datetime.datetime.combine(effective_date, datetime.time(0, 0, 0))
    entry_dt += datetime.timedelta(days=lag_days, hours=lag_hours, minutes=lag_minutes, seconds=lag_seconds)
    return entry_dt


def generate_dob(rng: random.Random, hire_date: datetime.date) -> datetime.date:
    gen = weighted_choice_from_named_tuples(rng, config.GENERATION_BANDS, weight_idx=3)
    birth_year = rng.randint(gen[1], gen[2])
    birth_month = rng.randint(1, 12)
    birth_day = rng.randint(1, 28)
    dob = datetime.date(birth_year, birth_month, birth_day)

    safe_day = min(hire_date.day, 28)
    min_dob = datetime.date(hire_date.year - 65, hire_date.month, safe_day)
    max_dob = datetime.date(hire_date.year - 18, hire_date.month, safe_day)

    if dob > max_dob:
        dob = datetime.date(max_dob.year, birth_month, min(birth_day, 28))
    if dob < min_dob:
        dob = datetime.date(min_dob.year, birth_month, min(birth_day, 28))

    return dob


def compute_age(dob: datetime.date, as_of: datetime.date) -> int:
    age = as_of.year - dob.year
    if (as_of.month, as_of.day) < (dob.month, dob.day):
        age -= 1
    return max(0, age)


def age_band(age: int) -> str:
    if age <= 19:
        return "19 and under"
    elif age <= 24:
        return "20 - 24"
    elif age <= 29:
        return "25 - 29"
    elif age <= 34:
        return "30 - 34"
    elif age <= 39:
        return "35 - 39"
    elif age <= 44:
        return "40 - 44"
    elif age <= 49:
        return "45 - 49"
    elif age <= 54:
        return "50 - 54"
    elif age <= 59:
        return "55 - 59"
    elif age >= 60:
        return "60 and over"
    else:
        return "Unknown Age"


def bool_to_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, float)):
        return "1" if val else "0"
    if isinstance(val, str):
        return val
    return str(val)


def none_to_empty(val) -> str:
    if val is None:
        return ""
    return str(val)


def write_csv(filepath: str, rows: List[Dict], fieldnames: List[str]):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            clean_row = {k: none_to_empty(v) for k, v in row.items()}
            writer.writerow(clean_row)
    return len(rows)


def feed_filename(feed_key: str) -> str:
    base = config.FEED_FILE_MAP[feed_key]
    return f"{base}.{config.FEED_TIMESTAMP}.csv"


def salary_for_grade(rng: random.Random, grade_id: str) -> float:
    grade = next((g for g in config.GRADES if g["id"] == grade_id), None)
    if not grade:
        return 50000.0

    salary = rng.gauss(grade["mid"], (grade["max"] - grade["min"]) / 4)
    salary = max(grade["min"], min(grade["max"], salary))
    return round(salary, 2)


def next_grade(current_grade_id: str, direction: str = "up") -> Optional[str]:
    grade_ids = [g["id"] for g in config.GRADES]
    try:
        idx = grade_ids.index(current_grade_id)
    except ValueError:
        return None

    if direction == "up" and idx < len(grade_ids) - 1:
        return grade_ids[idx + 1]
    elif direction == "down" and idx > 0:
        return grade_ids[idx - 1]
    return None


def location_for_company(rng: random.Random, company_id: str) -> Dict:
    country = config.COMPANY_COUNTRY.get(company_id, "CA")
    if country == "CA":
        locs = config.LOCATIONS_CA
    else:
        locs = config.LOCATIONS_US
    return weighted_choice(rng, locs)
