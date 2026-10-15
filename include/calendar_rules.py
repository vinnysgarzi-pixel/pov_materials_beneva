"""Calendar rules on top of the Quebec statutory working-day calendar.

Every rule is a pure function ``(date) -> bool`` so it can be unit-tested
without Airflow. ``RULES`` maps a rule name to its function and is what the
DAGs (and ``schedule_objects.yaml``) reference.

A "working day" is Monday-Friday that is not a Quebec statutory holiday.
TODO: add Beneva-specific non-working days (company closures) to
``EXTRA_NON_WORKING_DAYS`` if they differ from the statutory calendar.
"""
from __future__ import annotations

import calendar
from datetime import date, timedelta
from functools import lru_cache
from typing import Callable

import holidays

EXTRA_NON_WORKING_DAYS: set[date] = set()  # TODO: Beneva company closures


@lru_cache(maxsize=None)
def _holidays(year: int) -> frozenset[date]:
    return frozenset(holidays.Canada(subdiv="QC", years=year).keys())


def is_working_day(d: date) -> bool:
    return d.weekday() < 5 and d not in _holidays(d.year) and d not in EXTRA_NON_WORKING_DAYS


def previous_working_day(d: date) -> date:
    """``d`` itself if it is a working day, otherwise the closest earlier one."""
    while not is_working_day(d):
        d -= timedelta(days=1)
    return d


def working_days_of_month(year: int, month: int) -> list[date]:
    last = calendar.monthrange(year, month)[1]
    return [date(year, month, n) for n in range(1, last + 1) if is_working_day(date(year, month, n))]


def working_days_of_week(d: date) -> list[date]:
    monday = d - timedelta(days=d.weekday())
    return [monday + timedelta(days=i) for i in range(5) if is_working_day(monday + timedelta(days=i))]


# --- Beneva criteria -------------------------------------------------------

def friday_or_previous_working_day(d: date) -> bool:
    """Every Friday, or the previous working day when that Friday is not one."""
    friday = d - timedelta(days=d.weekday()) + timedelta(days=4)
    return d == previous_working_day(friday)


def before_last_working_day_of_week(d: date) -> bool:
    """Second-to-last working day of the week (Thursday; Wednesday if Friday is off)."""
    days = working_days_of_week(d)
    return len(days) >= 2 and d == days[-2]


def make_nth_working_day_of_months(n: int, months: tuple[int, ...]) -> Callable[[date], bool]:
    """n-th working day of the given months (1-based)."""

    def rule(d: date) -> bool:
        if d.month not in months:
            return False
        days = working_days_of_month(d.year, d.month)
        return len(days) >= n and d == days[n - 1]

    return rule


def make_working_days_before_month_end(n: int) -> Callable[[date], bool]:
    """The n-th working day counting back from the last working day of the month.

    The last working day is day 1, so n=16 is 15 working days before it.
    """

    def rule(d: date) -> bool:
        days = working_days_of_month(d.year, d.month)
        return len(days) >= n and d == days[-n]

    return rule


third_working_day_of_quarter_start = make_nth_working_day_of_months(3, (1, 4, 7, 10))
sixteenth_working_day_before_month_end = make_working_days_before_month_end(16)

RULES: dict[str, Callable[[date], bool]] = {
    "friday_or_previous_working_day": friday_or_previous_working_day,
    "before_last_working_day_of_week": before_last_working_day_of_week,
    "third_working_day_of_quarter_start": third_working_day_of_quarter_start,
    "sixteenth_working_day_before_month_end": sixteenth_working_day_before_month_end,
    "every_working_day": is_working_day,
}
