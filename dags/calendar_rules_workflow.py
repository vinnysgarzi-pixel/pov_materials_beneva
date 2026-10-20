"""Criteria: complex calendar rules, and different calendar rules for jobs in the same workflow.

The DAG fires every day (Quebec time); each job sits behind its own calendar
gate (include/calendar_rules.py) so it runs only on matching days:

  job_friday            every Friday, or previous working day
  job_before_last_wd    before-last working day of the week
  job_quarter_start     3rd working day of Jan/Apr/Jul/Oct
  job_month_end_minus16 16 working days before end of month

Set the `as_of_date` param (YYYY-MM-DD) on a manual run to test any date.
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator  # TODO: replace with real jobs
from airflow.sdk import DAG, Param
from airflow.timetables.trigger import CronTriggerTimetable

from include.alerts import notify_failure
from include.gates import TIMEZONE, calendar_gate

JOBS = {
    "job_friday": "friday_or_previous_working_day",
    "job_before_last_wd": "before_last_working_day_of_week",
    "job_quarter_start": "third_working_day_of_quarter_start",
    "job_month_end_minus16": "sixteenth_working_day_before_month_end",
}

with DAG(
    dag_id="calendar_rules_workflow",
    schedule=CronTriggerTimetable("0 6 * * *", timezone=str(TIMEZONE)),  # gates decide per job
    start_date=datetime(2026, 1, 1, tzinfo=TIMEZONE),
    catchup=False,
    params={"as_of_date": Param(None, type=["null", "string"], description="YYYY-MM-DD override")},
    default_args={"on_failure_callback": notify_failure},
    tags=["beneva", "calendar"],
):
    for job_id, rule in JOBS.items():
        calendar_gate(f"gate_{job_id}", rule) >> EmptyOperator(task_id=job_id)
