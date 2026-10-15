"""Criterion: scheduling objects defining the frequency of multiple recurring jobs (Automic EVNT).

Edit include/schedule_objects.yaml: each schedule object becomes a DAG that
fires on its cron (optionally gated by a calendar rule) and triggers every
job listed. Changing one schedule object re-times all of its jobs at once.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from gates import calendar_gate

CONFIG = yaml.safe_load((Path(__file__).parent.parent / "include" / "schedule_objects.yaml").read_text())

# Placeholder target jobs (manually/EVNT-triggered only).
for job_id in CONFIG["jobs"]:
    with DAG(
        dag_id=job_id,
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["beneva", "scheduled-job"],
    ):
        BashOperator(task_id="run", bash_command=f"echo running {job_id}  # TODO: real job")

# One DAG per schedule object.
for obj in CONFIG["schedule_objects"]:
    with DAG(
        dag_id=obj["name"],
        schedule=CronTriggerTimetable(obj["cron"], timezone=obj.get("timezone", "UTC")),
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["beneva", "schedule-object"],
    ):
        start = EmptyOperator(task_id="start")
        if obj.get("calendar_rule"):
            start = calendar_gate("calendar_gate", obj["calendar_rule"]) >> start
        for job_id in obj["jobs"]:
            start >> TriggerDagRunOperator(
                task_id=f"trigger_{job_id}",
                trigger_dag_id=job_id,
                wait_for_completion=False,
            )
