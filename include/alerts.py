"""Shared failure callback; recipient comes from the global Variable `global_alert_email`."""
from __future__ import annotations

from airflow.sdk import Variable


def notify_failure(context) -> None:
    email = Variable.get("global_alert_email", default="TODO-alerts@example.com")
    ti = context["ti"]
    # TODO: replace the print with Beneva's channel (SES/SMTP via EmailOperator, Teams, PagerDuty...).
    print(f"ALERT -> {email}: {ti.dag_id}.{ti.task_id} failed")
