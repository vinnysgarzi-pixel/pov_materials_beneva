"""Criterion: global variables shared across workflows (e.g. alert email).

Two independent workflows read the same Airflow Variables. Values are
resolved at run time (templates / in-task), never at DAG parse time.
Manage them in Admin > Variables or a secrets backend (see airflow_settings.yaml).
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable, task

for name in ("workflow_a", "workflow_b"):
    with DAG(
        dag_id=f"global_variables_{name}",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["beneva", "variables"],
    ):
        # Jinja: resolved when the task runs
        BashOperator(
            task_id="show_via_template",
            bash_command="echo env={{ var.value.global_environment }} alert={{ var.value.global_alert_email }}",
        )

        @task
        def show_via_code():
            print("alert email:", Variable.get("global_alert_email"))

        show_via_code()
