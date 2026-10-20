"""Criterion: ability to stop a workflow on job failure.

Default trigger rule (all_success) means a failed job marks everything
downstream `upstream_failed`, so the workflow stops there. Trigger with
`simulate_failure=true` to see it.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, Param, task

from include.alerts import notify_failure
from include.job_helpers import run_with_outcome

with DAG(
    dag_id="stop_on_failure_workflow",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"simulate_failure": Param(False, type="boolean")},
    default_args={"on_failure_callback": notify_failure},
    tags=["beneva", "failure-handling"],
):

    @task
    def job_1():
        return run_with_outcome("job_1")

    @task
    def job_2(params=None):  # TODO: replace with the real job
        return run_with_outcome("job_2", "failed" if params["simulate_failure"] else "success")

    @task
    def job_3():
        return run_with_outcome("job_3")

    @task
    def job_4():
        return run_with_outcome("job_4")

    job_1() >> job_2() >> job_3() >> job_4()
