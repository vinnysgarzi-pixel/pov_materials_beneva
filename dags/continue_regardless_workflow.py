"""Criterion: continue a workflow regardless of a previous job's outcome.

`trigger_rule=ALL_DONE` runs the task once upstream is finished, whether it
succeeded, failed or was skipped. Use `outcome_of_job_1` to try each case.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, Param, task
from airflow.utils.trigger_rule import TriggerRule

from include.job_helpers import run_with_outcome

with DAG(
    dag_id="continue_regardless_workflow",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"outcome_of_job_1": Param("failed", enum=["success", "failed", "skipped"])},
    tags=["beneva", "failure-handling"],
):

    @task
    def job_1(params=None):
        return run_with_outcome("job_1", params["outcome_of_job_1"])

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def job_2_runs_regardless():
        return run_with_outcome("job_2")

    @task
    def job_3():  # default rule: runs only because job_2 succeeded
        return run_with_outcome("job_3")

    job_1() >> job_2_runs_regardless() >> job_3()
