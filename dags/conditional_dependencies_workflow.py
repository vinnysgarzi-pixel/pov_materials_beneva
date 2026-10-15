"""Criterion: conditional dependencies, e.g. job A runs only if B succeeds AND C succeeds-or-fails.

B -------------------------\
                             >-- A   (all_success: B ok AND c_done ok)
C --> c_done (ALL_DONE) ----/

`c_done` always succeeds once C has finished (any outcome), so it expresses
"C succeeded or failed (or was skipped)" as a single success-gated edge.
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, task
from airflow.utils.trigger_rule import TriggerRule

from job_helpers import run_with_outcome

OUTCOMES = ["success", "failed", "skipped"]

with DAG(
    dag_id="conditional_dependencies_workflow",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "outcome_of_b": Param("success", enum=OUTCOMES),
        "outcome_of_c": Param("failed", enum=OUTCOMES),
    },
    tags=["beneva", "dependencies"],
):

    @task
    def job_b(params=None):
        return run_with_outcome("job_b", params["outcome_of_b"])

    @task
    def job_c(params=None):
        return run_with_outcome("job_c", params["outcome_of_c"])

    c_done = EmptyOperator(task_id="c_finished_any_outcome", trigger_rule=TriggerRule.ALL_DONE)

    @task  # TODO: replace with the real job A
    def job_a():
        return run_with_outcome("job_a")

    job_c() >> c_done
    [job_b(), c_done] >> job_a()
