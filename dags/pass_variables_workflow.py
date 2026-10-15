"""Criterion: pass variables between jobs of the same workflow (XCom)."""
from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id="pass_variables_workflow",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["beneva", "variables"],
):

    @task(multiple_outputs=True)
    def produce():  # TODO: replace with the real producing job
        return {"file_name": "extract_001.csv", "row_count": 1234}

    @task
    def consume(file_name: str, row_count: int):
        print(f"Received {file_name} with {row_count} rows")

    out = produce()
    consume(out["file_name"], out["row_count"])

    # Classic operators can pull the same values through templates.
    BashOperator(
        task_id="consume_in_bash",
        bash_command="echo file={{ ti.xcom_pull(task_ids='produce', key='file_name') }}",
    ).set_upstream(out.operator)
