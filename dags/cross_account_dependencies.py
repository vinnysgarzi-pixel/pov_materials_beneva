"""Criterion: dependencies on workflows in other AWS accounts.

Two patterns, one DAG each. All connection ids / buckets / ARNs are TODOs.

A) cross_account_remote_airflow: wait for a successful run of a DAG on an
   Airflow deployment in another account (REST API).
B) cross_account_s3_marker: the upstream account writes a completion marker
   object to S3; we check it by assuming a cross-account IAM role
   (role_arn in the `aws_cross_account` connection extra).
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from include.remote_sensors import RemoteAirflowDagRunSensor

COMMON = dict(start_date=datetime(2026, 1, 1), catchup=False, tags=["beneva", "cross-account"])

with DAG(dag_id="cross_account_remote_airflow", schedule="0 7 * * *", **COMMON):
    wait = RemoteAirflowDagRunSensor(
        task_id="wait_for_remote_dag",
        remote_dag_id="TODO_remote_dag_id",            # TODO
        http_conn_id="remote_airflow_account_b",       # TODO: connection to the other account
        api_version="v2",                              # TODO: "v1" if remote is Airflow 2 / MWAA
        # Only count runs that finished today (run date, UTC midnight). TODO: adjust to upstream cadence.
        end_date_gte="{{ dag_run.run_after.strftime('%Y-%m-%dT00:00:00Z') }}",
        mode="reschedule",
        poke_interval=300,
        timeout=6 * 3600,
    )
    wait >> EmptyOperator(task_id="downstream_job")    # TODO: real downstream job

with DAG(dag_id="cross_account_s3_marker", schedule="0 7 * * *", **COMMON):
    wait = S3KeySensor(
        task_id="wait_for_marker",
        bucket_name="TODO-upstream-account-bucket",    # TODO
        bucket_key="TODO/prefix/{{ dag_run.run_after.strftime('%Y-%m-%d') }}/_SUCCESS",  # TODO
        aws_conn_id="aws_cross_account",               # TODO: connection with role_arn of the other account
        deferrable=True,                               # no worker slot held while waiting
        poke_interval=300,
        timeout=6 * 3600,
    )
    wait >> EmptyOperator(task_id="downstream_job")    # TODO: real downstream job
