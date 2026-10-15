"""Sensors for dependencies on workflows that live in OTHER AWS accounts."""
from __future__ import annotations

from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import BaseSensorOperator


class RemoteAirflowDagRunSensor(BaseSensorOperator):
    """Succeeds once a remote Airflow deployment has a successful run of ``remote_dag_id``.

    Talks to the remote Airflow REST API over an HTTP connection.
    TODO (Beneva): decide the network path between accounts (VPC peering,
    PrivateLink, API Gateway, public endpoint + allow-list) and the auth
    scheme. Auth headers are read from the connection's ``extra`` JSON.
    ``api_version``: Airflow 3 -> "v2" (``/api/v2``); Airflow 2 / MWAA -> "v1".
    """

    template_fields = ("remote_dag_id", "end_date_gte")

    def __init__(
        self,
        *,
        remote_dag_id: str,
        http_conn_id: str = "remote_airflow_account_b",
        api_version: str = "v2",
        end_date_gte: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.remote_dag_id = remote_dag_id
        self.http_conn_id = http_conn_id
        self.api_version = api_version
        self.end_date_gte = end_date_gte  # ISO timestamp; only runs finishing after this count

    def poke(self, context) -> bool:
        params = {"state": "success", "limit": 1}
        if self.end_date_gte:
            params["end_date_gte"] = self.end_date_gte
        hook = HttpHook(method="GET", http_conn_id=self.http_conn_id)
        resp = hook.run(
            endpoint=f"api/{self.api_version}/dags/{self.remote_dag_id}/dagRuns",
            data=params,
        )
        runs = resp.json().get("dag_runs", [])
        self.log.info("Remote %s: %d matching successful run(s)", self.remote_dag_id, len(runs))
        return bool(runs)
