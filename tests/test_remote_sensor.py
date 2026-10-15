from unittest import mock

from remote_sensors import RemoteAirflowDagRunSensor


def _poke(runs):
    sensor = RemoteAirflowDagRunSensor(task_id="t", remote_dag_id="d", end_date_gte="2026-01-01T00:00:00Z")
    with mock.patch("remote_sensors.HttpHook") as hook:
        hook.return_value.run.return_value.json.return_value = {"dag_runs": runs}
        result = sensor.poke({})
        return result, hook.return_value.run.call_args


def test_succeeds_when_remote_run_exists():
    ok, call = _poke([{"dag_run_id": "x"}])
    assert ok
    assert call.kwargs["endpoint"] == "api/v2/dags/d/dagRuns"
    assert call.kwargs["data"]["state"] == "success"


def test_waits_when_no_remote_run():
    assert _poke([])[0] is False
