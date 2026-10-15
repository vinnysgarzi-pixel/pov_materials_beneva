import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags(dagbag):
    for dag_id in [
        "stop_on_failure_workflow", "calendar_rules_workflow", "continue_regardless_workflow",
        "conditional_dependencies_workflow", "global_variables_workflow_a", "global_variables_workflow_b",
        "pass_variables_workflow", "evnt_hourly_business_hours", "evnt_daily_close",
        "evnt_weekly_friday", "cross_account_remote_airflow", "cross_account_s3_marker",
    ]:
        assert dag_id in dagbag.dags


def test_schedule_object_triggers_all_its_jobs(dagbag):
    dag = dagbag.dags["evnt_hourly_business_hours"]
    assert {"trigger_recurring_job_extract", "trigger_recurring_job_load"} <= set(dag.task_ids)


def test_calendar_workflow_has_distinct_rules(dagbag):
    dag = dagbag.dags["calendar_rules_workflow"]
    rules = {dag.get_task(t).op_kwargs["rule_name"] for t in dag.task_ids if t.startswith("gate_")}
    assert len(rules) == 4
