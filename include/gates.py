"""Calendar gate: a ShortCircuit task that lets downstream jobs run only on matching days."""
from __future__ import annotations

from datetime import date
from zoneinfo import ZoneInfo

from airflow.providers.standard.operators.python import ShortCircuitOperator

from calendar_rules import RULES

TIMEZONE = ZoneInfo("America/Toronto")  # Quebec; TODO: confirm with Beneva


def run_date(context) -> date:
    """Date the run is for. ``params.as_of_date`` (YYYY-MM-DD) overrides it for manual testing."""
    override = context["params"].get("as_of_date")
    if override:
        return date.fromisoformat(override)
    dag_run = context["dag_run"]
    ts = context.get("logical_date") or getattr(dag_run, "run_after", None) or dag_run.start_date
    return ts.astimezone(TIMEZONE).date()


def _check(rule_name: str, **context) -> bool:
    d = run_date(context)
    ok = RULES[rule_name](d)
    print(f"Rule '{rule_name}' on {d} ({d:%A}) -> {'RUN' if ok else 'SKIP'}")
    return ok


def calendar_gate(task_id: str, rule_name: str) -> ShortCircuitOperator:
    """Short-circuits (skips downstream) unless ``rule_name`` matches the run date."""
    if rule_name not in RULES:
        raise ValueError(f"Unknown calendar rule {rule_name!r}; known: {sorted(RULES)}")
    return ShortCircuitOperator(
        task_id=task_id,
        python_callable=_check,
        op_kwargs={"rule_name": rule_name},
    )
