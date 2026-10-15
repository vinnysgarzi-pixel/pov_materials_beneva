"""Placeholder job body used by the sample workflows. TODO: replace with real jobs."""
from __future__ import annotations

from airflow.exceptions import AirflowFailException, AirflowSkipException


def run_with_outcome(name: str, outcome: str = "success") -> str:
    """Simulate a job finishing with ``outcome``: success | failed | skipped."""
    print(f"{name}: simulated outcome = {outcome}")
    if outcome == "failed":
        raise AirflowFailException(f"{name} failed (simulated)")
    if outcome == "skipped":
        raise AirflowSkipException(f"{name} skipped (simulated)")
    return outcome
