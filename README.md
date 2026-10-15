# Beneva – Airflow success-criteria workflows

Astro CLI project (Astro Runtime 3.3-9, Airflow 3.3.2) with one workflow per success criterion.
Everything marked `TODO` (connections, hosts, ARNs, emails, real job bodies,
holiday closures, runtime version) must be filled in by Beneva.

| Criterion | Where | How |
|---|---|---|
| Stop workflow on job failure | `dags/stop_on_failure_workflow.py` | default `all_success`; downstream becomes `upstream_failed`. Trigger with `simulate_failure=true`. |
| Complex calendar rules (Fri/prev working day, before-last working day of week, 3rd working day of Jan/Apr/Jul/Oct, 16 working days before month end) | `include/calendar_rules.py`, `include/gates.py` | Pure, unit-tested rules on the Quebec working-day calendar; daily DAG + per-job `ShortCircuit` gate. |
| Different calendar rules per job in one workflow | `dags/calendar_rules_workflow.py` | each job has its own gate. `as_of_date` param tests any date. |
| Continue regardless of previous outcome | `dags/continue_regardless_workflow.py` | `trigger_rule=ALL_DONE` (success / failed / skipped). |
| Conditional dependencies (A runs if B succeeds and C succeeds-or-fails) | `dags/conditional_dependencies_workflow.py` | `ALL_DONE` join on C, then `all_success` on [B, join]. |
| Global variables across workflows | `dags/global_variables_workflows.py`, `airflow_settings.yaml`, `include/alerts.py` | Airflow Variables (`global_alert_email`, `global_environment`). |
| Scheduling objects (Automic EVNT) | `include/schedule_objects.yaml`, `dags/schedule_objects.py` | one schedule object = one cron (+ optional calendar rule) triggering many jobs. |
| Pass variables between jobs | `dags/pass_variables_workflow.py` | XCom (TaskFlow + templates). |
| Dependencies on other AWS accounts | `dags/cross_account_dependencies.py`, `include/remote_sensors.py` | (A) poll remote Airflow REST API; (B) deferrable `S3KeySensor` using an assumed cross-account role. |

## Assumptions to confirm
- "Working day" = Mon–Fri excluding Quebec statutory holidays (`holidays` package, `QC`). Company closures: `EXTRA_NON_WORKING_DAYS`.
- "16 working days before month end" = the last working day counts as day 1 (i.e. 15 working days before it).
- "Before-last working day of the week" = second-to-last working day of the Mon–Fri week.
- Timezone `America/Toronto`.
- Cross-account networking/auth (VPC peering, PrivateLink, API Gateway, IAM trust policy) is outside the code.

## Run
```
astro dev start      # needs Docker
pytest               # rules + DAG integrity (pip install apache-airflow holidays pytest)
```
