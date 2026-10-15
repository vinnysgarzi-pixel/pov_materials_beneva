# Beneva – Airflow Success-Criteria Workflows

An Astro CLI project (**Astro Runtime 3.3-9, Airflow 3.3.2**) that demonstrates, one workflow per criterion, how Apache Airflow covers the job-scheduling requirements Beneva supplied (Automic-style capabilities).

This is a hand-off deliverable, not a demo: all environment-specific values (connections, hosts, ARNs, emails, real job bodies) are generic placeholders marked `TODO`. See [TODO checklist](#todo-checklist).

---

## Contents

1. [Quick start](#quick-start)
2. [Project layout](#project-layout)
3. [Success criteria → DAG map](#success-criteria--dag-map)
4. [How each DAG proves its criterion](#how-each-dag-proves-its-criterion)
5. [Calendars, holidays and custom calendars](#calendars-holidays-and-custom-calendars)
6. [The `include/` directory](#the-include-directory)
7. [Assumptions to confirm](#assumptions-to-confirm)
8. [TODO checklist](#todo-checklist)
9. [Testing](#testing)

---

## Quick start

```bash
astro dev start          # requires Docker; UI at http://localhost:8080
```

`airflow_settings.yaml` is loaded automatically by `astro dev start` and creates the sample Variables and Connections (placeholder values).

Unit tests do not need Docker:

```bash
python -m venv .venv && source .venv/bin/activate
pip install "apache-airflow==3.3.2" apache-airflow-providers-http apache-airflow-providers-amazon holidays pytest
pytest
```

> Tests were developed against Airflow 3.0.6 locally; the project targets Runtime 3.3-9 (Airflow 3.3.2). Run `astro dev start` as the first check on the target version.

## Project layout

```
.
├── Dockerfile                 # Astro Runtime 3.3-9
├── requirements.txt           # holidays, http + amazon providers
├── packages.txt               # OS packages (empty)
├── airflow_settings.yaml      # local-dev Variables + Connections (placeholders)
├── pytest.ini                 # puts include/ and dags/ on the import path for tests
├── dags/                      # one file per criterion (see below)
├── include/                   # shared, non-DAG code and config (see below)
└── tests/                     # rule unit tests, DAG import tests, sensor test
```

## Success criteria → DAG map

| # | Beneva criterion | DAG id(s) | File |
|---|---|---|---|
| 1 | Stop a workflow on job failure | `stop_on_failure_workflow` | `dags/stop_on_failure_workflow.py` |
| 2 | Complex calendar scheduling rules | `calendar_rules_workflow` | `dags/calendar_rules_workflow.py` + `include/calendar_rules.py` |
| 3 | Different calendar rules for jobs in the same workflow | `calendar_rules_workflow` | same DAG as #2 |
| 4 | Continue a workflow regardless of a previous job's outcome | `continue_regardless_workflow` | `dags/continue_regardless_workflow.py` |
| 5 | Conditional dependencies between jobs | `conditional_dependencies_workflow` | `dags/conditional_dependencies_workflow.py` |
| 6 | Global variables shared across workflows | `global_variables_workflow_a`, `global_variables_workflow_b` | `dags/global_variables_workflows.py` |
| 7 | Scheduling objects (Automic EVNT equivalent) | `evnt_hourly_business_hours`, `evnt_daily_close`, `evnt_weekly_friday` (+ 4 `recurring_job_*` targets) | `dags/schedule_objects.py` + `include/schedule_objects.yaml` |
| 8 | Pass variables between jobs in a workflow | `pass_variables_workflow` | `dags/pass_variables_workflow.py` |
| 9 | Dependencies on workflows in other AWS accounts | `cross_account_remote_airflow`, `cross_account_s3_marker` | `dags/cross_account_dependencies.py` + `include/remote_sensors.py` |

Criterion 2 is four rules; each maps to a job in `calendar_rules_workflow`:

| Rule | Gate task → job | Rule function |
|---|---|---|
| Every Friday or the previous working day | `gate_job_friday` → `job_friday` | `friday_or_previous_working_day` |
| Before-last working day of the week | `gate_job_before_last_wd` → `job_before_last_wd` | `before_last_working_day_of_week` |
| Third working day of Jan, Apr, Jul, Oct | `gate_job_quarter_start` → `job_quarter_start` | `third_working_day_of_quarter_start` |
| 16 working days before end of month | `gate_job_month_end_minus16` → `job_month_end_minus16` | `sixteenth_working_day_before_month_end` |

---

## How each DAG proves its criterion

All sample jobs are placeholders. Many accept an `outcome` parameter (`success` / `failed` / `skipped`) via **Trigger DAG w/ config** in the UI so each behaviour can be exercised on demand.

### 1. Stop on failure — `stop_on_failure_workflow`

Chain: `job_1 → job_2 → job_3 → job_4`. Trigger with `simulate_failure = true`.

- **What happens:** `job_2` fails; Airflow's default trigger rule (`all_success`) marks `job_3` and `job_4` as `upstream_failed`. The workflow stops at the failed job and the DAG run ends in `failed`.
- **What to look for:** in the Grid view, `job_1` green, `job_2` red, `job_3`/`job_4` orange (`upstream_failed`) – they never executed.
- **Alerting:** the DAG sets `on_failure_callback=notify_failure` (from `include/alerts.py`), which reads the global alert email Variable (criterion 6).
- **Control:** with `simulate_failure = false` every job runs to completion.

### 2 & 3. Complex calendar rules, different rules per job — `calendar_rules_workflow`

- **How it works:** the DAG fires **every day at 06:00 Quebec time**. Each job is preceded by its own *calendar gate* (`calendar_gate(...)` in `include/gates.py`, a `ShortCircuitOperator`). The gate evaluates its rule against the run date; if the rule doesn't match, the gate (and the job after it) are **skipped**, otherwise the job runs.
- **Criterion 3:** the four jobs live in the *same* DAG/workflow but each is bound to a *different* rule, so different calendar rules apply within one workflow.
- **How to prove it for any date:** trigger manually with `as_of_date = 2026-04-02` (a Thursday before Good Friday). `gate_job_friday` passes (Thursday is the "previous working day" because Good Friday is a holiday) and `gate_job_before_last_wd` is skipped. With `as_of_date = 2026-04-01` (Wednesday) it is the reverse: `gate_job_before_last_wd` passes and `gate_job_friday` is skipped. The gate logs show e.g. `Rule 'friday_or_previous_working_day' on 2026-04-02 (Thursday) -> RUN`.
- **Rules are unit-tested** in `tests/test_calendar_rules.py` against known 2026 dates (see [Calendars](#calendars-holidays-and-custom-calendars)).

### 4. Continue regardless of outcome — `continue_regardless_workflow`

Chain: `job_1 → job_2_runs_regardless → job_3`. Param `outcome_of_job_1` ∈ `success | failed | skipped` (default `failed`).

- `job_2_runs_regardless` uses `trigger_rule=ALL_DONE`: it runs once `job_1` has *finished*, whatever the result.
- **Proof:** run the DAG three times, once per outcome; `job_2_runs_regardless` executes every time.
- `job_3` keeps the default rule, so it runs only because `job_2` succeeded – showing that "continue regardless" can be applied per edge, while the rest of the workflow keeps normal semantics.

### 5. Conditional dependencies — `conditional_dependencies_workflow`

Requirement: *job A runs only if B succeeds **and** C succeeds or fails.*

```
job_b ───────────────────────────┐
                                  ├──► job_a        (trigger_rule = all_success)
job_c ──► c_finished_any_outcome ─┘                   (c_finished... = all_done)
```

- `c_finished_any_outcome` (`ALL_DONE`) succeeds whenever `job_c` has finished with *any* outcome. `job_a` then depends on `[job_b, c_finished_any_outcome]` with `all_success`.
- **Proof matrix** (params `outcome_of_b`, `outcome_of_c`):

| B | C | Result for `job_a` |
|---|---|---|
| success | success | runs |
| success | failed | runs |
| success | skipped | runs |
| failed | any | `upstream_failed` (does not run) |
| skipped | any | skipped |

### 6. Global variables — `global_variables_workflow_a` / `_b`

- Two independent DAGs read the same Airflow Variables `global_alert_email` and `global_environment`.
- Each reads them two ways: a Jinja template in a `BashOperator` (`{{ var.value.global_alert_email }}`) and in code via `Variable.get(...)` in a `@task`.
- **Proof:** change the Variable once in *Admin → Variables*; the next run of **both** DAGs (and the failure callback in `include/alerts.py`) shows the new value. No DAG file edits or redeploys needed.
- Values are resolved at run time, never at DAG-parse time (avoids a metadata-DB call on every parse).
- For production, back Variables with a secrets manager (e.g. AWS Secrets Manager) – see TODOs.

### 7. Scheduling objects (EVNT equivalent) — `evnt_*` DAGs

In Automic, an EVNT defines *when* and *how often* and launches many jobs. Here, a **schedule object** is an entry in `include/schedule_objects.yaml`:

```yaml
- name: evnt_daily_close
  cron: "30 18 * * *"
  timezone: America/Toronto
  calendar_rule: every_working_day      # optional
  jobs: [recurring_job_report, recurring_job_archive]
```

- `dags/schedule_objects.py` reads the YAML and generates (a) one placeholder DAG per entry in `jobs:` (`schedule=None`) and (b) one DAG per schedule object, which fires on the cron, optionally passes a calendar gate, then uses `TriggerDagRunOperator` to launch **every** listed job.
- **Proof:** one schedule object → one cron → many recurring jobs. Edit the cron in one place and all its jobs re-time together. Adding a job to a schedule object is a one-line YAML change.
- The three samples show different frequencies: hourly during business hours Mon–Fri, daily on working days, and weekly on "Friday or previous working day".

### 8. Pass variables between jobs — `pass_variables_workflow`

- `produce` returns `{"file_name": ..., "row_count": ...}` (`multiple_outputs=True`, so each key is its own XCom).
- `consume` receives `file_name` and `row_count` as ordinary function arguments (TaskFlow wires the XCom for you).
- `consume_in_bash` pulls the same value with a template: `{{ ti.xcom_pull(task_ids='produce', key='file_name') }}`.
- **Proof:** the task logs show the values received; the *XCom* tab of `produce` shows what was stored. Note XComs are for small values (names, counts, paths) – pass large data by reference (S3 path), not by value.

### 9. Cross-AWS-account dependencies — `cross_account_remote_airflow`, `cross_account_s3_marker`

Two patterns are provided; choose per upstream system.

**A. `cross_account_remote_airflow`** – the upstream workflow runs in an Airflow deployment in another AWS account.
- `RemoteAirflowDagRunSensor` (`include/remote_sensors.py`) polls that deployment's REST API (`/api/v2/dags/{dag_id}/dagRuns?state=success&end_date_gte=...`) through an HTTP connection (`remote_airflow_account_b`) and succeeds when a matching successful run exists.
- Runs in `reschedule` mode, so it doesn't hold a worker slot between pokes.
- Network path and authentication between accounts are Beneva's decision (VPC peering, PrivateLink, API Gateway, or an allow-listed public endpoint).

**B. `cross_account_s3_marker`** – the upstream account writes a completion marker (e.g. `_SUCCESS`) to S3.
- A **deferrable** `S3KeySensor` checks for the marker using the `aws_cross_account` connection, whose `extra` contains `role_arn`: Airflow **assumes a role in the other account** (STS `AssumeRole`) to read the object. Being deferrable it frees the worker while waiting.
- Requires an IAM role in the upstream account that trusts the Airflow execution role and can `s3:GetObject`/`ListBucket` on the marker location.

**Proof:** both DAGs hold their downstream job until the cross-account signal exists. Because real accounts are needed to run them end-to-end, the repo proves the mechanism with `tests/test_remote_sensor.py` (mocked HTTP) and DAG-import tests; real validation happens once the TODO connections are filled in.

---

## Calendars, holidays and custom calendars

### What is a "working day"?

`include/calendar_rules.py` defines it in one function:

```
working day = Monday–Friday  AND  not a Quebec statutory holiday  AND  not in EXTRA_NON_WORKING_DAYS
```

Statutory holidays come from the open-source [`holidays`](https://pypi.org/project/holidays/) package: `holidays.Canada(subdiv="QC")`. For 2026 this yields:

| Date | Holiday |
|---|---|
| 2026-01-01 | New Year's Day |
| 2026-04-03 | Good Friday |
| 2026-05-18 | National Patriots' Day |
| 2026-06-24 | Saint-Jean-Baptiste Day |
| 2026-07-01 | Canada Day |
| 2026-09-07 | Labour Day |
| 2026-10-12 | Thanksgiving |
| 2026-12-25 | Christmas Day |

Notes: this list does **not** include Easter Monday, Dec 26, or Jan 2, and the package may differ from Beneva's actual business calendar (e.g. financial-sector or company-specific closures). **Beneva should confirm this list** and add any differences via the custom calendar mechanisms below.

### The rules

All rules are pure functions `date -> bool` registered in the `RULES` dict. They are independent of Airflow, so they are unit-tested directly.

| Rule key | Definition | Example (2026) |
|---|---|---|
| `friday_or_previous_working_day` | The Friday of the week; if Friday is not a working day, the closest earlier working day | Normal week: Fri. Good-Friday week: Thu 2026-04-02 |
| `before_last_working_day_of_week` | Second-to-last working day of the Mon–Fri week | Normal week: Thu. Good-Friday week: Wed 2026-04-01 |
| `third_working_day_of_quarter_start` | 3rd working day of Jan, Apr, Jul, Oct | Jan 6 (Jan 1 holiday; Jan 2, 5, 6), Apr 6 (Apr 1, 2, 6) |
| `sixteenth_working_day_before_month_end` | Counting back from the last working day of the month (as day 1), the 16th working day | See interpretation below |
| `every_working_day` | Any working day | Used by `evnt_daily_close` |

**Interpretation of "16 working days before the end of the month":** the last working day of the month is counted as day 1, so the target is the 16th working day counting back (i.e. 15 working days before the last working day). If Beneva means 16 working days *before* the last working day, change `make_working_days_before_month_end(16)` to `(17)` in `include/calendar_rules.py` (one-line change) and update the test.

### How a gate applies a rule at run time

`include/gates.py::calendar_gate(task_id, rule_name)` returns a `ShortCircuitOperator`. It computes the run date in **America/Toronto** (from the run's logical date, or the `as_of_date` param if provided), calls `RULES[rule_name](date)`, and:
- `True` → downstream tasks run;
- `False` → downstream tasks are skipped (not failed).

Skipped runs on non-matching days are expected and show as skipped (pink) in the UI.

### Adding a custom rule

1. In `include/calendar_rules.py`, write a function `(date) -> bool` (or a factory like `make_nth_working_day_of_months`).
2. Add it to the `RULES` dict under a name.
3. Use it in a DAG: `calendar_gate("gate_my_job", "my_rule") >> my_job`, **or** reference it in `include/schedule_objects.yaml` as `calendar_rule: my_rule`.
4. Add a test in `tests/test_calendar_rules.py`.

Examples of ready-made factories: `make_nth_working_day_of_months(n, months)` (e.g. "first working day of March and September" → `(1, (3, 9))`), `make_working_days_before_month_end(n)`.

### Custom holiday / company calendars

| Need | How |
|---|---|
| A few extra closure days (company closure, bridge day) | Add dates to `EXTRA_NON_WORKING_DAYS` in `include/calendar_rules.py` |
| A different holiday set (another province / country, or financial-market calendar) | Change `holidays.Canada(subdiv="QC", ...)` in `_holidays()` – e.g. `holidays.Canada(subdiv="ON")`, `holidays.financial_holidays("XNYS")` |
| Calendars that differ per job/workflow (e.g. a Quebec job and an Ontario job) | Create a second set of rule functions (or parameterise factories with a calendar object) and register each under its own key in `RULES` |
| Business-owned calendar maintained outside code | Load dates from a file in `include/` (e.g. `holidays.yaml`) or an Airflow Variable and merge into `EXTRA_NON_WORKING_DAYS` – maintainers then change data, not code |
| Different timezone | Change `TIMEZONE` in `include/gates.py` |

Remember `_holidays()` is cached per year (`lru_cache`); a restart/new parse picks up changes.

### Why gates and not custom Airflow Timetables?

Airflow supports custom `Timetable` plugins that compute the next run date from a rule, so the DAG only *runs* on matching days. This project uses **daily DAG + per-job gate** instead because:
- different jobs in the *same* DAG can follow different rules (criterion 3) – a timetable applies to the whole DAG;
- no plugin registration, so simpler to deploy and test;
- the gate decision is logged and visible per job.

Trade-off: the DAG creates a run every day, with non-matching jobs shown as skipped. If Beneva prefers DAG runs *only* on matching days, the same rule functions in `calendar_rules.py` can be wrapped in a custom Timetable.

---

## The `include/` directory

`include/` is the Astro convention for **everything a DAG needs that isn't itself a DAG**: shared Python modules, SQL, config files, templates, etc.

**Why it exists / what it's good for**
- **Keeps `dags/` clean.** The scheduler parses every file in `dags/` every few seconds; helpers and data files there add parse cost and clutter. Only DAG definitions belong in `dags/`.
- **Reuse.** Logic written once (calendar rules, alerting, sensors) is imported by many DAGs, so a fix or change applies everywhere.
- **Testability.** Plain Python with no Airflow dependency (like `calendar_rules.py`) can be unit-tested in milliseconds.
- **Config as data.** Things like `schedule_objects.yaml` let non-developers change schedules without touching DAG code.
- **Deployed with the project.** Astro copies `include/` into the image alongside `dags/`, and it is on the Python path in Astro Runtime so DAGs can `import` from it directly (`from calendar_rules import RULES`).

**What's in it**

| File | Purpose |
|---|---|
| `calendar_rules.py` | Working-day calendar (Quebec holidays + custom closures) and all calendar rule functions; the `RULES` registry |
| `gates.py` | `calendar_gate()` – turns a rule into a ShortCircuit task; run-date and timezone logic; `as_of_date` override |
| `alerts.py` | `notify_failure` callback; reads the global `global_alert_email` Variable |
| `job_helpers.py` | `run_with_outcome()` – placeholder job body that simulates success / failed / skipped. Replace/remove when real jobs exist |
| `remote_sensors.py` | `RemoteAirflowDagRunSensor` for cross-account dependencies on another Airflow |
| `schedule_objects.yaml` | Scheduling-object definitions (EVNT equivalent) |

**Guidelines:** put reusable code, sensors/operators, SQL, and static config here; keep secrets out of it (use Connections/Variables/secrets backend); keep DAG definitions in `dags/`.

---

## Assumptions to confirm

- "Working day" = Mon–Fri excluding Quebec statutory holidays per the `holidays` package (see list above).
- "16 working days before the end of the month" counts the last working day as day 1.
- "Before-last working day of the week" = second-to-last working day of the Mon–Fri week.
- Timezone is `America/Toronto` (Quebec).
- Cross-account networking, authentication and IAM trust are infrastructure outside this repo.
- Sample jobs are placeholders (`EmptyOperator`, `BashOperator echo`, simulated outcomes).

---

## TODO checklist

Every item below is tagged `TODO` in the code or config.

### Environment & configuration

| # | Where | What to do |
|---|---|---|
| 1 | `airflow_settings.yaml` → variable `global_alert_email` | Set the real alert distribution list (currently `TODO-alerts@example.com`); also the default in `include/alerts.py` |
| 2 | `airflow_settings.yaml` → variable `global_environment` | Set `dev` / `qa` / `prod` per environment |
| 3 | `airflow_settings.yaml` header | Choose how Variables/Connections are managed in deployed environments (Airflow UI, Astro environment manager, or a secrets backend such as AWS Secrets Manager). `airflow_settings.yaml` is for **local dev only** |
| 4 | `include/gates.py` → `TIMEZONE` | Confirm `America/Toronto` |
| 5 | `include/calendar_rules.py` → `EXTRA_NON_WORKING_DAYS` and header note | Confirm the statutory holiday list and add Beneva company closures |
| 6 | `include/calendar_rules.py` → rule definitions | Confirm the 16-working-days interpretation and the before-last-working-day definition |

### Alerting

| # | Where | What to do |
|---|---|---|
| 7 | `include/alerts.py` | Replace the `print` with a real channel (SMTP/SES `EmailOperator`, Teams, PagerDuty, …) |

### Cross-account (criterion 9)

| # | Where | What to do |
|---|---|---|
| 8 | `airflow_settings.yaml` → `remote_airflow_account_b` `conn_host` | URL of the Airflow deployment in the other account, reachable from this one |
| 9 | same connection `conn_extra` | Real authentication (e.g. bearer token) – store in a secrets backend, never commit |
| 10 | `include/remote_sensors.py` docstring | Decide the cross-account network path (VPC peering, PrivateLink, API Gateway, allow-listed endpoint) and auth scheme |
| 11 | `dags/cross_account_dependencies.py` → `remote_dag_id` | The upstream DAG id |
| 12 | same file → `api_version` | `"v2"` for Airflow 3 upstream; `"v1"` for Airflow 2 / MWAA |
| 13 | same file → `end_date_gte` | Adjust the freshness window to the upstream's cadence (currently "finished today") |
| 14 | `airflow_settings.yaml` → `aws_cross_account` `conn_extra` | Real `role_arn` (`arn:aws:iam::<ACCOUNT>:role/<ROLE>`) and region; create the IAM role in the upstream account trusting this account's Airflow role |
| 15 | `dags/cross_account_dependencies.py` → `bucket_name`, `bucket_key` | Upstream bucket and marker key pattern |

### Replace placeholder jobs

| # | Where | What to do |
|---|---|---|
| 16 | `dags/stop_on_failure_workflow.py` | Real jobs in place of `job_1..job_4` |
| 17 | `dags/calendar_rules_workflow.py` | Real jobs in place of the `EmptyOperator`s |
| 18 | `dags/conditional_dependencies_workflow.py` | Real job A (and B, C) |
| 19 | `dags/pass_variables_workflow.py` | Real producing job and its outputs |
| 20 | `dags/cross_account_dependencies.py` | Real `downstream_job` in both DAGs |
| 21 | `include/schedule_objects.yaml` / `dags/schedule_objects.py` | Real schedule objects, crons, and the real job DAG ids (remove the generated `recurring_job_*` placeholders once real DAGs exist) |
| 22 | `include/job_helpers.py` | Remove once sample jobs are replaced |

---

## Testing

```bash
pytest
```

| Test file | Covers |
|---|---|
| `tests/test_calendar_rules.py` | Each calendar rule against known 2026 dates, including Good Friday and Labour Day edge cases; exactly one match per week/month where expected |
| `tests/test_dag_integrity.py` | All DAGs import without errors; expected DAG ids exist; schedule objects trigger all their jobs; the calendar DAG uses four distinct rules |
| `tests/test_remote_sensor.py` | `RemoteAirflowDagRunSensor` poke logic with a mocked HTTP hook |

Behavioural checks (stop-on-failure, trigger rules, conditional dependencies) are exercised by triggering the DAGs with the parameters described above and observing the Grid view.
