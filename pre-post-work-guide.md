# Enforcing Pre- and Post-Work on DAG Runs (Airflow + Astro)

**Requirement:** a set of jobs must run shared pre-work (e.g. validate an SSH connection) before they start, and optionally post-work (cleanup, audit) after, without copying that code into every DAG.

**Recommended approach:** three layers, each covering a different part of the problem.

| Layer | Mechanism | Purpose |
|---|---|---|
| 1. Execute | Airflow **setup / teardown tasks** | Run the pre/post work with the right failure semantics |
| 2. Reuse | **Shared helper** in `include/` (or a private package) | Write the check once; every DAG calls it |
| 3. Enforce | **Cluster policy** + CI test | Guarantee DAGs actually use it |

---

## 1. Setup and teardown tasks

Airflow has built-in task roles for this (`@setup`, `@teardown`, or `.as_setup()` / `.as_teardown()`):

- A **setup** task runs before the tasks in its scope. If it fails, the work tasks do not run, so a failed SSH validation stops the jobs.
- A **teardown** task runs after the scope completes, **even if a work task failed**. It is intended for cleanup (close tunnels, remove temp files) and audit/logging.
- By default a teardown failure does not fail the DAG run (configurable).
- Setup and teardown tasks are shown distinctly in the Airflow UI.

> Confirm exact behaviour for a failed setup on the target version (Airflow 3.3.2) during implementation.

## 2. Shared helper, written once

Put the logic in `include/` (the project's shared-code directory) and expose a single wrapper. Example shape:

```python
# include/prepost.py
@contextmanager
def ssh_guarded(ssh_conn_id: str):
    check = SSHValidateOperator(task_id="validate_ssh", ssh_conn_id=ssh_conn_id).as_setup()
    audit = EmptyOperator(task_id="post_audit").as_teardown(setups=check)
    with check >> audit:
        yield
```

Usage in any DAG, one line of boilerplate:

```python
with ssh_guarded("beneva_sftp"):
    job_1() >> job_2()
```

- `SSHValidateOperator` is a thin custom operator over the SSH provider's `SSHHook` that runs a trivial command and fails with a clear message.
- For stronger standardisation, offer a `@beneva_dag(...)` decorator or DAG factory that builds the DAG and injects the standard pre/post tasks automatically.
- Changing the check later means editing one file; every DAG picks it up.

**Distribution on Astro**

| Scope | How |
|---|---|
| One Airflow project | Keep helpers in `include/` |
| Many projects / deployments | Package helpers as a private Python package (internal PyPI or git URL) and add it to `requirements.txt`; update by bumping the version |

## 3. Enforcement

A helper only works if people use it. Two complementary guards:

**Cluster policy** (`airflow_local_settings.py`, runs at DAG parse time)
- `dag_policy(dag)` rejects a DAG that should have the check but doesn't, e.g. any DAG tagged `ssh` (or using an SSH operator) without a `validate_ssh` setup task. It raises `AirflowClusterPolicyViolation`, and the DAG appears as an import error instead of running unguarded.
- `task_policy` / `task_instance_mutation_hook` can apply defaults to every task (retries, pools, etc.).
- On Astro, place the file where the Astro docs specify for `airflow_local_settings.py` (typically `plugins/`).

**CI test**
- A `pytest` DagBag test applies the same rule, so violations fail in the pull request before deploy.

Policies are best at **verifying** that pre/post tasks exist; the shared helper is what **creates** them.

## Alternatives considered

| Option | Verdict |
|---|---|
| `on_execute_callback` / `on_success_callback` | Good for notifications; not a reliable gate (a failing callback does not cleanly block a task) |
| Custom base operator with `pre_execute` / `post_execute` | Good for per-task checks; does not cover a group of jobs sharing one check |
| Separate "preflight" DAG that triggers others | Works, but couples DAGs and is harder to trace; prefer in-DAG setup tasks |

## Summary

1. Implement the check as a **setup task** and cleanup/audit as a **teardown task**.
2. Wrap both in a **single helper or DAG factory** in `include/` (or a shared package).
3. Enforce usage with a **cluster policy** and a **CI test**.

This gives central control, one place to change the logic, no copy-paste across DAGs, and failures that stop dependent jobs while still guaranteeing cleanup.
