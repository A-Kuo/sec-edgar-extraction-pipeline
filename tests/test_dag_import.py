"""
Real-Airflow DAG import validation.

Why this file exists
--------------------
``tests/test_dag.py`` injects mock Airflow classes into ``sys.modules`` before
importing the DAG module. That makes the structural assertions fast and
dependency-free, but it also means those tests pass even when the DAG cannot be
imported by Airflow at all. That is exactly what happened: an unbounded
``apache-airflow>=2.8.0`` pin resolved to Airflow 3.x, which removed the
``schedule_interval`` argument, and the suite stayed green at 131/131 while the
scheduler would have failed to parse the DAG.

These tests import the DAG in a **subprocess** using whatever Airflow is
actually installed. A subprocess is required because ``test_dag.py`` pollutes
``sys.modules`` at collection time and pytest shares one interpreter across the
run — an in-process import here would resolve to the mocks, reproducing the
original blind spot.

If Airflow is not installed, the tests skip rather than fail, so the unit suite
still runs in a minimal environment.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _airflow_installed() -> bool:
    result = subprocess.run(
        [sys.executable, "-c", "import airflow"],
        capture_output=True,
        cwd=REPO_ROOT,
    )
    return result.returncode == 0


requires_airflow = pytest.mark.skipif(
    not _airflow_installed(),
    reason="apache-airflow is not installed in this environment",
)


def _run_in_subprocess(code: str) -> subprocess.CompletedProcess[str]:
    """Execute *code* in a clean interpreter rooted at the repo, returning the result."""
    env = {
        **os.environ,
        "MOCK_EDGAR": "true",
        "PYTHONPATH": str(REPO_ROOT),
        # Keep Airflow from touching a real metastore or loading example DAGs.
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
    }
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=env,
        timeout=180,
    )


@requires_airflow
def test_dag_module_imports_against_installed_airflow() -> None:
    """The DAG module must import with no mocking, on the installed Airflow."""
    result = _run_in_subprocess(
        """
        import dags.edgar_pipeline as m
        assert m.dag.dag_id == "edgar_pipeline"
        print("OK")
        """
    )
    assert result.returncode == 0, (
        "dags/edgar_pipeline.py failed to import against the installed Airflow.\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )


@requires_airflow
def test_dag_exposes_expected_tasks() -> None:
    """Task IDs and the alert trigger rule must survive a real Airflow parse."""
    result = _run_in_subprocess(
        """
        import json
        import dags.edgar_pipeline as m

        alert = m.dag.get_task("send_alerts_on_failure")
        # Airflow 2 stores trigger_rule as a plain string ("one_failed");
        # Airflow 3 stores a TriggerRule enum whose str() is
        # "TriggerRule.ONE_FAILED". Normalise here rather than pin to either.
        rule = alert.trigger_rule
        rule_value = getattr(rule, "value", rule)
        print("__RESULT__" + json.dumps({
            "task_ids": sorted(t.task_id for t in m.dag.tasks),
            "alert_trigger_rule": str(rule_value),
            "max_active_runs": m.dag.max_active_runs,
        }))
        """
    )
    assert result.returncode == 0, result.stderr

    payload = json.loads(result.stdout.split("__RESULT__")[1].splitlines()[0])

    assert payload["task_ids"] == [
        "download_raw_documents",
        "fetch_new_filings",
        "load_to_warehouse",
        "parse_xbrl_facts",
        "score_anomalies",
        "send_alerts_on_failure",
        "update_audit_trail",
        "validate_quality_gates",
    ]
    assert payload["alert_trigger_rule"] == "one_failed"
    assert payload["max_active_runs"] == 1


@requires_airflow
def test_dag_has_no_import_errors_in_dagbag() -> None:
    """
    Load the dags/ folder through Airflow's own DagBag.

    This is the closest in-process equivalent to what the scheduler does, and it
    catches failures the plain import above would miss (bad default_args,
    duplicate task IDs, cycles).
    """
    result = _run_in_subprocess(
        """
        import json

        # Airflow 3 moved DagBag and dropped `include_examples` from its
        # constructor (the examples flag became a class attribute /
        # environment setting); Airflow 2 has it at the old path with the old
        # signature. Try the new location first, matching the shim pattern in
        # dags/edgar_pipeline.py itself.
        try:
            from airflow.dag_processing.dagbag import DagBag
            bag = DagBag(dag_folder="dags")
        except ImportError:
            from airflow.models.dagbag import DagBag
            bag = DagBag(dag_folder="dags", include_examples=False)

        print("__RESULT__" + json.dumps({
            "import_errors": {str(k): str(v) for k, v in bag.import_errors.items()},
            "dag_ids": sorted(bag.dag_ids),
        }))
        """
    )
    assert result.returncode == 0, result.stderr

    payload = json.loads(result.stdout.split("__RESULT__")[1].splitlines()[0])

    assert payload["import_errors"] == {}, f"DagBag import errors: {payload['import_errors']}"
    assert "edgar_pipeline" in payload["dag_ids"]
