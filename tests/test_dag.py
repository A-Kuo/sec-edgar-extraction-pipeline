"""
Tests for dags/edgar_pipeline.py

Because apache-airflow requires Python ≤ 3.12 and this environment uses
Python 3.14, all Airflow classes are replaced with lightweight mocks injected
into sys.modules BEFORE the dag module is imported.  The mocks faithfully
replicate the ``>>`` operator chaining and trigger_rule plumbing so every
structural assertion is meaningful.

Covers:
  - DAG has the correct 7 task IDs
  - Linear dependency chain (1 → 2 → … → 6)
  - All pipeline tasks are upstream of send_alerts_on_failure
  - send_alerts_on_failure uses TriggerRule.ONE_FAILED
  - Task callables can be invoked in MOCK_EDGAR mode
  - AirflowSkipException raised on empty facts (validate stage)
  - Mock fixture data integrity (required keys, required facts)
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Inject mock Airflow into sys.modules BEFORE importing the dag module.
# This must happen at the module level so the imports at the top of
# dags/edgar_pipeline.py resolve to our mocks.
# ---------------------------------------------------------------------------

os.environ["MOCK_EDGAR"] = "true"
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")


# ── Exceptions ──────────────────────────────────────────────────────────────

class AirflowSkipException(Exception):
    pass


class AirflowFailException(Exception):
    pass


# ── TriggerRule ─────────────────────────────────────────────────────────────

class TriggerRule:
    ONE_FAILED = "one_failed"
    ALL_SUCCESS = "all_success"
    ALL_DONE = "all_done"


# ── PythonOperator ───────────────────────────────────────────────────────────

class PythonOperator:
    """Minimal PythonOperator stub tracking task graph edges."""

    _active_dag: "DAG | None" = None

    def __init__(
        self,
        task_id: str,
        python_callable,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ) -> None:
        self.task_id = task_id
        self.python_callable = python_callable
        self.trigger_rule = trigger_rule
        self._upstream: list[PythonOperator] = []
        self._downstream: list[PythonOperator] = []
        if PythonOperator._active_dag is not None:
            PythonOperator._active_dag._tasks.append(self)

    @property
    def upstream_list(self) -> list[PythonOperator]:
        return self._upstream

    @property
    def downstream_list(self) -> list[PythonOperator]:
        return self._downstream

    # self >> other  (chains tasks left → right)
    def __rshift__(self, other):
        targets = other if isinstance(other, list) else [other]
        for t in targets:
            if t not in self._downstream:
                self._downstream.append(t)
            if self not in t._upstream:
                t._upstream.append(self)
        return other  # enables a >> b >> c chaining

    # [a, b, c] >> self  (Python calls self.__rrshift__([a, b, c]))
    def __rrshift__(self, other):
        sources = other if isinstance(other, list) else [other]
        for s in sources:
            if self not in s._downstream:
                s._downstream.append(self)
            if s not in self._upstream:
                self._upstream.append(s)
        return self


# ── DAG ─────────────────────────────────────────────────────────────────────

class DAG:
    def __init__(self, dag_id: str, **kwargs) -> None:
        self.dag_id = dag_id
        self.schedule_interval = kwargs.get("schedule_interval")
        self.catchup = kwargs.get("catchup", True)
        self.max_active_runs = kwargs.get("max_active_runs", 16)
        self.default_args = kwargs.get("default_args", {})
        self.tags = kwargs.get("tags", [])
        self._tasks: list[PythonOperator] = []

    def __enter__(self) -> "DAG":
        PythonOperator._active_dag = self
        return self

    def __exit__(self, *args) -> None:
        PythonOperator._active_dag = None

    def get_task(self, task_id: str) -> PythonOperator | None:
        return next((t for t in self._tasks if t.task_id == task_id), None)

    @property
    def task_ids(self) -> list[str]:
        return [t.task_id for t in self._tasks]


# ── Inject into sys.modules ──────────────────────────────────────────────────

def _make_mod(**attrs):
    m = MagicMock()
    for k, v in attrs.items():
        setattr(m, k, v)
    return m


sys.modules["airflow"] = _make_mod(DAG=DAG)
sys.modules["airflow.exceptions"] = _make_mod(
    AirflowSkipException=AirflowSkipException,
    AirflowFailException=AirflowFailException,
)
sys.modules["airflow.operators"] = MagicMock()
sys.modules["airflow.operators.python"] = _make_mod(PythonOperator=PythonOperator)
sys.modules["airflow.utils"] = MagicMock()
sys.modules["airflow.utils.trigger_rule"] = _make_mod(TriggerRule=TriggerRule)

# Now it is safe to import the DAG module.
import dags.edgar_pipeline as dag_module  # noqa: E402

import pytest  # noqa: E402

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

EXPECTED_TASK_IDS = {
    "fetch_new_filings",
    "download_raw_documents",
    "parse_xbrl_facts",
    "validate_quality_gates",
    "load_to_warehouse",
    "update_audit_trail",
    "send_alerts_on_failure",
}

LINEAR_CHAIN = [
    "fetch_new_filings",
    "download_raw_documents",
    "parse_xbrl_facts",
    "validate_quality_gates",
    "load_to_warehouse",
    "update_audit_trail",
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dag():
    return dag_module.dag


# ---------------------------------------------------------------------------
# 1. DAG structure
# ---------------------------------------------------------------------------


class TestDAGStructure:
    def test_dag_id(self, dag):
        assert dag.dag_id == "edgar_pipeline"

    def test_task_count(self, dag):
        assert len(dag.task_ids) == 7, (
            f"Expected 7 tasks, got {len(dag.task_ids)}: {sorted(dag.task_ids)}"
        )

    def test_all_expected_task_ids_present(self, dag):
        assert set(dag.task_ids) == EXPECTED_TASK_IDS

    def test_dag_has_schedule(self, dag):
        assert dag.schedule_interval is not None

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_max_active_runs_is_one(self, dag):
        assert dag.max_active_runs == 1


# ---------------------------------------------------------------------------
# 2. Task dependency chain
# ---------------------------------------------------------------------------


class TestTaskDependencies:
    def test_linear_chain_downstream(self, dag):
        for i in range(len(LINEAR_CHAIN) - 1):
            up_id = LINEAR_CHAIN[i]
            down_id = LINEAR_CHAIN[i + 1]
            task = dag.get_task(up_id)
            downstream_ids = {t.task_id for t in task.downstream_list}
            assert down_id in downstream_ids, (
                f"{down_id} not downstream of {up_id}; got {downstream_ids}"
            )

    def test_fetch_has_no_upstream(self, dag):
        task = dag.get_task("fetch_new_filings")
        assert len(task.upstream_list) == 0

    def test_all_pipeline_tasks_upstream_of_alert(self, dag):
        alert = dag.get_task("send_alerts_on_failure")
        upstream_ids = {t.task_id for t in alert.upstream_list}
        for task_id in LINEAR_CHAIN:
            assert task_id in upstream_ids, (
                f"{task_id} not upstream of send_alerts_on_failure; got {upstream_ids}"
            )

    def test_alert_trigger_rule_is_one_failed(self, dag):
        alert = dag.get_task("send_alerts_on_failure")
        assert alert.trigger_rule == TriggerRule.ONE_FAILED


# ---------------------------------------------------------------------------
# 3. Task callable invocations in MOCK_EDGAR mode
# ---------------------------------------------------------------------------


def _make_context(run_id: str = "test-run-mock") -> dict:
    """Build a minimal fake Airflow task context."""
    ti = MagicMock()
    ti.xcom_pull.return_value = dag_module._MOCK_FILINGS
    dag_run = MagicMock()
    dag_run.get_task_instances.return_value = []
    return {"run_id": run_id, "ti": ti, "dag_run": dag_run}


class TestTaskCallablesInMockMode:
    def test_fetch_new_filings_returns_list(self):
        ctx = _make_context()
        result = dag_module.fetch_new_filings(**ctx)
        assert isinstance(result, list)
        assert len(result) > 0
        assert result[0]["filing_type"] in ("10-K", "10-Q")

    def test_fetch_pushes_to_xcom(self):
        ctx = _make_context()
        dag_module.fetch_new_filings(**ctx)
        ctx["ti"].xcom_push.assert_called_with(key="filings", value=dag_module._MOCK_FILINGS)

    def test_download_returns_accession_list(self):
        ctx = _make_context()
        ctx["ti"].xcom_pull.return_value = dag_module._MOCK_FILINGS
        result = dag_module.download_raw_documents(**ctx)
        assert isinstance(result, list)
        assert all(isinstance(a, str) for a in result)

    def test_parse_xbrl_returns_facts(self):
        ctx = _make_context()
        ctx["ti"].xcom_pull.return_value = dag_module._MOCK_FILINGS
        result = dag_module.parse_xbrl_facts(**ctx)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_validate_quality_gates_passes_with_mock_facts(self):
        ctx = _make_context()
        ctx["ti"].xcom_pull.return_value = dag_module._MOCK_FACTS
        dag_module.validate_quality_gates(**ctx)  # Should not raise

    def test_validate_raises_skip_on_empty_facts(self):
        ctx = _make_context()
        ctx["ti"].xcom_pull.return_value = []
        with pytest.raises(AirflowSkipException):
            dag_module.validate_quality_gates(**ctx)

    def test_load_to_warehouse_returns_count(self):
        ctx = _make_context()
        ctx["ti"].xcom_pull.return_value = dag_module._MOCK_FACTS
        count = dag_module.load_to_warehouse(**ctx)
        assert count == len(dag_module._MOCK_FACTS)

    def test_update_audit_trail_does_not_raise(self):
        ctx = _make_context()
        dag_module.update_audit_trail(**ctx)

    def test_send_alerts_does_not_raise_in_mock_mode(self):
        ctx = _make_context()
        dag_module.send_alerts_on_failure(**ctx)


# ---------------------------------------------------------------------------
# 4. Mock fixture integrity
# ---------------------------------------------------------------------------


class TestMockFixtures:
    def test_mock_filings_have_required_keys(self):
        required = {"accession_number", "cik", "ticker", "filing_type", "filing_date"}
        for f in dag_module._MOCK_FILINGS:
            assert required <= set(f.keys()), f"Mock filing missing: {required - set(f.keys())}"

    def test_mock_facts_match_schema_columns(self):
        required = {"accession_number", "fact_name", "fact_value", "unit", "period_end"}
        for fact in dag_module._MOCK_FACTS:
            assert required <= set(fact.keys()), f"Mock fact missing: {required - set(fact.keys())}"

    def test_mock_facts_include_required_facts(self):
        from src.quality import REQUIRED_FACTS

        present = {f["fact_name"] for f in dag_module._MOCK_FACTS}
        for req in REQUIRED_FACTS:
            assert req in present, f"REQUIRED_FACT {req!r} not in mock facts"

    def test_mock_filings_are_10k_or_10q(self):
        for f in dag_module._MOCK_FILINGS:
            assert f["filing_type"] in ("10-K", "10-Q"), (
                f"Unexpected filing type: {f['filing_type']}"
            )
