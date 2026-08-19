"""
python scripts/demo.py

Runs the edgar_pipeline DAG's task chain once, end-to-end, against
MOCK_EDGAR fixture data and prints what each stage produced: fetched
filings, downloaded accessions, parsed XBRL facts, quality-gate result,
and warehouse load count.

No live SEC network calls, no Postgres, no Redis, and no Airflow scheduler
required -- this is meant to be the one command a reviewer runs to see the
pipeline actually do something, in well under a minute.

If ``apache-airflow`` isn't installed, this script installs the same
lightweight stand-ins used by tests/test_load_idempotency.py so the DAG
module still imports cleanly.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

os.environ["MOCK_EDGAR"] = "true"
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _ensure_airflow_importable() -> None:
    """Install minimal airflow stand-ins if the real package isn't installed."""
    try:
        import airflow  # noqa: F401

        return
    except ImportError:
        pass

    from unittest.mock import MagicMock

    class _AirflowSkipException(Exception):
        pass

    class _AirflowFailException(Exception):
        pass

    class _PythonOperator:
        def __init__(self, task_id, python_callable, trigger_rule="all_success", **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable

        def __rshift__(self, other):
            return other

        def __rrshift__(self, other):
            return self

    class _DAG:
        def __init__(self, dag_id, **kwargs):
            self.dag_id = dag_id

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    def _make_mod(**attrs):
        m = MagicMock()
        for k, v in attrs.items():
            setattr(m, k, v)
        return m

    sys.modules["airflow"] = _make_mod(DAG=_DAG)
    sys.modules["airflow.exceptions"] = _make_mod(
        AirflowSkipException=_AirflowSkipException,
        AirflowFailException=_AirflowFailException,
    )
    sys.modules["airflow.operators"] = MagicMock()
    sys.modules["airflow.operators.python"] = _make_mod(PythonOperator=_PythonOperator)
    sys.modules["airflow.utils"] = MagicMock()
    sys.modules["airflow.utils.trigger_rule"] = _make_mod(
        TriggerRule=type("TriggerRule", (), {"ONE_FAILED": "one_failed"})
    )


_ensure_airflow_importable()

import dags.edgar_pipeline as dag_module  # noqa: E402


class _InMemoryTaskInstance:
    """Minimal stand-in for Airflow's TaskInstance XCom push/pull, backed by a dict."""

    def __init__(self) -> None:
        self._store: dict[str, object] = {}

    def xcom_push(self, key: str, value: object) -> None:
        self._store[key] = value

    def xcom_pull(self, key: str, task_ids: str | None = None) -> object:
        return self._store.get(key)


def _hr(title: str) -> None:
    print(f"\n{'-' * 64}\n{title}\n{'-' * 64}")


def _fmt(value: object) -> str:
    try:
        return f"{value:,}"
    except (TypeError, ValueError):
        return str(value)


def main() -> None:
    run_id = f"demo-{uuid.uuid4().hex[:8]}"
    context = {"run_id": run_id, "ti": _InMemoryTaskInstance(), "dag_run": None}

    print(f"SEC EDGAR pipeline demo -- run_id={run_id}")
    print("MOCK_EDGAR=true: fixture data only, no live network, no Postgres/Redis.\n")

    _hr("1. fetch_new_filings")
    filings = dag_module.fetch_new_filings(**context)
    for f in filings:
        print(f"  {f['accession_number']}  {f['filing_type']:5s} {f['ticker']:6s} filed {f['filing_date']}")

    _hr("2. download_raw_documents")
    accessions = dag_module.download_raw_documents(**context)
    print(f"  downloaded: {accessions}")

    _hr("3. parse_xbrl_facts")
    facts = dag_module.parse_xbrl_facts(**context)
    for fact in facts:
        print(f"  {fact['fact_name']:45s} {_fmt(fact['fact_value']):>18s}  {fact['unit']}")

    _hr("4. validate_quality_gates")
    dag_module.validate_quality_gates(**context)
    print("  completeness + PSI gates passed (src/quality.py)")

    _hr("5. load_to_warehouse")
    loaded = dag_module.load_to_warehouse(**context)
    print(f"  {loaded} fact row(s) loaded (idempotent UPSERT against real Postgres)")

    _hr("6. update_audit_trail")
    dag_module.update_audit_trail(**context)
    print(f"  audit trail updated for run_id={run_id}")

    print(f"\nDone -- full task chain ran end-to-end for run_id={run_id}.")
    print("For a real run against live data:")
    print("  docker-compose up -d && alembic upgrade head")
    print("  unset MOCK_EDGAR && airflow dags test edgar_pipeline 2024-01-15")


if __name__ == "__main__":
    main()
