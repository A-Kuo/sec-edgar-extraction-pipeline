"""
Tests for the idempotent upsert in dags/edgar_pipeline.py.

Covers the Week 1 priority fix: ``_upsert_facts`` (called from the
``load_to_warehouse`` task) must be safe to call more than once for the
same ``(accession_number, fact_name, period_end, segment)`` — whether
because Airflow retried the task (``retries=2`` in ``default_args``) or
because an amendment was re-processed — without creating duplicate rows
in ``financial_facts``.

Uses a real file-based SQLite database (not MOCK_EDGAR) so the actual
``INSERT ... ON CONFLICT DO UPDATE`` path is exercised, not the mock
branch. SQLite and Postgres expose the same ``on_conflict_do_update`` API
in SQLAlchemy, so this is a faithful proxy for the production Postgres
behavior.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# Minimal Airflow mocks, mirroring tests/test_dag.py, so this file can import
# dags.edgar_pipeline without apache-airflow installed.
# ---------------------------------------------------------------------------

os.environ["MOCK_EDGAR"] = "true"
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")


def _install_airflow_mocks() -> None:
    if "airflow" in sys.modules and hasattr(sys.modules.get("airflow"), "DAG"):
        return

    class _AirflowSkipException(Exception):
        pass

    class _AirflowFailException(Exception):
        pass

    class _PythonOperator:
        def __init__(self, task_id, python_callable, trigger_rule="all_success", **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.trigger_rule = trigger_rule

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


_install_airflow_mocks()

import dags.edgar_pipeline as dag_module  # noqa: E402
from src.schema import Base, FilingRaw, FinancialFact  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sqlite_db(tmp_path, monkeypatch):
    """Point dags.edgar_pipeline.DATABASE_URL at a fresh on-disk SQLite DB."""
    db_path = tmp_path / "idempotency_test.db"
    db_url = f"sqlite:///{db_path}"
    monkeypatch.setattr(dag_module, "DATABASE_URL", db_url)

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(
            FilingRaw(
                accession_number="0000320193-24-000123",
                cik="320193",
                filing_type="10-K",
                filing_date=date(2024, 11, 1),
            )
        )
        session.commit()

    engine.dispose()
    return db_url


def _sample_fact(value: str = "391035000000") -> dict:
    return {
        "accession_number": "0000320193-24-000123",
        "fact_name": "us-gaap:Revenues",
        "fact_value": Decimal(value),
        "unit": "USD",
        "period_start": date(2023, 10, 1),
        "period_end": date(2024, 9, 28),
        "segment": None,
    }


def _all_facts(db_url: str) -> list[FinancialFact]:
    with Session(create_engine(db_url)) as session:
        return list(session.scalars(select(FinancialFact)))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestUpsertFactsIdempotency:
    def test_single_call_inserts_row(self, sqlite_db):
        loaded = dag_module._upsert_facts([_sample_fact()])
        assert loaded == 1

        rows = _all_facts(sqlite_db)
        assert len(rows) == 1
        assert rows[0].fact_value == Decimal("391035000000")

    def test_retry_with_identical_facts_does_not_duplicate(self, sqlite_db):
        """Simulates Airflow retrying load_to_warehouse after a transient failure."""
        dag_module._upsert_facts([_sample_fact()])
        dag_module._upsert_facts([_sample_fact()])  # retry of the same task

        rows = _all_facts(sqlite_db)
        assert len(rows) == 1, "Retrying the load stage must not create duplicate rows"

    def test_reprocessing_updates_value_in_place(self, sqlite_db):
        """Simulates a re-processed amendment correcting a previously loaded fact."""
        dag_module._upsert_facts([_sample_fact(value="391035000000")])
        dag_module._upsert_facts([_sample_fact(value="400000000000")])

        rows = _all_facts(sqlite_db)
        assert len(rows) == 1
        assert rows[0].fact_value == Decimal("400000000000")

    def test_different_period_end_inserts_separate_row(self, sqlite_db):
        """Sanity check: the unique constraint shouldn't be overly broad."""
        with Session(create_engine(sqlite_db)) as session:
            session.add(
                FilingRaw(
                    accession_number="0000320193-23-000077",
                    cik="320193",
                    filing_type="10-Q",
                    filing_date=date(2023, 8, 4),
                )
            )
            session.commit()

        fact_q3 = _sample_fact()
        fact_q3["accession_number"] = "0000320193-23-000077"
        fact_q3["period_end"] = date(2023, 7, 1)

        dag_module._upsert_facts([_sample_fact()])
        dag_module._upsert_facts([fact_q3])

        rows = _all_facts(sqlite_db)
        assert len(rows) == 2

    def test_empty_facts_list_is_a_noop(self, sqlite_db):
        assert dag_module._upsert_facts([]) == 0
        assert _all_facts(sqlite_db) == []

    def test_null_segment_and_period_end_still_dedupes(self, sqlite_db):
        """
        Regression test for the NULL-safety bug: a plain multi-column
        unique constraint would NOT catch this, because both Postgres and
        SQLite treat every NULL as distinct from every other NULL, and
        segment/period_end are NULL for the common case (consolidated
        facts with an unresolved period). The unique index must COALESCE
        these to sentinels for ON CONFLICT to actually fire here.
        """
        fact = _sample_fact()
        fact["segment"] = None
        fact["period_end"] = None

        dag_module._upsert_facts([fact])
        dag_module._upsert_facts([fact])  # retry with the same NULL-bearing row

        rows = _all_facts(sqlite_db)
        assert len(rows) == 1, (
            "Two identical facts with NULL segment/period_end must still "
            "dedupe — this is exactly the case a plain UniqueConstraint "
            "would silently fail to catch."
        )
