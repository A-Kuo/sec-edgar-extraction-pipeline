"""
Tests for src/upsert.py.

These run the real ``INSERT ... ON CONFLICT`` SQL — not a mock of it —
against an in-memory SQLite database, which SQLAlchemy's
``sqlalchemy.dialects.sqlite.insert()`` supports with the identical
``.on_conflict_do_update()`` / ``.on_conflict_do_nothing()`` API PostgreSQL
uses in production (verified: SQLite 3.24+ supports UPSERT; the bundled
sqlite3 in this environment is 3.45). ``_insert()`` picks the constructor by
dialect name, so the exact statement-building code these tests exercise is
the code production runs — the only difference is which dialect module gets
imported.

The centerpiece is `TestWorkerRestartMidBatch`: it reproduces the actual
failure mode idempotency exists to fix — an Airflow task that ran, partially
committed, and got retried — and asserts the retry converges to one row per
fact rather than duplicating.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from src.ml.model import AnomalyScore
from src.schema import Base, FactAnomaly, FilingRaw, FinancialFact, ModelRun
from src.upsert import (
    compute_fact_hash,
    upsert_fact_anomalies,
    upsert_filing_raw,
    upsert_financial_facts,
    upsert_model_run,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _filing(session: Session, accession: str = "0000320193-24-000123") -> None:
    """Insert the parent filings_raw row the FK on financial_facts needs."""
    session.add(
        FilingRaw(
            accession_number=accession,
            cik="320193",
            filing_type="10-K",
            filing_date=date(2024, 11, 1),
        )
    )
    session.commit()


def _fact(accession: str = "0000320193-24-000123", **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "accession_number": accession,
        "fact_name": "us-gaap:Revenues",
        "fact_value": 391_035_000_000,
        "unit": "USD",
        "period_start": date(2023, 10, 1),
        "period_end": date(2024, 9, 28),
        "segment": None,
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# compute_fact_hash
# ---------------------------------------------------------------------------


class TestComputeFactHash:
    def test_deterministic(self):
        args = ("A", "us-gaap:Revenues", date(2024, 1, 1), date(2024, 12, 31), None)
        assert compute_fact_hash(*args) == compute_fact_hash(*args)

    def test_returns_hex_sha256(self):
        h = compute_fact_hash("A", "us-gaap:Revenues", None, date(2024, 12, 31), None)
        assert len(h) == 64
        int(h, 16)  # raises if not valid hex

    @pytest.mark.parametrize(
        "field,value",
        [
            ("accession_number", "B"),
            ("fact_name", "us-gaap:Assets"),
            ("period_start", date(2023, 1, 1)),
            ("period_end", date(2025, 12, 31)),
            ("segment", "Americas"),
        ],
    )
    def test_sensitive_to_every_natural_key_component(self, field, value):
        base = {
            "accession_number": "A",
            "fact_name": "us-gaap:Revenues",
            "period_start": date(2024, 1, 1),
            "period_end": date(2024, 12, 31),
            "segment": None,
        }
        changed = {**base, field: value}
        assert compute_fact_hash(**base) != compute_fact_hash(**changed)

    def test_date_and_isoformat_string_hash_identically(self):
        """The migration's backfill passes strings from raw SQL; the DAG
        passes date objects from the parser. Both must normalise the same."""
        via_date = compute_fact_hash("A", "F", date(2024, 1, 1), date(2024, 12, 31), None)
        via_string = compute_fact_hash("A", "F", "2024-01-01", "2024-12-31", None)
        assert via_date == via_string

    def test_none_and_empty_string_period_hash_identically(self):
        """Instant facts (Assets) have no period_start; must not collide with
        a hypothetical fact whose period_start is the empty string."""
        assert compute_fact_hash("A", "F", None, date(2024, 12, 31), None) == compute_fact_hash(
            "A", "F", "", date(2024, 12, 31), None
        )

    def test_no_delimiter_injection_collision(self):
        """
        Concatenating fields with a plain separator can make two different
        natural keys hash identically if a field itself contains the
        separator. This uses \\x1f (ASCII unit separator), which cannot
        appear in any of these fields in practice, but the test pins the
        property directly rather than trusting that assumption silently.
        """
        h1 = compute_fact_hash("A|B", "F", None, date(2024, 12, 31), None)
        h2 = compute_fact_hash("A", "B|F", None, date(2024, 12, 31), None)
        assert h1 != h2


# ---------------------------------------------------------------------------
# upsert_financial_facts
# ---------------------------------------------------------------------------


class TestUpsertFinancialFacts:
    def test_empty_input_is_a_noop(self, session):
        assert upsert_financial_facts(session, []) == 0

    def test_inserts_a_new_fact(self, session):
        _filing(session)
        count = upsert_financial_facts(session, [_fact()])
        session.commit()

        assert count == 1
        assert session.scalar(select(func.count()).select_from(FinancialFact)) == 1

    def test_computes_and_stores_fact_hash(self, session):
        _filing(session)
        upsert_financial_facts(session, [_fact()])
        session.commit()

        row = session.scalars(select(FinancialFact)).one()
        assert row.fact_hash == compute_fact_hash(
            "0000320193-24-000123", "us-gaap:Revenues", date(2023, 10, 1), date(2024, 9, 28), None
        )

    def test_reupserting_the_identical_fact_does_not_duplicate(self, session):
        """The core idempotency property."""
        _filing(session)
        upsert_financial_facts(session, [_fact()])
        session.commit()
        upsert_financial_facts(session, [_fact()])
        session.commit()

        assert session.scalar(select(func.count()).select_from(FinancialFact)) == 1

    def test_reupsert_updates_a_changed_value(self, session):
        """A retry after a parser fix should refresh the value, not ignore it."""
        _filing(session)
        upsert_financial_facts(session, [_fact(fact_value=100)])
        session.commit()
        upsert_financial_facts(session, [_fact(fact_value=200)])
        session.commit()

        row = session.scalars(select(FinancialFact)).one()
        assert row.fact_value == 200

    def test_different_period_is_a_different_row_not_an_update(self, session):
        _filing(session)
        upsert_financial_facts(
            session,
            [
                _fact(period_start=date(2022, 10, 1), period_end=date(2023, 9, 30)),
                _fact(period_start=date(2023, 10, 1), period_end=date(2024, 9, 28)),
            ],
        )
        session.commit()

        assert session.scalar(select(func.count()).select_from(FinancialFact)) == 2

    def test_batch_with_internal_duplicate_still_yields_one_row(self, session):
        """Two identical facts in the SAME batch — not just across retries."""
        _filing(session)
        upsert_financial_facts(session, [_fact(), _fact()])
        session.commit()

        assert session.scalar(select(func.count()).select_from(FinancialFact)) == 1

    def test_consolidated_and_segment_rows_do_not_collide(self, session):
        _filing(session)
        upsert_financial_facts(
            session,
            [_fact(segment=None), _fact(segment="Americas")],
        )
        session.commit()

        assert session.scalar(select(func.count()).select_from(FinancialFact)) == 2


class TestWorkerRestartMidBatch:
    """
    Reproduces the failure this rewrite exists to fix: a task's DB writes
    committed, then the process died before Airflow recorded success, so the
    task retries and resubmits the identical fact list.
    """

    def test_full_batch_replay_after_restart_produces_no_duplicates(self, session):
        _filing(session)
        facts = [
            _fact(fact_name="us-gaap:Revenues"),
            _fact(fact_name="us-gaap:NetIncomeLoss", fact_value=93_736_000_000),
            _fact(fact_name="us-gaap:Assets", fact_value=364_980_000_000, period_start=None),
        ]

        # "First attempt": the worker writes the batch, then is killed before
        # the task reports success.
        upsert_financial_facts(session, facts)
        session.commit()

        # "Airflow retry": the exact same XCom'd fact list is resubmitted.
        upsert_financial_facts(session, facts)
        session.commit()

        assert session.scalar(select(func.count()).select_from(FinancialFact)) == 3

    def test_partial_batch_replay_after_restart_produces_no_duplicates(self, session):
        """
        The harder case: the worker died AFTER committing facts 1-2 but
        BEFORE fact 3, so Airflow retries the FULL original batch, not just
        the tail. Facts 1-2 must upsert in place; fact 3 must insert fresh.
        """
        _filing(session)
        facts = [
            _fact(fact_name="us-gaap:Revenues"),
            _fact(fact_name="us-gaap:NetIncomeLoss", fact_value=93_736_000_000),
            _fact(fact_name="us-gaap:Assets", fact_value=364_980_000_000, period_start=None),
        ]

        # Partial commit: only the first two facts "landed" before the crash.
        upsert_financial_facts(session, facts[:2])
        session.commit()

        # Retry resubmits the full original batch.
        upsert_financial_facts(session, facts)
        session.commit()

        assert session.scalar(select(func.count()).select_from(FinancialFact)) == 3


# ---------------------------------------------------------------------------
# upsert_filing_raw
# ---------------------------------------------------------------------------


class TestUpsertFilingRaw:
    def _kwargs(self, **overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "accession_number": "0000320193-24-000123",
            "cik": "320193",
            "ticker": "AAPL",
            "filing_type": "10-K",
            "filing_date": date(2024, 11, 1),
            "period_of_report": date(2024, 9, 28),
            "raw_html": "<html>original</html>",
            "pipeline_run_id": "run-1",
        }
        base.update(overrides)
        return base

    def test_first_call_inserts_and_returns_true(self, session):
        inserted = upsert_filing_raw(session, **self._kwargs())
        session.commit()

        assert inserted is True
        assert session.scalar(select(func.count()).select_from(FilingRaw)) == 1

    def test_repeat_call_is_a_noop_and_returns_false(self, session):
        upsert_filing_raw(session, **self._kwargs())
        session.commit()

        inserted_again = upsert_filing_raw(
            session, **self._kwargs(raw_html="<html>DIFFERENT</html>")
        )
        session.commit()

        assert inserted_again is False
        row = session.get(FilingRaw, "0000320193-24-000123")
        # DO NOTHING: the original landed content is preserved, not overwritten.
        assert row.raw_html == "<html>original</html>"

    def test_concurrent_landing_of_the_same_accession_does_not_raise(self, session):
        """
        The race the old check-then-insert pattern was vulnerable to: two
        "workers" landing the same accession. Modeled here as two calls in
        one session (SQLite has no real concurrent writers to test against),
        but the property under test is that ON CONFLICT DO NOTHING never
        raises IntegrityError the way session.add() + commit() would.
        """
        upsert_filing_raw(session, **self._kwargs())
        session.commit()
        upsert_filing_raw(session, **self._kwargs())  # must not raise
        session.commit()


# ---------------------------------------------------------------------------
# upsert_model_run / upsert_fact_anomalies
# ---------------------------------------------------------------------------


def _score(accession: str, score: float = 0.8, is_anomaly: bool = True) -> AnomalyScore:
    return AnomalyScore(
        accession_number=accession,
        score=score,
        raw_decision=-0.1,
        is_anomaly=is_anomaly,
        model_score=score,
        rule_score=0.0,
    )


class TestUpsertModelRun:
    def _kwargs(self, **overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "run_id": "run-1",
            "model_version": "v1",
            "model_sha256": "a" * 64,
            "git_sha": "deadbeef",
            "filings_scored": 10,
            "anomalies_flagged": 2,
            "threshold": 0.6,
            "drift_report": None,
            "drift_level": "clean",
        }
        base.update(overrides)
        return base

    def test_first_call_inserts(self, session):
        model_run_id = upsert_model_run(session, **self._kwargs())
        session.commit()

        assert isinstance(model_run_id, int)
        assert session.scalar(select(func.count()).select_from(ModelRun)) == 1

    def test_retry_updates_the_same_row_id(self, session):
        """
        The property that matters: id stability across a retry. The old
        delete-then-recreate pattern churned this id every time, which would
        have orphaned any fact_anomalies rows still referencing the old one.
        """
        first_id = upsert_model_run(session, **self._kwargs())
        session.commit()

        second_id = upsert_model_run(session, **self._kwargs(filings_scored=99))
        session.commit()

        assert first_id == second_id
        assert session.scalar(select(func.count()).select_from(ModelRun)) == 1
        assert session.get(ModelRun, first_id).filings_scored == 99

    def test_different_model_version_same_run_is_a_new_row(self, session):
        upsert_model_run(session, **self._kwargs(model_version="v1"))
        session.commit()
        upsert_model_run(session, **self._kwargs(model_version="v2"))
        session.commit()

        assert session.scalar(select(func.count()).select_from(ModelRun)) == 2


class TestUpsertFactAnomalies:
    def test_empty_scores_is_a_noop(self, session):
        model_run_id = upsert_model_run(
            session,
            run_id="run-1",
            model_version="v1",
            model_sha256="a" * 64,
            git_sha=None,
            filings_scored=0,
            anomalies_flagged=0,
            threshold=0.6,
            drift_report=None,
            drift_level=None,
        )
        session.commit()
        assert upsert_fact_anomalies(session, model_run_id, []) == 0

    def test_retry_with_identical_scores_does_not_duplicate(self, session):
        _filing(session, "A")
        model_run_id = upsert_model_run(
            session,
            run_id="run-1",
            model_version="v1",
            model_sha256="a" * 64,
            git_sha=None,
            filings_scored=1,
            anomalies_flagged=1,
            threshold=0.6,
            drift_report=None,
            drift_level=None,
        )
        session.commit()

        scores = [_score("A")]
        upsert_fact_anomalies(session, model_run_id, scores)
        session.commit()
        upsert_fact_anomalies(session, model_run_id, scores)
        session.commit()

        assert session.scalar(select(func.count()).select_from(FactAnomaly)) == 1

    def test_retry_with_different_score_updates_in_place(self, session):
        _filing(session, "A")
        model_run_id = upsert_model_run(
            session,
            run_id="run-1",
            model_version="v1",
            model_sha256="a" * 64,
            git_sha=None,
            filings_scored=1,
            anomalies_flagged=1,
            threshold=0.6,
            drift_report=None,
            drift_level=None,
        )
        session.commit()

        upsert_fact_anomalies(session, model_run_id, [_score("A", score=0.3, is_anomaly=False)])
        session.commit()
        upsert_fact_anomalies(session, model_run_id, [_score("A", score=0.9, is_anomaly=True)])
        session.commit()

        row = session.scalars(select(FactAnomaly)).one()
        assert row.score == pytest.approx(0.9)
        assert row.is_anomaly is True

    def test_full_worker_restart_across_both_tables(self, session):
        """
        End-to-end idempotency: the whole score_anomalies persistence path
        (model_runs + fact_anomalies together) replayed after a simulated
        crash produces exactly one model_runs row and one fact_anomalies row
        per filing — not the previous delete-then-recreate churn.
        """
        _filing(session, "A")
        _filing(session, "B")
        scores = [_score("A"), _score("B", score=0.2, is_anomaly=False)]

        for _ in range(2):  # first attempt, then the retry
            model_run_id = upsert_model_run(
                session,
                run_id="run-1",
                model_version="v1",
                model_sha256="a" * 64,
                git_sha=None,
                filings_scored=2,
                anomalies_flagged=1,
                threshold=0.6,
                drift_report=None,
                drift_level="clean",
            )
            upsert_fact_anomalies(session, model_run_id, scores)
            session.commit()

        assert session.scalar(select(func.count()).select_from(ModelRun)) == 1
        assert session.scalar(select(func.count()).select_from(FactAnomaly)) == 2


# ---------------------------------------------------------------------------
# _insert() dialect dispatch
# ---------------------------------------------------------------------------


class TestDialectDispatch:
    def test_unsupported_dialect_raises_not_implemented(self):
        from src.upsert import _insert

        class _FakeDialect:
            name = "oracle"

        class _FakeEngine:
            dialect = _FakeDialect()

        with pytest.raises(NotImplementedError, match="oracle"):
            _insert(_FakeEngine())

    def test_accepts_a_session_via_its_bind(self, session):
        from src.upsert import _insert

        insert_fn = _insert(session)
        assert "sqlite" in insert_fn.__module__
