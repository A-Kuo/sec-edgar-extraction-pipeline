"""
Smoke tests for src/schema.py.

The ORM models had 0% coverage: every existing test builds rows with a raw
``text()`` query rather than instantiating the SQLAlchemy classes, so nothing
ever imported the declarative mappings or exercised a relationship. These
tests build each model directly against an in-memory SQLite database, which
catches the class of bug integration tests miss — a bad column type, a foreign
key pointing at a table that no longer exists, a relationship whose
``back_populates`` name drifted from the other side.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.schema import (
    Base,
    FactAnomaly,
    FilingRaw,
    FilingVersion,
    FinancialFact,
    ModelRun,
    PipelineAudit,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


class TestFilingRawAndFacts:
    def test_insert_and_query_a_filing(self, session):
        filing = FilingRaw(
            accession_number="0000320193-24-000123",
            cik="0000320193",
            ticker="AAPL",
            filing_type="10-K",
            filing_date=date(2024, 11, 1),
        )
        session.add(filing)
        session.commit()

        fetched = session.get(FilingRaw, "0000320193-24-000123")
        assert fetched.ticker == "AAPL"
        assert fetched.ingested_at is not None  # server_default fired

    def test_facts_relationship_and_cascade_delete(self, session):
        filing = FilingRaw(
            accession_number="A1",
            cik="1",
            filing_type="10-K",
            filing_date=date(2024, 1, 1),
        )
        filing.facts.append(FinancialFact(fact_name="us-gaap:Revenues", fact_value=100))
        session.add(filing)
        session.commit()

        assert len(session.get(FilingRaw, "A1").facts) == 1

        session.delete(session.get(FilingRaw, "A1"))
        session.commit()
        assert session.query(FinancialFact).count() == 0

    def test_fact_repr_includes_name_and_value(self, session):
        fact = FinancialFact(accession_number="A1", fact_name="us-gaap:Assets", fact_value=42)
        assert "us-gaap:Assets" in repr(fact)

    def test_filing_repr_includes_accession(self):
        filing = FilingRaw(
            accession_number="A1", cik="1", filing_type="10-K", filing_date=date(2024, 1, 1)
        )
        assert "A1" in repr(filing)


class TestFilingVersion:
    def test_insert_amendment_chain(self, session):
        session.add_all(
            [
                FilingVersion(
                    cik="1",
                    filing_type="10-K",
                    period_of_report=date(2024, 1, 1),
                    accession_number="orig",
                    is_amendment=False,
                    superseded_by="amend",
                ),
                FilingVersion(
                    cik="1",
                    filing_type="10-K/A",
                    period_of_report=date(2024, 1, 1),
                    accession_number="amend",
                    is_amendment=True,
                ),
            ]
        )
        session.commit()

        original = session.query(FilingVersion).filter_by(accession_number="orig").one()
        assert original.superseded_by == "amend"


class TestPipelineAudit:
    def test_insert_a_stage_row(self, session):
        session.add(
            PipelineAudit(run_id="run-1", stage="ingest", status="completed", records_processed=5)
        )
        session.commit()

        row = session.query(PipelineAudit).one()
        assert row.status == "completed"
        assert row.created_at is not None

    def test_repr_includes_run_and_stage(self):
        audit = PipelineAudit(run_id="run-1", stage="parse", status="started")
        assert "run-1" in repr(audit)
        assert "parse" in repr(audit)


class TestModelRunAndFactAnomaly:
    def test_model_run_and_anomalies_relationship(self, session):
        filing = FilingRaw(
            accession_number="A1", cik="1", filing_type="10-K", filing_date=date(2024, 1, 1)
        )
        session.add(filing)
        session.commit()

        model_run = ModelRun(
            run_id="run-1",
            model_version="v1",
            model_sha256="a" * 64,
            filings_scored=1,
            anomalies_flagged=1,
            threshold=0.6,
            drift_level="clean",
        )
        model_run.anomalies.append(
            FactAnomaly(
                accession_number="A1",
                score=0.82,
                model_score=0.4,
                rule_score=0.82,
                is_anomaly=True,
                triggered_by="rule",
                reason="eps mismatch",
                rule_violations=[{"rule_id": "eps_reconciliation_failed", "severity": 0.82}],
            )
        )
        session.add(model_run)
        session.commit()

        fetched = session.query(ModelRun).filter_by(run_id="run-1").one()
        assert len(fetched.anomalies) == 1
        assert fetched.anomalies[0].accession_number == "A1"
        assert fetched.anomalies[0].rule_violations[0]["rule_id"] == "eps_reconciliation_failed"

    def test_cascade_delete_removes_anomalies(self, session):
        model_run = ModelRun(
            run_id="run-1", model_version="v1", model_sha256="a" * 64, threshold=0.6
        )
        model_run.anomalies.append(FactAnomaly(accession_number="A1", score=0.5, is_anomaly=False))
        session.add(model_run)
        session.commit()

        session.delete(session.query(ModelRun).one())
        session.commit()
        assert session.query(FactAnomaly).count() == 0

    def test_unique_constraint_on_run_and_accession(self, session):
        model_run = ModelRun(
            run_id="run-1", model_version="v1", model_sha256="a" * 64, threshold=0.6
        )
        session.add(model_run)
        session.commit()

        session.add(FactAnomaly(model_run_id=model_run.id, accession_number="A1", score=0.5))
        session.commit()

        session.add(FactAnomaly(model_run_id=model_run.id, accession_number="A1", score=0.9))
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

    def test_model_run_repr(self):
        run = ModelRun(
            run_id="run-1",
            model_version="v1",
            model_sha256="a" * 64,
            filings_scored=10,
            anomalies_flagged=2,
            threshold=0.6,
        )
        assert "run-1" in repr(run)
        assert "2/10" in repr(run)

    def test_fact_anomaly_repr(self):
        anomaly = FactAnomaly(accession_number="A1", score=0.82, is_anomaly=True)
        assert "A1" in repr(anomaly)
        assert "0.820" in repr(anomaly)


def test_all_tables_registered_on_base_metadata():
    """Guards against a model class existing but never being wired to Base."""
    expected = {
        "filings_raw",
        "financial_facts",
        "filing_versions",
        "pipeline_audit",
        "model_runs",
        "fact_anomalies",
    }
    assert expected <= set(Base.metadata.tables)
