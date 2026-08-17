"""
Unit tests for metrics collection.
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.metrics import (
    _append_to_metrics_file,
    _compute_stage_durations,
    _compute_success_rates,
    _count_filings_by_stage,
    _get_anomaly_counts,
    collect_run_metrics,
)
from src.schema import Base, ExtractionAudit, ModelRun, PipelineAudit


@pytest.fixture
def in_memory_db() -> str:
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return "sqlite:///:memory:"


@pytest.fixture
def session_factory(in_memory_db: str):
    """Factory for creating SQLAlchemy sessions."""
    engine = create_engine(in_memory_db)
    return lambda: Session(engine)


def test_count_filings_by_stage(session_factory):
    """Test counting filings at each pipeline stage."""
    session = session_factory()
    run_id = "test_run_1"

    session.add(PipelineAudit(run_id=run_id, stage="ingest", status="completed", records_processed=5))
    session.add(PipelineAudit(run_id=run_id, stage="parse", status="completed", records_processed=5))
    session.add(PipelineAudit(run_id=run_id, stage="validate", status="completed", records_processed=4))
    session.add(PipelineAudit(run_id=run_id, stage="load", status="completed", records_processed=4))
    session.commit()

    result = _count_filings_by_stage(session, run_id)

    assert result["filings_ingested"] == 5
    assert result["filings_parsed"] == 5
    assert result["filings_validated"] == 4
    assert result["filings_loaded"] == 4
    assert result["filings_downloaded"] == 5


def test_compute_success_rates(session_factory):
    """Test computing parsing success rates from extraction audit."""
    session = session_factory()
    run_id = "test_run_2"

    session.add(ExtractionAudit(
        run_id=run_id,
        system_id="sys1",
        accession_number="acc1",
        stage="parse",
        extraction_status="success",
        row_hash="hash1",
    ))
    session.add(ExtractionAudit(
        run_id=run_id,
        system_id="sys2",
        accession_number="acc2",
        stage="parse",
        extraction_status="success",
        row_hash="hash2",
    ))
    session.add(ExtractionAudit(
        run_id=run_id,
        system_id="sys3",
        accession_number="acc3",
        stage="parse",
        extraction_status="failure",
        detail="Error parsing",
        row_hash="hash3",
    ))
    session.commit()

    result = _compute_success_rates(session, run_id)

    assert result["parsing_success_rate"] == pytest.approx(0.667, abs=0.01)
    assert result["parsing_failure_count"] == 1
    assert result["validation_success_rate"] == 1.0


def test_compute_success_rates_all_success(session_factory):
    """Test success rates when all filings parse successfully."""
    session = session_factory()
    run_id = "test_run_3"

    for i in range(3):
        session.add(ExtractionAudit(
            run_id=run_id,
            system_id=f"sys{i}",
            accession_number=f"acc{i}",
            stage="parse",
            extraction_status="success",
            row_hash=f"hash{i}",
        ))
    session.commit()

    result = _compute_success_rates(session, run_id)

    assert result["parsing_success_rate"] == 1.0
    assert result["parsing_failure_count"] == 0


def test_get_anomaly_counts(session_factory):
    """Test retrieving anomaly counts from model run."""
    session = session_factory()
    run_id = "test_run_4"

    session.add(ModelRun(
        run_id=run_id,
        model_version="v1.0.0",
        model_sha256="abc123",
        filings_scored=10,
        anomalies_flagged=3,
        threshold=0.75,
    ))
    session.commit()

    result = _get_anomaly_counts(session, run_id)

    assert result["anomalies_flagged"] == 3
    assert result["anomaly_rate"] == pytest.approx(0.3, abs=0.01)


def test_get_anomaly_counts_no_run(session_factory):
    """Test anomaly counts when no model run exists."""
    session = session_factory()
    result = _get_anomaly_counts(session, "nonexistent_run")

    assert result["anomalies_flagged"] == 0
    assert result["anomaly_rate"] == 0.0


def test_compute_stage_durations(session_factory):
    """Test computing per-stage execution durations."""
    session = session_factory()
    run_id = "test_run_5"

    now = datetime.utcnow()

    session.add(PipelineAudit(
        run_id=run_id,
        stage="ingest",
        status="started",
        created_at=now,
    ))
    session.add(PipelineAudit(
        run_id=run_id,
        stage="ingest",
        status="completed",
        records_processed=5,
        created_at=now + timedelta(seconds=1.5),
    ))
    session.add(PipelineAudit(
        run_id=run_id,
        stage="parse",
        status="started",
        created_at=now + timedelta(seconds=1.5),
    ))
    session.add(PipelineAudit(
        run_id=run_id,
        stage="parse",
        status="completed",
        records_processed=5,
        created_at=now + timedelta(seconds=3.2),
    ))
    session.commit()

    result = _compute_stage_durations(session, run_id)

    assert "fetch_new_filings" in result
    assert result["fetch_new_filings"] == pytest.approx(1.5, abs=0.05)
    assert "parse_xbrl_facts" in result
    assert result["parse_xbrl_facts"] == pytest.approx(1.7, abs=0.05)
    assert "total" in result
    assert result["total"] == pytest.approx(3.2, abs=0.05)


def test_append_to_metrics_file():
    """Test appending metrics to the JSON file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_file = Path(tmpdir) / "metrics.json"

        run_record = {
            "run_id": "test_run_1",
            "run_date": "2026-08-17",
            "filings_ingested": 5,
            "filings_downloaded": 5,
            "filings_parsed": 5,
            "filings_validated": 4,
            "filings_loaded": 4,
            "parsing_success_rate": 1.0,
            "parsing_failure_count": 0,
            "validation_success_rate": 1.0,
            "anomalies_flagged": 1,
            "anomaly_rate": 0.2,
            "durations_seconds": {
                "fetch_new_filings": 0.1,
                "total": 0.5,
            },
        }

        _append_to_metrics_file(metrics_file, run_record)

        assert metrics_file.exists()
        with open(metrics_file) as f:
            data = json.load(f)

        assert "generated_at" in data
        assert "runs" in data
        assert len(data["runs"]) == 1
        assert data["runs"][0]["run_id"] == "test_run_1"
        assert data["runs"][0]["filings_ingested"] == 5


def test_append_to_metrics_file_multiple_runs():
    """Test appending multiple runs to the JSON file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_file = Path(tmpdir) / "metrics.json"

        for i in range(3):
            run_record = {
                "run_id": f"test_run_{i}",
                "run_date": "2026-08-17",
                "filings_ingested": i + 1,
                "durations_seconds": {"total": 0.5},
            }
            _append_to_metrics_file(metrics_file, run_record)

        with open(metrics_file) as f:
            data = json.load(f)

        assert len(data["runs"]) == 3
        assert data["runs"][0]["run_id"] == "test_run_0"
        assert data["runs"][1]["run_id"] == "test_run_1"
        assert data["runs"][2]["run_id"] == "test_run_2"


def test_collect_run_metrics_integration(session_factory):
    """Integration test for full metrics collection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_file = Path(tmpdir) / "metrics.json"
        run_id = "test_run_integration"
        db_url = "sqlite:///:memory:"

        engine = create_engine(db_url)
        Base.metadata.create_all(engine)
        session = Session(engine)

        now = datetime.utcnow()

        session.add(PipelineAudit(
            run_id=run_id,
            stage="ingest",
            status="started",
            created_at=now,
        ))
        session.add(PipelineAudit(
            run_id=run_id,
            stage="ingest",
            status="completed",
            records_processed=2,
            created_at=now + timedelta(seconds=0.1),
        ))

        session.add(ExtractionAudit(
            run_id=run_id,
            system_id="sys1",
            accession_number="acc1",
            stage="parse",
            extraction_status="success",
            row_hash="h1",
        ))
        session.add(ExtractionAudit(
            run_id=run_id,
            system_id="sys2",
            accession_number="acc2",
            stage="parse",
            extraction_status="success",
            row_hash="h2",
        ))

        session.add(ModelRun(
            run_id=run_id,
            model_version="v1.0.0",
            model_sha256="xyz",
            filings_scored=2,
            anomalies_flagged=1,
            threshold=0.75,
        ))

        session.commit()

        result = collect_run_metrics(run_id, db_url, metrics_file)

        assert result["run_id"] == run_id
        assert result["filings_ingested"] == 2
        assert result["parsing_success_rate"] == 1.0
        assert result["anomalies_flagged"] == 1
        assert metrics_file.exists()
