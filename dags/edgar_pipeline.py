"""
Airflow DAG: edgar_pipeline
===========================

Daily ETL that ingests SEC 10-K / 10-Q filings into the data warehouse.

Task chain
----------
  fetch_new_filings
      └── download_raw_documents
              └── parse_xbrl_facts
                      └── validate_quality_gates
                              └── score_anomalies
                                      └── load_to_warehouse
                                              └── update_audit_trail
                                                      └── send_alerts_on_failure  (ONE_FAILED)

Every task writes a ``started`` row to ``pipeline_audit`` on entry and a
``completed`` / ``failed`` row on exit.

``score_anomalies`` sits after the deterministic quality gates and before the
load, so a filing arrives in the warehouse already carrying its anomaly score.
It is the one non-blocking stage: model problems degrade to "no scores" rather
than stopping filings that already passed the quality gates.

Mock mode
---------
Set ``MOCK_EDGAR=true`` (env var) to run end-to-end with fixture data —
no live SEC API calls, no real database required.  Useful for CI and
local development.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment flags
# ---------------------------------------------------------------------------

MOCK_EDGAR: bool = os.getenv("MOCK_EDGAR", "false").lower() == "true"
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")

# ---------------------------------------------------------------------------
# Airflow imports
#
# Airflow 3 relocated the operator, exception, and trigger-rule modules and
# removed the `schedule_interval` DAG argument. These shims keep the DAG
# importable on both 2.x and 3.x; `tests/test_dag_import.py` asserts the module
# imports against whichever Airflow is actually installed, so a future
# relocation fails CI instead of silently breaking the DAG.
# ---------------------------------------------------------------------------

from airflow import DAG

try:  # Airflow 3.x
    from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
except ImportError:  # Airflow 2.x
    from airflow.exceptions import AirflowFailException, AirflowSkipException

try:  # Airflow 3.x / apache-airflow-providers-standard
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x core
    from airflow.operators.python import PythonOperator

try:  # Airflow 3.x
    from airflow.task.trigger_rule import TriggerRule
except ImportError:  # Airflow 2.x
    from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Fixture data for MOCK_EDGAR=true
# ---------------------------------------------------------------------------

_MOCK_FILINGS: list[dict] = [
    {
        "accession_number": "0000320193-24-000123",
        "cik": "320193",
        "ticker": "AAPL",
        "filing_type": "10-K",
        "filing_date": "2024-11-01",
        "period_of_report": "2024-09-28",
    },
    {
        "accession_number": "0000320193-23-000077",
        "cik": "320193",
        "ticker": "AAPL",
        "filing_type": "10-Q",
        "filing_date": "2023-08-04",
        "period_of_report": "2023-07-01",
    },
]

_MOCK_FACTS: list[dict] = [
    {
        "accession_number": "0000320193-24-000123",
        "fact_name": "us-gaap:Revenues",
        "fact_value": 391035000000,
        "unit": "USD",
        "period_start": "2023-10-01",
        "period_end": "2024-09-28",
        "segment": None,
    },
    {
        "accession_number": "0000320193-24-000123",
        "fact_name": "us-gaap:NetIncomeLoss",
        "fact_value": 93736000000,
        "unit": "USD",
        "period_start": "2023-10-01",
        "period_end": "2024-09-28",
        "segment": None,
    },
    {
        "accession_number": "0000320193-24-000123",
        "fact_name": "us-gaap:Assets",
        "fact_value": 364980000000,
        "unit": "USD",
        "period_start": None,
        "period_end": "2024-09-28",
        "segment": None,
    },
]

# ---------------------------------------------------------------------------
# Audit logging helpers
# ---------------------------------------------------------------------------


def _audit_start(run_id: str, stage: str) -> None:
    """Write a 'started' row to pipeline_audit (or log in mock mode)."""
    if MOCK_EDGAR:
        logger.info("[MOCK] audit started  run=%s stage=%s", run_id, stage)
        return
    _write_audit(run_id, stage, "started", 0, None)


def _audit_complete(run_id: str, stage: str, records: int) -> None:
    """Write a 'completed' row to pipeline_audit (or log in mock mode)."""
    if MOCK_EDGAR:
        logger.info("[MOCK] audit completed run=%s stage=%s records=%d", run_id, stage, records)
        return
    _write_audit(run_id, stage, "completed", records, None)


def _audit_failed(run_id: str, stage: str, error: str) -> None:
    """Write a 'failed' row to pipeline_audit (or log in mock mode)."""
    if MOCK_EDGAR:
        logger.error("[MOCK] audit failed    run=%s stage=%s error=%s", run_id, stage, error)
        return
    _write_audit(run_id, stage, "failed", 0, error)


def _write_audit(run_id: str, stage: str, status: str, records: int, error: str | None) -> None:
    """Write a single row to pipeline_audit using SQLAlchemy."""
    from sqlalchemy import create_engine, text

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO pipeline_audit "
                "(run_id, stage, status, records_processed, error_message) "
                "VALUES (:run_id, :stage, :status, :records, :error)"
            ),
            {
                "run_id": run_id,
                "stage": stage,
                "status": status,
                "records": records,
                "error": error,
            },
        )


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def fetch_new_filings(**context: Any) -> list[dict]:
    """
    Stage 1 — Query EDGAR for filings submitted since the last successful run.

    Pushes a list of filing metadata dicts to XCom.
    Raises ``AirflowSkipException`` if no new filings are found.
    """
    run_id: str = context["run_id"]
    _audit_start(run_id, "ingest")

    try:
        if MOCK_EDGAR:
            filings = _MOCK_FILINGS
        else:
            from src.edgar_client import EdgarClient

            client = EdgarClient()
            # Real implementation: query EDGAR for filings since last DAG run
            # Here we pull Apple as a demonstration; production code would
            # iterate over a configurable watchlist of CIKs.
            data = client.get_company_filings("320193")
            recent = data.get("filings", {}).get("recent", {})
            filings = [
                {
                    "accession_number": recent["accessionNumber"][i],
                    "cik": data["cik"],
                    "ticker": (data.get("tickers") or [""])[0],
                    "filing_type": recent["form"][i],
                    "filing_date": recent["filingDate"][i],
                    "period_of_report": recent.get("reportDate", [""])[i],
                }
                for i in range(len(recent.get("accessionNumber", [])))
                if recent["form"][i] in ("10-K", "10-Q")
            ]

        if not filings:
            _audit_complete(run_id, "ingest", 0)
            raise AirflowSkipException("No new filings found for this run window.")

        _audit_complete(run_id, "ingest", len(filings))
        context["ti"].xcom_push(key="filings", value=filings)
        return filings

    except AirflowSkipException:
        raise
    except Exception as exc:
        _audit_failed(run_id, "ingest", str(exc))
        raise


def download_raw_documents(**context: Any) -> list[str]:
    """
    Stage 2 — Download raw HTML/XBRL documents and persist to filings_raw.

    Returns a list of accession numbers that were successfully downloaded.
    """
    run_id: str = context["run_id"]
    filings: list[dict] = context["ti"].xcom_pull(key="filings", task_ids="fetch_new_filings")
    _audit_start(run_id, "ingest")

    try:
        downloaded: list[str] = []

        if MOCK_EDGAR:
            downloaded = [f["accession_number"] for f in filings]
            logger.info("[MOCK] downloaded %d documents", len(downloaded))
        else:
            from src.edgar_client import EdgarClient

            client = EdgarClient()
            for filing in filings:
                try:
                    html = client.get_filing_document(
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{filing['cik']}/{filing['accession_number'].replace('-', '')}/"
                    )
                    _persist_raw_filing(filing, html, run_id)
                    downloaded.append(filing["accession_number"])
                except Exception as exc:
                    logger.warning("Failed to download %s: %s", filing["accession_number"], exc)

        _audit_complete(run_id, "ingest", len(downloaded))
        context["ti"].xcom_push(key="downloaded_accessions", value=downloaded)
        return downloaded

    except Exception as exc:
        _audit_failed(run_id, "ingest", str(exc))
        raise


def parse_xbrl_facts(**context: Any) -> list[dict]:
    """
    Stage 3 — Parse XBRL facts from downloaded HTML and store in financial_facts.
    """
    run_id: str = context["run_id"]
    accessions: list[str] = context["ti"].xcom_pull(
        key="downloaded_accessions", task_ids="download_raw_documents"
    )
    _audit_start(run_id, "parse")

    try:
        all_facts: list[dict] = []

        if MOCK_EDGAR:
            all_facts = _MOCK_FACTS
            logger.info("[MOCK] parsed %d facts", len(all_facts))
        else:
            from src.xbrl_parser import XBRLParser

            parser = XBRLParser()
            raw_html_map = _load_raw_html(accessions)
            for accession, html in raw_html_map.items():
                rows = parser.parse(html, accession)
                all_facts.extend(r.as_dict() for r in rows)

        _audit_complete(run_id, "parse", len(all_facts))
        context["ti"].xcom_push(key="facts", value=all_facts)
        return all_facts

    except Exception as exc:
        _audit_failed(run_id, "parse", str(exc))
        raise


def validate_quality_gates(**context: Any) -> None:
    """
    Stage 4 — Run completeness + PSI checks.

    Raises ``AirflowSkipException`` if there are no new facts.
    Raises ``AirflowFailException`` if quality checks fail.
    """
    run_id: str = context["run_id"]
    facts: list[dict] = context["ti"].xcom_pull(key="facts", task_ids="parse_xbrl_facts")
    _audit_start(run_id, "validate")

    try:
        if not facts:
            _audit_complete(run_id, "validate", 0)
            raise AirflowSkipException("No facts to validate — skipping quality gates.")

        # Build accession → present fact names map
        facts_by_accession: dict[str, list[str]] = {}
        for row in facts:
            acc = row["accession_number"]
            facts_by_accession.setdefault(acc, []).append(row["fact_name"])

        if not MOCK_EDGAR:
            from src.quality import check_completeness

            try:
                check_completeness(run_id, facts_by_accession)
            except ValueError as exc:
                _audit_failed(run_id, "validate", str(exc))
                raise AirflowFailException(str(exc)) from exc

        _audit_complete(run_id, "validate", len(facts))

    except (AirflowSkipException, AirflowFailException):
        raise
    except Exception as exc:
        _audit_failed(run_id, "validate", str(exc))
        raise


def score_anomalies(**context: Any) -> dict[str, Any]:
    """
    Stage 5 — Score parsed facts with the promoted anomaly model.

    Runs *after* the quality gates and *before* the load, so a filing reaches
    the warehouse already carrying its score. Deliberately non-blocking: a
    missing model, or a model that errors, logs and returns rather than failing
    the DAG. Anomaly scoring is a review aid, and an outage in it must not stop
    filings that passed the deterministic quality gates from landing.

    Drift is the exception worth shouting about — an ALERT-level drift report
    means the scores in this batch should not be trusted, so it is recorded on
    the ``model_runs`` row and surfaced in the task's return value.
    """
    run_id: str = context["run_id"]
    facts: list[dict] = context["ti"].xcom_pull(key="facts", task_ids="parse_xbrl_facts") or []
    _audit_start(run_id, "score")

    try:
        if not facts:
            _audit_complete(run_id, "score", 0)
            raise AirflowSkipException("No facts to score — skipping anomaly detection.")

        from src.ml.features import build_features
        from src.ml.registry import ModelNotFoundError, ModelRegistry

        registry = ModelRegistry(os.getenv("MODEL_REGISTRY_ROOT", "models"))

        try:
            detector, metadata = registry.load_production()
        except (ModelNotFoundError, ValueError) as exc:
            # ValueError here means an artifact hash mismatch — a tampered or
            # corrupted model. Refusing to score is the correct response; using
            # it anyway would attach unverifiable scores to audited records.
            logger.warning("Anomaly scoring skipped: %s", exc)
            _audit_complete(run_id, "score", 0)
            return {"scored": 0, "flagged": 0, "reason": str(exc)}

        filings = _filings_metadata_for(facts, context)
        matrix = build_features(facts, filings)
        scores = detector.score(matrix)
        flagged = [s for s in scores if s.is_anomaly]

        for score in flagged[:10]:
            logger.warning("Anomaly %s — %s", score.accession_number, score.explain())

        drift = _build_drift_report(detector, matrix, metadata)

        if MOCK_EDGAR:
            logger.info(
                "[MOCK] scored %d filings with model %s, %d flagged",
                len(scores),
                metadata.version,
                len(flagged),
            )
        else:
            _persist_anomaly_scores(run_id, metadata, detector, scores, drift)

        _audit_complete(run_id, "score", len(scores))

        result = {
            "scored": len(scores),
            "flagged": len(flagged),
            "model_version": metadata.version,
            "drift_level": drift.worst_level.value if drift else None,
        }
        context["ti"].xcom_push(key="anomaly_summary", value=result)
        return result

    except AirflowSkipException:
        raise
    except Exception as exc:
        # Scoring failures are recorded but never fail the DAG — see docstring.
        logger.exception("Anomaly scoring failed for run=%s", run_id)
        _audit_failed(run_id, "score", str(exc))
        return {"scored": 0, "flagged": 0, "reason": str(exc)}


def _filings_metadata_for(facts: list[dict], context: Any) -> dict[str, dict]:
    """
    Assemble the accession -> filing metadata map the continuity features need.

    Falls back to the upstream XCom when the warehouse is unavailable; without
    it the year-over-year features are neutral-filled rather than wrong.
    """
    upstream = context["ti"].xcom_pull(key="filings", task_ids="fetch_new_filings") or []
    metadata = {f["accession_number"]: f for f in upstream if f.get("accession_number")}

    if MOCK_EDGAR:
        return metadata

    accessions = sorted({f["accession_number"] for f in facts if f.get("accession_number")})
    try:
        metadata.update(_load_filing_metadata(accessions))
    except Exception as exc:
        logger.warning("Could not load filing metadata for continuity features: %s", exc)
    return metadata


def _build_drift_report(detector: Any, matrix: Any, metadata: Any) -> Any:
    """
    Compare this batch against the model's training baseline.

    Returns None when no baseline snapshot is stored alongside the model — a
    first run has nothing to drift from, and inventing a baseline would produce
    a meaningless number.
    """
    from src.ml.monitoring import build_drift_report
    from src.ml.registry import ModelRegistry

    registry = ModelRegistry(os.getenv("MODEL_REGISTRY_ROOT", "models"))
    baseline_path = registry.version_dir(metadata.version) / "baseline_features.npz"
    if not baseline_path.exists():
        logger.info("No baseline snapshot for %s — skipping drift check", metadata.version)
        return None

    try:
        import numpy as np

        from src.ml.features import FeatureMatrix

        stored = np.load(baseline_path, allow_pickle=False)
        baseline = FeatureMatrix(
            X=stored["X"],
            accessions=[str(a) for a in stored["accessions"]],
            feature_names=tuple(str(n) for n in stored["feature_names"]),
        )
        return build_drift_report(
            baseline,
            matrix,
            baseline_scores=detector.score_values(baseline).tolist(),
            current_scores=detector.score_values(matrix).tolist(),
            threshold=detector.threshold,
        )
    except Exception as exc:
        logger.warning("Drift check failed (scores still valid): %s", exc)
        return None


def load_to_warehouse(**context: Any) -> int:
    """
    Stage 5 — Bulk-insert parsed facts into the PostgreSQL warehouse.

    Returns the number of rows inserted.
    """
    run_id: str = context["run_id"]
    facts: list[dict] = context["ti"].xcom_pull(key="facts", task_ids="parse_xbrl_facts")
    _audit_start(run_id, "load")

    try:
        if MOCK_EDGAR:
            loaded = len(facts or [])
            logger.info("[MOCK] loaded %d rows to warehouse", loaded)
        else:
            loaded = _bulk_insert_facts(facts or [])

        _audit_complete(run_id, "load", loaded)
        return loaded

    except Exception as exc:
        _audit_failed(run_id, "load", str(exc))
        raise


def update_audit_trail(**context: Any) -> None:
    """
    Stage 6 — Mark the DAG run as fully complete in the audit trail.
    """
    run_id: str = context["run_id"]
    _audit_start(run_id, "load")

    try:
        if MOCK_EDGAR:
            logger.info("[MOCK] audit trail updated for run=%s", run_id)
        else:
            _write_audit(run_id, "audit_trail", "completed", 0, None)

        _audit_complete(run_id, "load", 0)

    except Exception as exc:
        _audit_failed(run_id, "load", str(exc))
        raise


def send_alerts_on_failure(**context: Any) -> None:
    """
    Stage 7 — Send failure alerts.  Only executes when an upstream task fails
    (trigger_rule=ONE_FAILED).
    """
    run_id: str = context["run_id"]

    if MOCK_EDGAR:
        logger.warning("[MOCK] alert dispatched for failed run=%s", run_id)
        return

    from src.alerts import send_pipeline_failure_alert

    # Find which task failed by inspecting the DAG run
    failed_tasks = [t for t in context["dag_run"].get_task_instances() if t.state == "failed"]
    stage = failed_tasks[0].task_id if failed_tasks else "unknown"
    error = "One or more pipeline tasks failed"

    send_pipeline_failure_alert(
        run_id=run_id,
        stage=stage,
        error_message=error,
    )


# ---------------------------------------------------------------------------
# Database helpers (production-only, not called in MOCK_EDGAR mode)
# ---------------------------------------------------------------------------


def _persist_raw_filing(filing: dict, html: str, run_id: str) -> None:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from src.schema import FilingRaw

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with Session(engine) as session:
        existing = session.get(FilingRaw, filing["accession_number"])
        if existing is None:
            session.add(
                FilingRaw(
                    accession_number=filing["accession_number"],
                    cik=filing["cik"],
                    ticker=filing.get("ticker"),
                    filing_type=filing["filing_type"],
                    filing_date=filing["filing_date"],
                    period_of_report=filing.get("period_of_report"),
                    raw_html=html,
                    pipeline_run_id=run_id,
                )
            )
            session.commit()


def _load_raw_html(accessions: list[str]) -> dict[str, str]:
    from sqlalchemy import create_engine, text

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT accession_number, raw_html FROM filings_raw WHERE accession_number = ANY(:acc)"
            ),
            {"acc": accessions},
        ).fetchall()
    return {r[0]: r[1] for r in rows if r[1]}


def _load_filing_metadata(accessions: list[str]) -> dict[str, dict]:
    """Fetch cik / filing_date / period_of_report for the given accessions."""
    from sqlalchemy import create_engine, text

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT accession_number, cik, filing_type, filing_date, period_of_report "
                "FROM filings_raw WHERE accession_number = ANY(:acc)"
            ),
            {"acc": accessions},
        ).fetchall()

    return {
        r[0]: {
            "accession_number": r[0],
            "cik": r[1],
            "filing_type": r[2],
            "filing_date": r[3],
            "period_of_report": r[4],
        }
        for r in rows
    }


def _persist_anomaly_scores(
    run_id: str,
    metadata: Any,
    detector: Any,
    scores: list,
    drift: Any,
) -> None:
    """
    Write one ``model_runs`` row and its ``fact_anomalies`` children.

    Scoring is re-runnable: a retried task deletes this run's prior scores
    before inserting, so an Airflow retry replaces rather than duplicates.
    Only ``fact_anomalies`` is rewritten — ``pipeline_audit`` stays append-only.
    """
    from sqlalchemy import create_engine, delete
    from sqlalchemy.orm import Session

    from src.schema import FactAnomaly, ModelRun

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with Session(engine) as session:
        existing = (
            session.query(ModelRun)
            .filter(ModelRun.run_id == run_id, ModelRun.model_version == metadata.version)
            .one_or_none()
        )
        if existing is not None:
            session.execute(delete(FactAnomaly).where(FactAnomaly.model_run_id == existing.id))
            session.delete(existing)
            session.flush()

        model_run = ModelRun(
            run_id=run_id,
            model_version=metadata.version,
            model_sha256=metadata.artifact_sha256,
            git_sha=metadata.git_sha,
            filings_scored=len(scores),
            anomalies_flagged=sum(1 for s in scores if s.is_anomaly),
            threshold=detector.threshold,
            drift_report=drift.as_dict() if drift else None,
            drift_level=drift.worst_level.value if drift else None,
        )
        session.add(model_run)
        session.flush()  # assign model_run.id

        session.add_all(
            FactAnomaly(
                model_run_id=model_run.id,
                accession_number=s.accession_number,
                score=s.score,
                model_score=s.model_score,
                rule_score=s.rule_score,
                is_anomaly=s.is_anomaly,
                triggered_by=s.triggered_by,
                reason=s.explain(),
                rule_violations=[v.as_dict() for v in s.rule_violations],
                top_contributors=[[n, z] for n, z in s.top_contributors],
            )
            for s in scores
        )
        session.commit()

    logger.info(
        "Persisted %d anomaly scores for run=%s (model %s)",
        len(scores),
        run_id,
        metadata.version,
    )


def _bulk_insert_facts(facts: list[dict]) -> int:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from src.schema import FinancialFact

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with Session(engine) as session:
        objects = [FinancialFact(**f) for f in facts]
        session.bulk_save_objects(objects)
        session.commit()
    return len(objects)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="edgar_pipeline",
    default_args=default_args,
    description="Daily SEC EDGAR 10-K/10-Q ingestion pipeline",
    # `schedule` (not `schedule_interval`) — the latter was removed in Airflow 3
    # and has been the supported spelling since 2.4.
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["edgar", "ingestion", "data-engineering"],
    max_active_runs=1,
) as dag:
    task_fetch = PythonOperator(
        task_id="fetch_new_filings",
        python_callable=fetch_new_filings,
    )

    task_download = PythonOperator(
        task_id="download_raw_documents",
        python_callable=download_raw_documents,
    )

    task_parse = PythonOperator(
        task_id="parse_xbrl_facts",
        python_callable=parse_xbrl_facts,
    )

    task_validate = PythonOperator(
        task_id="validate_quality_gates",
        python_callable=validate_quality_gates,
    )

    task_score = PythonOperator(
        task_id="score_anomalies",
        python_callable=score_anomalies,
    )

    task_load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )

    task_audit = PythonOperator(
        task_id="update_audit_trail",
        python_callable=update_audit_trail,
    )

    task_alerts = PythonOperator(
        task_id="send_alerts_on_failure",
        python_callable=send_alerts_on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Linear chain for the happy path
    (
        task_fetch
        >> task_download
        >> task_parse
        >> task_validate
        >> task_score
        >> task_load
        >> task_audit
    )

    # Alert task depends on all pipeline tasks so it fires on any failure
    [
        task_fetch,
        task_download,
        task_parse,
        task_validate,
        task_score,
        task_load,
        task_audit,
    ] >> task_alerts
