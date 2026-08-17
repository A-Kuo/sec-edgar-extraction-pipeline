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
                              └── load_to_warehouse
                                      └── update_audit_trail
                                              └── send_alerts_on_failure  (ONE_FAILED)

Every task writes a ``started`` row to ``pipeline_audit`` on entry and a
``completed`` / ``failed`` row on exit.

Mock mode
---------
Set ``MOCK_EDGAR=true`` (env var) to run end-to-end with fixture data —
no live SEC API calls, no real database required.  Useful for CI and
local development.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment flags
# ---------------------------------------------------------------------------

MOCK_EDGAR: bool = os.getenv("MOCK_EDGAR", "false").lower() == "true"
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")

# ---------------------------------------------------------------------------
# Airflow imports (inside functions where possible so the module can be
# imported without a running Airflow metastore during unit tests)
# ---------------------------------------------------------------------------

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.python import PythonOperator
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


def _write_audit(
    run_id: str, stage: str, status: str, records: int, error: str | None
) -> None:
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
            {"run_id": run_id, "stage": stage, "status": status,
             "records": records, "error": error},
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
                        f"{filing['cik']}/{filing['accession_number'].replace('-', '')}/")
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


def load_to_warehouse(**context: Any) -> int:
    """
    Stage 5 — Upsert parsed facts into the PostgreSQL warehouse.

    Idempotent by design: this stage is safe to retry (Airflow is
    configured with ``retries=2``) or re-run for the same accession
    numbers without creating duplicate rows in ``financial_facts``. See
    ``_upsert_facts``.

    Returns the number of rows processed (inserted or updated).
    """
    run_id: str = context["run_id"]
    facts: list[dict] = context["ti"].xcom_pull(key="facts", task_ids="parse_xbrl_facts")
    _audit_start(run_id, "load")

    try:
        if MOCK_EDGAR:
            loaded = len(facts or [])
            logger.info("[MOCK] loaded %d rows to warehouse", loaded)
        else:
            loaded = _upsert_facts(facts or [])

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

    ti = context["ti"]
    # Find which task failed by inspecting the DAG run
    failed_tasks = [
        t for t in context["dag_run"].get_task_instances()
        if t.state == "failed"
    ]
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
            session.add(FilingRaw(
                accession_number=filing["accession_number"],
                cik=filing["cik"],
                ticker=filing.get("ticker"),
                filing_type=filing["filing_type"],
                filing_date=filing["filing_date"],
                period_of_report=filing.get("period_of_report"),
                raw_html=html,
                pipeline_run_id=run_id,
            ))
            session.commit()


def _load_raw_html(accessions: list[str]) -> dict[str, str]:
    from sqlalchemy import create_engine, text

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT accession_number, raw_html FROM filings_raw WHERE accession_number = ANY(:acc)"),
            {"acc": accessions},
        ).fetchall()
    return {r[0]: r[1] for r in rows if r[1]}


def _upsert_facts(facts: list[dict]) -> int:
    """
    Upsert *facts* into ``financial_facts``, targeting the table's
    ``uq_financial_facts_accession_fact_period_segment`` unique expression
    index (see ``src/schema.py``).

    This replaces a plain bulk INSERT specifically to make the load stage
    idempotent: Airflow retries a failed task (``retries=2`` in
    ``default_args``), and re-processed amendments legitimately re-parse
    facts for an accession that was already loaded. Without an upsert,
    either of those would insert duplicate rows instead of updating the
    existing ones.

    The ON CONFLICT target must use the *same* COALESCE expressions as
    the index (not the bare nullable columns), otherwise Postgres/SQLite
    won't recognize it as matching the index and will fall back to a
    plain insert — which is exactly the bug a first pass at this function
    had, caught by tests/test_load_idempotency.py.

    Uses the dialect-appropriate ``INSERT ... ON CONFLICT DO UPDATE``
    construct (Postgres in production; SQLite in tests), since both
    dialects expose the same ``on_conflict_do_update`` API.
    """
    if not facts:
        return 0

    from sqlalchemy import create_engine, func, text
    from src.schema import FinancialFact

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        if engine.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as _insert
        else:
            from sqlalchemy.dialects.postgresql import insert as _insert

        table = FinancialFact.__table__
        with engine.begin() as conn:
            stmt = _insert(table).values(facts)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    table.c.accession_number,
                    table.c.fact_name,
                    func.coalesce(table.c.period_end, text("'1900-01-01'")),
                    func.coalesce(table.c.segment, text("''")),
                ],
                set_={
                    "fact_value": stmt.excluded.fact_value,
                    "unit": stmt.excluded.unit,
                    "period_start": stmt.excluded.period_start,
                    "parsed_at": func.now(),
                },
            )
            conn.execute(stmt)
    finally:
        engine.dispose()

    return len(facts)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="edgar_pipeline",
    default_args=default_args,
    description="Daily SEC EDGAR 10-K/10-Q ingestion pipeline",
    schedule_interval="@daily",
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
    task_fetch >> task_download >> task_parse >> task_validate >> task_load >> task_audit

    # Alert task depends on all pipeline tasks so it fires on any failure
    [task_fetch, task_download, task_parse, task_validate, task_load, task_audit] >> task_alerts
