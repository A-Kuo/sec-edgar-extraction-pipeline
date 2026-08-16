"""
FastAPI serving layer for the SEC EDGAR extraction pipeline.

Endpoints
---------
  GET  /health
  GET  /filings/{ticker}            list filings with metadata (paginated)
  GET  /filing/{accession}          parsed financial_facts for one filing
  GET  /facts/{ticker}/{fact_name}  time-series of a specific fact
  GET  /anomalies/{ticker}          anomaly scores + reasons for a ticker
  GET  /model/current               provenance of the promoted anomaly model
  GET  /audit/{accession}           hash-chained extraction audit history
  POST /trigger/{ticker}            trigger on-demand ingestion

Fact and filing reads check Redis before hitting PostgreSQL; cache misses
populate Redis so subsequent calls are served from memory. The anomaly,
model, and audit endpoints deliberately bypass the cache — scores and audit
records change independently of the cache's TTLs, and serving a stale answer
is worse than serving a slow one, especially for the audit trail.

Run with:
    uvicorn api.main:app --reload
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Generator
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.cache import FilingCache

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SEC EDGAR Extraction Pipeline API",
    description=(
        "Serves structured financial facts ingested from SEC EDGAR 10-K/10-Q filings. "
        "All read endpoints use a Redis cache (TTL per key type) before hitting PostgreSQL."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# Database + cache singletons (created once at startup)
# ---------------------------------------------------------------------------

_DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

_engine = create_engine(_DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)
_SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)
_cache = FilingCache(_REDIS_URL)


# ---------------------------------------------------------------------------
# Dependency injection
# ---------------------------------------------------------------------------


def get_db() -> Generator[Session, None, None]:
    db = _SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_cache() -> FilingCache:
    return _cache


DBDep = Annotated[Session, Depends(get_db)]
CacheDep = Annotated[FilingCache, Depends(get_cache)]


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    database: str
    cache: str


class FilingMeta(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    accession_number: str
    cik: str
    ticker: str | None
    filing_type: str
    filing_date: date
    period_of_report: date | None
    file_size_bytes: int | None
    ingested_at: datetime


class FilingsResponse(BaseModel):
    ticker: str
    total: int
    limit: int
    offset: int
    items: list[FilingMeta]


class FactRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    accession_number: str
    fact_name: str
    fact_value: float | None
    unit: str | None
    period_start: date | None
    period_end: date | None
    segment: str | None
    parsed_at: datetime


class FilingFactsResponse(BaseModel):
    accession_number: str
    filing_type: str | None
    period_of_report: date | None
    facts: list[FactRow]


class TimeSeriesPoint(BaseModel):
    accession_number: str
    filing_type: str | None
    period_start: date | None
    period_end: date | None
    fact_value: float | None
    unit: str | None
    segment: str | None


class TimeSeriesResponse(BaseModel):
    ticker: str
    fact_name: str
    series: list[TimeSeriesPoint]


class TriggerResponse(BaseModel):
    ticker: str
    cik: str | None
    status: str
    message: str


class RuleViolationOut(BaseModel):
    rule_id: str
    severity: float
    message: str


class AnomalyRow(BaseModel):
    accession_number: str
    filing_type: str | None
    filing_date: date | None
    score: float
    model_score: float
    rule_score: float
    is_anomaly: bool
    triggered_by: str | None
    reason: str | None
    rule_violations: list[RuleViolationOut] = []
    model_version: str
    scored_at: datetime


class AnomaliesResponse(BaseModel):
    ticker: str
    total: int
    limit: int
    offset: int
    items: list[AnomalyRow]


class ModelInfoResponse(BaseModel):
    """Provenance of the model currently serving scores."""

    version: str
    trained_at: str
    artifact_sha256: str
    training_data_sha256: str
    n_training_filings: int
    git_sha: str
    threshold: float
    contamination: float
    seed: int
    feature_names: list[str]
    metrics: dict[str, float]
    verified: bool


class ExtractionAuditRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: str
    system_id: str
    stage: str
    extraction_status: str
    detail: str | None
    content_hash: str | None
    row_hash: str
    prev_row_hash: str | None
    created_at: datetime


class AccessionAuditResponse(BaseModel):
    accession_number: str
    total: int
    items: list[ExtractionAuditRow]


# ---------------------------------------------------------------------------
# Helper: serialise Decimal / date for JSON cache storage
# ---------------------------------------------------------------------------


def _to_json(obj: Any) -> str:
    def default(o: Any) -> Any:
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        raise TypeError(f"Not serialisable: {type(o)}")

    return json.dumps(obj, default=default)


def _decode_violations(raw: Any) -> list[RuleViolationOut]:
    """
    Normalise a ``rule_violations`` column into typed rows.

    SQLAlchemy's JSON type round-trips to a list on PostgreSQL but comes back as
    a string from a raw ``text()`` query on SQLite, so both shapes are handled
    rather than letting the response model differ by backend.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Could not decode rule_violations payload: %.80s", raw)
            return []
    if not isinstance(raw, list):
        return []
    return [RuleViolationOut(**v) for v in raw if isinstance(v, dict)]


def _lookup_cik(ticker: str, db: Session, cache: FilingCache) -> str | None:
    """Return the CIK for *ticker*, checking cache first."""
    cik = cache.get_cik(ticker)
    if cik:
        return cik
    row = db.execute(
        text("SELECT cik FROM filings_raw WHERE ticker = :t LIMIT 1"),
        {"t": ticker.upper()},
    ).fetchone()
    if row:
        cache.set_cik(ticker, row[0])
        return row[0]
    return None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Returns the liveness status of the API, database, and Redis cache.",
    tags=["ops"],
)
def health(db: DBDep, cache: CacheDep) -> HealthResponse:
    # Database probe
    db_status = "ok"
    try:
        db.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("DB health check failed: %s", exc)
        db_status = f"error: {exc}"

    # Cache probe
    cache_status = "ok" if cache.ping() else "unavailable"

    return HealthResponse(status="ok", database=db_status, cache=cache_status)


@app.get(
    "/filings/{ticker}",
    response_model=FilingsResponse,
    summary="List filings for a ticker",
    description=(
        "Returns a paginated list of 10-K/10-Q filings for the given ticker symbol. "
        "Results are sorted by filing_date descending. "
        "The filing index is cached in Redis for 24 hours."
    ),
    tags=["filings"],
)
def list_filings(
    ticker: str,
    db: DBDep,
    cache: CacheDep,
    limit: Annotated[int, Query(ge=1, le=200, description="Max results to return")] = 20,
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0,
) -> FilingsResponse:
    ticker_upper = ticker.upper()

    # Cache check — store the full filing list, slice in memory for pagination
    cached = cache.get_filings(ticker_upper)
    if cached is not None:
        total = len(cached)
        page = cached[offset : offset + limit]
        return FilingsResponse(
            ticker=ticker_upper,
            total=total,
            limit=limit,
            offset=offset,
            items=[FilingMeta(**row) for row in page],
        )

    # DB query
    rows = db.execute(
        text(
            """
            SELECT accession_number, cik, ticker, filing_type, filing_date,
                   period_of_report, file_size_bytes, ingested_at
            FROM filings_raw
            WHERE UPPER(ticker) = :ticker
            ORDER BY filing_date DESC
            """
        ),
        {"ticker": ticker_upper},
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No filings found for ticker '{ticker}'")

    all_items = [
        {
            "accession_number": r[0],
            "cik": r[1],
            "ticker": r[2],
            "filing_type": r[3],
            "filing_date": r[4].isoformat() if hasattr(r[4], "isoformat") else str(r[4]),
            "period_of_report": r[5].isoformat()
            if r[5] and hasattr(r[5], "isoformat")
            else (str(r[5]) if r[5] else None),
            "file_size_bytes": r[6],
            "ingested_at": r[7].isoformat() if hasattr(r[7], "isoformat") else str(r[7]),
        }
        for r in rows
    ]

    # Populate cache with full list (pagination is applied after)
    cache.set_filings(ticker_upper, all_items)

    total = len(all_items)
    page = all_items[offset : offset + limit]
    return FilingsResponse(
        ticker=ticker_upper,
        total=total,
        limit=limit,
        offset=offset,
        items=[FilingMeta(**row) for row in page],
    )


@app.get(
    "/filing/{accession}",
    response_model=FilingFactsResponse,
    summary="Get parsed facts for one filing",
    description=(
        "Returns all parsed financial facts for the given accession number. "
        "Facts are cached in Redis for 7 days once computed."
    ),
    tags=["filings"],
)
def get_filing_facts(
    accession: str,
    db: DBDep,
    cache: CacheDep,
) -> FilingFactsResponse:
    # Cache check
    cached_facts = cache.get_facts(accession)
    if cached_facts is not None:
        # Fetch the filing metadata separately (not cached with facts)
        meta = db.execute(
            text(
                "SELECT filing_type, period_of_report FROM filings_raw WHERE accession_number = :a"
            ),
            {"a": accession},
        ).fetchone()
        return FilingFactsResponse(
            accession_number=accession,
            filing_type=meta[0] if meta else None,
            period_of_report=meta[1] if meta else None,
            facts=[FactRow(**f) for f in cached_facts],
        )

    # DB query — filing metadata
    meta = db.execute(
        text("SELECT filing_type, period_of_report FROM filings_raw WHERE accession_number = :a"),
        {"a": accession},
    ).fetchone()
    if meta is None:
        raise HTTPException(status_code=404, detail=f"Accession '{accession}' not found")

    # DB query — facts
    fact_rows = db.execute(
        text(
            """
            SELECT id, accession_number, fact_name, fact_value, unit,
                   period_start, period_end, segment, parsed_at
            FROM financial_facts
            WHERE accession_number = :a
            ORDER BY fact_name, period_end
            """
        ),
        {"a": accession},
    ).fetchall()

    facts_dicts = [
        {
            "id": r[0],
            "accession_number": r[1],
            "fact_name": r[2],
            "fact_value": float(r[3]) if r[3] is not None else None,
            "unit": r[4],
            "period_start": r[5].isoformat()
            if r[5] and hasattr(r[5], "isoformat")
            else (str(r[5]) if r[5] else None),
            "period_end": r[6].isoformat()
            if r[6] and hasattr(r[6], "isoformat")
            else (str(r[6]) if r[6] else None),
            "segment": r[7],
            "parsed_at": r[8].isoformat() if hasattr(r[8], "isoformat") else str(r[8]),
        }
        for r in fact_rows
    ]

    # Populate cache
    cache.set_facts(accession, facts_dicts)

    return FilingFactsResponse(
        accession_number=accession,
        filing_type=meta[0],
        period_of_report=meta[1],
        facts=[FactRow(**f) for f in facts_dicts],
    )


@app.get(
    "/facts/{ticker}/{fact_name}",
    response_model=TimeSeriesResponse,
    summary="Time-series for a specific financial fact",
    description=(
        "Returns the full historical time-series of one XBRL fact for a given ticker. "
        "Only consolidated rows (segment IS NULL) are returned by default. "
        "Results are ordered by period_end ascending. "
        "Cached per accession in Redis; lookup is assembled across cached fact sets."
    ),
    tags=["facts"],
)
def get_fact_time_series(
    ticker: str,
    fact_name: str,
    db: DBDep,
    cache: CacheDep,
    segment: Annotated[str | None, Query(description="Filter to a specific segment label")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> TimeSeriesResponse:
    ticker_upper = ticker.upper()

    cik = _lookup_cik(ticker_upper, db, cache)
    if cik is None:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found")

    seg_filter = "AND ff.segment = :seg" if segment else "AND ff.segment IS NULL"
    params: dict[str, Any] = {"cik": cik, "fact": fact_name, "limit": limit, "offset": offset}
    if segment:
        params["seg"] = segment

    rows = db.execute(
        text(
            f"""
            SELECT ff.accession_number, fr.filing_type,
                   ff.period_start, ff.period_end,
                   ff.fact_value, ff.unit, ff.segment
            FROM financial_facts ff
            JOIN filings_raw fr ON fr.accession_number = ff.accession_number
            WHERE fr.cik = :cik
              AND ff.fact_name = :fact
              {seg_filter}
            ORDER BY ff.period_end ASC
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for ticker '{ticker}' fact '{fact_name}'",
        )

    return TimeSeriesResponse(
        ticker=ticker_upper,
        fact_name=fact_name,
        series=[
            TimeSeriesPoint(
                accession_number=r[0],
                filing_type=r[1],
                period_start=r[2],
                period_end=r[3],
                fact_value=float(r[4]) if r[4] is not None else None,
                unit=r[5],
                segment=r[6],
            )
            for r in rows
        ],
    )


@app.get(
    "/anomalies/{ticker}",
    response_model=AnomaliesResponse,
    summary="Anomaly scores for a ticker's filings",
    description=(
        "Returns the most recent anomaly score for each of a ticker's filings, "
        "highest score first. Each row carries the reason it was flagged and the "
        "model version that produced it, so a reviewer can action it without "
        "re-running the model. Set `only_anomalies=true` to return just the "
        "review queue. Not cached — scores change when a new model is promoted, "
        "and a stale score is worse than a slow one."
    ),
    tags=["anomalies"],
)
def list_anomalies(
    ticker: str,
    db: DBDep,
    only_anomalies: Annotated[
        bool, Query(description="Return only filings flagged as anomalous")
    ] = False,
    min_score: Annotated[float, Query(ge=0.0, le=1.0)] = 0.0,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AnomaliesResponse:
    ticker_upper = ticker.upper()

    # `fa.id IN (SELECT MAX(id) ...)` keeps only the newest score per filing, so
    # re-scoring under a new model supersedes rather than duplicates. Expressed
    # with MAX(id) rather than a window function to stay portable across the
    # SQLite used in tests and the PostgreSQL used in production.
    filters = ["UPPER(fr.ticker) = :ticker", "fa.score >= :min_score"]
    if only_anomalies:
        filters.append("fa.is_anomaly = TRUE")

    where_clause = " AND ".join(filters)
    params: dict[str, Any] = {
        "ticker": ticker_upper,
        "min_score": min_score,
        "limit": limit,
        "offset": offset,
    }

    rows = db.execute(
        text(
            f"""
            SELECT fa.accession_number, fr.filing_type, fr.filing_date,
                   fa.score, fa.model_score, fa.rule_score, fa.is_anomaly,
                   fa.triggered_by, fa.reason, fa.rule_violations,
                   mr.model_version, fa.scored_at
            FROM fact_anomalies fa
            JOIN filings_raw fr ON fr.accession_number = fa.accession_number
            JOIN model_runs mr ON mr.id = fa.model_run_id
            WHERE {where_clause}
              AND fa.id IN (SELECT MAX(id) FROM fact_anomalies GROUP BY accession_number)
            ORDER BY fa.score DESC, fa.accession_number
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No anomaly scores found for ticker '{ticker}'. "
                "The filings may not have been scored yet — check that a model "
                "is promoted and the score_anomalies task has run."
            ),
        )

    total = db.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM fact_anomalies fa
            JOIN filings_raw fr ON fr.accession_number = fa.accession_number
            WHERE {where_clause}
              AND fa.id IN (SELECT MAX(id) FROM fact_anomalies GROUP BY accession_number)
            """
        ),
        {"ticker": ticker_upper, "min_score": min_score},
    ).scalar_one()

    return AnomaliesResponse(
        ticker=ticker_upper,
        total=int(total),
        limit=limit,
        offset=offset,
        items=[
            AnomalyRow(
                accession_number=r[0],
                filing_type=r[1],
                filing_date=r[2],
                score=float(r[3]),
                model_score=float(r[4]),
                rule_score=float(r[5]),
                is_anomaly=bool(r[6]),
                triggered_by=r[7],
                reason=r[8],
                rule_violations=_decode_violations(r[9]),
                model_version=r[10],
                scored_at=r[11],
            )
            for r in rows
        ],
    )


@app.get(
    "/model/current",
    response_model=ModelInfoResponse,
    summary="Provenance of the promoted anomaly model",
    description=(
        "Returns the metadata of the model currently serving scores, including "
        "the artifact SHA-256, the training-data hash, the git commit it was "
        "trained from, and its evaluation metrics. `verified` is the result of "
        "re-hashing the artifact on disk and comparing it against the digest "
        "recorded at registration — false means the file has changed since it "
        "was registered and its scores should not be trusted."
    ),
    tags=["ops"],
)
def model_info() -> ModelInfoResponse:
    from src.ml.registry import ModelNotFoundError, ModelRegistry

    registry = ModelRegistry(os.getenv("MODEL_REGISTRY_ROOT", "models"))

    try:
        version = registry.production_version()
        if version is None:
            raise ModelNotFoundError("No model has been promoted to production")
        metadata = registry.get_metadata(version)
    except (ModelNotFoundError, FileNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        verified = registry.verify(version)
    except (ValueError, FileNotFoundError) as exc:
        logger.error("Model artifact verification failed for %s: %s", version, exc)
        verified = False

    return ModelInfoResponse(
        version=metadata.version,
        trained_at=metadata.trained_at,
        artifact_sha256=metadata.artifact_sha256,
        training_data_sha256=metadata.training_data_sha256,
        n_training_filings=metadata.n_training_filings,
        git_sha=metadata.git_sha,
        threshold=metadata.threshold,
        contamination=metadata.contamination,
        seed=metadata.seed,
        feature_names=metadata.feature_names,
        metrics=metadata.metrics,
        verified=verified,
    )


@app.get(
    "/audit/{accession}",
    response_model=AccessionAuditResponse,
    summary="Extraction audit history for one filing",
    description=(
        "Returns every recorded extraction-audit event for one accession "
        "number — one row per stage per attempt (download/parse/load), "
        "newest first — each carrying the system that ran it, the outcome, "
        "and a hash chained to the row written immediately before it. This "
        "is what answers a regulator's 'where did this number come from, "
        "and prove it' in a single query instead of a manual reconstruction. "
        "Not cached — an audit trail serving a stale answer defeats its "
        "purpose. Chain integrity is checked separately: see "
        "`scripts/verify_audit_chain.py`, since verifying the WHOLE chain "
        "on every request to this endpoint would be needlessly expensive "
        "for what is fundamentally a read of one accession's history."
    ),
    tags=["audit"],
)
def get_accession_audit(
    accession: str,
    db: DBDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AccessionAuditResponse:
    rows = db.execute(
        text(
            """
            SELECT id, run_id, system_id, stage, extraction_status, detail,
                   content_hash, row_hash, prev_row_hash, created_at
            FROM extraction_audit
            WHERE accession_number = :accession
            ORDER BY id DESC
            LIMIT :limit
            """
        ),
        {"accession": accession, "limit": limit},
    ).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No extraction_audit records found for accession '{accession}'. "
                "Either it hasn't been processed yet, or the audit trail feature "
                "(dags/edgar_pipeline.py's per-stage writes) predates this filing."
            ),
        )

    total = db.execute(
        text("SELECT COUNT(*) FROM extraction_audit WHERE accession_number = :accession"),
        {"accession": accession},
    ).scalar_one()

    return AccessionAuditResponse(
        accession_number=accession,
        total=int(total),
        items=[
            ExtractionAuditRow(
                id=r[0],
                run_id=r[1],
                system_id=r[2],
                stage=r[3],
                extraction_status=r[4],
                detail=r[5],
                content_hash=r[6],
                row_hash=r[7],
                prev_row_hash=r[8],
                created_at=r[9],
            )
            for r in rows
        ],
    )


@app.post(
    "/trigger/{ticker}",
    response_model=TriggerResponse,
    status_code=202,
    summary="Trigger on-demand ingestion for a ticker",
    description=(
        "Enqueues an on-demand ingestion run for the given ticker by calling the "
        "Airflow REST API to unpause and trigger the edgar_pipeline DAG. "
        "Returns 202 Accepted immediately; monitor pipeline_audit for progress. "
        "In environments where AIRFLOW_API_URL is not set, returns a mock response."
    ),
    tags=["ingestion"],
)
def trigger_ingestion(
    ticker: str,
    db: DBDep,
    cache: CacheDep,
) -> TriggerResponse:
    ticker_upper = ticker.upper()
    cik = _lookup_cik(ticker_upper, db, cache)

    airflow_url = os.getenv("AIRFLOW_API_URL")

    if not airflow_url:
        # No Airflow configured — return informational mock response
        return TriggerResponse(
            ticker=ticker_upper,
            cik=cik,
            status="queued",
            message=(
                "AIRFLOW_API_URL not set — set it to trigger a real DAG run. "
                "Example: http://localhost:8080"
            ),
        )

    import requests as _requests

    airflow_user = os.getenv("AIRFLOW_USER", "airflow")
    airflow_pass = os.getenv("AIRFLOW_PASSWORD", "airflow")

    dag_run_payload: dict[str, Any] = {
        "conf": {"ticker": ticker_upper, "cik": cik},
        "note": f"On-demand trigger for {ticker_upper}",
    }

    try:
        resp = _requests.post(
            f"{airflow_url}/api/v1/dags/edgar_pipeline/dagRuns",
            json=dag_run_payload,
            auth=(airflow_user, airflow_pass),
            timeout=10,
        )
        resp.raise_for_status()
        run_id = resp.json().get("dag_run_id", "unknown")
        return TriggerResponse(
            ticker=ticker_upper,
            cik=cik,
            status="queued",
            message=f"DAG run enqueued: {run_id}",
        )
    except Exception as exc:
        logger.error("Failed to trigger Airflow DAG for %s: %s", ticker_upper, exc)
        raise HTTPException(status_code=502, detail=f"Airflow trigger failed: {exc}") from exc
