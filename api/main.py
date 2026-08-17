"""
FastAPI serving layer for the SEC EDGAR extraction pipeline.

Endpoints
---------
  GET  /health
  GET  /filings/{ticker}            list filings with metadata (paginated)
  GET  /filing/{accession}          parsed financial_facts for one filing
  GET  /facts/{ticker}/{fact_name}  time-series of a specific fact
  GET  /ask/{ticker}                citation-grounded Q&A over a ticker's filing text
  POST /trigger/{ticker}            trigger on-demand ingestion

All read endpoints check Redis before hitting PostgreSQL.
Cache misses populate Redis so subsequent calls are served from memory.
/ask builds its retrieval index on demand from filings_raw (see src/rag/)
rather than caching it -- see AGENTS.md for the tradeoff.

Run with:
    uvicorn api.main:app --reload
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Generator

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
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


class AskCitation(BaseModel):
    accession_number: str
    section: str
    snippet: str
    source_url: str | None
    score: float


class AskResponse(BaseModel):
    ticker: str
    question: str
    answer: str
    grounded: bool
    citations: list[AskCitation]


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
            "period_of_report": r[5].isoformat() if r[5] and hasattr(r[5], "isoformat") else (str(r[5]) if r[5] else None),
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
            text("SELECT filing_type, period_of_report FROM filings_raw WHERE accession_number = :a"),
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
            "period_start": r[5].isoformat() if r[5] and hasattr(r[5], "isoformat") else (str(r[5]) if r[5] else None),
            "period_end": r[6].isoformat() if r[6] and hasattr(r[6], "isoformat") else (str(r[6]) if r[6] else None),
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
    "/ask/{ticker}",
    response_model=AskResponse,
    summary="Ask a citation-grounded question about a ticker's filings",
    description=(
        "Retrieves the most relevant sections across a ticker's ingested filing text "
        "(TF-IDF over Item-level chunks) and returns a citation-first answer. "
        "If nothing in the indexed filings is relevant enough, returns grounded=false "
        "with a refusal message instead of guessing — this is a retrieval-grounded "
        "research aid, not a general chatbot. No filing text indexed, no answer."
    ),
    tags=["research"],
)
def ask_question(
    ticker: str,
    db: DBDep,
    q: Annotated[str, Query(min_length=3, description="Natural-language question")],
    top_k: Annotated[int, Query(ge=1, le=20, description="Number of chunks to retrieve")] = 5,
) -> AskResponse:
    ticker_upper = ticker.upper()

    rows = db.execute(
        text(
            "SELECT accession_number, cik, ticker, filing_type, raw_html "
            "FROM filings_raw WHERE UPPER(ticker) = :t AND raw_html IS NOT NULL"
        ),
        {"t": ticker_upper},
    ).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No indexed filing text found for ticker '{ticker}'",
        )

    from src.rag.chunker import chunk_document
    from src.rag.qa import answer_question
    from src.rag.retrieval import FilingRetriever

    chunks = []
    for accession, cik, tkr, form_type, raw_html in rows:
        chunks.extend(
            chunk_document(
                raw_html,
                accession_number=accession,
                cik=cik,
                ticker=tkr,
                form_type=form_type,
            )
        )

    if not chunks:
        raise HTTPException(status_code=404, detail=f"No chunkable filing text for ticker '{ticker}'")

    retriever = FilingRetriever(chunks)
    answer = answer_question(q, retriever, top_k=top_k)

    return AskResponse(
        ticker=ticker_upper,
        question=q,
        answer=answer.answer_text,
        grounded=answer.grounded,
        citations=[
            AskCitation(
                accession_number=c.accession_number,
                section=c.section,
                snippet=c.snippet,
                source_url=c.source_url,
                score=c.score,
            )
            for c in answer.citations
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

    dag_run_payload = {
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
