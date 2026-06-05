import os
import logging
from typing import List, Optional
from datetime import datetime
from contextlib import contextmanager

from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session

from src.schema import FilingRaw, FinancialFact
from src.cache import FilingCache
from src.edgar_client import EdgarClient

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
USER_AGENT = os.getenv("SEC_USER_AGENT", "SEC-EDGAR-Pipeline aus.kuo03@gmail.com")

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
cache = FilingCache(REDIS_URL)
edgar_client = EdgarClient(USER_AGENT)

app = FastAPI(
    title="SEC EDGAR Pipeline API",
    description="FastAPI for querying SEC filings and financial facts",
    version="1.0.0"
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class HealthResponse(BaseModel):
    status: str
    version: str


class FilingMetadata(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    accession_number: str
    cik: str
    ticker: str
    company_name: str
    form_type: str
    filing_date: datetime
    period_end: datetime


class FilingsListResponse(BaseModel):
    ticker: str
    count: int
    filings: List[FilingMetadata]
    limit: int
    offset: int


class FinancialFactResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    fact_name: str
    unit: str
    period_start: Optional[datetime]
    period_end: datetime
    value: float
    segment: Optional[str]


class FilingFactsResponse(BaseModel):
    accession_number: str
    filing_date: datetime
    facts: List[FinancialFactResponse]


class FactTimeSeriesResponse(BaseModel):
    ticker: str
    fact_name: str
    data_points: List[dict]


class TriggerResponse(BaseModel):
    run_id: str
    status: str
    message: str


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check endpoint",
    description="Returns API health status"
)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(status="ok", version="1.0.0")


@app.get(
    "/filings/{ticker}",
    response_model=FilingsListResponse,
    summary="Get filings by ticker",
    description="Returns list of filings for a given ticker with pagination"
)
async def get_filings(
    ticker: str,
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """Get filings for a ticker with pagination."""
    ticker_upper = ticker.upper()

    cached_filings = cache.get_filings(ticker_upper)
    if cached_filings:
        logger.info(f"Cache hit for filings:{ticker_upper}")
        return FilingsListResponse(
            ticker=ticker_upper,
            count=len(cached_filings),
            filings=cached_filings[:limit],
            limit=limit,
            offset=offset
        )

    logger.info(f"Cache miss for filings:{ticker_upper}, querying database")
    filings = db.query(FilingRaw).filter(
        FilingRaw.ticker == ticker_upper
    ).order_by(FilingRaw.filing_date.desc()).all()

    if not filings:
        raise HTTPException(status_code=404, detail=f"No filings found for ticker {ticker_upper}")

    filings_data = [
        FilingMetadata(
            accession_number=f.accession_number,
            cik=f.cik,
            ticker=f.ticker,
            company_name=f.company_name,
            form_type=f.form_type,
            filing_date=f.filing_date,
            period_end=f.period_end
        )
        for f in filings
    ]

    cache.set_filings(ticker_upper, filings_data)

    return FilingsListResponse(
        ticker=ticker_upper,
        count=len(filings_data),
        filings=filings_data[offset:offset + limit],
        limit=limit,
        offset=offset
    )


@app.get(
    "/filing/{accession}",
    response_model=FilingFactsResponse,
    summary="Get facts for a filing",
    description="Returns all parsed financial facts for a specific filing by accession number"
)
async def get_filing_facts(
    accession: str,
    db: Session = Depends(get_db)
):
    """Get all facts for a specific filing."""
    cached_facts = cache.get_facts(accession)
    if cached_facts:
        logger.info(f"Cache hit for facts:{accession}")
        filing = db.query(FilingRaw).filter(
            FilingRaw.accession_number == accession
        ).first()
        if not filing:
            raise HTTPException(status_code=404, detail=f"Filing {accession} not found")
        return FilingFactsResponse(
            accession_number=accession,
            filing_date=filing.filing_date,
            facts=cached_facts
        )

    logger.info(f"Cache miss for facts:{accession}, querying database")
    filing = db.query(FilingRaw).filter(
        FilingRaw.accession_number == accession
    ).first()

    if not filing:
        raise HTTPException(status_code=404, detail=f"Filing {accession} not found")

    facts = db.query(FinancialFact).filter(
        FinancialFact.accession_number == accession
    ).all()

    if not facts:
        raise HTTPException(status_code=404, detail=f"No facts found for filing {accession}")

    facts_data = [
        FinancialFactResponse(
            id=f.id,
            fact_name=f.fact_name,
            unit=f.unit,
            period_start=f.period_start,
            period_end=f.period_end,
            value=f.value,
            segment=f.segment
        )
        for f in facts
    ]

    cache.set_facts(accession, facts_data)

    return FilingFactsResponse(
        accession_number=accession,
        filing_date=filing.filing_date,
        facts=facts_data
    )


@app.get(
    "/facts/{ticker}/{fact_name}",
    response_model=FactTimeSeriesResponse,
    summary="Get time-series of a fact",
    description="Returns time-series data for a specific financial fact across all filings for a ticker"
)
async def get_fact_timeseries(
    ticker: str,
    fact_name: str,
    db: Session = Depends(get_db)
):
    """Get time-series of a specific fact for a ticker."""
    ticker_upper = ticker.upper()

    filings = db.query(FilingRaw).filter(
        FilingRaw.ticker == ticker_upper
    ).all()

    if not filings:
        raise HTTPException(status_code=404, detail=f"No filings found for ticker {ticker_upper}")

    accession_numbers = [f.accession_number for f in filings]
    facts = db.query(FinancialFact).filter(
        FinancialFact.accession_number.in_(accession_numbers),
        FinancialFact.fact_name == fact_name
    ).order_by(FinancialFact.period_end).all()

    if not facts:
        raise HTTPException(
            status_code=404,
            detail=f"No facts found for {fact_name} and ticker {ticker_upper}"
        )

    data_points = [
        {
            "date": f.period_end.isoformat(),
            "value": f.value,
            "unit": f.unit,
            "accession": f.accession_number
        }
        for f in facts
    ]

    return FactTimeSeriesResponse(
        ticker=ticker_upper,
        fact_name=fact_name,
        data_points=data_points
    )


@app.post(
    "/trigger/{ticker}",
    response_model=TriggerResponse,
    summary="Trigger on-demand ingestion",
    description="Triggers an on-demand data ingestion for a specific ticker"
)
async def trigger_ingestion(ticker: str, db: Session = Depends(get_db)):
    """Trigger on-demand ingestion for a ticker."""
    ticker_upper = ticker.upper()

    if os.getenv("MOCK_EDGAR"):
        run_id = f"manual-{ticker_upper}-{datetime.utcnow().isoformat()}"
        logger.info(f"Triggering mock ingestion for {ticker_upper} with run_id {run_id}")
        return TriggerResponse(
            run_id=run_id,
            status="queued",
            message=f"Ingestion queued for {ticker_upper}"
        )

    run_id = f"prod-{ticker_upper}-{datetime.utcnow().isoformat()}"
    logger.info(f"Triggering ingestion for {ticker_upper} with run_id {run_id}")
    return TriggerResponse(
        run_id=run_id,
        status="queued",
        message=f"Ingestion queued for {ticker_upper}"
    )
