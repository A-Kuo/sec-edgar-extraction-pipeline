#!/usr/bin/env python
"""
Backfill script for historical SEC EDGAR data ingestion.

Usage:
    python scripts/backfill.py --cik 0000320193 --start-date 2020-01-01 --end-date 2024-01-01
"""

import argparse
import logging
import os
from datetime import datetime
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.schema import FilingRaw, FinancialFact, Base
from src.edgar_client import EdgarClient
from src.xbrl_parser import XBRLParser
from src.cache import FilingCache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")
USER_AGENT = os.getenv("SEC_USER_AGENT", "SEC-EDGAR-Pipeline aus.kuo03@gmail.com")


def backfill_historical_data(
    cik: str,
    start_date: datetime,
    end_date: datetime
) -> None:
    """Backfill historical filing data for a CIK."""
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()

    Base.metadata.create_all(bind=engine)

    client = EdgarClient(USER_AGENT)
    parser = XBRLParser()
    cache = FilingCache()

    try:
        logger.info(f"Starting backfill for CIK {cik}")
        logger.info(f"Date range: {start_date} to {end_date}")

        filings_data = client.get_company_filings(cik, form_type="10-K")
        if not filings_data:
            logger.warning(f"No filings found for CIK {cik}")
            return

        filings = filings_data.get("filings", {}).get("recent", [])
        logger.info(f"Found {len(filings)} filings for CIK {cik}")

        count = 0
        for filing in filings:
            filing_date_str = filing.get("filingDate")
            if not filing_date_str:
                continue

            try:
                filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"Invalid filing date: {filing_date_str}")
                continue

            if not (start_date <= filing_date <= end_date):
                continue

            accession_number = filing.get("accessionNumber")
            if not accession_number:
                continue

            ticker = filing.get("ticker", "UNKNOWN")
            company_name = filing.get("entityName", "Unknown")
            form_type = filing.get("form", "10-K")
            period_end_str = filing.get("reportDate")

            if not period_end_str:
                continue

            try:
                period_end = datetime.strptime(period_end_str, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"Invalid period end: {period_end_str}")
                continue

            existing = session.query(FilingRaw).filter(
                FilingRaw.accession_number == accession_number
            ).first()

            if existing:
                logger.debug(f"Filing {accession_number} already exists, skipping")
                continue

            logger.info(f"Processing filing {accession_number} ({ticker})")

            document_url = f"https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik}&accession_number={accession_number}&xbrl_type=v"
            raw_html = client.get_filing_document(accession_number) or ""

            if not raw_html:
                logger.warning(f"Failed to retrieve document for {accession_number}")
                raw_html = "<html></html>"

            filing_raw = FilingRaw(
                accession_number=accession_number,
                cik=cik.zfill(10),
                ticker=ticker,
                company_name=company_name,
                form_type=form_type,
                filing_date=filing_date,
                period_end=period_end,
                document_url=document_url,
                raw_html=raw_html,
                pipeline_run_id=f"backfill-{cik}-{datetime.utcnow().isoformat()}"
            )

            session.add(filing_raw)
            session.flush()

            facts = parser.parse(raw_html, accession_number)
            for fact in facts:
                financial_fact = FinancialFact(
                    accession_number=accession_number,
                    fact_name=fact.fact_name,
                    unit=fact.unit,
                    period_start=fact.period_start,
                    period_end=fact.period_end,
                    value=fact.value,
                    segment=fact.segment
                )
                session.add(financial_fact)

            session.commit()
            logger.info(f"Loaded {len(facts)} facts for {accession_number}")
            count += 1

        logger.info(f"Backfill complete: {count} filings loaded")

    except Exception as e:
        logger.error(f"Error during backfill: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
        cache.clear_all()


def main():
    parser = argparse.ArgumentParser(description="Backfill historical SEC EDGAR data")
    parser.add_argument("--cik", required=True, help="CIK of the company to backfill")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        parser.print_help()
        return

    backfill_historical_data(args.cik, start_date, end_date)


if __name__ == "__main__":
    main()
