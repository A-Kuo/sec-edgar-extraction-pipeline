"""
Historical backfill CLI.

Downloads and parses all 10-K / 10-Q filings for one or more CIKs within a
date range, then writes them to the PostgreSQL warehouse.

Usage
-----
    python -m scripts.backfill --cik 320193 --start-date 2020-01-01 --end-date 2024-12-31
    python -m scripts.backfill --cik 320193 789019 --form 10-K --dry-run
    python -m scripts.backfill --ticker AAPL MSFT --start-date 2023-01-01

Environment variables required
-------------------------------
    DATABASE_URL   postgresql://...
    REDIS_URL      redis://...  (optional — skipped if absent)
    SEC_USER_AGENT "Your Name email@example.com"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from src.cache import FilingCache
    from src.edgar_client import EdgarClient
    from src.xbrl_parser import XBRLParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("backfill")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="backfill",
        description="Historical ingestion of SEC EDGAR 10-K/10-Q filings.",
    )

    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--cik",
        nargs="+",
        metavar="CIK",
        help="One or more 10-digit CIK numbers (leading zeros optional).",
    )
    target.add_argument(
        "--ticker",
        nargs="+",
        metavar="TICKER",
        help="One or more ticker symbols; CIKs are resolved via EDGAR.",
    )

    p.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        default="2010-01-01",
        help="Earliest filing_date to ingest (default: 2010-01-01).",
    )
    p.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        default=date.today().isoformat(),
        help="Latest filing_date to ingest (default: today).",
    )
    p.add_argument(
        "--form",
        choices=["10-K", "10-Q", "both"],
        default="both",
        help="Filing form type to backfill (default: both).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be ingested without writing to the database.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Number of parallel download workers (default: 1).",
    )
    return p.parse_args()


def _resolve_ciks(tickers: list[str], client: EdgarClient) -> list[str]:
    """Resolve ticker symbols to CIK strings via EDGAR."""
    ciks: list[str] = []
    for ticker in tickers:
        try:
            # EDGAR ticker-to-CIK: search the company tickers JSON
            import requests as _r

            resp = _r.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers={"User-Agent": os.getenv("SEC_USER_AGENT", "BackfillCLI user@example.com")},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            match = next(
                (v["cik_str"] for v in data.values() if v["ticker"].upper() == ticker.upper()),
                None,
            )
            if match:
                ciks.append(str(match))
                logger.info("Resolved %s → CIK %s", ticker, match)
            else:
                logger.warning("Could not resolve ticker %s — skipping", ticker)
        except Exception as exc:
            logger.error("Ticker resolution failed for %s: %s", ticker, exc)
    return ciks


def _form_types(form_arg: str) -> list[str]:
    if form_arg == "both":
        return ["10-K", "10-Q"]
    return [form_arg]


def _ingest_cik(
    cik: str,
    start_date: str,
    end_date: str,
    form_types: list[str],
    dry_run: bool,
    client: EdgarClient,
    parser: XBRLParser,
    engine: Engine,
    cache: FilingCache | None,
) -> tuple[int, int]:
    """
    Ingest all matching filings for a single CIK.

    Returns (filings_processed, facts_inserted).
    """
    from sqlalchemy.orm import Session

    from src.schema import FilingRaw, FinancialFact

    logger.info("Fetching submission history for CIK %s", cik)
    submissions = client.get_company_filings(cik)
    recent = submissions.get("filings", {}).get("recent", {})

    accessions = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    periods = recent.get("reportDate", [])
    ticker = (submissions.get("tickers") or [""])[0]

    # Filter to requested date range and form types
    candidates = [
        {
            "accession_number": accessions[i],
            "cik": cik,
            "ticker": ticker,
            "filing_type": forms[i],
            "filing_date": dates[i],
            "period_of_report": periods[i] if i < len(periods) else None,
        }
        for i in range(len(accessions))
        if forms[i] in form_types and start_date <= dates[i] <= end_date
    ]

    logger.info(
        "CIK %s: %d filings in range [%s, %s] for form(s) %s",
        cik,
        len(candidates),
        start_date,
        end_date,
        form_types,
    )

    if dry_run:
        for c in candidates:
            print(f"  [DRY RUN] {c['accession_number']}  {c['filing_type']}  {c['filing_date']}")
        return len(candidates), 0

    filings_done = 0
    facts_total = 0

    for filing in candidates:
        acc = filing["accession_number"]
        logger.info("  ↳ Downloading %s (%s %s)", acc, filing["filing_type"], filing["filing_date"])

        try:
            # Check cache first
            cached_facts = cache.get_facts(acc) if cache else None

            if cached_facts is None:
                html = client.get_filing_document(
                    f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/"
                )
                fact_rows = parser.parse(html, acc)
            else:
                fact_rows = None  # already cached — skip DB write for facts

            with Session(engine) as session:
                # Upsert filing_raw
                existing = session.get(FilingRaw, acc)
                if existing is None:
                    session.add(
                        FilingRaw(
                            accession_number=acc,
                            cik=cik,
                            ticker=filing.get("ticker"),
                            filing_type=filing["filing_type"],
                            filing_date=filing["filing_date"],
                            period_of_report=filing.get("period_of_report") or None,
                            raw_html=html if cached_facts is None else None,
                            pipeline_run_id="backfill",
                        )
                    )

                # Insert facts
                if fact_rows:
                    for row in fact_rows:
                        d = row.as_dict()
                        # Convert Decimal to float for SQLAlchemy
                        if d.get("fact_value") is not None:
                            d["fact_value"] = float(d["fact_value"])
                        session.add(FinancialFact(**d))
                    facts_total += len(fact_rows)

                    # Cache the parsed facts
                    if cache:
                        cache.set_facts(acc, [r.as_dict() for r in fact_rows])

                session.commit()

            filings_done += 1
            logger.info("    ✓ %d facts inserted for %s", len(fact_rows) if fact_rows else 0, acc)

        except KeyboardInterrupt:
            raise
        except Exception as exc:
            logger.error("    ✗ Failed for %s: %s", acc, exc)

    return filings_done, facts_total


def main() -> None:
    args = _parse_args()

    database_url = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")
    redis_url = os.getenv("REDIS_URL")

    from sqlalchemy import create_engine

    from src.cache import FilingCache
    from src.edgar_client import EdgarClient
    from src.xbrl_parser import XBRLParser

    engine = create_engine(database_url, pool_pre_ping=True)
    client = EdgarClient()
    parser = XBRLParser()
    cache = FilingCache(redis_url) if redis_url else None

    # Resolve CIKs
    if args.ticker:
        ciks = _resolve_ciks(args.ticker, client)
    else:
        ciks = [c.lstrip("0").zfill(1) for c in args.cik]  # normalise

    if not ciks:
        logger.error("No valid CIKs to process — exiting.")
        sys.exit(1)

    form_types = _form_types(args.form)
    total_filings = 0
    total_facts = 0

    for cik in ciks:
        f, facts = _ingest_cik(
            cik=cik,
            start_date=args.start_date,
            end_date=args.end_date,
            form_types=form_types,
            dry_run=args.dry_run,
            client=client,
            parser=parser,
            engine=engine,
            cache=cache,
        )
        total_filings += f
        total_facts += facts

    print(
        f"\n{'[DRY RUN] ' if args.dry_run else ''}Backfill complete: "
        f"{total_filings} filings processed, {total_facts} facts inserted."
    )


if __name__ == "__main__":
    main()
