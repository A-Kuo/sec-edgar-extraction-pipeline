"""
Tests for the fct_company_year / fct_company_year_yoy analytics views
added in migrations/versions/0002_analytics_marts.py.

Runs the real Alembic migration chain (0001 -> 0002) against a fresh,
file-based SQLite database, then inserts sample filings_raw/financial_facts
rows and queries the views directly -- this exercises the actual SQL in
the migration, not a re-implementation of it in Python.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def marts_db(tmp_path, monkeypatch):
    """Apply migrations 0001+0002 to a fresh on-disk SQLite DB; return its engine."""
    db_path = tmp_path / "marts_test.db"
    db_url = f"sqlite:///{db_path}"
    monkeypatch.setenv("DATABASE_URL", db_url)

    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    command.upgrade(cfg, "head")

    engine = create_engine(db_url)
    yield engine
    engine.dispose()


def _seed_filing(conn, accession: str, cik: str, ticker: str, filing_type: str, filing_date: str) -> None:
    conn.execute(
        text(
            "INSERT INTO filings_raw (accession_number, cik, ticker, filing_type, filing_date) "
            "VALUES (:acc, :cik, :ticker, :ft, :fd)"
        ),
        {"acc": accession, "cik": cik, "ticker": ticker, "ft": filing_type, "fd": filing_date},
    )


def _seed_fact(conn, accession: str, fact_name: str, value: float, period_end: str, segment=None) -> None:
    conn.execute(
        text(
            "INSERT INTO financial_facts (accession_number, fact_name, fact_value, period_end, segment) "
            "VALUES (:acc, :fn, :val, :pe, :seg)"
        ),
        {"acc": accession, "fn": fact_name, "val": value, "pe": period_end, "seg": segment},
    )


class TestFctCompanyYear:
    def test_pivots_facts_into_one_row_per_company_year(self, marts_db):
        with marts_db.begin() as conn:
            _seed_filing(conn, "0000320193-24-000123", "320193", "AAPL", "10-K", "2024-11-01")
            _seed_fact(conn, "0000320193-24-000123", "us-gaap:Revenues", 391035000000, "2024-09-28")
            _seed_fact(conn, "0000320193-24-000123", "us-gaap:NetIncomeLoss", 93736000000, "2024-09-28")
            _seed_fact(conn, "0000320193-24-000123", "us-gaap:Assets", 364980000000, "2024-09-28")

        with marts_db.connect() as conn:
            row = conn.execute(text("SELECT * FROM fct_company_year")).mappings().one()

        assert row["cik"] == "320193"
        assert row["ticker"] == "AAPL"
        assert row["fiscal_year"] == 2024
        assert float(row["revenue"]) == 391035000000
        assert float(row["net_income"]) == 93736000000
        assert float(row["total_assets"]) == 364980000000

    def test_revenue_alias_is_coalesced_into_one_column(self, marts_db):
        """us-gaap:Revenues and its ExcludingAssessedTax alias both map to `revenue`."""
        with marts_db.begin() as conn:
            _seed_filing(conn, "0000789019-24-000010", "789019", "MSFT", "10-K", "2024-07-30")
            _seed_fact(
                conn,
                "0000789019-24-000010",
                "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
                245122000000,
                "2024-06-30",
            )

        with marts_db.connect() as conn:
            row = conn.execute(text("SELECT * FROM fct_company_year")).mappings().one()

        assert float(row["revenue"]) == 245122000000

    def test_segmented_facts_are_excluded_from_consolidated_view(self, marts_db):
        """Segment-level facts shouldn't be double-counted alongside consolidated totals."""
        with marts_db.begin() as conn:
            _seed_filing(conn, "0000320193-24-000123", "320193", "AAPL", "10-K", "2024-11-01")
            _seed_fact(conn, "0000320193-24-000123", "us-gaap:Revenues", 391035000000, "2024-09-28", segment=None)
            _seed_fact(
                conn, "0000320193-24-000123", "us-gaap:Revenues", 201183000000, "2024-09-28", segment="iPhone"
            )

        with marts_db.connect() as conn:
            rows = conn.execute(text("SELECT * FROM fct_company_year")).mappings().all()

        assert len(rows) == 1
        assert float(rows[0]["revenue"]) == 391035000000


class TestFctCompanyYearYoy:
    def test_yoy_growth_computed_across_two_fiscal_years(self, marts_db):
        with marts_db.begin() as conn:
            _seed_filing(conn, "0000320193-23-000077", "320193", "AAPL", "10-K", "2023-11-01")
            _seed_fact(conn, "0000320193-23-000077", "us-gaap:Revenues", 100000000, "2023-09-30")
            _seed_fact(conn, "0000320193-23-000077", "us-gaap:NetIncomeLoss", 20000000, "2023-09-30")

            _seed_filing(conn, "0000320193-24-000123", "320193", "AAPL", "10-K", "2024-11-01")
            _seed_fact(conn, "0000320193-24-000123", "us-gaap:Revenues", 110000000, "2024-09-28")
            _seed_fact(conn, "0000320193-24-000123", "us-gaap:NetIncomeLoss", 25000000, "2024-09-28")

        with marts_db.connect() as conn:
            rows = {
                r["fiscal_year"]: r
                for r in conn.execute(
                    text("SELECT * FROM fct_company_year_yoy ORDER BY fiscal_year")
                ).mappings().all()
            }

        assert rows[2023]["prior_year_revenue"] is None
        assert rows[2023]["revenue_yoy_growth_pct"] is None

        assert float(rows[2024]["prior_year_revenue"]) == 100000000
        assert float(rows[2024]["revenue_yoy_growth_pct"]) == 10.0
        assert float(rows[2024]["net_income_yoy_growth_pct"]) == 25.0

    def test_zero_prior_year_value_does_not_divide_by_zero(self, marts_db):
        with marts_db.begin() as conn:
            _seed_filing(conn, "0000320193-23-000077", "320193", "AAPL", "10-K", "2023-11-01")
            _seed_fact(conn, "0000320193-23-000077", "us-gaap:Revenues", 0, "2023-09-30")

            _seed_filing(conn, "0000320193-24-000123", "320193", "AAPL", "10-K", "2024-11-01")
            _seed_fact(conn, "0000320193-24-000123", "us-gaap:Revenues", 50000000, "2024-09-28")

        with marts_db.connect() as conn:
            rows = {
                r["fiscal_year"]: r
                for r in conn.execute(
                    text("SELECT * FROM fct_company_year_yoy ORDER BY fiscal_year")
                ).mappings().all()
            }

        assert rows[2024]["revenue_yoy_growth_pct"] is None  # would otherwise be a ZeroDivisionError
