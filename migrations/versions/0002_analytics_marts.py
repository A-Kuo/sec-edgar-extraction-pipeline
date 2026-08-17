"""add fct_company_year and fct_company_year_yoy analytics views

Revision ID: 0002_analytics_marts
Revises: 0001_initial_schema
Create Date: 2026-08-17

financial_facts is an OLTP table -- one row per (accession, fact_name,
period, segment). "What's AAPL's revenue trend over 5 years" currently
means hand-writing a pivot query against it every time. These two views
close that gap:

  fct_company_year      one row per (cik, fiscal_year), with the target
                         GAAP facts pivoted into columns instead of rows.
  fct_company_year_yoy  the same, plus prior-year values and YoY growth
                         percentages via a window function.

Both are plain SQL views (not materialized) -- the underlying fact volume
this project targets doesn't need a materialized/refreshed mart, and a
view stays trivially consistent with financial_facts on every read.
Segmented facts (segment IS NOT NULL) are excluded so consolidated
company-level figures aren't double-counted alongside their segment
breakdowns.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0002_analytics_marts"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _fiscal_year_expr(dialect_name: str) -> str:
    if dialect_name == "postgresql":
        return "EXTRACT(YEAR FROM ff.period_end)::int"
    # SQLite (used in tests) has no EXTRACT(); strftime is the equivalent.
    return "CAST(strftime('%Y', ff.period_end) AS INTEGER)"


def upgrade() -> None:
    dialect_name = op.get_bind().dialect.name
    fiscal_year = _fiscal_year_expr(dialect_name)

    op.execute(f"""
        CREATE VIEW fct_company_year AS
        SELECT
            fr.cik,
            fr.ticker,
            {fiscal_year} AS fiscal_year,
            MAX(CASE WHEN ff.fact_name IN (
                'us-gaap:Revenues',
                'us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax'
            ) THEN ff.fact_value END) AS revenue,
            MAX(CASE WHEN ff.fact_name = 'us-gaap:NetIncomeLoss'
                THEN ff.fact_value END) AS net_income,
            MAX(CASE WHEN ff.fact_name = 'us-gaap:OperatingIncomeLoss'
                THEN ff.fact_value END) AS operating_income,
            MAX(CASE WHEN ff.fact_name = 'us-gaap:Assets'
                THEN ff.fact_value END) AS total_assets,
            MAX(CASE WHEN ff.fact_name = 'us-gaap:Liabilities'
                THEN ff.fact_value END) AS total_liabilities,
            MAX(CASE WHEN ff.fact_name = 'us-gaap:EarningsPerShareBasic'
                THEN ff.fact_value END) AS eps_basic,
            MAX(CASE WHEN ff.fact_name = 'us-gaap:CommonStockSharesOutstanding'
                THEN ff.fact_value END) AS shares_outstanding
        FROM financial_facts ff
        JOIN filings_raw fr ON fr.accession_number = ff.accession_number
        WHERE ff.segment IS NULL
          AND ff.period_end IS NOT NULL
        GROUP BY fr.cik, fr.ticker, {fiscal_year}
    """)

    op.execute("""
        CREATE VIEW fct_company_year_yoy AS
        SELECT
            cik,
            ticker,
            fiscal_year,
            revenue,
            net_income,
            operating_income,
            total_assets,
            total_liabilities,
            eps_basic,
            shares_outstanding,
            LAG(revenue) OVER (PARTITION BY cik ORDER BY fiscal_year) AS prior_year_revenue,
            LAG(net_income) OVER (PARTITION BY cik ORDER BY fiscal_year) AS prior_year_net_income,
            CASE
                WHEN LAG(revenue) OVER (PARTITION BY cik ORDER BY fiscal_year) IS NULL
                  OR LAG(revenue) OVER (PARTITION BY cik ORDER BY fiscal_year) = 0
                THEN NULL
                ELSE ROUND(
                    (revenue - LAG(revenue) OVER (PARTITION BY cik ORDER BY fiscal_year)) * 100.0
                    / LAG(revenue) OVER (PARTITION BY cik ORDER BY fiscal_year),
                    2
                )
            END AS revenue_yoy_growth_pct,
            CASE
                WHEN LAG(net_income) OVER (PARTITION BY cik ORDER BY fiscal_year) IS NULL
                  OR LAG(net_income) OVER (PARTITION BY cik ORDER BY fiscal_year) = 0
                THEN NULL
                ELSE ROUND(
                    (net_income - LAG(net_income) OVER (PARTITION BY cik ORDER BY fiscal_year)) * 100.0
                    / LAG(net_income) OVER (PARTITION BY cik ORDER BY fiscal_year),
                    2
                )
            END AS net_income_yoy_growth_pct
        FROM fct_company_year
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS fct_company_year_yoy")
    op.execute("DROP VIEW IF EXISTS fct_company_year")
