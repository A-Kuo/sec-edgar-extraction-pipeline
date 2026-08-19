"""initial schema: filings_raw, financial_facts, filing_versions, pipeline_audit

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-08-04

Baseline migration matching src/schema.py. The ``versions/`` directory had
no captured migration history before this revision (models were only ever
applied via ``Base.metadata.create_all``), so this is written as a
from-scratch ``CREATE TABLE`` migration rather than an incremental change.

Includes ``uq_financial_facts_accession_fact_period_segment`` from day one,
which is what makes the pipeline's load stage idempotent (see
``dags/edgar_pipeline.py:_upsert_facts``).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Matches src/schema.py's _PKBigInteger: SQLite only auto-increments a
# column declared as exactly INTEGER, so surrogate PKs use BigInteger on
# Postgres and fall back to Integer on SQLite.
_PKBigInteger = sa.BigInteger().with_variant(sa.Integer, "sqlite")


def upgrade() -> None:
    op.create_table(
        "filings_raw",
        sa.Column("accession_number", sa.String(length=25), primary_key=True),
        sa.Column("cik", sa.String(length=10), nullable=False),
        sa.Column("ticker", sa.String(length=10), nullable=True),
        sa.Column("filing_type", sa.String(length=10), nullable=False),
        sa.Column("filing_date", sa.Date(), nullable=False),
        sa.Column("period_of_report", sa.Date(), nullable=True),
        sa.Column("raw_html", sa.Text(), nullable=True),
        sa.Column("raw_xbrl", sa.Text(), nullable=True),
        sa.Column("file_size_bytes", sa.Integer(), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("pipeline_run_id", sa.String(length=64), nullable=True),
    )
    op.create_index("ix_filings_raw_cik", "filings_raw", ["cik"])
    op.create_index("ix_filings_raw_ticker", "filings_raw", ["ticker"])
    op.create_index("ix_filings_raw_filing_date", "filings_raw", ["filing_date"])
    op.create_index("ix_filings_raw_pipeline_run_id", "filings_raw", ["pipeline_run_id"])

    op.create_table(
        "financial_facts",
        sa.Column("id", _PKBigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "accession_number",
            sa.String(length=25),
            sa.ForeignKey("filings_raw.accession_number", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("fact_name", sa.String(length=256), nullable=False),
        sa.Column("fact_value", sa.Numeric(precision=20, scale=4), nullable=True),
        sa.Column("unit", sa.String(length=32), nullable=True),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=True),
        sa.Column("segment", sa.String(length=256), nullable=True),
        sa.Column(
            "parsed_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_financial_facts_accession_number", "financial_facts", ["accession_number"])
    op.create_index("ix_financial_facts_fact_name", "financial_facts", ["fact_name"])
    op.create_index("ix_financial_facts_period_end", "financial_facts", ["period_end"])
    # Unique *expression* index rather than a plain UniqueConstraint: NULL
    # is not equal to NULL in either dialect, and `segment` is NULL for
    # the common (consolidated, non-segmented) case, so a plain
    # constraint on the bare columns would not catch duplicate
    # consolidated facts. COALESCE-ing to sentinels makes it NULL-safe;
    # see the matching explanation in src/schema.py.
    op.create_index(
        "uq_financial_facts_accession_fact_period_segment",
        "financial_facts",
        [
            "accession_number",
            "fact_name",
            sa.text("COALESCE(period_end, '1900-01-01')"),
            sa.text("COALESCE(segment, '')"),
        ],
        unique=True,
    )

    op.create_table(
        "filing_versions",
        sa.Column("id", _PKBigInteger, primary_key=True, autoincrement=True),
        sa.Column("cik", sa.String(length=10), nullable=False),
        sa.Column("filing_type", sa.String(length=10), nullable=False),
        sa.Column("period_of_report", sa.Date(), nullable=False),
        sa.Column("accession_number", sa.String(length=25), nullable=True),
        sa.Column("is_amendment", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("superseded_by", sa.String(length=25), nullable=True),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_filing_versions_cik", "filing_versions", ["cik"])

    op.create_table(
        "pipeline_audit",
        sa.Column("id", _PKBigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("records_processed", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_pipeline_audit_run_id", "pipeline_audit", ["run_id"])


def downgrade() -> None:
    op.drop_table("pipeline_audit")
    op.drop_table("filing_versions")
    op.drop_table("financial_facts")
    op.drop_table("filings_raw")
