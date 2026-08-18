"""
SQLAlchemy ORM models for the SEC EDGAR extraction pipeline.

This schema stores iXBRL-tagged financial facts extracted deterministically
from SEC filings. All rows in ``financial_facts`` are currently
``method='xbrl'`` (implicit — no column yet). When the LLM extraction repo
is ready to write ``method='llm'`` facts, a ``method`` column (plus
``confidence``, ``model_version``) should be added via migration. See
``docs/BOUNDARY.md`` for the precedence rule and handoff surface.

Tables:
  filings_raw      — raw HTML/XBRL landing zone, keyed by accession_number
  financial_facts  — parsed iXBRL fact rows referencing filings_raw
  filing_versions  — immutable version history tracking amendments
  pipeline_audit   — append-only stage-level audit trail
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    column,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# SQLite only treats a column as an auto-incrementing alias of the rowid
# when its declared type is exactly INTEGER; BigInteger compiles to BIGINT
# there and silently stops autoincrementing (Postgres is unaffected — it
# uses sequences, not rowid aliasing). All surrogate primary keys use this
# variant so the schema behaves the same way against the SQLite used in
# tests as it does against the Postgres used in production.
_PKBigInteger = BigInteger().with_variant(Integer, "sqlite")


class FilingRaw(Base):
    """Raw landing zone — one row per accession number."""

    __tablename__ = "filings_raw"

    accession_number: Mapped[str] = mapped_column(String(25), primary_key=True)
    cik: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    ticker: Mapped[Optional[str]] = mapped_column(String(10), index=True)
    filing_type: Mapped[str] = mapped_column(String(10), nullable=False)  # 10-K, 10-Q
    filing_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    period_of_report: Mapped[Optional[date]] = mapped_column(Date)
    raw_html: Mapped[Optional[str]] = mapped_column(Text)
    raw_xbrl: Mapped[Optional[str]] = mapped_column(Text)
    file_size_bytes: Mapped[Optional[int]] = mapped_column(Integer)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    pipeline_run_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)

    facts: Mapped[list["FinancialFact"]] = relationship(
        "FinancialFact", back_populates="filing", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<FilingRaw {self.accession_number} ({self.filing_type})>"


class FinancialFact(Base):
    """
    Parsed financial facts extracted from XBRL data.

    The unique index below is what makes ``load_to_warehouse`` in
    ``dags/edgar_pipeline.py`` idempotent: re-running the load stage for
    facts that were already loaded (e.g. an Airflow task retry, or a
    re-processed amendment) updates the existing row instead of inserting
    a duplicate. See ``_upsert_facts`` for the ON CONFLICT logic that
    targets this index.

    A plain ``UniqueConstraint(accession_number, fact_name, period_end,
    segment)`` would *not* actually prevent duplicates: both Postgres and
    SQLite treat every NULL as distinct from every other NULL, and
    ``segment`` is NULL for the large majority of facts (any consolidated,
    non-segmented value — the common case). Two identical consolidated
    facts would silently bypass a plain constraint. Instead this uses a
    unique *expression* index that COALESCEs the nullable columns to
    sentinel values, so NULL segments/periods are treated consistently
    for deduplication purposes. The mapped ``period_end``/``segment``
    attributes themselves are untouched — still nullable, still meaning
    "no period" / "consolidated" everywhere else (xbrl_parser.py,
    api/main.py).
    """

    __tablename__ = "financial_facts"
    __table_args__ = (
        Index(
            "uq_financial_facts_accession_fact_period_segment",
            "accession_number",
            "fact_name",
            func.coalesce(column("period_end"), text("'1900-01-01'")),
            func.coalesce(column("segment"), text("''")),
            unique=True,
        ),
    )

    id: Mapped[int] = mapped_column(_PKBigInteger, primary_key=True, autoincrement=True)
    accession_number: Mapped[str] = mapped_column(
        String(25),
        ForeignKey("filings_raw.accession_number", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    fact_name: Mapped[str] = mapped_column(
        String(256), nullable=False, index=True
    )  # e.g. us-gaap:Revenues
    fact_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=20, scale=4))
    unit: Mapped[Optional[str]] = mapped_column(String(32))  # USD, shares, etc.
    period_start: Mapped[Optional[date]] = mapped_column(Date)
    period_end: Mapped[Optional[date]] = mapped_column(Date, index=True)
    segment: Mapped[Optional[str]] = mapped_column(String(256))
    parsed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    filing: Mapped["FilingRaw"] = relationship("FilingRaw", back_populates="facts")

    def __repr__(self) -> str:
        return f"<FinancialFact {self.fact_name}={self.fact_value} ({self.accession_number})>"


class FilingVersion(Base):
    """Immutable version history — tracks amendments and supersessions."""

    __tablename__ = "filing_versions"

    id: Mapped[int] = mapped_column(_PKBigInteger, primary_key=True, autoincrement=True)
    cik: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    filing_type: Mapped[str] = mapped_column(String(10), nullable=False)
    period_of_report: Mapped[date] = mapped_column(Date, nullable=False)
    accession_number: Mapped[Optional[str]] = mapped_column(String(25))
    is_amendment: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    superseded_by: Mapped[Optional[str]] = mapped_column(
        String(25)
    )  # accession_number of amendment
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<FilingVersion cik={self.cik} {self.filing_type} {self.period_of_report}>"
        )


class PipelineAudit(Base):
    """Append-only audit trail — no UPDATEs or DELETEs should ever run on this table."""

    __tablename__ = "pipeline_audit"

    id: Mapped[int] = mapped_column(_PKBigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    stage: Mapped[str] = mapped_column(
        String(32), nullable=False
    )  # ingest | parse | validate | load
    status: Mapped[str] = mapped_column(
        String(16), nullable=False
    )  # started | completed | failed
    records_processed: Mapped[Optional[int]] = mapped_column(Integer)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return f"<PipelineAudit run={self.run_id} stage={self.stage} status={self.status}>"
