"""
SQLAlchemy ORM models for the SEC EDGAR extraction pipeline.

Tables:
  filings_raw      — raw HTML/XBRL landing zone, keyed by accession_number
  financial_facts  — parsed fact rows referencing filings_raw
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
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


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
    """Parsed financial facts extracted from XBRL data."""

    __tablename__ = "financial_facts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
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

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
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

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
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
