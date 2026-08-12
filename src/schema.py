"""
SQLAlchemy ORM models for the SEC EDGAR extraction pipeline.

Tables:
  filings_raw      — raw HTML/XBRL landing zone, keyed by accession_number
  financial_facts  — parsed fact rows referencing filings_raw
  filing_versions  — immutable version history tracking amendments
  pipeline_audit   — append-only stage-level audit trail
  model_runs       — one row per scoring run, pinning the model version used
  fact_anomalies   — per-filing anomaly scores, joined back to model_runs
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# Every surrogate primary key below is declared BigInteger().with_variant(Integer,
# "sqlite") rather than plain BigInteger. SQLAlchemy only gives a column
# SQLite's autoincrementing rowid-alias behavior when it is exactly `Integer`;
# a `BigInteger` primary key compiles to `BIGINT`, which SQLite does not treat
# as a rowid alias, so every autoincrement insert fails with a NOT NULL
# violation. PostgreSQL is unaffected (BigInteger maps to BIGSERIAL there
# regardless), so this widens portability without changing production
# behavior — it is what makes this project's stated testing strategy ("tests
# run against an in-memory SQLite database") actually hold for ORM-level
# writes, not just for raw text() queries.


class FilingRaw(Base):
    """Raw landing zone — one row per accession number."""

    __tablename__ = "filings_raw"

    accession_number: Mapped[str] = mapped_column(String(25), primary_key=True)
    cik: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    ticker: Mapped[str | None] = mapped_column(String(10), index=True)
    filing_type: Mapped[str] = mapped_column(String(10), nullable=False)  # 10-K, 10-Q
    filing_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    period_of_report: Mapped[date | None] = mapped_column(Date)
    raw_html: Mapped[str | None] = mapped_column(Text)
    raw_xbrl: Mapped[str | None] = mapped_column(Text)
    file_size_bytes: Mapped[int | None] = mapped_column(Integer)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    pipeline_run_id: Mapped[str | None] = mapped_column(String(64), index=True)

    facts: Mapped[list[FinancialFact]] = relationship(
        "FinancialFact", back_populates="filing", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<FilingRaw {self.accession_number} ({self.filing_type})>"


class FinancialFact(Base):
    """
    Parsed financial facts extracted from XBRL data.

    ``fact_hash`` is a SHA-256 of the row's natural key (accession_number,
    fact_name, period_start, period_end, segment) — see
    ``src.upsert.compute_fact_hash``. It is the UPSERT conflict target: a
    re-parse of the same filing (e.g. an Airflow retry after a worker crash
    mid-batch) produces the identical hash for the identical fact and updates
    the existing row instead of inserting a duplicate. Two independent facts
    can never collide on it short of a SHA-256 collision, and any change to
    the natural key (a different period, a different segment) is a different
    fact by definition and correctly gets a different hash.
    """

    __tablename__ = "financial_facts"
    __table_args__ = (UniqueConstraint("fact_hash", name="uq_financial_facts_fact_hash"),)

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    accession_number: Mapped[str] = mapped_column(
        String(25),
        ForeignKey("filings_raw.accession_number", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    fact_name: Mapped[str] = mapped_column(
        String(256), nullable=False, index=True
    )  # e.g. us-gaap:Revenues
    fact_value: Mapped[Decimal | None] = mapped_column(Numeric(precision=20, scale=4))
    unit: Mapped[str | None] = mapped_column(String(32))  # USD, shares, etc.
    period_start: Mapped[date | None] = mapped_column(Date)
    period_end: Mapped[date | None] = mapped_column(Date, index=True)
    segment: Mapped[str | None] = mapped_column(String(256))
    fact_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    parsed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    filing: Mapped[FilingRaw] = relationship("FilingRaw", back_populates="facts")

    def __repr__(self) -> str:
        return f"<FinancialFact {self.fact_name}={self.fact_value} ({self.accession_number})>"


class FilingVersion(Base):
    """Immutable version history — tracks amendments and supersessions."""

    __tablename__ = "filing_versions"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    cik: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    filing_type: Mapped[str] = mapped_column(String(10), nullable=False)
    period_of_report: Mapped[date] = mapped_column(Date, nullable=False)
    accession_number: Mapped[str | None] = mapped_column(String(25))
    is_amendment: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    superseded_by: Mapped[str | None] = mapped_column(String(25))  # accession_number of amendment
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return f"<FilingVersion cik={self.cik} {self.filing_type} {self.period_of_report}>"


class PipelineAudit(Base):
    """Append-only audit trail — no UPDATEs or DELETEs should ever run on this table."""

    __tablename__ = "pipeline_audit"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    stage: Mapped[str] = mapped_column(
        String(32), nullable=False
    )  # ingest | parse | validate | load
    status: Mapped[str] = mapped_column(String(16), nullable=False)  # started | completed | failed
    records_processed: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return f"<PipelineAudit run={self.run_id} stage={self.stage} status={self.status}>"


class ModelRun(Base):
    """
    One anomaly-scoring pass, pinning exactly which model produced the scores.

    ``model_version`` and ``model_sha256`` together answer the question the rest
    of this pipeline is built to answer, but for a model rather than a fact:
    given a score somebody is disputing, which artifact produced it, from which
    commit, against which training set. Without this row a score is an unsourced
    assertion — the same problem the raw/parsed/audit split solves for facts.
    """

    __tablename__ = "model_runs"
    __table_args__ = (
        # One row per (run_id, model_version): a retried scoring task upserts
        # the aggregate counts in place instead of orphaning the previous
        # attempt's row and its fact_anomalies children.
        UniqueConstraint("run_id", "model_version", name="uq_model_runs_run_id_version"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    model_version: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    model_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    git_sha: Mapped[str | None] = mapped_column(String(40))
    filings_scored: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    anomalies_flagged: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)
    #: Serialised DriftReport for this batch — null when no baseline existed yet.
    drift_report: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    drift_level: Mapped[str | None] = mapped_column(String(16))  # clean | warn | alert
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    anomalies: Mapped[list[FactAnomaly]] = relationship(
        "FactAnomaly", back_populates="model_run", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<ModelRun run={self.run_id} model={self.model_version} "
            f"flagged={self.anomalies_flagged}/{self.filings_scored}>"
        )


class FactAnomaly(Base):
    """
    An anomaly score for one filing under one model run.

    A score is an observation about a filing, never a correction to it — no
    column here can alter a value in ``financial_facts``. ``reason`` stores the
    human-readable explanation and ``rule_violations`` the structured detail, so
    a reviewer can action the flag without re-running the model.
    """

    __tablename__ = "fact_anomalies"
    __table_args__ = (
        # One score per filing per run. Makes the DAG's scoring stage
        # re-runnable: a retried task upserts rather than duplicating.
        UniqueConstraint("model_run_id", "accession_number", name="uq_anomaly_run_accession"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    model_run_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("model_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    accession_number: Mapped[str] = mapped_column(
        String(25),
        ForeignKey("filings_raw.accession_number", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    score: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    model_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    rule_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    is_anomaly: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    #: "rule", "model", or "both" — which half of the detector fired.
    triggered_by: Mapped[str | None] = mapped_column(String(8))
    reason: Mapped[str | None] = mapped_column(Text)
    rule_violations: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)
    top_contributors: Mapped[list[list[Any]] | None] = mapped_column(JSON)
    scored_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    model_run: Mapped[ModelRun] = relationship("ModelRun", back_populates="anomalies")

    def __repr__(self) -> str:
        return (
            f"<FactAnomaly {self.accession_number} score={self.score:.3f} "
            f"anomaly={self.is_anomaly}>"
        )
