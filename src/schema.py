from datetime import datetime
from sqlalchemy import (
    Column, String, DateTime, Float, Integer, ForeignKey, Index, Text, Enum
)
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()


class FilingRaw(Base):
    __tablename__ = "filing_raw"

    accession_number = Column(String(20), primary_key=True)
    cik = Column(String(10), nullable=False, index=True)
    ticker = Column(String(5), nullable=False, index=True)
    company_name = Column(String(255), nullable=False)
    form_type = Column(String(10), nullable=False)
    filing_date = Column(DateTime, nullable=False, index=True)
    period_end = Column(DateTime, nullable=False)
    document_url = Column(String(500), nullable=False)
    raw_html = Column(Text, nullable=False)
    pipeline_run_id = Column(String(50), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    financial_facts = relationship(
        "FinancialFact",
        back_populates="filing",
        cascade="all, delete-orphan"
    )
    versions = relationship(
        "FilingVersion",
        back_populates="original_filing",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_cik_ticker_date", "cik", "ticker", "filing_date"),
        Index("idx_pipeline_run", "pipeline_run_id"),
    )


class FinancialFact(Base):
    __tablename__ = "financial_fact"

    id = Column(Integer, primary_key=True)
    accession_number = Column(String(20), ForeignKey("filing_raw.accession_number"), nullable=False)
    fact_name = Column(String(100), nullable=False, index=True)
    unit = Column(String(50), nullable=False)
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=False, index=True)
    value = Column(Float, nullable=False)
    segment = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    filing = relationship("FilingRaw", back_populates="financial_facts")

    __table_args__ = (
        Index("idx_fact_name_period", "fact_name", "period_end"),
    )


class FilingVersion(Base):
    __tablename__ = "filing_version"

    id = Column(Integer, primary_key=True)
    original_accession = Column(String(20), ForeignKey("filing_raw.accession_number"), nullable=False)
    amendment_accession = Column(String(20), nullable=True)
    superseded_by = Column(String(20), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    original_filing = relationship("FilingRaw", back_populates="versions")


class PipelineStage(str, enum.Enum):
    FETCH = "fetch_new_filings"
    DOWNLOAD = "download_raw_documents"
    PARSE = "parse_xbrl_facts"
    VALIDATE = "validate_quality_gates"
    LOAD = "load_to_warehouse"
    AUDIT = "update_audit_trail"
    ALERT = "send_alerts_on_failure"


class PipelineAudit(Base):
    __tablename__ = "pipeline_audit"

    id = Column(Integer, primary_key=True)
    run_id = Column(String(50), nullable=False, index=True)
    stage = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False)  # started, completed, failed
    row_count = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index("idx_run_id_stage", "run_id", "stage"),
    )
