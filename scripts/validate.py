#!/usr/bin/env python
"""
Validation script for quality checks against a pipeline run.

Usage:
    python scripts/validate.py --run-id backfill-0000320193-2024-01-01
"""

import argparse
import logging
import os
from typing import Dict, List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.schema import FilingRaw, FinancialFact, PipelineAudit
from src.quality import check_completeness, check_psi_drift

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")


def validate_run(run_id: str) -> None:
    """Run quality validation for a pipeline run."""
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()

    try:
        logger.info(f"Starting validation for run {run_id}")

        audit_records = session.query(PipelineAudit).filter(
            PipelineAudit.run_id == run_id
        ).all()

        if not audit_records:
            logger.warning(f"No audit records found for run {run_id}")
            return

        logger.info(f"Found {len(audit_records)} audit records for run {run_id}")

        filings = session.query(FilingRaw).filter(
            FilingRaw.pipeline_run_id == run_id
        ).all()

        if not filings:
            logger.warning(f"No filings found for run {run_id}")
            return

        logger.info(f"Found {len(filings)} filings for run {run_id}")

        facts_by_accession: Dict[str, List[FinancialFact]] = {}
        for filing in filings:
            facts = session.query(FinancialFact).filter(
                FinancialFact.accession_number == filing.accession_number
            ).all()
            facts_by_accession[filing.accession_number] = facts
            logger.info(f"Filing {filing.accession_number}: {len(facts)} facts")

        logger.info("Running completeness check...")
        try:
            quality_result = check_completeness(run_id, facts_by_accession, threshold=0.95)
            logger.info(
                f"Completeness check PASSED: {quality_result.completeness_pct:.2%} "
                f"({quality_result.total_expected - quality_result.missing_count}/{quality_result.total_expected})"
            )
        except ValueError as e:
            logger.error(f"Completeness check FAILED: {e}")
            return

        logger.info("Running PSI drift checks...")
        fact_names = set()
        for facts in facts_by_accession.values():
            for fact in facts:
                fact_names.add(fact.fact_name)

        for fact_name in fact_names:
            values = []
            for facts in facts_by_accession.values():
                for fact in facts:
                    if fact.fact_name == fact_name:
                        values.append(fact.value)

            if len(values) < 2:
                logger.debug(f"Skipping PSI for {fact_name}: insufficient data points")
                continue

            baseline = values[:-1]
            current = values[-1:]

            psi_result = check_psi_drift(fact_name, baseline, current)
            logger.info(f"PSI {fact_name}: {psi_result.psi_value:.4f} ({psi_result.level.value})")

        logger.info(f"Validation complete for run {run_id}")

    except Exception as e:
        logger.error(f"Error during validation: {e}", exc_info=True)
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Validate quality checks for a pipeline run")
    parser.add_argument("--run-id", required=True, help="Pipeline run ID to validate")

    args = parser.parse_args()
    validate_run(args.run_id)


if __name__ == "__main__":
    main()
