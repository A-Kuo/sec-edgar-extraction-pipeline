"""
Verify the extraction_audit hash chain — the CMMC AU-12 non-repudiation check.

Walks every row in ``extraction_audit``, recomputes each row's hash from its
own stored fields, and confirms it both matches the stored ``row_hash`` and
correctly chains to the previous row's hash. Reports exactly where the chain
breaks, if it does, rather than only a pass/fail bit — see
``src.audit.verify_chain`` for the algorithm and what it does and doesn't
prove.

This is the tool an internal reviewer or an external auditor actually runs.
It's a separate command from a per-request API check deliberately: walking
the whole chain is O(n) in the number of audit rows ever written, which is
fine for a scheduled or on-demand integrity check and would be wasteful to
repeat on every ``GET /audit/{accession}`` call.

Usage
-----
    python -m scripts.verify_audit_chain
    python -m scripts.verify_audit_chain --accession 0000320193-24-000123
    python -m scripts.verify_audit_chain --json

Environment variables required
-------------------------------
    DATABASE_URL   postgresql://...

Exit codes
----------
    0   chain valid
    1   chain broken — see output for where
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from src.audit import AuditChainVerification

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("verify_audit_chain")

_GREEN = "\033[32m"
_RED = "\033[31m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def _c(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}" if sys.stdout.isatty() else text


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="verify_audit_chain",
        description="Verify the extraction_audit hash chain is intact and untampered.",
    )
    p.add_argument(
        "--accession",
        metavar="ACCESSION",
        help="Only report rows for this accession (the chain itself is still verified in full — "
        "a chain is only meaningful checked end to end; this just filters what's printed).",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Print the result as JSON instead of a formatted report.",
    )
    return p.parse_args()


def _get_session() -> Session:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SessionCls

    url = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")
    engine = create_engine(url, pool_pre_ping=True)
    return SessionCls(engine)


def _print_report(
    result: AuditChainVerification, accession_filter: str | None, session: Session
) -> None:
    print()
    print(_c("Extraction audit chain verification", _BOLD))
    print("-" * 60)
    print(f"  Total rows checked : {result.total_rows}")

    if result.valid:
        print(f"  Result             : {_c('VALID', _GREEN)} — chain intact")
    else:
        print(f"  Result             : {_c('BROKEN', _RED)}")
        print(f"  Broken at row id   : {result.broken_at_id}")
        print(f"  Reason             : {result.broken_reason}")

    if accession_filter:
        _print_accession_rows(session, accession_filter)

    print()


def _print_accession_rows(session: Session, accession: str) -> None:
    from sqlalchemy import select

    from src.schema import ExtractionAudit

    rows = (
        session.execute(
            select(ExtractionAudit)
            .where(ExtractionAudit.accession_number == accession)
            .order_by(ExtractionAudit.id.desc())
        )
        .scalars()
        .all()
    )

    print()
    print(_c(f"Rows for accession {accession}", _BOLD))
    if not rows:
        print("  (none found)")
        return
    print(f"  {'stage':<12} {'status':<10} {'system_id':<24} {'created_at'}")
    print("  " + "-" * 70)
    for row in rows:
        print(f"  {row.stage:<12} {row.extraction_status:<10} {row.system_id:<24} {row.created_at}")
        if row.detail:
            print(f"    detail: {row.detail}")


def main() -> int:
    args = _parse_args()

    from src.audit import verify_chain

    session = _get_session()
    try:
        result = verify_chain(session)
    finally:
        session.close()

    if args.json:
        print(json.dumps(result.as_dict(), indent=2))
    else:
        session = _get_session()
        try:
            _print_report(result, args.accession, session)
        finally:
            session.close()

    if not result.valid:
        logger.error("Audit chain verification FAILED: %s", result.summary())
        return 1

    logger.info("Audit chain verification passed: %s", result.summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
