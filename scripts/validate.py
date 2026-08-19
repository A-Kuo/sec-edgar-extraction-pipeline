"""
Manual data quality validation CLI.

Runs completeness checks and PSI drift checks against the live database
for a given pipeline run ID and prints a formatted report.

Usage
-----
    python -m scripts.validate --run-id scheduled__2024-11-01T00:00:00
    python -m scripts.validate --run-id scheduled__2024-11-01T00:00:00 --baseline-days 90
    python -m scripts.validate --run-id backfill --facts us-gaap:Revenues us-gaap:Assets
    python -m scripts.validate --list-runs

Environment variables required
-------------------------------
    DATABASE_URL   postgresql://...
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from src.quality import PSIResult, QualityResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("validate")

# Colour helpers (degrade gracefully on Windows without ANSI)
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_RESET = "\033[0m"
_BOLD = "\033[1m"


def _c(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}" if sys.stdout.isatty() else text


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="validate",
        description="Run completeness and PSI drift checks against the pipeline database.",
    )
    p.add_argument(
        "--run-id",
        metavar="RUN_ID",
        help="Pipeline run ID to validate (from pipeline_audit).",
    )
    p.add_argument(
        "--list-runs",
        action="store_true",
        help="List recent run IDs from pipeline_audit and exit.",
    )
    p.add_argument(
        "--baseline-days",
        type=int,
        default=90,
        metavar="N",
        help="Number of days of historical data to use as PSI baseline (default: 90).",
    )
    p.add_argument(
        "--facts",
        nargs="+",
        metavar="FACT",
        default=["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets"],
        help="XBRL fact names to run PSI on.",
    )
    p.add_argument(
        "--completeness-threshold",
        type=float,
        default=0.95,
        metavar="RATIO",
        help="Minimum completeness ratio before flagging failure (default: 0.95).",
    )
    p.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colour output.",
    )
    return p.parse_args()


def _get_engine() -> Engine:
    from sqlalchemy import create_engine

    url = os.getenv("DATABASE_URL", "postgresql://sec_user:sec_pass@localhost/sec_edgar")
    return create_engine(url, pool_pre_ping=True)


def _list_runs(engine: Engine) -> None:
    from sqlalchemy import text

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT run_id, stage, status, records_processed, created_at
                FROM pipeline_audit
                ORDER BY created_at DESC
                LIMIT 40
                """
            )
        ).fetchall()

    if not rows:
        print("No pipeline runs found in pipeline_audit.")
        return

    print(f"\n{'Run ID':<50} {'Stage':<12} {'Status':<12} {'Records':>8}  {'Created At'}")
    print("-" * 110)
    for r in rows:
        print(f"{r[0]:<50} {r[1]:<12} {r[2]:<12} {r[3] or 0:>8}  {r[4]}")


def _get_facts_by_accession(run_id: str, engine: Engine) -> dict[str, list[str]]:
    """Return {accession_number: [fact_name, ...]} for filings in *run_id*."""
    from sqlalchemy import text

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT ff.accession_number, ff.fact_name
                FROM financial_facts ff
                JOIN filings_raw fr ON fr.accession_number = ff.accession_number
                WHERE fr.pipeline_run_id = :run_id
                  AND ff.segment IS NULL
                """
            ),
            {"run_id": run_id},
        ).fetchall()

    result: dict[str, list[str]] = {}
    for acc, fact in rows:
        result.setdefault(acc, []).append(fact)
    return result


def _get_fact_values(
    fact_name: str, days_back: int, engine: Engine
) -> tuple[list[float], list[float]]:
    """
    Return (baseline_values, current_values) for PSI computation.

    baseline: older than *days_back/2* but within *days_back*
    current:  most recent *days_back/2* days
    """
    from sqlalchemy import text

    half = days_back // 2

    with engine.connect() as conn:
        baseline = conn.execute(
            text(
                """
                SELECT CAST(fact_value AS FLOAT)
                FROM financial_facts
                WHERE fact_name = :fact
                  AND segment IS NULL
                  AND fact_value IS NOT NULL
                  AND period_end >= CURRENT_DATE - INTERVAL ':days days'
                  AND period_end < CURRENT_DATE - INTERVAL ':half days'
                """
            ),
            {"fact": fact_name, "days": days_back, "half": half},
        ).fetchall()

        current = conn.execute(
            text(
                """
                SELECT CAST(fact_value AS FLOAT)
                FROM financial_facts
                WHERE fact_name = :fact
                  AND segment IS NULL
                  AND fact_value IS NOT NULL
                  AND period_end >= CURRENT_DATE - INTERVAL ':half days'
                """
            ),
            {"fact": fact_name, "half": half},
        ).fetchall()

    return [r[0] for r in baseline], [r[0] for r in current]


def _print_completeness(result: QualityResult, threshold: float) -> None:
    ratio = result.completeness_ratio
    pct = f"{ratio:.1%}"
    if ratio >= threshold:
        label = _c("PASS", _GREEN)
        pct_str = _c(pct, _GREEN)
    else:
        label = _c("FAIL", _RED)
        pct_str = _c(pct, _RED)

    print(f"\n{_c('Completeness check', _BOLD)}")
    print(f"  Run ID         : {result.run_id}")
    print(f"  Total filings  : {result.total_filings}")
    print(f"  Complete        : {result.complete_filings}")
    print(f"  Ratio           : {pct_str}  (threshold {threshold:.0%})  [{label}]")

    if result.missing_facts_by_accession:
        print(f"  Missing facts ({len(result.missing_facts_by_accession)} filings):")
        for acc, missing in list(result.missing_facts_by_accession.items())[:10]:
            print(f"    {acc}: {', '.join(missing)}")
        if len(result.missing_facts_by_accession) > 10:
            print(f"    … and {len(result.missing_facts_by_accession) - 10} more")


def _print_psi(results: list[PSIResult]) -> None:
    from src.quality import PSILevel

    print(f"\n{_c('PSI drift checks', _BOLD)}")
    print(f"  {'Fact':<60} {'PSI':>8}  {'Level'}")
    print("  " + "-" * 80)
    for r in results:
        if r.level == PSILevel.CLEAN:
            level_str = _c(r.level.value.upper(), _GREEN)
        elif r.level == PSILevel.WARN:
            level_str = _c(r.level.value.upper(), _YELLOW)
        else:
            level_str = _c(r.level.value.upper(), _RED)
        psi_str = f"{r.psi:.4f}"
        print(f"  {r.fact_name:<60} {psi_str:>8}  {level_str}")


def main() -> None:
    args = _parse_args()

    if args.no_color:
        global _GREEN, _YELLOW, _RED, _RESET, _BOLD
        _GREEN = _YELLOW = _RED = _RESET = _BOLD = ""

    engine = _get_engine()

    if args.list_runs:
        _list_runs(engine)
        return

    if not args.run_id:
        print("Error: --run-id is required (or use --list-runs to browse available runs).")
        sys.exit(1)

    from src.quality import (
        REQUIRED_FACTS,
        check_completeness,
        check_psi_drift,
    )

    exit_code = 0

    # ── Completeness ─────────────────────────────────────────────────────────
    print(f"\nValidating run: {_c(args.run_id, _BOLD)}")
    facts_by_acc = _get_facts_by_accession(args.run_id, engine)

    if not facts_by_acc:
        print(_c(f"  No financial_facts rows found for run_id='{args.run_id}'.", _YELLOW))
        print("  (Run the backfill or check that pipeline_run_id was set correctly.)")
    else:
        try:
            result = check_completeness(
                args.run_id,
                facts_by_acc,
                required_facts=REQUIRED_FACTS,
                threshold=args.completeness_threshold,
            )
            _print_completeness(result, args.completeness_threshold)
        except ValueError as exc:
            # check_completeness raises on failure — still print the result
            # Re-run without raising to get the result object for display
            from src.quality import check_completeness as _cc

            with contextlib.suppress(Exception):
                _cc(args.run_id, facts_by_acc, threshold=0.0)
            print(_c(f"\n  COMPLETENESS FAILED: {exc}", _RED))
            exit_code = 1

    # ── PSI drift ────────────────────────────────────────────────────────────
    psi_results = []
    for fact_name in args.facts:
        try:
            baseline, current = _get_fact_values(fact_name, args.baseline_days, engine)
            if not baseline or not current:
                logger.warning(
                    "Not enough data for PSI on %s (baseline=%d, current=%d)",
                    fact_name,
                    len(baseline),
                    len(current),
                )
                continue
            psi_result = check_psi_drift(fact_name, baseline, current)
            psi_results.append(psi_result)
            from src.quality import PSILevel

            if psi_result.level == PSILevel.ALERT:
                exit_code = 1
        except Exception as exc:
            logger.error("PSI check failed for %s: %s", fact_name, exc)

    if psi_results:
        _print_psi(psi_results)
    else:
        print(_c("\n  No PSI data available (insufficient history).", _YELLOW))

    print()
    if exit_code == 0:
        print(_c("✓ All checks passed.", _GREEN))
    else:
        print(_c("✗ One or more checks failed — see details above.", _RED))

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
