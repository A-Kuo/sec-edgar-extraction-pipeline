"""
Evaluate a model against absolute floors and the promoted baseline — the CI gate.

Usage
-----
    # CI: train a candidate, then gate it
    python -m scripts.train_model --synthetic --no-register --metrics-out metrics.json
    python -m scripts.evaluate_model --metrics metrics.json

    # Gate a registered version against whatever is currently promoted
    python -m scripts.evaluate_model --version v20260811T142530Z-a1b2c3d4

    # Verify artifact integrity only
    python -m scripts.evaluate_model --verify-only --version latest

Exit codes
----------
    0   passed
    1   failed a floor or regressed against the baseline
    2   could not evaluate (missing metrics, unreadable registry)

Two gates, deliberately
-----------------------
*Floors* stop a model that is bad in absolute terms from ever shipping, even if
it is the first one and has nothing to compare against.

*Regression* stops a model that is worse than what is already serving, even
when it clears the floors. Without it, a series of individually-acceptable
merges can walk performance down indefinitely.

The regression check has a tolerance because the metrics are measured on a
finite corrupted sample and move slightly between runs; only a drop larger than
that noise band counts as a regression.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("evaluate_model")

#: Absolute floors. A model below any of these does not ship.
DEFAULT_FLOORS: dict[str, float] = {
    "recall": 0.60,
    "precision": 0.35,
    "roc_auc": 0.85,
}

#: Metrics must not fall more than this below the promoted model.
DEFAULT_REGRESSION_TOLERANCE = 0.05

#: A flag rate far above the configured contamination means the review queue is
#: about to become unworkable, regardless of how good recall looks.
MAX_FLAG_RATE = 0.30

_GREEN, _RED, _YELLOW, _RESET, _BOLD = "\033[32m", "\033[31m", "\033[33m", "\033[0m", "\033[1m"


def _c(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}" if sys.stdout.isatty() else text


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="evaluate_model",
        description="Gate a candidate model on absolute floors and baseline regression.",
    )
    p.add_argument(
        "--metrics",
        metavar="PATH",
        help="Metrics JSON from train_model --metrics-out.",
    )
    p.add_argument(
        "--version",
        help="Registered version to gate ('latest' allowed). Used instead of --metrics.",
    )
    p.add_argument(
        "--registry",
        default=os.getenv("MODEL_REGISTRY_ROOT", "models"),
        help="Registry root directory (default: models).",
    )
    p.add_argument(
        "--baseline-version",
        help="Version to compare against (default: the promoted PRODUCTION model).",
    )
    p.add_argument(
        "--min-recall", type=float, default=DEFAULT_FLOORS["recall"], help="Recall floor."
    )
    p.add_argument(
        "--min-precision",
        type=float,
        default=DEFAULT_FLOORS["precision"],
        help="Precision floor.",
    )
    p.add_argument(
        "--min-roc-auc", type=float, default=DEFAULT_FLOORS["roc_auc"], help="ROC AUC floor."
    )
    p.add_argument(
        "--max-flag-rate",
        type=float,
        default=MAX_FLAG_RATE,
        help=f"Reject if the flag rate exceeds this (default: {MAX_FLAG_RATE}).",
    )
    p.add_argument(
        "--regression-tolerance",
        type=float,
        default=DEFAULT_REGRESSION_TOLERANCE,
        help=f"Allowed drop vs baseline (default: {DEFAULT_REGRESSION_TOLERANCE}).",
    )
    p.add_argument(
        "--skip-regression",
        action="store_true",
        help="Check floors only. For bootstrapping the very first model.",
    )
    p.add_argument(
        "--verify-only",
        action="store_true",
        help="Only re-hash the artifact and check it matches its recorded digest.",
    )
    p.add_argument(
        "--summary-out",
        metavar="PATH",
        help="Write a Markdown summary (for a CI job summary or PR comment).",
    )
    return p.parse_args()


def _load_candidate_metrics(args: argparse.Namespace) -> tuple[dict[str, float], str]:
    """
    Return ``(metrics, label)`` for whichever source was specified.

    Raises ``FileNotFoundError`` for a missing metrics file or an empty
    registry — both are "could not evaluate" per the exit-code contract in the
    module docstring (exit 2), which ``main()`` maps by catching this
    exception, not ``SystemExit`` (exit 1, reserved for CLI usage errors like
    passing neither ``--metrics`` nor ``--version`` at all).
    """
    if args.metrics:
        path = Path(args.metrics)
        if not path.exists():
            raise FileNotFoundError(f"Metrics file not found: {path}")
        return json.loads(path.read_text(encoding="utf-8")), str(path)

    if not args.version:
        raise SystemExit("Pass either --metrics or --version.")

    from src.ml.registry import ModelRegistry

    registry = ModelRegistry(args.registry)
    version = registry.latest_version() if args.version == "latest" else args.version
    if version is None:
        raise FileNotFoundError(f"No models registered in {args.registry}")
    return registry.get_metadata(version).metrics, version


def _load_baseline_metrics(args: argparse.Namespace) -> tuple[dict[str, float] | None, str | None]:
    from src.ml.registry import ModelNotFoundError, ModelRegistry

    registry = ModelRegistry(args.registry)
    version = args.baseline_version or registry.production_version()
    if version is None:
        return None, None
    try:
        return registry.get_metadata(version).metrics, version
    except ModelNotFoundError:
        logger.warning("Baseline version %s not found; skipping regression check", version)
        return None, None


def main() -> int:
    args = _parse_args()

    from src.ml.registry import ModelRegistry

    # --- integrity only -------------------------------------------------
    if args.verify_only:
        registry = ModelRegistry(args.registry)
        version = registry.latest_version() if args.version in (None, "latest") else args.version
        if version is None:
            logger.error("No models registered in %s", args.registry)
            return 2
        try:
            registry.verify(version)
        except (ValueError, FileNotFoundError) as exc:
            print(_c(f"FAIL  artifact verification: {exc}", _RED))
            return 1
        print(_c(f"PASS  {version} artifact matches its recorded SHA-256", _GREEN))
        return 0

    # --- load ------------------------------------------------------------
    try:
        candidate, label = _load_candidate_metrics(args)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Could not read candidate metrics: %s", exc)
        return 2

    baseline, baseline_version = _load_baseline_metrics(args)

    floors = {
        "recall": args.min_recall,
        "precision": args.min_precision,
        "roc_auc": args.min_roc_auc,
    }

    failures: list[str] = []
    warnings: list[str] = []
    lines: list[str] = []

    print()
    print(_c(f"Gating {label}", _BOLD))
    print("-" * 72)

    # --- floors ----------------------------------------------------------
    print(f"{'metric':<14}{'value':>10}{'floor':>10}   result")
    for metric, floor in floors.items():
        value = candidate.get(metric)
        if value is None:
            failures.append(f"{metric}: missing from candidate metrics")
            print(f"{metric:<14}{'—':>10}{floor:>10.3f}   {_c('MISSING', _RED)}")
            lines.append(f"| `{metric}` | — | {floor:.3f} | MISSING |")
            continue
        ok = value >= floor
        if not ok:
            failures.append(f"{metric} {value:.3f} is below the floor of {floor:.3f}")
        print(
            f"{metric:<14}{value:>10.3f}{floor:>10.3f}   "
            f"{_c('PASS', _GREEN) if ok else _c('FAIL', _RED)}"
        )
        lines.append(f"| `{metric}` | {value:.3f} | {floor:.3f} | {'PASS' if ok else 'FAIL'} |")

    flag_rate = candidate.get("flag_rate")
    if flag_rate is not None:
        ok = flag_rate <= args.max_flag_rate
        if not ok:
            failures.append(
                f"flag_rate {flag_rate:.3f} exceeds the maximum of {args.max_flag_rate:.3f} — "
                "the review queue would be unworkable"
            )
        print(
            f"{'flag_rate':<14}{flag_rate:>10.3f}{args.max_flag_rate:>10.3f}   "
            f"{_c('PASS', _GREEN) if ok else _c('FAIL', _RED)}  (max)"
        )
        lines.append(
            f"| `flag_rate` | {flag_rate:.3f} | ≤ {args.max_flag_rate:.3f} | "
            f"{'PASS' if ok else 'FAIL'} |"
        )

    # --- regression ------------------------------------------------------
    print()
    if args.skip_regression:
        print(_c("Regression check skipped (--skip-regression)", _YELLOW))
    elif baseline is None:
        print(_c("No promoted baseline — regression check skipped (first model)", _YELLOW))
    else:
        print(_c(f"Versus baseline {baseline_version}", _BOLD))
        print(f"{'metric':<14}{'candidate':>12}{'baseline':>11}{'delta':>9}   result")
        for metric in floors:
            new = candidate.get(metric)
            old = baseline.get(metric)
            if new is None or old is None:
                continue
            delta = new - old
            ok = delta >= -args.regression_tolerance
            if not ok:
                failures.append(
                    f"{metric} regressed by {abs(delta):.3f} versus {baseline_version} "
                    f"(tolerance {args.regression_tolerance:.3f})"
                )
            elif delta < 0:
                warnings.append(f"{metric} slipped {abs(delta):.3f} but is within tolerance")
            print(
                f"{metric:<14}{new:>12.3f}{old:>11.3f}{delta:>+9.3f}   "
                f"{_c('PASS', _GREEN) if ok else _c('FAIL', _RED)}"
            )

    # --- verdict ---------------------------------------------------------
    print()
    print("-" * 72)
    for warning in warnings:
        print(_c(f"WARN  {warning}", _YELLOW))

    if failures:
        for failure in failures:
            print(_c(f"FAIL  {failure}", _RED))
        print()
        print(_c(f"Model gate FAILED ({len(failures)} problem(s))", _RED + _BOLD))
        verdict = "FAILED"
        exit_code = 1
    else:
        print(_c("Model gate PASSED", _GREEN + _BOLD))
        verdict = "PASSED"
        exit_code = 0
    print()

    if args.summary_out:
        _write_summary(
            Path(args.summary_out), label, verdict, lines, failures, warnings, baseline_version
        )

    return exit_code


def _write_summary(
    path: Path,
    label: str,
    verdict: str,
    rows: list[str],
    failures: list[str],
    warnings: list[str],
    baseline_version: str | None,
) -> None:
    """Write a Markdown summary suitable for $GITHUB_STEP_SUMMARY."""
    parts = [
        f"## Model gate: {verdict}",
        "",
        f"**Candidate:** `{label}`  ",
        f"**Baseline:** `{baseline_version or 'none (first model)'}`",
        "",
        "| Metric | Value | Threshold | Result |",
        "|---|---|---|---|",
        *rows,
    ]
    if failures:
        parts += ["", "### Failures", *[f"- {f}" for f in failures]]
    if warnings:
        parts += ["", "### Warnings", *[f"- {w}" for w in warnings]]

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")
    logger.info("Wrote gate summary to %s", path)


if __name__ == "__main__":
    raise SystemExit(main())
