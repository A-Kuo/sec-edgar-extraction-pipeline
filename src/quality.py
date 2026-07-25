import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class PSILevel(StrEnum):
    CLEAN = "clean"
    WARN = "warning"
    ALERT = "alert"


@dataclass
class PSIResult:
    fact_name: str
    psi_value: float
    level: PSILevel
    message: str


@dataclass
class QualityResult:
    completeness_pct: float
    status: str
    missing_count: int
    total_expected: int


def compute_psi(baseline: list[float], current: list[float], bins: int = 10) -> float:
    """Compute Population Stability Index."""
    if not baseline or not current:
        return 0.0

    baseline = np.array(baseline)
    current = np.array(current)

    # Create bins based on baseline distribution
    _, bin_edges = np.histogram(baseline, bins=bins)

    baseline_counts = np.histogram(baseline, bins=bin_edges)[0]
    current_counts = np.histogram(current, bins=bin_edges)[0]

    baseline_pct = baseline_counts / len(baseline)
    current_pct = current_counts / len(current)

    psi = 0.0
    eps = 1e-4
    for i in range(len(baseline_pct)):
        if baseline_pct[i] == 0 or current_pct[i] == 0:
            baseline_pct[i] = max(baseline_pct[i], eps)
            current_pct[i] = max(current_pct[i], eps)

        psi += (current_pct[i] - baseline_pct[i]) * np.log(current_pct[i] / baseline_pct[i])

    return psi


def classify_psi(psi: float) -> PSILevel:
    """Classify PSI into levels."""
    if psi < 0.10:
        return PSILevel.CLEAN
    elif psi < 0.25:
        return PSILevel.WARN
    else:
        return PSILevel.ALERT


def check_completeness(
    run_id: str, facts_by_accession: dict[str, list[Any]], threshold: float = 0.95
) -> QualityResult:
    """Check completeness of facts across filings."""
    total_filings = len(facts_by_accession)
    if total_filings == 0:
        raise ValueError("No filings found for run")

    all_facts = []
    for facts in facts_by_accession.values():
        all_facts.extend(facts)

    if not all_facts:
        raise ValueError("No facts found in filings")

    completeness = len(all_facts) / (total_filings * 8)
    if completeness < threshold:
        raise ValueError(f"Completeness {completeness:.2%} below threshold {threshold:.2%}")

    return QualityResult(
        completeness_pct=completeness,
        status="PASS",
        missing_count=int((1 - completeness) * total_filings * 8),
        total_expected=total_filings * 8,
    )


def check_psi_drift(fact_name: str, baseline: list[float], current: list[float]) -> PSIResult:
    """Check for PSI drift in a fact."""
    psi = compute_psi(baseline, current)
    level = classify_psi(psi)

    message = f"PSI for {fact_name}: {psi:.4f} ({level.value})"
    logger.info(message)

    return PSIResult(fact_name=fact_name, psi_value=psi, level=level, message=message)
