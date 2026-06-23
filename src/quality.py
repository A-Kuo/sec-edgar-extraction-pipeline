"""
Data quality monitoring for the SEC EDGAR extraction pipeline.

Two checks are implemented:

1. **Completeness** — what fraction of filings in a run have all
   ``REQUIRED_FACTS`` populated in ``financial_facts``.  Fails if the
   fraction falls below *threshold* (default 95 %).

2. **PSI (Population Stability Index)** — measures distribution drift
   between a baseline period and the current batch for each fact's
   numeric values.

   PSI thresholds (industry standard):
     PSI < 0.10  → no significant drift   (CLEAN)
     PSI 0.10–0.25 → moderate drift       (WARN)
     PSI > 0.25  → significant drift      (ALERT)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Sequence

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_FACTS: list[str] = [
    "us-gaap:Revenues",
    "us-gaap:NetIncomeLoss",
    "us-gaap:Assets",
]

PSI_WARN_THRESHOLD = 0.10
PSI_ALERT_THRESHOLD = 0.25

_EPSILON = 1e-4  # Avoid log(0) / division-by-zero in PSI calculation


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


class PSILevel(str, Enum):
    CLEAN = "clean"
    WARN = "warn"
    ALERT = "alert"


@dataclass
class QualityResult:
    """Outcome of a completeness check."""

    run_id: str
    total_filings: int
    complete_filings: int
    missing_facts_by_accession: dict[str, list[str]] = field(default_factory=dict)

    @property
    def completeness_ratio(self) -> float:
        if self.total_filings == 0:
            return 1.0
        return self.complete_filings / self.total_filings

    @property
    def passed(self) -> bool:
        return self._threshold is not None and self.completeness_ratio >= self._threshold

    # Set externally after construction by check_completeness()
    _threshold: float = field(default=0.95, init=False, repr=False)

    def __str__(self) -> str:
        return (
            f"QualityResult(run={self.run_id}, "
            f"complete={self.complete_filings}/{self.total_filings}, "
            f"ratio={self.completeness_ratio:.1%})"
        )


@dataclass
class PSIResult:
    """Outcome of a PSI drift check for one fact."""

    fact_name: str
    psi: float
    level: PSILevel
    baseline_count: int
    current_count: int

    def __str__(self) -> str:
        return (
            f"PSIResult(fact={self.fact_name}, psi={self.psi:.4f}, "
            f"level={self.level.value})"
        )


# ---------------------------------------------------------------------------
# 1. Completeness check
# ---------------------------------------------------------------------------


def check_completeness(
    run_id: str,
    facts_by_accession: dict[str, list[str]],
    required_facts: list[str] = REQUIRED_FACTS,
    threshold: float = 0.95,
) -> QualityResult:
    """
    Verify that at least *threshold* fraction of filings in *run_id* have
    all *required_facts* present.

    Args:
        run_id:               Airflow run ID (used for labelling only).
        facts_by_accession:   Mapping of accession_number → list of
                              fact_names present in ``financial_facts``.
                              Build this from a DB query before calling.
        required_facts:       Facts that must be present; defaults to
                              ``REQUIRED_FACTS``.
        threshold:            Minimum acceptable completeness ratio
                              (0–1).  Raise ``ValueError`` if below.

    Returns:
        ``QualityResult`` — inspect ``.passed`` / ``.completeness_ratio``.

    Raises:
        ``ValueError`` if completeness is below *threshold*.
    """
    total = len(facts_by_accession)
    complete = 0
    missing: dict[str, list[str]] = {}

    for accession, present in facts_by_accession.items():
        present_set = set(present)
        absent = [f for f in required_facts if f not in present_set]
        if not absent:
            complete += 1
        else:
            missing[accession] = absent

    result = QualityResult(
        run_id=run_id,
        total_filings=total,
        complete_filings=complete,
        missing_facts_by_accession=missing,
    )
    result._threshold = threshold

    logger.info(
        "Completeness check run=%s  %d/%d filings complete (%.1f%%)",
        run_id,
        complete,
        total,
        result.completeness_ratio * 100,
    )

    if not result.passed:
        raise ValueError(
            f"Completeness below threshold: {result.completeness_ratio:.1%} < {threshold:.1%} "
            f"(run={run_id}, missing in {len(missing)} filings)"
        )

    return result


# ---------------------------------------------------------------------------
# 2. PSI computation
# ---------------------------------------------------------------------------


def compute_psi(
    baseline_values: Sequence[float],
    current_values: Sequence[float],
    bins: int = 10,
) -> float:
    """
    Compute the Population Stability Index between two numeric distributions.

    PSI = Σ (A_i − E_i) × ln(A_i / E_i)

    where:
      E_i = fraction of *baseline_values* falling in bin i
      A_i = fraction of *current_values*  falling in bin i

    A small epsilon replaces zero proportions to avoid log(0).

    Args:
        baseline_values:  Reference (historical) numeric values.
        current_values:   Current period numeric values.
        bins:             Number of equal-width bins (default 10).

    Returns:
        PSI as a float (≥ 0).  Returns 0.0 for trivially identical inputs.

    Raises:
        ``ValueError`` if either sequence is empty.
    """
    if not baseline_values:
        raise ValueError("baseline_values must not be empty")
    if not current_values:
        raise ValueError("current_values must not be empty")

    base = list(baseline_values)
    curr = list(current_values)

    # Determine bin edges from the combined range
    all_vals = base + curr
    min_val = min(all_vals)
    max_val = max(all_vals)

    if min_val == max_val:
        # All values identical → no drift
        return 0.0

    step = (max_val - min_val) / bins
    edges = [min_val + i * step for i in range(bins + 1)]
    edges[-1] = max_val + 1e-9  # ensure the max value is included

    def proportions(values: list[float]) -> list[float]:
        counts = [0] * bins
        n = len(values)
        for v in values:
            idx = 0
            for b in range(bins):
                if v < edges[b + 1]:
                    idx = b
                    break
            else:
                idx = bins - 1
            counts[idx] += 1
        return [c / n for c in counts]

    base_props = proportions(base)
    curr_props = proportions(curr)

    psi = 0.0
    for e_i, a_i in zip(base_props, curr_props):
        # Replace zeros with epsilon
        e = e_i if e_i > 0 else _EPSILON
        a = a_i if a_i > 0 else _EPSILON
        psi += (a - e) * math.log(a / e)

    return psi


def classify_psi(psi: float) -> PSILevel:
    """Map a raw PSI value to a ``PSILevel``."""
    if psi < PSI_WARN_THRESHOLD:
        return PSILevel.CLEAN
    if psi < PSI_ALERT_THRESHOLD:
        return PSILevel.WARN
    return PSILevel.ALERT


def check_psi_drift(
    fact_name: str,
    baseline_values: Sequence[float],
    current_values: Sequence[float],
    bins: int = 10,
) -> PSIResult:
    """
    Compute PSI for one fact and return a labelled result.

    Logs a warning at WARN level and an error at ALERT level.
    """
    psi = compute_psi(baseline_values, current_values, bins)
    level = classify_psi(psi)

    result = PSIResult(
        fact_name=fact_name,
        psi=psi,
        level=level,
        baseline_count=len(list(baseline_values)),
        current_count=len(list(current_values)),
    )

    if level == PSILevel.WARN:
        logger.warning("PSI drift WARN: %s", result)
    elif level == PSILevel.ALERT:
        logger.error("PSI drift ALERT: %s", result)
    else:
        logger.info("PSI drift CLEAN: %s", result)

    return result
