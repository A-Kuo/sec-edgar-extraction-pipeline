"""
Drift monitoring for the anomaly detector.

A model trained on last year's filings quietly stops being valid when the
population it scores moves — a new sector enters the watchlist, a taxonomy
change shifts how a concept is tagged, an upstream parser fix alters a feature's
distribution. None of that raises an exception; scores just become meaningless.

This module reuses :func:`src.quality.compute_psi` rather than introducing a
second drift metric, so a feature-drift alert and a fact-drift alert are read on
the same scale and against the same thresholds the team already knows:

    PSI < 0.10      no significant drift    CLEAN
    0.10 ≤ PSI < 0.25   moderate drift      WARN
    PSI ≥ 0.25      significant drift       ALERT

Two surfaces are watched:

*Feature drift*     per-feature PSI, baseline (training) vs current batch.
                    Tells you *what* moved, which is what you need to decide
                    whether to retrain or fix the parser.
*Prediction drift*  PSI on the score distribution itself, plus the flag rate.
                    Catches the case where individual features look stable but
                    their joint distribution has shifted.

Known limitation: clustered sampling
------------------------------------
Filings cluster by company — one filer contributes five or ten rows whose
feature values are highly correlated, and scale features follow a
multiplicative random walk per company. PSI treats every row as an independent
observation, so a company-level feature's batch mean carries real sampling
variance that shrinks with more *companies* in the batch, not more filings per
company. At 200 companies, ``log_assets`` alone alerted in 1 of 5 independent
seed comparisons against an equivalent population (WARN in the rest); at 400
companies the same comparison was clean across every seed tried.

Two mitigations are in place, neither of which fully solves it:
:data:`MIN_SAMPLES_FOR_ALERT` suppresses escalation on thin batches, and
prediction drift is far more stable than per-feature drift, which is why it is
the signal worth alerting on. When comparing batches, prefer one of comparable
size to the baseline (several hundred companies, not tens), and read a single
feature's WARN on a small batch as noise until a second batch agrees.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from src.ml.features import FeatureMatrix
from src.quality import PSI_ALERT_THRESHOLD, PSI_WARN_THRESHOLD, PSILevel, classify_psi, compute_psi

logger = logging.getLogger(__name__)

#: A feature whose PSI is computed from fewer than this many observations is
#: reported but never escalated — small samples produce large PSI values by
#: chance, and paging on noise trains people to ignore the alert.
MIN_SAMPLES_FOR_ALERT = 30

#: Equal-frequency binning against the baseline. Every feature here is skewed
#: (log-magnitudes, ratios that pile up near zero), and equal-width bins on
#: skewed data manufacture PSI from near-empty bins: two samples from the same
#: generator measured PSI 0.37 — an ALERT — on ``log_assets`` whose means
#: differed by 0.2%. See :func:`src.quality.compute_psi` for the mechanism.
PSI_BINNING_STRATEGY = "quantile"

#: Flag rate is expected to sit near the model's `contamination`. Deviating by
#: more than this multiple means the population moved even if PSI looks calm.
FLAG_RATE_TOLERANCE = 3.0


@dataclass
class FeatureDrift:
    """PSI for a single feature."""

    feature_name: str
    psi: float
    level: PSILevel
    baseline_count: int
    current_count: int
    baseline_mean: float
    current_mean: float

    @property
    def is_actionable(self) -> bool:
        """Escalate only when the level is bad *and* the sample supports it."""
        return (
            self.level in (PSILevel.WARN, PSILevel.ALERT)
            and self.current_count >= MIN_SAMPLES_FOR_ALERT
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "feature_name": self.feature_name,
            "psi": round(self.psi, 6),
            "level": self.level.value,
            "baseline_count": self.baseline_count,
            "current_count": self.current_count,
            "baseline_mean": round(self.baseline_mean, 6),
            "current_mean": round(self.current_mean, 6),
            "is_actionable": self.is_actionable,
        }


@dataclass
class DriftReport:
    """Aggregate drift verdict for one scoring batch."""

    features: list[FeatureDrift] = field(default_factory=list)
    prediction_psi: float | None = None
    prediction_level: PSILevel | None = None
    baseline_flag_rate: float | None = None
    current_flag_rate: float | None = None

    @property
    def worst_level(self) -> PSILevel:
        levels = [f.level for f in self.features if f.is_actionable]
        if self.prediction_level is not None:
            levels.append(self.prediction_level)
        if PSILevel.ALERT in levels:
            return PSILevel.ALERT
        if PSILevel.WARN in levels:
            return PSILevel.WARN
        return PSILevel.CLEAN

    @property
    def drifted_features(self) -> list[FeatureDrift]:
        """Actionable drifts, worst first."""
        return sorted(
            (f for f in self.features if f.is_actionable),
            key=lambda f: f.psi,
            reverse=True,
        )

    @property
    def flag_rate_anomalous(self) -> bool:
        """True when the flag rate has moved far enough to warrant a look."""
        if self.baseline_flag_rate is None or self.current_flag_rate is None:
            return False
        if self.baseline_flag_rate <= 0:
            return self.current_flag_rate > 0
        ratio = self.current_flag_rate / self.baseline_flag_rate
        return ratio > FLAG_RATE_TOLERANCE or ratio < 1.0 / FLAG_RATE_TOLERANCE

    @property
    def passed(self) -> bool:
        """False when the batch should not be trusted without review."""
        return self.worst_level != PSILevel.ALERT and not self.flag_rate_anomalous

    def as_dict(self) -> dict[str, Any]:
        return {
            "worst_level": self.worst_level.value,
            "passed": self.passed,
            "prediction_psi": (
                round(self.prediction_psi, 6) if self.prediction_psi is not None else None
            ),
            "prediction_level": (
                self.prediction_level.value if self.prediction_level is not None else None
            ),
            "baseline_flag_rate": self.baseline_flag_rate,
            "current_flag_rate": self.current_flag_rate,
            "flag_rate_anomalous": self.flag_rate_anomalous,
            "features": [f.as_dict() for f in self.features],
        }

    def summary(self) -> str:
        drifted = self.drifted_features
        if not drifted and not self.flag_rate_anomalous:
            return f"Drift {self.worst_level.value.upper()}: no actionable drift detected"
        parts = [f"{f.feature_name} (PSI={f.psi:.3f}, {f.level.value})" for f in drifted[:5]]
        if self.flag_rate_anomalous:
            parts.append(f"flag rate {self.baseline_flag_rate:.3f} → {self.current_flag_rate:.3f}")
        return f"Drift {self.worst_level.value.upper()}: " + "; ".join(parts)


# ---------------------------------------------------------------------------
# Feature drift
# ---------------------------------------------------------------------------


def check_feature_drift(
    baseline: FeatureMatrix,
    current: FeatureMatrix,
    *,
    bins: int = 10,
) -> list[FeatureDrift]:
    """
    Compute per-feature PSI between the training baseline and a current batch.

    Constant features (zero variance in both matrices) are reported with
    ``psi=0.0`` rather than skipped, so the report always has one row per
    feature and a missing row unambiguously means a schema mismatch.
    """
    if baseline.feature_names != current.feature_names:
        raise ValueError(
            "Cannot compare matrices with different features.\n"
            f"  baseline: {baseline.feature_names}\n"
            f"  current:  {current.feature_names}"
        )
    if len(baseline) == 0 or len(current) == 0:
        raise ValueError("Both baseline and current matrices must be non-empty")

    drifts: list[FeatureDrift] = []
    for name in baseline.feature_names:
        base_col = baseline.column(name)
        curr_col = current.column(name)
        psi = compute_psi(
            base_col.tolist(),
            curr_col.tolist(),
            bins=bins,
            strategy=PSI_BINNING_STRATEGY,
        )

        drift = FeatureDrift(
            feature_name=name,
            psi=psi,
            level=classify_psi(psi),
            baseline_count=len(base_col),
            current_count=len(curr_col),
            baseline_mean=float(base_col.mean()),
            current_mean=float(curr_col.mean()),
        )
        drifts.append(drift)

        if drift.is_actionable:
            logger.warning(
                "Feature drift %s: %s PSI=%.4f (baseline mean %.4f → current %.4f)",
                drift.level.value.upper(),
                name,
                psi,
                drift.baseline_mean,
                drift.current_mean,
            )

    return drifts


# ---------------------------------------------------------------------------
# Prediction drift
# ---------------------------------------------------------------------------


def check_prediction_drift(
    baseline_scores: Sequence[float],
    current_scores: Sequence[float],
    *,
    threshold: float,
    bins: int = 10,
) -> tuple[float, PSILevel, float, float]:
    """
    Compare two score distributions.

    Returns ``(psi, level, baseline_flag_rate, current_flag_rate)``. The flag
    rates are carried alongside PSI because they are what a human actually
    reacts to — "we are flagging 4× as many filings today" is legible in a way
    that a PSI number is not.
    """
    if not baseline_scores or not current_scores:
        raise ValueError("Both baseline_scores and current_scores must be non-empty")

    base = np.asarray(baseline_scores, dtype=np.float64)
    curr = np.asarray(current_scores, dtype=np.float64)

    psi = compute_psi(base.tolist(), curr.tolist(), bins=bins, strategy=PSI_BINNING_STRATEGY)
    level = classify_psi(psi)

    baseline_flag_rate = float((base >= threshold).mean())
    current_flag_rate = float((curr >= threshold).mean())

    if level != PSILevel.CLEAN:
        logger.warning(
            "Prediction drift %s: PSI=%.4f, flag rate %.3f → %.3f",
            level.value.upper(),
            psi,
            baseline_flag_rate,
            current_flag_rate,
        )

    return psi, level, baseline_flag_rate, current_flag_rate


# ---------------------------------------------------------------------------
# Combined report
# ---------------------------------------------------------------------------


def build_drift_report(
    baseline: FeatureMatrix,
    current: FeatureMatrix,
    *,
    baseline_scores: Sequence[float] | None = None,
    current_scores: Sequence[float] | None = None,
    threshold: float,
    bins: int = 10,
) -> DriftReport:
    """
    Run both drift checks and combine them into one verdict.

    Score arguments are optional so the feature half can run before a model
    exists — useful when validating a candidate training set.
    """
    report = DriftReport(features=check_feature_drift(baseline, current, bins=bins))

    if baseline_scores is not None and current_scores is not None:
        psi, level, base_rate, curr_rate = check_prediction_drift(
            baseline_scores,
            current_scores,
            threshold=threshold,
            bins=bins,
        )
        report.prediction_psi = psi
        report.prediction_level = level
        report.baseline_flag_rate = base_rate
        report.current_flag_rate = curr_rate

    logger.info(
        "%s (warn≥%.2f, alert≥%.2f)",
        report.summary(),
        PSI_WARN_THRESHOLD,
        PSI_ALERT_THRESHOLD,
    )
    return report
