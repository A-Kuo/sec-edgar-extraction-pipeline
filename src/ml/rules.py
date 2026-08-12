"""
Deterministic plausibility rules over engineered features.

Why rules alongside a model
---------------------------
An IsolationForest is good at "this filing does not look like the others" and
bad at "this filing violates an identity that must hold". The failure that made
the split obvious: every training filing has ``fact_coverage == 1.0``, so the
column has zero variance, ``StandardScaler`` flattens it, and the forest cannot
see a dropped fact at all — recall on that failure mode measured 0.17. No amount
of retraining fixes it, because the information is destroyed before the forest
ever sees it.

Rules cover the known failure modes with high precision and a legible reason
string. The forest covers what nobody thought to write a rule for. Together the
detector reports *why* a filing was flagged, which is the difference between a
review queue an analyst can work and a score they have to take on faith.

Rules read the same :class:`~src.ml.features.FeatureMatrix` the model consumes,
so there is exactly one definition of "leverage" in the system, and a rule can
never disagree with the model about what it is looking at.

Calibration
-----------
Thresholds sit outside the range any well-formed filing produces, not at the
edge of it. A rule that fires on legitimate filings is worse than no rule: it
trains reviewers to dismiss the queue. Severities encode confidence — a broken
accounting identity is near-certain (0.95), an unusual year-over-year jump is
suggestive (0.80) — and the highest-severity violation becomes the filing's
rule score.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.ml.features import FeatureMatrix

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Rule:
    """One plausibility check over a filing's feature dict."""

    rule_id: str
    description: str
    severity: float
    predicate: Callable[[dict[str, float]], bool]
    explain: Callable[[dict[str, float]], str]

    def check(self, features: dict[str, float]) -> RuleViolation | None:
        try:
            triggered = self.predicate(features)
        except (KeyError, ZeroDivisionError, ValueError):
            # A rule referencing a feature that is not present must not take
            # down a scoring run; the missing feature is itself a schema
            # problem that the feature-drift check will surface.
            logger.debug("Rule %s could not evaluate; skipping", self.rule_id)
            return None
        if not triggered:
            return None
        return RuleViolation(
            rule_id=self.rule_id,
            severity=self.severity,
            message=self.explain(features),
        )


@dataclass
class RuleViolation:
    """A fired rule, carrying the reason a reviewer needs."""

    rule_id: str
    severity: float
    message: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": round(self.severity, 4),
            "message": self.message,
        }


# ---------------------------------------------------------------------------
# Thresholds
#
# Each is set beyond the range a well-formed filing can reach. The comment
# gives the observed clean-data range that justifies the bound.
# ---------------------------------------------------------------------------

EPS_RECONCILIATION_TOLERANCE = 0.25  # clean: ~0.0; 25% slack covers dilution/rounding
MAX_LEVERAGE = 1.5  # clean: 0.05–0.95; >1.5 means liabilities dwarf assets
MAX_ABS_MARGIN = 1.5  # clean: -0.9–0.6; |margin| > 1.5 is arithmetically implausible
MAX_ASSET_TURNOVER = 5.0  # clean: 0.02–4.0
MAX_YOY_LOG_CHANGE = 0.7  # ~5x year-over-year; real growth rarely exceeds this


def _fires_yoy(value: float) -> bool:
    """Year-over-year rules must not fire on a company's first filing.

    The continuity features are neutral-filled (0.0) when there is no prior
    filing, which is indistinguishable from "no change" — so exactly 0.0 is
    always treated as "not evaluable" rather than "no drift".
    """
    return value != 0.0 and abs(value) > MAX_YOY_LOG_CHANGE


RULES: tuple[Rule, ...] = (
    Rule(
        rule_id="missing_required_fact",
        description="A fact required by downstream consumers was not extracted.",
        severity=0.95,
        predicate=lambda f: f["required_coverage"] < 1.0,
        explain=lambda f: (
            f"only {f['required_coverage']:.0%} of required facts "
            f"(Revenues, NetIncomeLoss, Assets) were extracted"
        ),
    ),
    Rule(
        rule_id="eps_reconciliation_failed",
        description="NetIncome does not reconcile with EPS x shares outstanding.",
        severity=0.95,
        predicate=lambda f: abs(f["eps_consistency"]) > EPS_RECONCILIATION_TOLERANCE,
        explain=lambda f: (
            f"NetIncome / (EPS x shares) is off by {f['eps_consistency']:+.1%}; "
            f"one of the three was parsed at the wrong scale or context"
        ),
    ),
    Rule(
        rule_id="non_positive_scale",
        description="Assets or revenue is missing, zero, or negative.",
        severity=0.9,
        predicate=lambda f: f["log_assets"] <= 0.0 or f["log_revenue"] <= 0.0,
        explain=lambda f: (
            f"log_assets={f['log_assets']:.2f}, log_revenue={f['log_revenue']:.2f}; "
            f"a filer cannot report non-positive assets or revenue"
        ),
    ),
    Rule(
        rule_id="implausible_leverage",
        description="Liabilities-to-assets is negative or far above 1.",
        severity=0.9,
        predicate=lambda f: f["leverage"] < 0.0 or f["leverage"] > MAX_LEVERAGE,
        explain=lambda f: (
            f"liabilities/assets = {f['leverage']:.2f}, outside the plausible "
            f"range [0, {MAX_LEVERAGE}]"
        ),
    ),
    Rule(
        rule_id="implausible_net_margin",
        description="Net margin is arithmetically implausible.",
        severity=0.85,
        predicate=lambda f: abs(f["net_margin"]) > MAX_ABS_MARGIN,
        explain=lambda f: (
            f"net income / revenue = {f['net_margin']:.2f}, "
            f"|margin| > {MAX_ABS_MARGIN} implies a scale mismatch"
        ),
    ),
    Rule(
        rule_id="implausible_operating_margin",
        description="Operating margin is arithmetically implausible.",
        severity=0.85,
        predicate=lambda f: abs(f["operating_margin"]) > MAX_ABS_MARGIN,
        explain=lambda f: (
            f"operating income / revenue = {f['operating_margin']:.2f}, "
            f"|margin| > {MAX_ABS_MARGIN} implies a scale mismatch"
        ),
    ),
    Rule(
        rule_id="implausible_asset_turnover",
        description="Revenue-to-assets is negative or implausibly high.",
        severity=0.85,
        predicate=lambda f: f["asset_turnover"] < 0.0 or f["asset_turnover"] > MAX_ASSET_TURNOVER,
        explain=lambda f: (
            f"revenue/assets = {f['asset_turnover']:.2f}, outside "
            f"[0, {MAX_ASSET_TURNOVER}]; typically a revenue or assets scale error"
        ),
    ),
    Rule(
        rule_id="revenue_discontinuity",
        description="Revenue jumped by more than ~5x against the prior filing.",
        severity=0.8,
        predicate=lambda f: _fires_yoy(f["revenue_yoy_log_change"]),
        explain=lambda f: (
            f"revenue moved {10 ** f['revenue_yoy_log_change']:.1f}x versus the "
            f"prior filing; check for a scale error or a restatement"
        ),
    ),
    Rule(
        rule_id="assets_discontinuity",
        description="Total assets jumped by more than ~5x against the prior filing.",
        severity=0.8,
        predicate=lambda f: _fires_yoy(f["assets_yoy_log_change"]),
        explain=lambda f: (
            f"total assets moved {10 ** f['assets_yoy_log_change']:.1f}x versus the "
            f"prior filing; check for a scale error or a restatement"
        ),
    ),
    Rule(
        rule_id="incomplete_fact_set",
        description="An optional target concept is missing.",
        # Low severity by design: below the flag threshold, so this alone never
        # creates a review item. It raises a filing's score enough to surface if
        # something else is also off.
        severity=0.5,
        predicate=lambda f: f["fact_coverage"] < 1.0,
        explain=lambda f: f"only {f['fact_coverage']:.0%} of target concepts were extracted",
    ),
)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def evaluate_rules(
    features: dict[str, float],
    rules: tuple[Rule, ...] = RULES,
) -> list[RuleViolation]:
    """Run every rule against one filing's features, highest severity first."""
    violations = [v for rule in rules if (v := rule.check(features)) is not None]
    violations.sort(key=lambda v: v.severity, reverse=True)
    return violations


def evaluate_matrix(
    matrix: FeatureMatrix,
    rules: tuple[Rule, ...] = RULES,
) -> dict[str, list[RuleViolation]]:
    """Run the rule set across a whole matrix, keyed by accession number."""
    results = {
        accession: evaluate_rules(matrix.row_as_dict(accession), rules)
        for accession in matrix.accessions
    }

    fired = sum(1 for v in results.values() if v)
    if fired:
        logger.info(
            "Rule engine: %d/%d filings triggered at least one rule",
            fired,
            len(matrix),
        )
    return results


def rule_score(violations: list[RuleViolation]) -> float:
    """
    Collapse a filing's violations into a single [0, 1] score.

    The maximum severity wins rather than a sum: two independent moderate
    signals should not add up to more certainty than one near-certain
    accounting-identity break. Additional violations show up in the reason
    string, where they inform the reviewer without inflating the score.
    """
    return max((v.severity for v in violations), default=0.0)
