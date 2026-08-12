"""
Feature engineering for filing-level anomaly detection.

Turns the row-per-fact shape of ``financial_facts`` into one numeric vector per
filing. Every feature is a deterministic function of already-extracted facts —
no network calls, no randomness, no model inference — so a given set of rows
always produces byte-identical features. That property is what makes the
training pipeline reproducible and the scores auditable: given an accession
number you can recompute its exact feature vector and see why it scored the way
it did.

Feature groups
--------------
*Scale*        log-magnitudes of assets and revenue. Catches unit errors —
               a ``scale="6"`` attribute parsed as ``scale="0"`` moves a
               company six orders of magnitude away from its peers.
*Ratio*        margins and leverage. Bounded and comparable across companies
               of wildly different size.
*Consistency*  internal cross-checks that should hold in any well-formed
               filing (e.g. ``NetIncome ≈ EPS × SharesOutstanding``). These
               are the highest-signal extraction-error detectors.
*Continuity*   year-over-year log changes against the same company's prior
               filing. Catches restatements and sign flips.
*Coverage*     how much of the target fact set was populated at all.

Missing inputs are imputed to a neutral value and recorded in the coverage
features rather than dropped, so a filing with sparse XBRL still gets scored
instead of silently disappearing from the run.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical fact names
# ---------------------------------------------------------------------------

#: Facts the feature builder consumes, in canonical (prefix-stripped) form.
#: ``Revenues`` absorbs the ASC 606 alias so both spellings feed one feature.
_REVENUE_ALIASES: tuple[str, ...] = (
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
)

TARGET_CONCEPTS: tuple[str, ...] = (
    "Revenues",
    "NetIncomeLoss",
    "Assets",
    "Liabilities",
    "OperatingIncomeLoss",
    "EarningsPerShareBasic",
    "CommonStockSharesOutstanding",
)

#: The subset without which a filing is not usable downstream. Mirrors
#: ``src.quality.REQUIRED_FACTS`` — tracked separately from ``TARGET_CONCEPTS``
#: because a missing *optional* concept is normal and a missing *required* one
#: is a hard defect, and the rule engine needs to tell those apart.
REQUIRED_CONCEPTS: tuple[str, ...] = (
    "Revenues",
    "NetIncomeLoss",
    "Assets",
)

FEATURE_NAMES: tuple[str, ...] = (
    "log_assets",
    "log_revenue",
    "net_margin",
    "operating_margin",
    "leverage",
    "return_on_assets",
    "asset_turnover",
    "eps_consistency",
    "revenue_yoy_log_change",
    "assets_yoy_log_change",
    "fact_coverage",
    "required_coverage",
    "period_days",
)

#: Ratios are clipped to this symmetric bound. Real margins live well inside
#: it; values outside are almost always parse errors, and clipping keeps a
#: single bad row from dominating the model's variance.
_RATIO_CLIP = 10.0

#: Neutral fill for a feature that cannot be computed from the available facts.
_MISSING_FILL = 0.0


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class FeatureMatrix:
    """A dense feature matrix with the row labels needed to trace a score back."""

    X: np.ndarray
    accessions: list[str]
    feature_names: tuple[str, ...] = FEATURE_NAMES
    coverage: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.X.ndim != 2:
            raise ValueError(f"X must be 2-dimensional, got shape {self.X.shape}")
        if self.X.shape[0] != len(self.accessions):
            raise ValueError(
                f"X has {self.X.shape[0]} rows but {len(self.accessions)} accessions were given"
            )
        if self.X.shape[1] != len(self.feature_names):
            raise ValueError(
                f"X has {self.X.shape[1]} columns but "
                f"{len(self.feature_names)} feature names were given"
            )

    def __len__(self) -> int:
        return len(self.accessions)

    @property
    def n_features(self) -> int:
        return len(self.feature_names)

    def column(self, name: str) -> np.ndarray:
        """Return the column for *name*, for drift checks and debugging."""
        try:
            idx = self.feature_names.index(name)
        except ValueError:
            raise KeyError(f"Unknown feature {name!r}") from None
        return self.X[:, idx]

    def row_as_dict(self, accession: str) -> dict[str, float]:
        """Return one filing's features keyed by name — the explainability hook."""
        try:
            idx = self.accessions.index(accession)
        except ValueError:
            raise KeyError(f"Unknown accession {accession!r}") from None
        return dict(zip(self.feature_names, self.X[idx].tolist(), strict=True))


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def _canonical(fact_name: str) -> str:
    """Strip the taxonomy prefix and fold revenue aliases onto one concept."""
    bare = fact_name.split(":", 1)[-1]
    return "Revenues" if bare in _REVENUE_ALIASES else bare


def _as_float(value: Any) -> float | None:
    """Coerce a fact value to float, returning None for anything unusable."""
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(result) or math.isinf(result) else result


def _as_date(value: Any) -> date | None:
    if value is None or (isinstance(value, date) and not isinstance(value, datetime)):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _safe_ratio(numerator: float | None, denominator: float | None) -> float:
    """Divide with a zero-denominator guard, clipped to ``_RATIO_CLIP``."""
    if numerator is None or denominator is None or denominator == 0:
        return _MISSING_FILL
    return float(np.clip(numerator / denominator, -_RATIO_CLIP, _RATIO_CLIP))


def _signed_log(value: float | None) -> float:
    """log10 of magnitude, sign preserved. Handles the 10^12 spread in filings."""
    if value is None:
        return _MISSING_FILL
    return float(math.copysign(math.log10(1.0 + abs(value)), value))


# ---------------------------------------------------------------------------
# Fact grouping
# ---------------------------------------------------------------------------


def group_facts_by_filing(
    facts: Iterable[Mapping[str, Any]],
    *,
    consolidated_only: bool = True,
) -> dict[str, dict[str, float]]:
    """
    Collapse fact rows into ``{accession: {concept: value}}``.

    Segment rows are excluded by default: a segment-level revenue figure is not
    comparable to a consolidated one, and mixing them would make the margin
    features meaningless. When a concept appears more than once for a filing,
    the row with the latest ``period_end`` wins, which resolves the common case
    of a 10-K carrying both the current and prior-year values of the same tag.
    """
    grouped: dict[str, dict[str, tuple[float, date | None]]] = {}

    for row in facts:
        accession = row.get("accession_number")
        if not accession:
            continue
        if consolidated_only and row.get("segment") is not None:
            continue

        value = _as_float(row.get("fact_value"))
        if value is None:
            continue

        concept = _canonical(str(row.get("fact_name", "")))
        period_end = _as_date(row.get("period_end"))

        bucket = grouped.setdefault(accession, {})
        existing = bucket.get(concept)
        if existing is None:
            bucket[concept] = (value, period_end)
            continue

        # Prefer the later period; a row with no period never displaces one with.
        _, existing_end = existing
        if period_end is not None and (existing_end is None or period_end > existing_end):
            bucket[concept] = (value, period_end)

    return {acc: {k: v for k, (v, _) in vals.items()} for acc, vals in grouped.items()}


def _period_length_days(facts_for_filing: Sequence[Mapping[str, Any]]) -> float:
    """Longest duration period in the filing, in days (0.0 for instant-only)."""
    longest = 0.0
    for row in facts_for_filing:
        start = _as_date(row.get("period_start"))
        end = _as_date(row.get("period_end"))
        if start and end and end >= start:
            longest = max(longest, float((end - start).days))
    return longest


# ---------------------------------------------------------------------------
# Feature construction
# ---------------------------------------------------------------------------


def _build_row(
    concepts: Mapping[str, float],
    period_days: float,
    prior: Mapping[str, float] | None,
) -> list[float]:
    """Compute one filing's feature vector."""
    assets = concepts.get("Assets")
    revenue = concepts.get("Revenues")
    net_income = concepts.get("NetIncomeLoss")
    operating_income = concepts.get("OperatingIncomeLoss")
    liabilities = concepts.get("Liabilities")
    eps = concepts.get("EarningsPerShareBasic")
    shares = concepts.get("CommonStockSharesOutstanding")

    # Consistency: NetIncome should be reconstructable from EPS × shares.
    # A ratio far from 1.0 means one of the three was parsed wrong (wrong
    # scale, wrong context, or a per-share figure read as an absolute).
    if eps is not None and shares is not None and net_income is not None and eps * shares != 0:
        eps_consistency = _safe_ratio(net_income, eps * shares) - 1.0
    else:
        eps_consistency = _MISSING_FILL

    revenue_yoy = _MISSING_FILL
    assets_yoy = _MISSING_FILL
    if prior:
        prior_revenue = prior.get("Revenues")
        if revenue is not None and prior_revenue:
            revenue_yoy = _signed_log(revenue) - _signed_log(prior_revenue)
        prior_assets = prior.get("Assets")
        if assets is not None and prior_assets:
            assets_yoy = _signed_log(assets) - _signed_log(prior_assets)

    present = sum(1 for c in TARGET_CONCEPTS if concepts.get(c) is not None)
    coverage = present / len(TARGET_CONCEPTS)

    required_present = sum(1 for c in REQUIRED_CONCEPTS if concepts.get(c) is not None)
    required_coverage = required_present / len(REQUIRED_CONCEPTS)

    return [
        _signed_log(assets),
        _signed_log(revenue),
        _safe_ratio(net_income, revenue),
        _safe_ratio(operating_income, revenue),
        _safe_ratio(liabilities, assets),
        _safe_ratio(net_income, assets),
        _safe_ratio(revenue, assets),
        float(np.clip(eps_consistency, -_RATIO_CLIP, _RATIO_CLIP)),
        float(np.clip(revenue_yoy, -_RATIO_CLIP, _RATIO_CLIP)),
        float(np.clip(assets_yoy, -_RATIO_CLIP, _RATIO_CLIP)),
        coverage,
        required_coverage,
        # Scaled to roughly [0, 1] so it doesn't dominate the distance metric.
        min(period_days / 366.0, _RATIO_CLIP),
    ]


def build_features(
    facts: Sequence[Mapping[str, Any]],
    filings: Mapping[str, Mapping[str, Any]] | None = None,
    *,
    consolidated_only: bool = True,
) -> FeatureMatrix:
    """
    Build a filing-level feature matrix from ``financial_facts`` rows.

    Args:
        facts:
            Fact rows with at least ``accession_number``, ``fact_name`` and
            ``fact_value``; ``period_start``, ``period_end`` and ``segment``
            are used when present.
        filings:
            Optional ``{accession: {"cik": ..., "period_of_report": ...}}``
            metadata. Required for the year-over-year continuity features —
            without it those columns are filled with the neutral value, since
            prior-period comparison needs to know which filings belong to the
            same company.
        consolidated_only:
            Exclude segment-level rows (default). See
            :func:`group_facts_by_filing`.

    Returns:
        A :class:`FeatureMatrix` with rows ordered by accession number, so the
        output is stable regardless of input row order.
    """
    grouped = group_facts_by_filing(facts, consolidated_only=consolidated_only)
    if not grouped:
        return FeatureMatrix(
            X=np.zeros((0, len(FEATURE_NAMES)), dtype=np.float64),
            accessions=[],
        )

    facts_by_accession: dict[str, list[Mapping[str, Any]]] = {}
    for row in facts:
        accession = row.get("accession_number")
        if accession in grouped:
            facts_by_accession.setdefault(str(accession), []).append(row)

    prior_by_accession = _resolve_prior_filings(grouped, filings)

    accessions = sorted(grouped)
    rows = [
        _build_row(
            grouped[accession],
            _period_length_days(facts_by_accession.get(accession, [])),
            prior_by_accession.get(accession),
        )
        for accession in accessions
    ]

    X = np.asarray(rows, dtype=np.float64)

    coverage = {
        name: float(np.count_nonzero(X[:, i]) / len(accessions))
        for i, name in enumerate(FEATURE_NAMES)
    }

    logger.info(
        "Built features for %d filings (%d features); mean fact coverage %.1f%%",
        len(accessions),
        X.shape[1],
        float(X[:, FEATURE_NAMES.index("fact_coverage")].mean()) * 100,
    )

    return FeatureMatrix(X=X, accessions=accessions, coverage=coverage)


def _resolve_prior_filings(
    grouped: Mapping[str, Mapping[str, float]],
    filings: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, Mapping[str, float]]:
    """
    Map each accession to the same company's immediately preceding filing.

    Returns an empty mapping when *filings* metadata is absent — the caller
    fills the continuity features with the neutral value in that case.
    """
    if not filings:
        return {}

    by_cik: dict[str, list[tuple[date, str]]] = {}
    for accession in grouped:
        meta = filings.get(accession)
        if not meta:
            continue
        cik = meta.get("cik")
        period = _as_date(meta.get("period_of_report")) or _as_date(meta.get("filing_date"))
        if cik is None or period is None:
            continue
        by_cik.setdefault(str(cik), []).append((period, accession))

    prior: dict[str, Mapping[str, float]] = {}
    for entries in by_cik.values():
        entries.sort()
        for i in range(1, len(entries)):
            _, current = entries[i]
            _, previous = entries[i - 1]
            prior[current] = grouped[previous]

    return prior
