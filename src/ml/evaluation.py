"""
Evaluation harness — corruption injection and the metrics CI gates on.

The measurement problem
-----------------------
There is no labelled corpus of mis-extracted filings. Nobody publishes "10-Ks
we parsed wrong", and hand-labelling enough of them to compute a stable recall
figure is not realistic. Reporting only unsupervised diagnostics (flag rate,
score distribution) would mean the model could regress arbitrarily and every
number in the model card would still look fine.

So the harness manufactures the labels: take held-out filings that are known to
be well-formed, inject the specific failure modes this pipeline actually
produces, and measure whether the detector surfaces them. The recall number is
therefore "recall against *these* injected defects at *this* flag rate" — a real
number with an explicit scope, which is what makes it safe to gate a merge on.

Injected failure modes
----------------------
``SCALE_ERROR``   value multiplied by 10^±3/6 — a misread ``scale`` attribute,
                  the single most common iXBRL parsing defect.
``SIGN_FLIP``     sign inverted — a ``sign="-"`` attribute applied twice or not
                  at all, or a parenthesised negative missed.
``DROPPED_FACT``  a required concept removed — the context selector matched
                  nothing and the fact silently vanished.
``EPS_MISMATCH``  EPS rescaled so it no longer reconciles with net income and
                  shares — a per-share figure read from the wrong context.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import numpy as np

from src.ml.features import build_features
from src.ml.model import AnomalyDetector

logger = logging.getLogger(__name__)

DEFAULT_CORRUPTION_RATE = 0.10
DEFAULT_EVAL_SEED = 1337


class CorruptionKind(str, Enum):
    SCALE_ERROR = "scale_error"
    SIGN_FLIP = "sign_flip"
    DROPPED_FACT = "dropped_fact"
    EPS_MISMATCH = "eps_mismatch"


#: Concepts worth corrupting — the ones the features actually depend on.
_CORRUPTIBLE = (
    "us-gaap:Revenues",
    "us-gaap:NetIncomeLoss",
    "us-gaap:Assets",
    "us-gaap:Liabilities",
    "us-gaap:OperatingIncomeLoss",
)


@dataclass
class EvalMetrics:
    """Detection performance against injected corruptions."""

    n_filings: int
    n_corrupted: int
    recall: float
    precision: float
    f1: float
    flag_rate: float
    roc_auc: float
    mean_score_clean: float
    mean_score_corrupted: float
    corruption_rate: float
    threshold: float
    seed: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def as_metrics(self) -> dict[str, float]:
        """The subset stored in model metadata and compared across versions."""
        return {
            "recall": self.recall,
            "precision": self.precision,
            "f1": self.f1,
            "roc_auc": self.roc_auc,
            "flag_rate": self.flag_rate,
        }

    def summary(self) -> str:
        return (
            f"recall={self.recall:.3f} precision={self.precision:.3f} "
            f"f1={self.f1:.3f} roc_auc={self.roc_auc:.3f} "
            f"flag_rate={self.flag_rate:.3f} "
            f"({self.n_corrupted}/{self.n_filings} filings corrupted)"
        )


# ---------------------------------------------------------------------------
# Corruption injection
# ---------------------------------------------------------------------------


def inject_corruptions(
    facts: Sequence[Mapping[str, Any]],
    *,
    rate: float = DEFAULT_CORRUPTION_RATE,
    seed: int = DEFAULT_EVAL_SEED,
) -> tuple[list[dict[str, Any]], set[str]]:
    """
    Corrupt a fraction of filings and return ``(facts, corrupted_accessions)``.

    Input rows are deep-copied; the caller's data is never mutated, so the same
    clean corpus can be reused across evaluation runs with different seeds.
    """
    if not 0.0 < rate < 1.0:
        raise ValueError(f"rate must be in (0, 1), got {rate}")

    rng = np.random.default_rng(seed)
    rows = [dict(copy.deepcopy(row)) for row in facts]

    by_accession: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        accession = row.get("accession_number")
        if accession:
            by_accession.setdefault(str(accession), []).append(row)

    accessions = sorted(by_accession)
    n_corrupt = max(1, round(len(accessions) * rate))
    chosen = set(rng.choice(accessions, size=min(n_corrupt, len(accessions)), replace=False))

    kinds = list(CorruptionKind)
    dropped: list[dict[str, Any]] = []

    for accession in sorted(chosen):
        filing_rows = by_accession[accession]
        kind = kinds[int(rng.integers(0, len(kinds)))]
        dropped.extend(_apply_corruption(filing_rows, kind, rng))

    if dropped:
        dropped_ids = {id(row) for row in dropped}
        rows = [row for row in rows if id(row) not in dropped_ids]

    logger.info(
        "Injected corruptions into %d/%d filings (rate=%.2f, seed=%d)",
        len(chosen),
        len(accessions),
        rate,
        seed,
    )
    return rows, chosen


def _apply_corruption(
    filing_rows: list[dict[str, Any]],
    kind: CorruptionKind,
    rng: np.random.Generator,
) -> list[dict[str, Any]]:
    """Mutate *filing_rows* in place. Returns rows the caller should delete."""
    candidates = [r for r in filing_rows if r.get("fact_name") in _CORRUPTIBLE]
    if not candidates:
        return []

    target = candidates[int(rng.integers(0, len(candidates)))]

    if kind is CorruptionKind.SCALE_ERROR:
        exponent = int(rng.choice([-6, -3, 3, 6]))
        target["fact_value"] = float(target["fact_value"]) * (10.0**exponent)

    elif kind is CorruptionKind.SIGN_FLIP:
        target["fact_value"] = -float(target["fact_value"])

    elif kind is CorruptionKind.DROPPED_FACT:
        return [target]

    elif kind is CorruptionKind.EPS_MISMATCH:
        eps_rows = [r for r in filing_rows if r.get("fact_name") == "us-gaap:EarningsPerShareBasic"]
        if not eps_rows:
            # No EPS to break — fall back to a scale error so the filing is
            # genuinely corrupted and the label stays honest.
            target["fact_value"] = float(target["fact_value"]) * 1000.0
            return []
        eps_rows[0]["fact_value"] = float(eps_rows[0]["fact_value"]) * float(
            rng.choice([50.0, 100.0, 0.01])
        )

    return []


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def _roc_auc(labels: np.ndarray, scores: np.ndarray) -> float:
    """
    AUC via the Mann-Whitney U identity, with tie correction.

    Computed directly rather than via ``sklearn.metrics.roc_auc_score`` so the
    metric stays defined (0.5) for a degenerate single-class batch instead of
    raising, which matters when CI evaluates a small sample.
    """
    positives = scores[labels == 1]
    negatives = scores[labels == 0]
    if len(positives) == 0 or len(negatives) == 0:
        return 0.5

    order = np.argsort(scores, kind="mergesort")
    ranks = np.empty(len(scores), dtype=np.float64)
    ranks[order] = np.arange(1, len(scores) + 1, dtype=np.float64)

    # Average ranks within tied groups.
    sorted_scores = scores[order]
    start = 0
    for i in range(1, len(sorted_scores) + 1):
        if i == len(sorted_scores) or sorted_scores[i] != sorted_scores[start]:
            if i - start > 1:
                ranks[order[start:i]] = ranks[order[start:i]].mean()
            start = i

    rank_sum_positive = ranks[labels == 1].sum()
    n_pos, n_neg = len(positives), len(negatives)
    return float((rank_sum_positive - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def evaluate_detector(
    detector: AnomalyDetector,
    facts: Sequence[Mapping[str, Any]],
    filings: Mapping[str, Mapping[str, Any]] | None = None,
    *,
    rate: float = DEFAULT_CORRUPTION_RATE,
    seed: int = DEFAULT_EVAL_SEED,
) -> EvalMetrics:
    """
    Score a corrupted copy of *facts* and measure detection performance.

    Corruption happens at the **fact** level, before feature construction, so
    the evaluation exercises the same ``build_features`` path production uses.
    Corrupting the feature matrix directly would be easier and would silently
    skip any defect the feature builder happens to absorb.
    """
    corrupted_facts, corrupted_accessions = inject_corruptions(facts, rate=rate, seed=seed)
    matrix = build_features(corrupted_facts, filings)

    if len(matrix) == 0:
        raise ValueError("Corrupted corpus produced no features to evaluate")

    scores = detector.score(matrix)
    by_accession = {s.accession_number: s for s in scores}

    labels = np.array(
        [1 if acc in corrupted_accessions else 0 for acc in matrix.accessions],
        dtype=np.int64,
    )
    score_values = np.array(
        [by_accession[acc].score for acc in matrix.accessions], dtype=np.float64
    )
    predictions = np.array(
        [1 if by_accession[acc].is_anomaly else 0 for acc in matrix.accessions],
        dtype=np.int64,
    )

    true_positives = int(np.sum((predictions == 1) & (labels == 1)))
    false_positives = int(np.sum((predictions == 1) & (labels == 0)))
    false_negatives = int(np.sum((predictions == 0) & (labels == 1)))

    recall = (
        true_positives / (true_positives + false_negatives)
        if (true_positives + false_negatives)
        else 0.0
    )
    precision = (
        true_positives / (true_positives + false_positives)
        if (true_positives + false_positives)
        else 0.0
    )
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    metrics = EvalMetrics(
        n_filings=len(matrix),
        n_corrupted=int(labels.sum()),
        recall=recall,
        precision=precision,
        f1=f1,
        flag_rate=float(predictions.mean()),
        roc_auc=_roc_auc(labels, score_values),
        mean_score_clean=float(score_values[labels == 0].mean()) if (labels == 0).any() else 0.0,
        mean_score_corrupted=float(score_values[labels == 1].mean())
        if (labels == 1).any()
        else 0.0,
        corruption_rate=rate,
        threshold=detector.threshold,
        seed=seed,
    )

    logger.info("Evaluation: %s", metrics.summary())
    return metrics
