"""
Hybrid anomaly detector: IsolationForest + deterministic plausibility rules.

What it is for
--------------
The detector flags filings whose extracted numbers look wrong — a revenue
figure six orders of magnitude off because a ``scale`` attribute was misread, a
leverage ratio that no operating company could report, net income that cannot
be reconciled with EPS × shares outstanding. It is a *review queue generator*,
not an extraction step: a flag routes a filing to a human, it never alters a
filed value.

Why IsolationForest
-------------------
Labelled extraction errors do not exist at any useful volume — nobody
maintains a corpus of "10-Ks we parsed wrong" — so a supervised classifier has
nothing to train on. IsolationForest needs only the unlabelled feature matrix,
isolates outliers in few splits rather than modelling the dense normal region,
and stays cheap enough to score a full backfill. The trade-off is that
``contamination`` is an assumption rather than a learned quantity, so it is an
explicit constructor argument and is recorded in the model metadata.

Why rules as well
-----------------
The forest alone measured 0.17 recall on dropped facts. The cause is structural,
not a tuning problem: every training filing has full fact coverage, so that
column has zero variance, ``StandardScaler`` flattens it, and the signal is gone
before any tree sees it. :mod:`src.ml.rules` asserts the invariants that must
hold in a well-formed filing; the forest catches what nobody wrote a rule for.
A filing's score is the **maximum** of the two components — a fired rule is a
positive assertion of a defect, and averaging it against a calm model score
would let the forest veto a known break. Each score records which half fired.

Determinism
-----------
``random_state`` is set on the estimator and defaults to :data:`DEFAULT_SEED`.
Training twice on the same rows produces the same forest and therefore the same
artifact hash — the property the registry relies on to prove which model
produced a given score.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.ml.features import FEATURE_NAMES, FeatureMatrix
from src.ml.rules import RULES, Rule, RuleViolation, evaluate_rules, rule_score

logger = logging.getLogger(__name__)

DEFAULT_SEED = 42
DEFAULT_CONTAMINATION = 0.05
DEFAULT_N_ESTIMATORS = 200

#: Filings scoring at or above this normalised value are flagged for review.
DEFAULT_ANOMALY_THRESHOLD = 0.6

#: How many features to cite when explaining a flag.
_TOP_FEATURES = 3


@dataclass
class AnomalyScore:
    """One filing's score, with enough context to action it without re-running."""

    accession_number: str
    #: Combined score in [0, 1]; higher means more anomalous.
    score: float
    #: Raw signed IsolationForest decision function (negative = outlier side).
    raw_decision: float
    is_anomaly: bool
    #: Features furthest from the training mean, as ``(name, z_score)``.
    top_contributors: list[tuple[str, float]] = field(default_factory=list)
    #: Normalised IsolationForest score alone, before the rule blend.
    model_score: float = 0.0
    #: Highest rule severity, or 0.0 when no rule fired.
    rule_score: float = 0.0
    #: Rules that fired, highest severity first.
    rule_violations: list[RuleViolation] = field(default_factory=list)

    @property
    def triggered_by(self) -> str:
        """Which half of the detector produced the flag — ``rule``, ``model``, or ``both``."""
        by_rule = self.rule_score >= self.model_score and self.rule_score > 0
        by_model = self.model_score >= self.rule_score and self.model_score > 0
        if by_rule and by_model:
            return "both"
        return "rule" if by_rule else "model"

    def as_dict(self) -> dict[str, Any]:
        return {
            "accession_number": self.accession_number,
            "score": round(self.score, 6),
            "model_score": round(self.model_score, 6),
            "rule_score": round(self.rule_score, 6),
            "raw_decision": round(self.raw_decision, 6),
            "is_anomaly": self.is_anomaly,
            "triggered_by": self.triggered_by,
            "top_contributors": [[n, round(z, 4)] for n, z in self.top_contributors],
            "rule_violations": [v.as_dict() for v in self.rule_violations],
        }

    def explain(self) -> str:
        """Human-readable reason string for the audit trail and alert payloads."""
        if self.rule_violations:
            reasons = "; ".join(
                f"{v.rule_id}: {v.message}" for v in self.rule_violations[:_TOP_FEATURES]
            )
            return f"score={self.score:.3f} — {reasons}"
        if not self.top_contributors:
            return f"score={self.score:.3f} (no dominant feature)"
        drivers = ", ".join(f"{name} (z={z:+.2f})" for name, z in self.top_contributors)
        return f"score={self.score:.3f} — statistical outlier driven by {drivers}"


class NotFittedError(RuntimeError):
    """Raised when scoring is attempted before ``fit``."""


class AnomalyDetector:
    """
    Scale-then-isolate anomaly detector with feature-level explanations.

    Example::

        detector = AnomalyDetector(seed=42)
        detector.fit(train_matrix)
        for score in detector.score(new_matrix):
            if score.is_anomaly:
                print(score.accession_number, score.explain())
    """

    def __init__(
        self,
        *,
        seed: int = DEFAULT_SEED,
        contamination: float = DEFAULT_CONTAMINATION,
        n_estimators: int = DEFAULT_N_ESTIMATORS,
        threshold: float = DEFAULT_ANOMALY_THRESHOLD,
        feature_names: tuple[str, ...] = FEATURE_NAMES,
        rules: tuple[Rule, ...] = RULES,
    ) -> None:
        if not 0.0 < contamination <= 0.5:
            raise ValueError(f"contamination must be in (0, 0.5], got {contamination}")
        if not 0.0 <= threshold <= 1.0:
            raise ValueError(f"threshold must be in [0, 1], got {threshold}")

        self.seed = seed
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.threshold = threshold
        self.feature_names = feature_names
        # Rules are code, not learned state — they are deliberately not
        # serialised with the artifact. A rule fix ships with the deployment and
        # applies to every registered model, rather than requiring a retrain.
        self.rules = rules

        self._pipeline: Pipeline | None = None
        self._train_mean: np.ndarray | None = None
        self._train_std: np.ndarray | None = None
        # Decision-function landmarks observed on the training set, used to map
        # raw scores into [0, 1]. Stored so scoring is stable across batches — a
        # per-batch normalisation would make one filing's score depend on which
        # other filings happened to run alongside it.
        self._decision_min: float = -0.5
        self._decision_max: float = 0.5
        # The training-set decision value at the `contamination` quantile. The
        # calibration in `_normalise` pins this point to exactly `threshold`.
        self._decision_cutoff: float = 0.0

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(self, matrix: FeatureMatrix) -> AnomalyDetector:
        """Fit the scaler and forest on *matrix*. Returns self for chaining."""
        if len(matrix) == 0:
            raise ValueError("Cannot fit on an empty feature matrix")
        if len(matrix) < 2:
            raise ValueError(f"Need at least 2 filings to fit, got {len(matrix)}")

        self.feature_names = matrix.feature_names

        self._pipeline = Pipeline(
            [
                ("scale", StandardScaler()),
                (
                    "isolate",
                    IsolationForest(
                        n_estimators=self.n_estimators,
                        contamination=self.contamination,
                        random_state=self.seed,
                        # Single-threaded: parallel tree building introduces
                        # scheduling nondeterminism that would break the
                        # reproducible-artifact guarantee.
                        n_jobs=1,
                    ),
                ),
            ]
        )
        self._pipeline.fit(matrix.X)

        self._train_mean = matrix.X.mean(axis=0)
        std = matrix.X.std(axis=0)
        # Zero-variance features would divide by zero in the z-score; treat them
        # as unit-variance so they simply never dominate an explanation.
        self._train_std = np.where(std > 0, std, 1.0)

        decisions = self._pipeline.decision_function(matrix.X)
        self._decision_min = float(decisions.min())
        self._decision_max = float(decisions.max())
        # Lower decision values are more anomalous, so the contaminated tail is
        # the lower `contamination` quantile.
        self._decision_cutoff = float(np.quantile(decisions, self.contamination))

        logger.info(
            "Fitted AnomalyDetector on %d filings (seed=%d, contamination=%.3f, trees=%d)",
            len(matrix),
            self.seed,
            self.contamination,
            self.n_estimators,
        )
        return self

    @property
    def is_fitted(self) -> bool:
        return self._pipeline is not None

    def _require_fitted(self) -> Pipeline:
        if self._pipeline is None:
            raise NotFittedError("AnomalyDetector.fit() must be called before scoring")
        return self._pipeline

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _normalise(self, decisions: np.ndarray) -> np.ndarray:
        """
        Map raw decision values to [0, 1] where 1 is most anomalous.

        The mapping is piecewise-linear and pinned at three training-set
        landmarks: the most anomalous training point maps to 1.0, the least
        anomalous to 0.0, and the ``contamination`` quantile maps to exactly
        ``threshold``.

        That middle pin is the point of the exercise. A plain min-max rescale
        put ``threshold=0.6`` at the training p90, so the detector flagged 10%
        of its own training data — twice the 5% ``contamination`` it was
        configured for, and all of it false. Pinning the quantile makes the
        configured contamination the actual flag rate on in-distribution data,
        which is what makes the precision figure in the model card mean
        anything.
        """
        lower_span = self._decision_cutoff - self._decision_min
        upper_span = self._decision_max - self._decision_cutoff

        if lower_span <= 0 and upper_span <= 0:
            # Degenerate training set: every point scored identically.
            return np.full_like(decisions, 0.5)

        scores = np.empty_like(decisions, dtype=np.float64)
        anomalous_side = decisions <= self._decision_cutoff

        # Anomalous side: cutoff -> threshold, decision_min -> 1.0
        if lower_span > 0:
            offset = (self._decision_cutoff - decisions[anomalous_side]) / lower_span
            scores[anomalous_side] = self.threshold + offset * (1.0 - self.threshold)
        else:
            scores[anomalous_side] = 1.0

        # Normal side: cutoff -> threshold, decision_max -> 0.0
        if upper_span > 0:
            offset = (decisions[~anomalous_side] - self._decision_cutoff) / upper_span
            scores[~anomalous_side] = self.threshold * (1.0 - offset)
        else:
            scores[~anomalous_side] = self.threshold

        return np.clip(scores, 0.0, 1.0)

    def _contributors(self, row: np.ndarray) -> list[tuple[str, float]]:
        """Rank features by absolute z-score against the training distribution."""
        if self._train_mean is None or self._train_std is None:
            return []
        z = (row - self._train_mean) / self._train_std
        order = np.argsort(np.abs(z))[::-1][:_TOP_FEATURES]
        return [(self.feature_names[i], float(z[i])) for i in order if abs(z[i]) > 0]

    def score(self, matrix: FeatureMatrix) -> list[AnomalyScore]:
        """Score every filing in *matrix*, most anomalous first."""
        pipeline = self._require_fitted()

        if len(matrix) == 0:
            return []
        if matrix.feature_names != self.feature_names:
            raise ValueError(
                "Feature mismatch between model and input matrix.\n"
                f"  model expects: {self.feature_names}\n"
                f"  matrix has:    {matrix.feature_names}"
            )

        decisions = pipeline.decision_function(matrix.X)
        normalised = self._normalise(decisions)

        results: list[AnomalyScore] = []
        for i, accession in enumerate(matrix.accessions):
            features = dict(zip(self.feature_names, matrix.X[i].tolist(), strict=True))
            violations = evaluate_rules(features, self.rules)

            model_component = float(normalised[i])
            rule_component = rule_score(violations)
            # Max, not a weighted blend: a fired rule is a positive assertion
            # that something is wrong, and averaging it against a calm model
            # score would let the forest veto a known defect.
            combined = max(model_component, rule_component)

            results.append(
                AnomalyScore(
                    accession_number=accession,
                    score=combined,
                    raw_decision=float(decisions[i]),
                    is_anomaly=bool(combined >= self.threshold),
                    top_contributors=self._contributors(matrix.X[i]),
                    model_score=model_component,
                    rule_score=rule_component,
                    rule_violations=violations,
                )
            )

        results.sort(key=lambda s: s.score, reverse=True)

        flagged = sum(1 for r in results if r.is_anomaly)
        logger.info(
            "Scored %d filings; %d flagged at threshold %.2f (%.1f%%) — "
            "%d by rule, %d by model alone",
            len(results),
            flagged,
            self.threshold,
            100.0 * flagged / len(results),
            sum(1 for r in results if r.is_anomaly and r.triggered_by in ("rule", "both")),
            sum(1 for r in results if r.is_anomaly and r.triggered_by == "model"),
        )
        return results

    def score_values(self, matrix: FeatureMatrix) -> np.ndarray:
        """
        Combined scores as a bare array, ordered to match ``matrix.accessions``.

        Prediction-drift checks compare this against the same quantity from the
        baseline batch, so it must be the *combined* score the pipeline acts on,
        not the model component alone.
        """
        self._require_fitted()
        if len(matrix) == 0:
            return np.zeros(0, dtype=np.float64)
        by_accession = {s.accession_number: s.score for s in self.score(matrix)}
        return np.array([by_accession[a] for a in matrix.accessions], dtype=np.float64)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def to_state(self) -> dict[str, Any]:
        """Serialisable state — everything needed to reconstruct the detector."""
        self._require_fitted()
        return {
            "pipeline": self._pipeline,
            "train_mean": self._train_mean,
            "train_std": self._train_std,
            "decision_min": self._decision_min,
            "decision_max": self._decision_max,
            "decision_cutoff": self._decision_cutoff,
            "seed": self.seed,
            "contamination": self.contamination,
            "n_estimators": self.n_estimators,
            "threshold": self.threshold,
            "feature_names": list(self.feature_names),
        }

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> AnomalyDetector:
        detector = cls(
            seed=state["seed"],
            contamination=state["contamination"],
            n_estimators=state["n_estimators"],
            threshold=state["threshold"],
            feature_names=tuple(state["feature_names"]),
        )
        detector._pipeline = state["pipeline"]
        detector._train_mean = state["train_mean"]
        detector._train_std = state["train_std"]
        detector._decision_min = state["decision_min"]
        detector._decision_max = state["decision_max"]
        detector._decision_cutoff = state["decision_cutoff"]
        return detector

    def save(self, path: str | Path) -> Path:
        """Persist to *path* with joblib. Returns the resolved path."""
        import joblib

        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.to_state(), target, compress=3)
        logger.info("Saved model to %s (%d bytes)", target, target.stat().st_size)
        return target

    @classmethod
    def load(cls, path: str | Path) -> AnomalyDetector:
        """Load a detector previously written by :meth:`save`."""
        import joblib

        source = Path(path)
        if not source.exists():
            raise FileNotFoundError(f"No model artifact at {source}")
        return cls.from_state(joblib.load(source))
