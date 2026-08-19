"""
Machine learning layer for the SEC EDGAR extraction pipeline.

Modules
-------
``features``    Deterministic feature engineering from ``financial_facts`` rows.
``model``       IsolationForest anomaly detector over engineered features.
``registry``    Content-addressed model registry with provenance metadata.
``monitoring``  Feature and prediction drift, reusing ``src.quality.compute_psi``.
``evaluation``  Corruption injection and the metrics the CI merge gate reads.
``synthetic``   Seeded filing generator so the ML path runs without a database.

Design constraint
-----------------
The model *scores* extracted facts; it never *produces* them. Extraction stays
deterministic (``src.xbrl_parser``) so a reported figure is always traceable to
a source document, and a model regression can never rewrite a filed number —
it can only raise a flag for review.
"""

from src.ml.evaluation import CorruptionKind, EvalMetrics, evaluate_detector, inject_corruptions
from src.ml.features import FEATURE_NAMES, FeatureMatrix, build_features
from src.ml.model import AnomalyDetector, AnomalyScore
from src.ml.monitoring import (
    DriftReport,
    build_drift_report,
    check_feature_drift,
    check_prediction_drift,
)
from src.ml.registry import ModelMetadata, ModelRegistry, render_model_card
from src.ml.synthetic import SyntheticCorpus, generate_corpus

__all__ = [
    "FEATURE_NAMES",
    "AnomalyDetector",
    "AnomalyScore",
    "CorruptionKind",
    "DriftReport",
    "EvalMetrics",
    "FeatureMatrix",
    "ModelMetadata",
    "ModelRegistry",
    "SyntheticCorpus",
    "build_drift_report",
    "build_features",
    "check_feature_drift",
    "check_prediction_drift",
    "evaluate_detector",
    "generate_corpus",
    "inject_corruptions",
    "render_model_card",
]
