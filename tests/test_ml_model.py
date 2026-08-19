"""
Tests for src/ml/model.py and src/ml/evaluation.py.

The properties worth pinning are the ones that would fail silently:
determinism (a nondeterministic model breaks the registry's hash identity),
threshold calibration (a miscalibrated model reports a precision figure that
means nothing), and the rule/model blend (a regression here would let the
forest veto a known defect).
"""

from __future__ import annotations

import numpy as np
import pytest

from src.ml.evaluation import CorruptionKind, _roc_auc, evaluate_detector, inject_corruptions
from src.ml.features import build_features
from src.ml.model import AnomalyDetector, NotFittedError

# ---------------------------------------------------------------------------
# Construction and fitting
# ---------------------------------------------------------------------------


class TestConstruction:
    @pytest.mark.parametrize("contamination", [0.0, -0.1, 0.6, 1.0])
    def test_rejects_invalid_contamination(self, contamination):
        with pytest.raises(ValueError, match="contamination"):
            AnomalyDetector(contamination=contamination)

    @pytest.mark.parametrize("threshold", [-0.1, 1.1])
    def test_rejects_invalid_threshold(self, threshold):
        with pytest.raises(ValueError, match="threshold"):
            AnomalyDetector(threshold=threshold)

    def test_not_fitted_before_fit(self):
        assert not AnomalyDetector().is_fitted

    def test_scoring_before_fit_raises(self, feature_matrix):
        with pytest.raises(NotFittedError):
            AnomalyDetector().score(feature_matrix)

    def test_fit_on_empty_matrix_raises(self):
        with pytest.raises(ValueError, match="empty"):
            AnomalyDetector().fit(build_features([]))

    def test_fit_returns_self_for_chaining(self, feature_matrix):
        detector = AnomalyDetector(seed=42)
        assert detector.fit(feature_matrix) is detector


# ---------------------------------------------------------------------------
# Determinism — the registry's hash identity depends on this
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_seed_gives_identical_scores(self, feature_matrix):
        a = AnomalyDetector(seed=42).fit(feature_matrix).score(feature_matrix)
        b = AnomalyDetector(seed=42).fit(feature_matrix).score(feature_matrix)
        assert [s.score for s in a] == [s.score for s in b]

    def test_different_seeds_give_different_models(self, feature_matrix):
        a = AnomalyDetector(seed=1).fit(feature_matrix).score(feature_matrix)
        b = AnomalyDetector(seed=999).fit(feature_matrix).score(feature_matrix)
        assert [s.raw_decision for s in a] != [s.raw_decision for s in b]

    def test_repeated_scoring_is_stable(self, fitted_detector, feature_matrix):
        first = fitted_detector.score(feature_matrix)
        second = fitted_detector.score(feature_matrix)
        assert [s.score for s in first] == [s.score for s in second]

    def test_score_is_independent_of_batch_composition(self, fitted_detector, feature_matrix):
        """
        A filing's score must not depend on which other filings ran alongside
        it. Per-batch normalisation would break this and make scores
        incomparable across runs.
        """
        full = {s.accession_number: s.score for s in fitted_detector.score(feature_matrix)}

        subset = type(feature_matrix)(
            X=feature_matrix.X[:5],
            accessions=feature_matrix.accessions[:5],
            feature_names=feature_matrix.feature_names,
        )
        partial = {s.accession_number: s.score for s in fitted_detector.score(subset)}

        for accession, score in partial.items():
            assert score == pytest.approx(full[accession])


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------


class TestCalibration:
    @pytest.mark.parametrize("contamination", [0.02, 0.05, 0.10])
    def test_training_flag_rate_matches_contamination(self, feature_matrix, contamination):
        """
        The whole point of the quantile pin in `_normalise`. A plain min-max
        rescale put threshold=0.6 at the training p90, so a model configured
        for 5% flagged 10% of its own training data.
        """
        detector = AnomalyDetector(seed=42, contamination=contamination).fit(feature_matrix)
        scores = detector.score(feature_matrix)
        flagged = sum(1 for s in scores if s.model_score >= detector.threshold)
        assert flagged / len(scores) == pytest.approx(contamination, abs=0.02)

    def test_scores_are_within_unit_interval(self, fitted_detector, feature_matrix):
        assert all(0.0 <= s.score <= 1.0 for s in fitted_detector.score(feature_matrix))

    def test_results_sorted_by_score_descending(self, fitted_detector, feature_matrix):
        scores = [s.score for s in fitted_detector.score(feature_matrix)]
        assert scores == sorted(scores, reverse=True)

    def test_empty_matrix_scores_to_empty_list(self, fitted_detector):
        assert fitted_detector.score(build_features([])) == []

    def test_feature_mismatch_raises(self, fitted_detector, feature_matrix):
        mismatched = type(feature_matrix)(
            X=feature_matrix.X[:, :3],
            accessions=feature_matrix.accessions,
            feature_names=("a", "b", "c"),
        )
        with pytest.raises(ValueError, match=r"[Ff]eature"):
            fitted_detector.score(mismatched)


# ---------------------------------------------------------------------------
# Rule / model blend
# ---------------------------------------------------------------------------


class TestHybridScoring:
    def _corrupt_one(self, corpus, concept, multiplier):
        facts = [dict(f) for f in corpus.facts]
        target = sorted({f["accession_number"] for f in facts})[0]
        for fact in facts:
            if fact["accession_number"] == target and fact["fact_name"] == concept:
                fact["fact_value"] *= multiplier
        return facts, target

    def test_combined_score_is_max_of_components(self, fitted_detector, feature_matrix):
        for score in fitted_detector.score(feature_matrix):
            assert score.score == pytest.approx(max(score.model_score, score.rule_score))

    def test_rule_violation_forces_a_flag(self, fitted_detector, synthetic_corpus):
        """
        A fired rule must flag the filing even when the forest is calm — that is
        the whole reason the blend is a max rather than an average.
        """
        facts, target = self._corrupt_one(synthetic_corpus, "us-gaap:EarningsPerShareBasic", 100.0)
        matrix = build_features(facts, synthetic_corpus.filings)
        result = next(s for s in fitted_detector.score(matrix) if s.accession_number == target)

        assert result.rule_violations
        assert result.is_anomaly
        assert result.rule_score >= fitted_detector.threshold

    def test_dropped_required_fact_is_caught(self, fitted_detector, synthetic_corpus):
        """
        The failure that motivated the rule engine. fact_coverage has zero
        variance in training, so StandardScaler flattens it and the forest alone
        measured 0.17 recall on this case.
        """
        target = sorted({f["accession_number"] for f in synthetic_corpus.facts})[0]
        facts = [
            f
            for f in synthetic_corpus.facts
            if not (f["accession_number"] == target and f["fact_name"] == "us-gaap:Assets")
        ]
        matrix = build_features(facts, synthetic_corpus.filings)
        result = next(s for s in fitted_detector.score(matrix) if s.accession_number == target)

        assert result.is_anomaly
        assert "missing_required_fact" in {v.rule_id for v in result.rule_violations}

    def test_triggered_by_reports_the_source(self, fitted_detector, synthetic_corpus):
        facts, target = self._corrupt_one(synthetic_corpus, "us-gaap:EarningsPerShareBasic", 100.0)
        matrix = build_features(facts, synthetic_corpus.filings)
        result = next(s for s in fitted_detector.score(matrix) if s.accession_number == target)
        assert result.triggered_by in ("rule", "both")

    def test_explain_cites_the_rule(self, fitted_detector, synthetic_corpus):
        facts, target = self._corrupt_one(synthetic_corpus, "us-gaap:EarningsPerShareBasic", 100.0)
        matrix = build_features(facts, synthetic_corpus.filings)
        result = next(s for s in fitted_detector.score(matrix) if s.accession_number == target)
        assert "eps_reconciliation_failed" in result.explain()

    def test_explain_falls_back_to_features(self, fitted_detector, feature_matrix):
        clean = [s for s in fitted_detector.score(feature_matrix) if not s.rule_violations]
        assert "outlier" in clean[0].explain()

    def test_as_dict_is_json_serialisable(self, fitted_detector, feature_matrix):
        import json

        json.dumps(fitted_detector.score(feature_matrix)[0].as_dict())

    def test_score_values_matches_score_order(self, fitted_detector, feature_matrix):
        values = fitted_detector.score_values(feature_matrix)
        by_accession = {s.accession_number: s.score for s in fitted_detector.score(feature_matrix)}
        expected = [by_accession[a] for a in feature_matrix.accessions]
        assert values.tolist() == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_round_trip_preserves_scores(self, fitted_detector, feature_matrix, tmp_path):
        path = fitted_detector.save(tmp_path / "model.joblib")
        loaded = AnomalyDetector.load(path)

        before = [s.score for s in fitted_detector.score(feature_matrix)]
        after = [s.score for s in loaded.score(feature_matrix)]
        assert before == after

    def test_round_trip_preserves_hyperparameters(self, tmp_path, feature_matrix):
        original = AnomalyDetector(seed=7, contamination=0.03, n_estimators=50, threshold=0.7).fit(
            feature_matrix
        )
        loaded = AnomalyDetector.load(original.save(tmp_path / "m.joblib"))

        assert loaded.seed == 7
        assert loaded.contamination == 0.03
        assert loaded.n_estimators == 50
        assert loaded.threshold == 0.7

    def test_saving_unfitted_raises(self, tmp_path):
        with pytest.raises(NotFittedError):
            AnomalyDetector().save(tmp_path / "m.joblib")

    def test_loading_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            AnomalyDetector.load(tmp_path / "nope.joblib")

    def test_identical_training_produces_identical_bytes(self, feature_matrix, tmp_path):
        """The property the registry treats an artifact hash as identity for."""
        import hashlib

        digests = []
        for name in ("a.joblib", "b.joblib"):
            path = AnomalyDetector(seed=42).fit(feature_matrix).save(tmp_path / name)
            digests.append(hashlib.sha256(path.read_bytes()).hexdigest())
        assert digests[0] == digests[1]


# ---------------------------------------------------------------------------
# Evaluation harness
# ---------------------------------------------------------------------------


class TestCorruptionInjection:
    def test_does_not_mutate_the_input(self, synthetic_corpus):
        before = [dict(f) for f in synthetic_corpus.facts]
        inject_corruptions(synthetic_corpus.facts, rate=0.2, seed=1)
        assert synthetic_corpus.facts == before

    def test_corrupts_roughly_the_requested_fraction(self, synthetic_corpus):
        _, corrupted = inject_corruptions(synthetic_corpus.facts, rate=0.2, seed=1)
        total = len({f["accession_number"] for f in synthetic_corpus.facts})
        assert corrupted
        assert abs(len(corrupted) / total - 0.2) < 0.05

    def test_is_deterministic_for_a_seed(self, synthetic_corpus):
        a_facts, a_set = inject_corruptions(synthetic_corpus.facts, rate=0.2, seed=1)
        b_facts, b_set = inject_corruptions(synthetic_corpus.facts, rate=0.2, seed=1)
        assert a_set == b_set
        assert a_facts == b_facts

    def test_different_seeds_pick_different_filings(self, synthetic_corpus):
        _, a = inject_corruptions(synthetic_corpus.facts, rate=0.2, seed=1)
        _, b = inject_corruptions(synthetic_corpus.facts, rate=0.2, seed=2)
        assert a != b

    @pytest.mark.parametrize("rate", [0.0, 1.0, -0.5, 1.5])
    def test_rejects_out_of_range_rate(self, synthetic_corpus, rate):
        with pytest.raises(ValueError, match="rate"):
            inject_corruptions(synthetic_corpus.facts, rate=rate)

    def test_actually_changes_the_data(self, synthetic_corpus):
        corrupted, _ = inject_corruptions(synthetic_corpus.facts, rate=0.3, seed=5)
        assert corrupted != list(synthetic_corpus.facts)

    def test_covers_every_corruption_kind(self):
        """Guards against a kind being added to the enum but never applied."""
        assert len(list(CorruptionKind)) == 4


class TestEvaluation:
    def test_detects_injected_corruptions(self, fitted_detector, synthetic_corpus):
        metrics = evaluate_detector(
            fitted_detector, synthetic_corpus.facts, synthetic_corpus.filings, rate=0.1, seed=1337
        )
        assert metrics.recall > 0.5
        assert metrics.roc_auc > 0.75

    def test_metrics_are_within_valid_ranges(self, fitted_detector, synthetic_corpus):
        metrics = evaluate_detector(
            fitted_detector, synthetic_corpus.facts, synthetic_corpus.filings, rate=0.1, seed=1
        )
        for name in ("recall", "precision", "f1", "roc_auc", "flag_rate"):
            assert 0.0 <= getattr(metrics, name) <= 1.0

    def test_corrupted_filings_score_higher_on_average(self, fitted_detector, synthetic_corpus):
        metrics = evaluate_detector(
            fitted_detector, synthetic_corpus.facts, synthetic_corpus.filings, rate=0.1, seed=1
        )
        assert metrics.mean_score_corrupted > metrics.mean_score_clean

    def test_as_metrics_exposes_the_gated_subset(self, fitted_detector, synthetic_corpus):
        metrics = evaluate_detector(
            fitted_detector, synthetic_corpus.facts, synthetic_corpus.filings, rate=0.1, seed=1
        )
        assert set(metrics.as_metrics()) == {
            "recall",
            "precision",
            "f1",
            "roc_auc",
            "flag_rate",
        }


class TestRocAuc:
    def test_perfect_separation_is_one(self):
        labels = np.array([0, 0, 1, 1])
        assert _roc_auc(labels, np.array([0.1, 0.2, 0.8, 0.9])) == pytest.approx(1.0)

    def test_inverted_separation_is_zero(self):
        labels = np.array([0, 0, 1, 1])
        assert _roc_auc(labels, np.array([0.9, 0.8, 0.2, 0.1])) == pytest.approx(0.0)

    def test_all_ties_is_one_half(self):
        labels = np.array([0, 0, 1, 1])
        assert _roc_auc(labels, np.array([0.5, 0.5, 0.5, 0.5])) == pytest.approx(0.5)

    def test_single_class_returns_one_half_rather_than_raising(self):
        """CI can evaluate a small sample where no corruption landed."""
        assert _roc_auc(np.array([0, 0, 0]), np.array([0.1, 0.2, 0.3])) == 0.5

    def test_matches_sklearn(self):
        rng = np.random.default_rng(0)
        labels = rng.integers(0, 2, size=200)
        scores = rng.random(200)
        if 0 < labels.sum() < len(labels):
            from sklearn.metrics import roc_auc_score

            assert _roc_auc(labels, scores) == pytest.approx(roc_auc_score(labels, scores))
