"""
Tests for src/ml/monitoring.py, plus the quantile-binning fix in src/quality.py.

Two properties matter here more than the rest: the monitor must stay quiet on
an equivalent population, and it must fire on a genuinely shifted one. A
monitor that fails either side is worthless — the first makes it noise that
gets muted, the second makes it decoration. See src/ml/monitoring.py's module
docstring for the false-alarm bug (PSI 0.37 between two draws from the same
generator) these tests were written to pin down.
"""

from __future__ import annotations

import pytest

from src.ml.features import build_features
from src.ml.monitoring import (
    MIN_SAMPLES_FOR_ALERT,
    build_drift_report,
    check_feature_drift,
    check_prediction_drift,
)
from src.ml.synthetic import generate_corpus
from src.quality import PSILevel, classify_psi, compute_psi

# ---------------------------------------------------------------------------
# compute_psi — the quantile-binning fix
# ---------------------------------------------------------------------------


class TestQuantileBinning:
    def test_uniform_is_still_the_default(self):
        """Existing callers must keep their calibrated behaviour unchanged."""
        base = [1.0] * 90 + [100.0] * 10
        curr = [1.0] * 85 + [100.0] * 15
        assert compute_psi(base, curr) == compute_psi(base, curr, strategy="uniform")

    def test_unknown_strategy_raises(self):
        with pytest.raises(ValueError, match="strategy"):
            compute_psi([1.0, 2.0], [1.0, 2.0], strategy="bogus")

    def test_quantile_is_near_zero_for_two_draws_from_the_same_distribution(self):
        """
        The regression case: two 60-company samples from an identical generator
        measured PSI 0.37 (ALERT) under uniform binning on a skewed feature.
        Quantile binning must not manufacture drift out of sampling noise.
        """
        import numpy as np

        rng = np.random.default_rng(0)
        # Log-normal, like a magnitude feature — skewed, the case uniform binning
        # mishandles.
        base = list(np.exp(rng.normal(10, 2, size=300)))
        curr = list(np.exp(rng.normal(10, 2, size=300)))

        psi_uniform = compute_psi(base, curr, strategy="uniform")
        psi_quantile = compute_psi(base, curr, strategy="quantile")

        assert psi_quantile < psi_uniform

    def test_quantile_still_detects_a_real_shift(self):
        base = list(range(1, 301))
        curr = [v * 5 for v in base]
        assert classify_psi(compute_psi(base, curr, strategy="quantile")) == PSILevel.ALERT

    def test_quantile_handles_heavy_ties(self):
        """A feature that is 0.0 for most rows (e.g. year-over-year on first
        filings) cannot support 10 distinct bins; the edge collapse must not
        raise."""
        base = [0.0] * 95 + [1.0] * 5
        curr = [0.0] * 90 + [1.0] * 10
        psi = compute_psi(base, curr, strategy="quantile")
        assert psi >= 0.0

    def test_quantile_handles_constant_baseline(self):
        assert compute_psi([5.0] * 50, [5.0] * 40 + [6.0] * 10, strategy="quantile") >= 0.0

    def test_quantile_values_outside_baseline_range_still_bin(self):
        """Open outer edges: a current value beyond the baseline's observed
        range must land in the first/last bin, not be silently dropped."""
        base = list(range(1, 101))
        curr = [-1000.0, 5000.0, *list(range(1, 99))]
        psi = compute_psi(base, curr, strategy="quantile")
        assert psi >= 0.0  # did not raise, and produced a real number


# ---------------------------------------------------------------------------
# Feature drift
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def baseline_matrix():
    # 400 companies, not the 150 used elsewhere: per-company scale follows a
    # multiplicative random walk, so a company-level feature's batch mean has
    # real sampling variance that shrinks with more *companies*, not more
    # filings per company. At 150 companies log_assets alone false-alerted in
    # 1 of 5 seed pairs tried (WARN in the rest) — noise, not drift. 400 was
    # clean across every seed tried. See the "Known limitation" note in
    # src/ml/monitoring.py.
    corpus = generate_corpus(n_companies=400, years_per_company=5, seed=7)
    return build_features(corpus.facts, corpus.filings)


class TestFeatureDrift:
    def test_identical_matrix_shows_no_drift(self, baseline_matrix):
        drifts = check_feature_drift(baseline_matrix, baseline_matrix)
        assert all(not d.is_actionable for d in drifts)

    def test_one_row_per_feature(self, baseline_matrix):
        drifts = check_feature_drift(baseline_matrix, baseline_matrix)
        assert {d.feature_name for d in drifts} == set(baseline_matrix.feature_names)

    def test_equivalent_population_does_not_alert(self, baseline_matrix):
        """
        Same size, same generator, different seed. This is the case that used to
        false-positive under uniform binning.
        """
        fresh = generate_corpus(n_companies=150, years_per_company=5, seed=808)
        current = build_features(fresh.facts, fresh.filings)

        drifts = check_feature_drift(baseline_matrix, current)
        alerting = [d for d in drifts if d.is_actionable and d.level == PSILevel.ALERT]
        assert alerting == [], f"False alert(s) on an equivalent population: {alerting}"

    def test_systematic_scale_shift_alerts(self, baseline_matrix):
        fresh = generate_corpus(n_companies=150, years_per_company=5, seed=808)
        for fact in fresh.facts:
            if fact["fact_name"] == "us-gaap:Revenues":
                fact["fact_value"] *= 1000
        current = build_features(fresh.facts, fresh.filings)

        drifts = check_feature_drift(baseline_matrix, current)
        by_name = {d.feature_name: d for d in drifts}
        assert by_name["log_revenue"].level == PSILevel.ALERT
        assert by_name["log_revenue"].is_actionable

    def test_mismatched_features_raises(self, baseline_matrix):
        mismatched = type(baseline_matrix)(
            X=baseline_matrix.X[:, :3],
            accessions=baseline_matrix.accessions,
            feature_names=("a", "b", "c"),
        )
        with pytest.raises(ValueError, match="different features"):
            check_feature_drift(baseline_matrix, mismatched)

    def test_empty_matrix_raises(self, baseline_matrix):
        with pytest.raises(ValueError, match="non-empty"):
            check_feature_drift(baseline_matrix, build_features([]))

    def test_small_batch_below_min_samples_is_not_actionable(self, baseline_matrix):
        """A batch too small to trust must be reported but never escalated."""
        tiny = type(
            baseline_matrix
        )(
            X=baseline_matrix.X[:5] + 5.0,  # shifted, to try to trigger a WARN/ALERT
            accessions=baseline_matrix.accessions[:5],
            feature_names=baseline_matrix.feature_names,
        )
        assert len(tiny) < MIN_SAMPLES_FOR_ALERT
        drifts = check_feature_drift(baseline_matrix, tiny)
        assert all(not d.is_actionable for d in drifts)


# ---------------------------------------------------------------------------
# Prediction drift
# ---------------------------------------------------------------------------


class TestPredictionDrift:
    def test_identical_distributions_show_no_drift(self):
        scores = [0.1, 0.2, 0.3, 0.4, 0.5] * 20
        psi, level, base_rate, curr_rate = check_prediction_drift(scores, scores, threshold=0.6)
        assert psi == pytest.approx(0.0)
        assert level == PSILevel.CLEAN
        assert base_rate == curr_rate

    def test_flag_rate_reflects_the_threshold(self):
        baseline = [0.1] * 90 + [0.9] * 10
        current = [0.1] * 80 + [0.9] * 20
        _, _, base_rate, curr_rate = check_prediction_drift(baseline, current, threshold=0.6)
        assert base_rate == pytest.approx(0.10)
        assert curr_rate == pytest.approx(0.20)

    def test_empty_inputs_raise(self):
        with pytest.raises(ValueError):
            check_prediction_drift([], [0.5], threshold=0.6)


# ---------------------------------------------------------------------------
# Combined drift report
# ---------------------------------------------------------------------------


class TestDriftReport:
    def test_worst_level_escalates_to_alert(self, baseline_matrix):
        fresh = generate_corpus(n_companies=150, years_per_company=5, seed=808)
        for fact in fresh.facts:
            if fact["fact_name"] == "us-gaap:Revenues":
                fact["fact_value"] *= 1000
        current = build_features(fresh.facts, fresh.filings)

        report = build_drift_report(baseline_matrix, current, threshold=0.6)
        assert report.worst_level == PSILevel.ALERT
        assert not report.passed

    def test_passes_on_an_equivalent_population(self, baseline_matrix):
        fresh = generate_corpus(n_companies=150, years_per_company=5, seed=808)
        current = build_features(fresh.facts, fresh.filings)

        report = build_drift_report(baseline_matrix, current, threshold=0.6)
        assert report.passed

    def test_prediction_scores_are_optional(self, baseline_matrix):
        """Feature-only drift checks must work before a model exists."""
        report = build_drift_report(baseline_matrix, baseline_matrix, threshold=0.6)
        assert report.prediction_psi is None
        assert report.prediction_level is None

    def test_flag_rate_anomalous_flags_large_swings(self):
        from src.ml.monitoring import DriftReport

        report = DriftReport(baseline_flag_rate=0.05, current_flag_rate=0.40)
        assert report.flag_rate_anomalous

    def test_flag_rate_anomalous_tolerates_small_swings(self):
        from src.ml.monitoring import DriftReport

        report = DriftReport(baseline_flag_rate=0.05, current_flag_rate=0.07)
        assert not report.flag_rate_anomalous

    def test_flag_rate_zero_to_zero_is_not_anomalous(self):
        from src.ml.monitoring import DriftReport

        report = DriftReport(baseline_flag_rate=0.0, current_flag_rate=0.0)
        assert not report.flag_rate_anomalous

    def test_flag_rate_zero_to_nonzero_is_anomalous(self):
        from src.ml.monitoring import DriftReport

        report = DriftReport(baseline_flag_rate=0.0, current_flag_rate=0.1)
        assert report.flag_rate_anomalous

    def test_as_dict_is_json_serialisable(self, baseline_matrix):
        import json

        report = build_drift_report(baseline_matrix, baseline_matrix, threshold=0.6)
        json.dumps(report.as_dict())

    def test_summary_mentions_drifted_features(self, baseline_matrix):
        fresh = generate_corpus(n_companies=150, years_per_company=5, seed=808)
        for fact in fresh.facts:
            if fact["fact_name"] == "us-gaap:Revenues":
                fact["fact_value"] *= 1000
        current = build_features(fresh.facts, fresh.filings)

        report = build_drift_report(baseline_matrix, current, threshold=0.6)
        assert "log_revenue" in report.summary()

    def test_drifted_features_sorted_worst_first(self, baseline_matrix):
        fresh = generate_corpus(n_companies=150, years_per_company=5, seed=808)
        for fact in fresh.facts:
            if fact["fact_name"] in ("us-gaap:Revenues", "us-gaap:Assets"):
                fact["fact_value"] *= 1000
        current = build_features(fresh.facts, fresh.filings)

        report = build_drift_report(baseline_matrix, current, threshold=0.6)
        psis = [f.psi for f in report.drifted_features]
        assert psis == sorted(psis, reverse=True)
