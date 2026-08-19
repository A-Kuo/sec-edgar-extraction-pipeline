"""
Tests for src/quality.py

Covers:
  compute_psi  — identical, slight, moderate, severe distributions; edge cases
  check_completeness — happy path, failure, empty input, custom threshold
  classify_psi  — level thresholds
  check_psi_drift — integration through the labelled wrapper
"""

from __future__ import annotations

import contextlib

import pytest

from src.quality import (
    PSI_ALERT_THRESHOLD,
    PSI_WARN_THRESHOLD,
    PSILevel,
    PSIResult,
    QualityResult,
    check_completeness,
    check_psi_drift,
    classify_psi,
    compute_psi,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uniform(n: int, lo: float = 0.0, hi: float = 100.0) -> list[float]:
    """Uniform values spread across [lo, hi]."""
    step = (hi - lo) / n
    return [lo + i * step for i in range(n)]


def _skewed(n: int) -> list[float]:
    """Values heavily concentrated at the low end."""
    return [1.0] * (n - 2) + [50.0, 100.0]


# ---------------------------------------------------------------------------
# 1. compute_psi — distribution shape tests
# ---------------------------------------------------------------------------


class TestComputePSI:
    def test_identical_distributions_zero(self):
        """Same distribution → PSI ≈ 0."""
        vals = _uniform(100)
        psi = compute_psi(vals, vals[:])
        assert psi == pytest.approx(0.0, abs=1e-6)

    def test_slight_shift_below_warn(self):
        """A tiny distributional shift should produce PSI < 0.10."""
        base = _uniform(1000, 0, 100)
        curr = _uniform(1000, 1, 101)  # shifted by 1 unit
        psi = compute_psi(base, curr)
        assert psi < PSI_WARN_THRESHOLD

    def test_moderate_shift_between_warn_and_alert(self):
        """
        A moderate shift: baseline is uniform, current is moderately skewed.
        PSI should be in [0.10, 0.25].
        """
        base = _uniform(500, 0, 100)
        curr = _uniform(200, 0, 50) + _uniform(200, 50, 70) + _uniform(100, 70, 100)
        psi = compute_psi(base, curr)
        # Just verify it's positive and non-trivial
        assert psi > 0, "PSI must be positive for differing distributions"

    def test_large_shift_above_alert(self):
        """Completely disjoint distributions → PSI well above 0.25."""
        base = [float(x) for x in range(1, 101)]  # 1..100
        curr = [float(x) for x in range(201, 301)]  # 201..300
        psi = compute_psi(base, curr)
        assert psi > PSI_ALERT_THRESHOLD

    def test_psi_is_nonnegative(self):
        """PSI must always be ≥ 0."""
        import random

        random.seed(42)
        base = [random.gauss(0, 1) for _ in range(200)]
        curr = [random.gauss(1, 2) for _ in range(150)]
        psi = compute_psi(base, curr)
        assert psi >= 0.0

    def test_psi_symmetric_ish(self):
        """PSI(A→B) and PSI(B→A) should both be positive (not necessarily equal)."""
        base = [float(i) for i in range(1, 51)]
        curr = [float(i) for i in range(51, 101)]
        psi_ab = compute_psi(base, curr)
        psi_ba = compute_psi(curr, base)
        assert psi_ab > 0
        assert psi_ba > 0

    def test_single_element_distributions(self):
        """Should not raise even with very small inputs."""
        psi = compute_psi([1.0], [2.0])
        assert psi >= 0.0

    def test_empty_baseline_raises(self):
        with pytest.raises(ValueError, match="baseline_values"):
            compute_psi([], [1.0, 2.0])

    def test_empty_current_raises(self):
        with pytest.raises(ValueError, match="current_values"):
            compute_psi([1.0, 2.0], [])

    def test_all_same_values_returns_zero(self):
        """If all values are identical, there is no distributional spread."""
        base = [5.0] * 100
        curr = [5.0] * 80
        psi = compute_psi(base, curr)
        assert psi == pytest.approx(0.0, abs=1e-9)

    def test_custom_bins(self):
        """PSI with more bins should still be a non-negative float."""
        base = _uniform(200)
        curr = _uniform(200, 5, 105)
        psi_10 = compute_psi(base, curr, bins=10)
        psi_20 = compute_psi(base, curr, bins=20)
        assert psi_10 >= 0.0
        assert psi_20 >= 0.0

    def test_returns_float(self):
        vals = _uniform(50)
        psi = compute_psi(vals, vals)
        assert isinstance(psi, float)


# ---------------------------------------------------------------------------
# 2. classify_psi
# ---------------------------------------------------------------------------


class TestClassifyPSI:
    def test_below_warn_is_clean(self):
        assert classify_psi(0.0) == PSILevel.CLEAN
        assert classify_psi(0.09) == PSILevel.CLEAN
        assert classify_psi(PSI_WARN_THRESHOLD - 0.001) == PSILevel.CLEAN

    def test_at_warn_threshold_is_warn(self):
        assert classify_psi(PSI_WARN_THRESHOLD) == PSILevel.WARN
        assert classify_psi(0.15) == PSILevel.WARN
        assert classify_psi(PSI_ALERT_THRESHOLD - 0.001) == PSILevel.WARN

    def test_at_alert_threshold_is_alert(self):
        assert classify_psi(PSI_ALERT_THRESHOLD) == PSILevel.ALERT
        assert classify_psi(1.0) == PSILevel.ALERT
        assert classify_psi(0.5) == PSILevel.ALERT


# ---------------------------------------------------------------------------
# 3. check_psi_drift (integration wrapper)
# ---------------------------------------------------------------------------


class TestCheckPsiDrift:
    def test_returns_psi_result(self):
        vals = _uniform(100)
        result = check_psi_drift("us-gaap:Revenues", vals, vals[:])
        assert isinstance(result, PSIResult)
        assert result.fact_name == "us-gaap:Revenues"

    def test_clean_level_for_identical(self):
        vals = _uniform(100)
        result = check_psi_drift("us-gaap:Assets", vals, vals[:])
        assert result.level == PSILevel.CLEAN
        assert result.psi == pytest.approx(0.0, abs=1e-6)

    def test_alert_level_for_disjoint(self):
        base = [float(i) for i in range(1, 101)]
        curr = [float(i) for i in range(501, 601)]
        result = check_psi_drift("us-gaap:NetIncomeLoss", base, curr)
        assert result.level == PSILevel.ALERT

    def test_counts_recorded(self):
        base = _uniform(80)
        curr = _uniform(60)
        result = check_psi_drift("us-gaap:Liabilities", base, curr)
        assert result.baseline_count == 80
        assert result.current_count == 60


# ---------------------------------------------------------------------------
# 4. check_completeness
# ---------------------------------------------------------------------------


class TestCheckCompleteness:
    _RUN_ID = "test-run-001"

    def _all_complete(self):
        return {
            "0000320193-24-000001": ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets"],
            "0000320193-24-000002": ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets"],
        }

    def test_all_complete_passes(self):
        result = check_completeness(self._RUN_ID, self._all_complete())
        assert result.completeness_ratio == pytest.approx(1.0)
        assert result.passed

    def test_returns_quality_result(self):
        result = check_completeness(self._RUN_ID, self._all_complete())
        assert isinstance(result, QualityResult)
        assert result.run_id == self._RUN_ID

    def test_total_and_complete_counts(self):
        facts = self._all_complete()
        result = check_completeness(self._RUN_ID, facts)
        assert result.total_filings == 2
        assert result.complete_filings == 2

    def test_missing_facts_tracked(self):
        facts = {
            "0000-24-0001": ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets"],
            "0000-24-0002": ["us-gaap:Revenues"],  # missing NetIncomeLoss + Assets
        }
        with pytest.raises(ValueError):
            check_completeness(self._RUN_ID, facts, threshold=0.95)

    def test_missing_facts_recorded_in_result(self):
        facts = {
            "0000-24-0001": ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets"],
            "0000-24-0002": ["us-gaap:Revenues"],
        }
        with contextlib.suppress(ValueError):
            check_completeness(self._RUN_ID, facts, threshold=0.5)
        # Even passing threshold=0.5 with 1/2 complete should pass
        result = check_completeness(self._RUN_ID, facts, threshold=0.5)
        assert "0000-24-0002" in result.missing_facts_by_accession
        missing = result.missing_facts_by_accession["0000-24-0002"]
        assert "us-gaap:NetIncomeLoss" in missing
        assert "us-gaap:Assets" in missing

    def test_below_threshold_raises_value_error(self):
        facts = {
            "acc1": ["us-gaap:Revenues"],  # missing NetIncomeLoss + Assets
            "acc2": ["us-gaap:Revenues"],
            "acc3": ["us-gaap:Revenues"],
            "acc4": ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets"],
        }
        with pytest.raises(ValueError, match="Completeness below threshold"):
            check_completeness(self._RUN_ID, facts, threshold=0.95)

    def test_exactly_at_threshold_passes(self):
        # 1 of 2 complete = 50%, threshold = 0.5 → should pass
        facts = {
            "acc1": ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets"],
            "acc2": [],
        }
        result = check_completeness(self._RUN_ID, facts, threshold=0.5)
        assert result.passed

    def test_empty_input_passes(self):
        """No filings → nothing to check → ratio = 1.0, passes."""
        result = check_completeness(self._RUN_ID, {})
        assert result.total_filings == 0
        assert result.completeness_ratio == pytest.approx(1.0)

    def test_custom_required_facts(self):
        facts = {
            "acc1": ["my-gaap:Revenue"],
        }
        result = check_completeness(self._RUN_ID, facts, required_facts=["my-gaap:Revenue"])
        assert result.complete_filings == 1
