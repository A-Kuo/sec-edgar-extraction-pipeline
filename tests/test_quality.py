import pytest
from src.quality import compute_psi, classify_psi, check_completeness, check_psi_drift, PSILevel


class TestPSI:
    def test_compute_psi_identical_distributions(self):
        baseline = [1, 2, 3, 4, 5] * 100
        current = [1, 2, 3, 4, 5] * 100
        psi = compute_psi(baseline, current)
        assert psi < 0.01

    def test_compute_psi_different_distributions(self):
        baseline = [1, 2, 3, 4, 5] * 100
        current = [10, 20, 30, 40, 50] * 100
        psi = compute_psi(baseline, current)
        assert psi > 0.1

    def test_compute_psi_empty(self):
        psi = compute_psi([], [])
        assert psi == 0.0

    def test_classify_psi_clean(self):
        level = classify_psi(0.05)
        assert level == PSILevel.CLEAN

    def test_classify_psi_warn(self):
        level = classify_psi(0.15)
        assert level == PSILevel.WARN

    def test_classify_psi_alert(self):
        level = classify_psi(0.3)
        assert level == PSILevel.ALERT


class TestCompleteness:
    def test_check_completeness_pass(self):
        facts_by_accession = {
            "0000320193-23-000001": [1] * 8,
            "0000320193-23-000002": [1] * 8,
            "0000320193-23-000003": [1] * 8,
        }
        result = check_completeness("run-1", facts_by_accession, threshold=0.95)
        assert result.completeness_pct >= 0.95
        assert result.status == "PASS"

    def test_check_completeness_fail(self):
        facts_by_accession = {
            "0000320193-23-000001": [1] * 2,
            "0000320193-23-000002": [1] * 2,
            "0000320193-23-000003": [1] * 2,
        }
        with pytest.raises(ValueError):
            check_completeness("run-1", facts_by_accession, threshold=0.95)

    def test_check_completeness_empty(self):
        facts_by_accession = {}
        with pytest.raises(ValueError):
            check_completeness("run-1", facts_by_accession, threshold=0.95)

    def test_check_completeness_no_facts(self):
        facts_by_accession = {
            "0000320193-23-000001": [],
        }
        with pytest.raises(ValueError):
            check_completeness("run-1", facts_by_accession, threshold=0.95)


class TestDrift:
    def test_check_psi_drift_clean(self):
        baseline = [1, 2, 3, 4, 5] * 100
        current = [1, 2, 3, 4, 5] * 100
        result = check_psi_drift("Revenues", baseline, current)
        assert result.level == PSILevel.CLEAN
        assert "Revenues" in result.message

    def test_check_psi_drift_alert(self):
        baseline = [1, 2, 3, 4, 5] * 100
        current = [10, 20, 30, 40, 50] * 100
        result = check_psi_drift("Revenues", baseline, current)
        assert result.level in [PSILevel.ALERT, PSILevel.WARN]
