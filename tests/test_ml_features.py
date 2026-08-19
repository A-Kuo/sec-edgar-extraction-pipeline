"""
Tests for src/ml/features.py and src/ml/rules.py.

Feature construction is the foundation everything else stands on: if a feature
is wrong, the model trains on it happily, the rules fire on it confidently, and
every downstream metric is measuring the wrong thing. These tests pin the
arithmetic against hand-computed values rather than asserting "it returns a
number".
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from src.ml.features import (
    FEATURE_NAMES,
    REQUIRED_CONCEPTS,
    TARGET_CONCEPTS,
    FeatureMatrix,
    build_features,
    group_facts_by_filing,
)
from src.ml.rules import RULES, evaluate_rules, rule_score


def _fact(accession, name, value, **kwargs):
    row = {
        "accession_number": accession,
        "fact_name": f"us-gaap:{name}",
        "fact_value": value,
        "unit": "USD",
        "period_start": None,
        "period_end": "2024-12-31",
        "segment": None,
    }
    row.update(kwargs)
    return row


def _well_formed(accession="0000000001-24-000001", **overrides):
    """A filing whose facts are internally consistent."""
    values = {
        "Revenues": 1_000_000.0,
        "NetIncomeLoss": 100_000.0,
        "OperatingIncomeLoss": 150_000.0,
        "Assets": 2_000_000.0,
        "Liabilities": 800_000.0,
        "EarningsPerShareBasic": 1.0,
        "CommonStockSharesOutstanding": 100_000.0,
    }
    values.update(overrides)
    return [
        _fact(accession, name, value, period_start="2024-01-01") for name, value in values.items()
    ]


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------


class TestGroupFactsByFiling:
    def test_groups_by_accession(self):
        facts = _well_formed("A") + _well_formed("B")
        grouped = group_facts_by_filing(facts)
        assert set(grouped) == {"A", "B"}

    def test_strips_taxonomy_prefix(self):
        grouped = group_facts_by_filing(_well_formed("A"))
        assert "Revenues" in grouped["A"]
        assert "us-gaap:Revenues" not in grouped["A"]

    def test_revenue_alias_folds_onto_revenues(self):
        facts = [
            _fact("A", "RevenueFromContractWithCustomerExcludingAssessedTax", 500.0),
        ]
        grouped = group_facts_by_filing(facts)
        assert grouped["A"]["Revenues"] == 500.0

    def test_segment_rows_excluded_by_default(self):
        facts = [*_well_formed("A"), _fact("A", "Revenues", 999.0, segment="Americas")]
        grouped = group_facts_by_filing(facts)
        assert grouped["A"]["Revenues"] == 1_000_000.0

    def test_segment_rows_included_when_requested(self):
        facts = [_fact("A", "Revenues", 999.0, segment="Americas")]
        grouped = group_facts_by_filing(facts, consolidated_only=False)
        assert grouped["A"]["Revenues"] == 999.0

    def test_later_period_wins_for_duplicate_concept(self):
        """A 10-K carries both current and prior-year values of the same tag."""
        facts = [
            _fact("A", "Revenues", 100.0, period_end="2023-12-31"),
            _fact("A", "Revenues", 200.0, period_end="2024-12-31"),
        ]
        assert group_facts_by_filing(facts)["A"]["Revenues"] == 200.0

    def test_undated_row_does_not_displace_dated_row(self):
        facts = [
            _fact("A", "Revenues", 200.0, period_end="2024-12-31"),
            _fact("A", "Revenues", 100.0, period_end=None),
        ]
        assert group_facts_by_filing(facts)["A"]["Revenues"] == 200.0

    def test_non_numeric_values_dropped(self):
        facts = [_fact("A", "Revenues", "not-a-number")]
        assert group_facts_by_filing(facts) == {}

    def test_nan_dropped(self):
        facts = [_fact("A", "Revenues", float("nan"))]
        assert group_facts_by_filing(facts) == {}

    def test_rows_without_accession_ignored(self):
        facts = [{"fact_name": "us-gaap:Revenues", "fact_value": 1.0}]
        assert group_facts_by_filing(facts) == {}


# ---------------------------------------------------------------------------
# Feature arithmetic
# ---------------------------------------------------------------------------


class TestBuildFeatures:
    def test_empty_input_returns_empty_matrix(self):
        matrix = build_features([])
        assert len(matrix) == 0
        assert matrix.X.shape == (0, len(FEATURE_NAMES))

    def test_shape_matches_feature_names(self, feature_matrix):
        assert feature_matrix.X.shape[1] == len(FEATURE_NAMES)
        assert feature_matrix.X.shape[0] == len(feature_matrix.accessions)

    def test_rows_sorted_by_accession(self):
        """Output order must not depend on input order."""
        facts = _well_formed("C") + _well_formed("A") + _well_formed("B")
        assert build_features(facts).accessions == ["A", "B", "C"]

    def test_ratios_computed_correctly(self):
        row = build_features(_well_formed("A")).row_as_dict("A")
        assert row["net_margin"] == pytest.approx(0.1)  # 100k / 1M
        assert row["operating_margin"] == pytest.approx(0.15)  # 150k / 1M
        assert row["leverage"] == pytest.approx(0.4)  # 800k / 2M
        assert row["return_on_assets"] == pytest.approx(0.05)  # 100k / 2M
        assert row["asset_turnover"] == pytest.approx(0.5)  # 1M / 2M

    def test_log_scale_is_log10_of_magnitude(self):
        row = build_features(_well_formed("A")).row_as_dict("A")
        assert row["log_assets"] == pytest.approx(math.log10(1 + 2_000_000.0))
        assert row["log_revenue"] == pytest.approx(math.log10(1 + 1_000_000.0))

    def test_eps_consistency_zero_when_reconciled(self):
        """NetIncome 100k = EPS 1.0 x 100k shares, so the residual is 0."""
        row = build_features(_well_formed("A")).row_as_dict("A")
        assert row["eps_consistency"] == pytest.approx(0.0)

    def test_eps_consistency_detects_mismatch(self):
        facts = _well_formed("A", EarningsPerShareBasic=2.0)
        row = build_features(facts).row_as_dict("A")
        # NetIncome / (2.0 x 100k) - 1 = 0.5 - 1 = -0.5
        assert row["eps_consistency"] == pytest.approx(-0.5)

    def test_coverage_full_for_complete_filing(self):
        row = build_features(_well_formed("A")).row_as_dict("A")
        assert row["fact_coverage"] == pytest.approx(1.0)
        assert row["required_coverage"] == pytest.approx(1.0)

    def test_required_coverage_drops_when_required_fact_missing(self):
        facts = [f for f in _well_formed("A") if "Assets" not in f["fact_name"]]
        row = build_features(facts).row_as_dict("A")
        assert row["required_coverage"] == pytest.approx(2 / 3)

    def test_optional_fact_missing_leaves_required_coverage_full(self):
        facts = [f for f in _well_formed("A") if "Liabilities" not in f["fact_name"]]
        row = build_features(facts).row_as_dict("A")
        assert row["required_coverage"] == pytest.approx(1.0)
        assert row["fact_coverage"] == pytest.approx(6 / 7)

    def test_missing_denominator_yields_neutral_fill(self):
        """A ratio with no denominator must not blow up or produce inf."""
        facts = [_fact("A", "NetIncomeLoss", 100.0)]
        row = build_features(facts).row_as_dict("A")
        assert row["net_margin"] == 0.0
        assert np.isfinite(row["net_margin"])

    def test_ratios_are_clipped(self):
        facts = _well_formed("A", Revenues=1.0)  # turnover -> 1/2M, margin -> 100k
        row = build_features(facts).row_as_dict("A")
        assert abs(row["net_margin"]) <= 10.0

    def test_period_days_scaled_by_year(self):
        row = build_features(_well_formed("A")).row_as_dict("A")
        # 2024-01-01 to 2024-12-31 is 365 days.
        assert row["period_days"] == pytest.approx(365 / 366.0)

    def test_all_features_finite(self, feature_matrix):
        assert np.all(np.isfinite(feature_matrix.X))


class TestYearOverYearFeatures:
    def _two_years(self, revenue_year2):
        facts = _well_formed("A") + _well_formed("B", Revenues=revenue_year2)
        filings = {
            "A": {"cik": "1", "period_of_report": "2023-12-31"},
            "B": {"cik": "1", "period_of_report": "2024-12-31"},
        }
        return facts, filings

    def test_neutral_without_filing_metadata(self):
        """Without metadata there is no way to know which filings are related."""
        facts, _ = self._two_years(1_000_000.0)
        matrix = build_features(facts)
        assert matrix.row_as_dict("B")["revenue_yoy_log_change"] == 0.0

    def test_first_filing_has_no_prior(self):
        facts, filings = self._two_years(1_000_000.0)
        matrix = build_features(facts, filings)
        assert matrix.row_as_dict("A")["revenue_yoy_log_change"] == 0.0

    def test_flat_revenue_gives_zero_change(self):
        facts, filings = self._two_years(1_000_000.0)
        matrix = build_features(facts, filings)
        assert matrix.row_as_dict("B")["revenue_yoy_log_change"] == pytest.approx(0.0)

    def test_tenfold_growth_is_one_log_unit(self):
        facts, filings = self._two_years(10_000_000.0)
        matrix = build_features(facts, filings)
        assert matrix.row_as_dict("B")["revenue_yoy_log_change"] == pytest.approx(1.0, abs=1e-6)

    def test_different_companies_are_not_compared(self):
        facts, filings = self._two_years(10_000_000.0)
        filings["B"] = {"cik": "2", "period_of_report": "2024-12-31"}
        matrix = build_features(facts, filings)
        assert matrix.row_as_dict("B")["revenue_yoy_log_change"] == 0.0


class TestFeatureMatrix:
    def test_rejects_row_count_mismatch(self):
        with pytest.raises(ValueError, match="accessions"):
            FeatureMatrix(X=np.zeros((2, len(FEATURE_NAMES))), accessions=["A"])

    def test_rejects_column_count_mismatch(self):
        with pytest.raises(ValueError, match="feature names"):
            FeatureMatrix(X=np.zeros((1, 3)), accessions=["A"])

    def test_column_lookup(self, feature_matrix):
        column = feature_matrix.column("fact_coverage")
        assert column.shape == (len(feature_matrix),)

    def test_unknown_column_raises(self, feature_matrix):
        with pytest.raises(KeyError):
            feature_matrix.column("does_not_exist")

    def test_unknown_accession_raises(self, feature_matrix):
        with pytest.raises(KeyError):
            feature_matrix.row_as_dict("nope")

    def test_required_concepts_are_a_subset_of_targets(self):
        assert set(REQUIRED_CONCEPTS) <= set(TARGET_CONCEPTS)


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------


class TestRules:
    def test_no_rule_fires_on_a_well_formed_filing(self):
        row = build_features(_well_formed("A")).row_as_dict("A")
        assert evaluate_rules(row) == []

    def test_no_rule_fires_across_a_clean_corpus(self, feature_matrix):
        """
        The precision guard. A rule that fires on legitimate filings trains
        reviewers to dismiss the queue, which is worse than having no rule.
        """
        fired = [
            (accession, v.rule_id)
            for accession in feature_matrix.accessions
            for v in evaluate_rules(feature_matrix.row_as_dict(accession))
        ]
        assert fired == []

    def test_missing_required_fact_fires(self):
        facts = [f for f in _well_formed("A") if "Assets" not in f["fact_name"]]
        row = build_features(facts).row_as_dict("A")
        assert "missing_required_fact" in {v.rule_id for v in evaluate_rules(row)}

    def test_eps_mismatch_fires(self):
        facts = _well_formed("A", EarningsPerShareBasic=50.0)
        row = build_features(facts).row_as_dict("A")
        assert "eps_reconciliation_failed" in {v.rule_id for v in evaluate_rules(row)}

    def test_negative_assets_fires(self):
        facts = _well_formed("A", Assets=-2_000_000.0)
        row = build_features(facts).row_as_dict("A")
        assert "non_positive_scale" in {v.rule_id for v in evaluate_rules(row)}

    def test_implausible_leverage_fires(self):
        facts = _well_formed("A", Liabilities=20_000_000.0)  # 10x assets
        row = build_features(facts).row_as_dict("A")
        assert "implausible_leverage" in {v.rule_id for v in evaluate_rules(row)}

    def test_scale_error_on_revenue_fires_a_rule(self):
        facts = _well_formed("A", Revenues=1_000_000.0 * 10**6)
        row = build_features(facts).row_as_dict("A")
        assert evaluate_rules(row), "a 10^6 revenue scale error should trigger some rule"

    def test_yoy_rule_does_not_fire_on_neutral_fill(self):
        """
        0.0 means "no prior filing", not "no change". Treating the two the same
        would make every first-time filer look stable rather than unevaluable —
        harmless here, but the reverse (firing on 0.0) would flag every one.
        """
        row = dict.fromkeys(FEATURE_NAMES, 0.0)
        row.update(required_coverage=1.0, fact_coverage=1.0, log_assets=1.0, log_revenue=1.0)
        assert not any(v.rule_id.endswith("_discontinuity") for v in evaluate_rules(row))

    def test_yoy_rule_fires_on_large_change(self):
        row = dict.fromkeys(FEATURE_NAMES, 0.0)
        row.update(
            required_coverage=1.0,
            fact_coverage=1.0,
            log_assets=1.0,
            log_revenue=1.0,
            revenue_yoy_log_change=3.0,
        )
        assert "revenue_discontinuity" in {v.rule_id for v in evaluate_rules(row)}

    def test_rule_score_is_the_maximum_severity(self):
        facts = _well_formed("A", Assets=-2_000_000.0, EarningsPerShareBasic=99.0)
        row = build_features(facts).row_as_dict("A")
        violations = evaluate_rules(row)
        assert rule_score(violations) == max(v.severity for v in violations)

    def test_rule_score_zero_without_violations(self):
        assert rule_score([]) == 0.0

    def test_violations_sorted_by_severity(self):
        facts = _well_formed("A", Assets=-2_000_000.0, EarningsPerShareBasic=99.0)
        row = build_features(facts).row_as_dict("A")
        severities = [v.severity for v in evaluate_rules(row)]
        assert severities == sorted(severities, reverse=True)

    def test_missing_feature_does_not_crash_the_engine(self):
        """A rule referencing an absent feature is skipped, not fatal."""
        assert evaluate_rules({"required_coverage": 1.0}) == []

    def test_every_rule_has_a_unique_id(self):
        ids = [r.rule_id for r in RULES]
        assert len(ids) == len(set(ids))

    def test_every_rule_severity_is_a_probability(self):
        assert all(0.0 < r.severity <= 1.0 for r in RULES)

    def test_optional_fact_rule_alone_cannot_flag(self):
        """
        incomplete_fact_set is deliberately below the flag threshold: a missing
        optional concept is common and should colour a score, not create a
        review item on its own.
        """
        rule = next(r for r in RULES if r.rule_id == "incomplete_fact_set")
        assert rule.severity < 0.6
