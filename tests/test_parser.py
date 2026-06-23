"""
Tests for src/xbrl_parser.py

Exercises:
  - Correct extraction of all 7 target facts from the iXBRL fixture
  - Duration vs instant context period handling
  - scale attribute conversion (millions, thousands, no scale)
  - Segment disaggregation
  - Revenue alias concept (RevenueFromContract…)
  - Amendment supersession helper
  - Parsing robustness (parenthesised negatives, missing context)
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from src.xbrl_parser import (
    TARGET_FACTS,
    XBRLParser,
    apply_amendment_supersession,
    _parse_fact_value,
    _parse_date,
)
from lxml import etree


FIXTURES_DIR = Path(__file__).parent / "fixtures"
ACCESSION = "0000320193-24-000123"


@pytest.fixture(scope="module")
def parser() -> XBRLParser:
    return XBRLParser()


@pytest.fixture(scope="module")
def fixture_html() -> str:
    return (FIXTURES_DIR / "sample_ixbrl.html").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def parsed_rows(parser, fixture_html):
    return parser.parse(fixture_html, ACCESSION, include_segments=True)


@pytest.fixture(scope="module")
def consolidated_rows(parser, fixture_html):
    return parser.parse(fixture_html, ACCESSION, include_segments=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_fact(rows, name, segment=None):
    return next(
        (r for r in rows if r.fact_name == name and r.segment == segment),
        None,
    )


# ---------------------------------------------------------------------------
# 1. All target facts are extracted
# ---------------------------------------------------------------------------

class TestFactExtraction:
    def test_revenues_extracted(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:Revenues")
        assert row is not None, "us-gaap:Revenues not found"

    def test_net_income_extracted(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:NetIncomeLoss")
        assert row is not None

    def test_assets_extracted(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:Assets")
        assert row is not None

    def test_liabilities_extracted(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:Liabilities")
        assert row is not None

    def test_operating_income_extracted(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:OperatingIncomeLoss")
        assert row is not None

    def test_eps_basic_extracted(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:EarningsPerShareBasic")
        assert row is not None

    def test_shares_outstanding_extracted(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:CommonStockSharesOutstanding")
        assert row is not None

    def test_revenue_alias_extracted(self, parsed_rows):
        """RevenueFromContract… alias should also be extracted."""
        row = get_fact(
            parsed_rows,
            "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        )
        assert row is not None

    def test_all_rows_have_accession_number(self, parsed_rows):
        for row in parsed_rows:
            assert row.accession_number == ACCESSION


# ---------------------------------------------------------------------------
# 2. Scale conversion (millions and thousands)
# ---------------------------------------------------------------------------

class TestScaleConversion:
    def test_revenues_millions_scale(self, parsed_rows):
        """scale="6" on value 391,035 → 391,035,000,000"""
        row = get_fact(parsed_rows, "us-gaap:Revenues")
        assert row.fact_value == Decimal("391035000000")

    def test_assets_millions_scale(self, parsed_rows):
        """scale="6" on value 364,980 → 364,980,000,000"""
        row = get_fact(parsed_rows, "us-gaap:Assets")
        assert row.fact_value == Decimal("364980000000")

    def test_shares_thousands_scale(self, parsed_rows):
        """scale="3" on value 15,115,823 → 15,115,823,000"""
        row = get_fact(parsed_rows, "us-gaap:CommonStockSharesOutstanding")
        assert row.fact_value == Decimal("15115823000")

    def test_eps_no_scale(self, parsed_rows):
        """EPS has no scale attribute — value should be as printed."""
        row = get_fact(parsed_rows, "us-gaap:EarningsPerShareBasic")
        assert row.fact_value == Decimal("6.11")


# ---------------------------------------------------------------------------
# 3. Period handling: duration vs instant
# ---------------------------------------------------------------------------

class TestPeriodHandling:
    def test_revenues_is_duration(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:Revenues")
        assert row.period_start == date(2023, 10, 1)
        assert row.period_end == date(2024, 9, 28)

    def test_assets_is_instant(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:Assets")
        assert row.period_start is None, "Instant context should have no start date"
        assert row.period_end == date(2024, 9, 28)

    def test_liabilities_is_instant(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:Liabilities")
        assert row.period_start is None
        assert row.period_end is not None

    def test_net_income_duration_dates(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:NetIncomeLoss")
        assert row.period_start is not None
        assert row.period_end is not None
        assert row.period_start < row.period_end


# ---------------------------------------------------------------------------
# 4. Segment disaggregation
# ---------------------------------------------------------------------------

class TestSegmentHandling:
    def test_segment_row_present_when_include_segments_true(self, parsed_rows):
        segment_rows = [r for r in parsed_rows if r.segment is not None]
        assert len(segment_rows) > 0, "Expected at least one segment row"

    def test_segment_row_excluded_when_include_segments_false(self, consolidated_rows):
        for row in consolidated_rows:
            assert row.segment is None, f"Unexpected segment row: {row}"

    def test_products_revenue_segment_value(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:Revenues", segment="aapl:ProductsMember")
        assert row is not None
        assert row.fact_value == Decimal("294866000000")

    def test_consolidated_revenue_differs_from_segment(self, parsed_rows):
        consolidated = get_fact(parsed_rows, "us-gaap:Revenues", segment=None)
        segment = get_fact(parsed_rows, "us-gaap:Revenues", segment="aapl:ProductsMember")
        assert consolidated is not None
        assert segment is not None
        assert consolidated.fact_value != segment.fact_value


# ---------------------------------------------------------------------------
# 5. Unit labels
# ---------------------------------------------------------------------------

class TestUnitLabels:
    def test_monetary_unit_is_usd(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:Revenues")
        assert row.unit == "USD"

    def test_shares_unit(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:CommonStockSharesOutstanding")
        assert row.unit == "shares"

    def test_eps_unit_is_ratio(self, parsed_rows):
        row = get_fact(parsed_rows, "us-gaap:EarningsPerShareBasic")
        # USDPerShare divide unit → "USD/shares"
        assert row.unit is not None
        assert "USD" in row.unit


# ---------------------------------------------------------------------------
# 6. Amendment supersession helper
# ---------------------------------------------------------------------------

class TestAmendmentSupersession:
    def _make_row(self, acc, fact_name, value, period_end=None, segment=None):
        from src.xbrl_parser import FinancialFactRow
        return FinancialFactRow(
            accession_number=acc,
            fact_name=fact_name,
            fact_value=Decimal(str(value)),
            unit="USD",
            period_start=None,
            period_end=period_end or date(2024, 9, 28),
            segment=segment,
        )

    def test_latest_accession_wins(self):
        original = self._make_row("0000-24-000100", "us-gaap:Revenues", 100)
        amendment = self._make_row("0000-24-000200", "us-gaap:Revenues", 200)
        result = apply_amendment_supersession(
            [original, amendment], latest_accession="0000-24-000200"
        )
        assert len(result) == 1
        assert result[0].fact_value == Decimal("200")

    def test_original_kept_when_no_amendment(self):
        original = self._make_row("0000-24-000100", "us-gaap:Revenues", 100)
        result = apply_amendment_supersession(
            [original], latest_accession="0000-24-000100"
        )
        assert len(result) == 1
        assert result[0].fact_value == Decimal("100")

    def test_different_facts_both_kept(self):
        rev = self._make_row("0000-24-000100", "us-gaap:Revenues", 100)
        ni = self._make_row("0000-24-000100", "us-gaap:NetIncomeLoss", 50)
        result = apply_amendment_supersession([rev, ni], latest_accession="0000-24-000100")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# 7. Low-level helpers
# ---------------------------------------------------------------------------

class TestParseHelpers:
    def test_parse_date_valid(self):
        d = _parse_date("2024-09-28")
        assert d == date(2024, 9, 28)

    def test_parse_date_invalid_returns_none(self):
        assert _parse_date("not-a-date") is None

    def test_parse_fact_value_with_commas(self):
        xml = b'<ix:nonFraction xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" name="x" scale="6">391,035</ix:nonFraction>'
        elem = etree.fromstring(xml)
        val = _parse_fact_value(elem)
        assert val == Decimal("391035000000")

    def test_parse_fact_value_parentheses_negative(self):
        xml = b'<ix:nonFraction xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" name="x">(42)</ix:nonFraction>'
        elem = etree.fromstring(xml)
        val = _parse_fact_value(elem)
        assert val == Decimal("-42")

    def test_parse_fact_value_sign_attribute(self):
        xml = b'<ix:nonFraction xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" name="x" sign="-">100</ix:nonFraction>'
        elem = etree.fromstring(xml)
        val = _parse_fact_value(elem)
        assert val == Decimal("-100")

    def test_parse_fact_value_empty_returns_none(self):
        xml = b'<ix:nonFraction xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" name="x"></ix:nonFraction>'
        elem = etree.fromstring(xml)
        assert _parse_fact_value(elem) is None

    def test_parse_bytes_input(self, fixture_html):
        parser = XBRLParser()
        rows = parser.parse(fixture_html.encode("utf-8"), ACCESSION)
        assert len(rows) > 0
