from datetime import datetime

import pytest

from src.xbrl_parser import FinancialFactRow, XBRLParser


class TestXBRLParser:
    @pytest.fixture
    def parser(self):
        return XBRLParser()

    def test_parse_basic_html(self, parser, sample_ixbrl_html):
        facts = parser.parse(sample_ixbrl_html, "0000320193-23-000077")
        assert isinstance(facts, list)

    def test_parse_empty_html(self, parser):
        facts = parser.parse("", "0000320193-23-000077")
        assert facts == []

    def test_parse_invalid_html(self, parser):
        invalid_html = "not valid html at all"
        facts = parser.parse(invalid_html, "0000320193-23-000077")
        assert isinstance(facts, list)

    def test_extract_fact_name(self, parser):
        from lxml import etree

        elem = etree.Element("Revenues")
        elem.set("name", "us-gaap:Revenues")
        fact_name = parser._extract_fact_name(elem)
        assert "Revenues" in fact_name

    def test_extract_value_positive(self, parser):
        from lxml import etree

        elem = etree.Element("span")
        elem.text = "123456789"
        value = parser._extract_value(elem)
        assert value == 123456789.0

    def test_extract_value_negative_parentheses(self, parser):
        from lxml import etree

        elem = etree.Element("span")
        elem.text = "(123456789)"
        value = parser._extract_value(elem)
        assert value == -123456789.0

    def test_extract_value_with_commas(self, parser):
        from lxml import etree

        elem = etree.Element("span")
        elem.text = "123,456,789"
        value = parser._extract_value(elem)
        assert value == 123456789.0

    def test_extract_value_invalid(self, parser):
        from lxml import etree

        elem = etree.Element("span")
        elem.text = "not a number"
        value = parser._extract_value(elem)
        assert value is None

    def test_extract_unit(self, parser):
        from lxml import etree

        elem = etree.Element("span")
        elem.set("unitRef", "USD")
        unit = parser._extract_unit(elem)
        assert "USD" in unit

    def test_extract_unit_with_scale(self, parser):
        from lxml import etree

        elem = etree.Element("span")
        elem.set("unitRef", "USD")
        elem.set("scale", "6")
        unit = parser._extract_unit(elem)
        assert "1000000" in unit

    def test_extract_periods(self, parser):
        from lxml import etree

        elem = etree.Element("span")
        elem.set("contextRef", "FY2023-2023-12-31")
        start_date, end_date = parser._extract_periods(elem)
        assert isinstance(end_date, (datetime, type(None)))

    def test_extract_segment(self, parser):
        from lxml import etree

        elem = etree.Element("span")
        elem.set("segmentRef", "Products_Segment")
        segment = parser._extract_segment(elem)
        assert segment == "Products Segment"

    def test_apply_amendment_supersession(self, parser):
        rows = [
            FinancialFactRow("Revenues", "USD", None, datetime(2023, 12, 31), 100.0, "Total"),
            FinancialFactRow("Revenues", "USD", None, datetime(2023, 12, 31), 110.0, "Total"),
        ]
        deduplicated = parser.apply_amendment_supersession(rows, "latest")
        assert len(deduplicated) == 1
        assert deduplicated[0].value == 110.0
