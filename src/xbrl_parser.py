import logging
import re
from dataclasses import dataclass
from datetime import datetime

from lxml import etree

logger = logging.getLogger(__name__)

TARGET_CONCEPTS = {
    "Revenues",
    "RevenueFromContract",
    "NetIncomeLoss",
    "Assets",
    "Liabilities",
    "OperatingIncomeLoss",
    "EarningsPerShareBasic",
    "CommonStockSharesOutstanding",
}

SCALE_MULTIPLIER = {
    "USD": 1.0,
    "shares": 1.0,
    "pure": 1.0,
    "USD/shares": 1.0,
}


@dataclass
class FinancialFactRow:
    fact_name: str
    unit: str
    period_start: datetime
    period_end: datetime
    value: float
    segment: str


class XBRLParser:
    def parse(self, html: str, accession_number: str) -> list[FinancialFactRow]:
        facts = []
        try:
            root = etree.HTML(html)
            if root is None:
                logger.warning(f"Failed to parse HTML for {accession_number}")
                return facts

            elements = root.xpath(
                '//*[contains(local-name(), "Revenues") or contains(local-name(), "NetIncome") or contains(local-name(), "Assets")]'
            )

            for elem in elements:
                try:
                    fact_name = self._extract_fact_name(elem)
                    if fact_name not in TARGET_CONCEPTS:
                        continue

                    value = self._extract_value(elem)
                    if value is None:
                        continue

                    unit = self._extract_unit(elem)
                    period_start, period_end = self._extract_periods(elem)
                    segment = self._extract_segment(elem)

                    facts.append(
                        FinancialFactRow(
                            fact_name=fact_name,
                            unit=unit,
                            period_start=period_start,
                            period_end=period_end,
                            value=value,
                            segment=segment or "Total",
                        )
                    )
                except Exception as e:
                    logger.debug(f"Failed to parse element: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to parse XBRL for {accession_number}: {e}")

        return facts

    @staticmethod
    def _extract_fact_name(elem) -> str:
        name = elem.get("name") or elem.tag
        match = re.search(r"([A-Z][a-zA-Z]+)$", name)
        return match.group(1) if match else name

    @staticmethod
    def _extract_value(elem) -> float:
        text = (elem.text or "").strip()
        if not text:
            return None

        text = text.replace(",", "")
        is_negative = text.startswith("(") and text.endswith(")")
        text = text.strip("()")

        try:
            value = float(text)
            return -value if is_negative else value
        except ValueError:
            return None

    @staticmethod
    def _extract_unit(elem) -> str:
        unit = elem.get("unitRef", "USD")
        scale = elem.get("scale")
        if scale:
            try:
                multiplier = 10 ** int(scale)
                return f"{unit}*{multiplier}"
            except ValueError:
                pass
        return unit

    @staticmethod
    def _extract_periods(elem) -> tuple[datetime, datetime]:
        context_ref = elem.get("contextRef", "")
        start_date = None
        end_date = None

        period_pattern = r"(\d{4})-(\d{2})-(\d{2})"
        matches = re.findall(period_pattern, context_ref)

        if len(matches) >= 2:
            try:
                start_date = datetime(*map(int, matches[0]))
                end_date = datetime(*map(int, matches[1]))
            except ValueError:
                pass
        elif len(matches) == 1:
            try:
                end_date = datetime(*map(int, matches[0]))
            except ValueError:
                pass

        return start_date, end_date

    @staticmethod
    def _extract_segment(elem) -> str:
        segment_ref = elem.get("segmentRef", "")
        if segment_ref:
            return segment_ref.replace("_", " ")
        return None

    @staticmethod
    def apply_amendment_supersession(
        rows: list[FinancialFactRow], latest_accession: str
    ) -> list[FinancialFactRow]:
        seen = {}
        for row in rows:
            key = (row.fact_name, row.period_end, row.segment)
            if key not in seen:
                seen[key] = row
        return list(seen.values())
