"""
iXBRL (Inline XBRL) parser for SEC 10-K / 10-Q filings.

Deterministic extraction of machine-tagged financial facts — no model
inference, no prose parsing. This is the core extraction component of
the tagged-XBRL pipeline. Numbers embedded in narrative prose (MD&A,
footnotes, non-GAAP tables) are out of scope and belong to the
Fine-Tuned-SEC-Filing-Extraction-Pipeline repo. See docs/BOUNDARY.md.

Extracts the 7 target financial facts and returns rows that match the
``financial_facts`` table schema (see src/schema.py).

Target concepts
---------------
  us-gaap:Revenues
  us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax  (alias)
  us-gaap:NetIncomeLoss
  us-gaap:Assets
  us-gaap:Liabilities
  us-gaap:OperatingIncomeLoss
  us-gaap:EarningsPerShareBasic
  us-gaap:CommonStockSharesOutstanding

Handles
-------
  • Duration  (startDate/endDate) vs Instant contexts
  • scale attribute  (scale="6"  →  value × 10⁶)
  • sign attribute   (sign="-"   →  negate)
  • Parenthesised negatives: (123) → -123
  • Unit resolution  (ISO 4217 currency codes, shares, USD/share)
  • Segment disaggregation  (consolidated vs named segment)
  • Amendment supersession  (caller passes the latest accession — parser
    stamps every row with it so the pipeline can mark superseded rows)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from lxml import etree

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Namespace map — covers all variants seen in SEC iXBRL filings
# ---------------------------------------------------------------------------

_NSMAP: dict[str, str] = {
    "ix": "http://www.xbrl.org/2013/inlineXBRL",
    "ix11": "http://www.xbrl.org/2011/inlineXBRL",
    "xbrli": "http://www.xbrl.org/2003/instance",
    "xbrldi": "http://xbrl.org/2006/xbrldi",
    "link": "http://www.xbrl.org/2003/linkbase",
}

# ---------------------------------------------------------------------------
# Target concept set
# ---------------------------------------------------------------------------

TARGET_FACTS: frozenset[str] = frozenset(
    {
        "us-gaap:Revenues",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:NetIncomeLoss",
        "us-gaap:Assets",
        "us-gaap:Liabilities",
        "us-gaap:OperatingIncomeLoss",
        "us-gaap:EarningsPerShareBasic",
        "us-gaap:CommonStockSharesOutstanding",
    }
)


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclass
class _Context:
    """Parsed xbrli:context element."""

    context_id: str
    period_start: date | None = None
    period_end: date | None = None
    segment: str | None = None  # None means consolidated


@dataclass
class _Unit:
    """Parsed xbrli:unit element."""

    unit_id: str
    label: str  # e.g. "USD", "shares", "USD/shares"


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------


@dataclass
class FinancialFactRow:
    """One row for the ``financial_facts`` table."""

    accession_number: str
    fact_name: str
    fact_value: Decimal | None
    unit: str | None
    period_start: date | None
    period_end: date | None
    segment: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "accession_number": self.accession_number,
            "fact_name": self.fact_name,
            "fact_value": self.fact_value,
            "unit": self.unit,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "segment": self.segment,
        }


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class XBRLParser:
    """
    Parse iXBRL inline HTML and return structured financial fact rows.

    Usage::

        parser = XBRLParser()
        rows = parser.parse(html_text, accession_number="0000320193-24-000123")
        # rows → list[FinancialFactRow]
    """

    def parse(
        self,
        html_content: str | bytes,
        accession_number: str,
        *,
        target_facts: frozenset[str] = TARGET_FACTS,
        include_segments: bool = True,
    ) -> list[FinancialFactRow]:
        """
        Parse *html_content* and return matching financial fact rows.

        Args:
            html_content:     Raw iXBRL HTML (str or bytes).
            accession_number: SEC accession number; stamped on every row.
            target_facts:     Set of concept names to extract.
            include_segments: If False, skip non-consolidated (segment) rows.
        """
        if isinstance(html_content, str):
            html_bytes = html_content.encode("utf-8")
        else:
            html_bytes = html_content

        root = self._parse_tree(html_bytes)
        contexts = self._extract_contexts(root)
        units = self._extract_units(root)
        rows = self._extract_facts(
            root, contexts, units, accession_number, target_facts, include_segments
        )
        return rows

    # ------------------------------------------------------------------
    # Tree parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_tree(html_bytes: bytes) -> etree._Element:
        """
        Parse bytes into an lxml element tree.

        Tries strict XML first (preserves namespace declarations); falls
        back to HTML parser with recovery for malformed documents.
        """
        xml_parser = etree.XMLParser(recover=True, huge_tree=True, ns_clean=True)
        try:
            return etree.fromstring(html_bytes, xml_parser)
        except etree.XMLSyntaxError:
            html_parser = etree.HTMLParser(recover=True, huge_tree=True)
            return etree.fromstring(html_bytes, html_parser)

    # ------------------------------------------------------------------
    # Context extraction
    # ------------------------------------------------------------------

    def _extract_contexts(self, root: etree._Element) -> dict[str, _Context]:
        """Build a mapping of contextRef → _Context from all xbrli:context elements."""
        contexts: dict[str, _Context] = {}

        # Try namespace-aware XPath first, then fall back to local-name()
        elems = root.xpath("//xbrli:context", namespaces=_NSMAP) or root.xpath(
            "//*[local-name()='context']"
        )

        for ctx_elem in elems:
            ctx_id = ctx_elem.get("id")
            if not ctx_id:
                continue

            ctx = _Context(context_id=ctx_id)

            # Period
            period = _first(ctx_elem, "xbrli:period", "period")
            if period is not None:
                instant = _first(period, "xbrli:instant", "instant")
                if instant is not None and instant.text:
                    d = _parse_date(instant.text.strip())
                    ctx.period_end = d
                    ctx.period_start = None
                else:
                    start = _first(period, "xbrli:startDate", "startDate")
                    end = _first(period, "xbrli:endDate", "endDate")
                    if start is not None and start.text:
                        ctx.period_start = _parse_date(start.text.strip())
                    if end is not None and end.text:
                        ctx.period_end = _parse_date(end.text.strip())

            # Segment — lives inside xbrli:entity, so needs descendant search
            seg_results = ctx_elem.xpath(
                ".//xbrli:segment", namespaces=_NSMAP
            ) or ctx_elem.xpath(".//*[local-name()='segment']")
            segment_elem = seg_results[0] if seg_results else None
            if segment_elem is not None:
                members = segment_elem.xpath(
                    "xbrldi:explicitMember", namespaces=_NSMAP
                ) or segment_elem.xpath(".//*[local-name()='explicitMember']")
                if members:
                    ctx.segment = "; ".join(
                        m.text.strip() for m in members if m.text
                    )

            contexts[ctx_id] = ctx

        logger.debug("Extracted %d contexts", len(contexts))
        return contexts

    # ------------------------------------------------------------------
    # Unit extraction
    # ------------------------------------------------------------------

    def _extract_units(self, root: etree._Element) -> dict[str, _Unit]:
        """Build a mapping of unitRef → _Unit from all xbrli:unit elements."""
        units: dict[str, _Unit] = {}

        elems = root.xpath("//xbrli:unit", namespaces=_NSMAP) or root.xpath(
            "//*[local-name()='unit']"
        )

        for unit_elem in elems:
            unit_id = unit_elem.get("id")
            if not unit_id:
                continue

            divide = _first(unit_elem, "xbrli:divide", "divide")
            if divide is not None:
                # Ratio unit, e.g. USD/shares
                # _measure_label expects the *parent* of the <xbrli:measure> element
                num = _first(divide, "xbrli:unitNumerator", "unitNumerator")
                den = _first(divide, "xbrli:unitDenominator", "unitDenominator")
                num_label = _measure_label(num) if num is not None else ""
                den_label = _measure_label(den) if den is not None else ""
                label = f"{num_label}/{den_label}" if den_label else num_label
            else:
                # Simple unit: pass unit_elem itself so _measure_label can find
                # the <xbrli:measure> child (not the measure element itself)
                label = _measure_label(unit_elem)
                if not label:
                    label = unit_id

            units[unit_id] = _Unit(unit_id=unit_id, label=label)

        logger.debug("Extracted %d units", len(units))
        return units

    # ------------------------------------------------------------------
    # Fact extraction
    # ------------------------------------------------------------------

    def _extract_facts(
        self,
        root: etree._Element,
        contexts: dict[str, _Context],
        units: dict[str, _Unit],
        accession_number: str,
        target_facts: frozenset[str],
        include_segments: bool,
    ) -> list[FinancialFactRow]:
        """Extract inline XBRL numeric facts matching *target_facts*."""
        rows: list[FinancialFactRow] = []

        # Find all ix:nonFraction elements (namespace-aware + fallback)
        fact_elems = root.xpath(
            "//ix:nonFraction", namespaces=_NSMAP
        ) or root.xpath("//*[local-name()='nonFraction']")

        for elem in fact_elems:
            name = elem.get("name", "")
            if name not in target_facts:
                continue

            ctx_ref = elem.get("contextRef", "")
            unit_ref = elem.get("unitRef", "")
            ctx = contexts.get(ctx_ref)
            unit = units.get(unit_ref)

            if ctx is None:
                logger.warning("Unknown contextRef=%r for fact %s — skipping", ctx_ref, name)
                continue

            if not include_segments and ctx.segment is not None:
                continue

            value = _parse_fact_value(elem)
            unit_label = unit.label if unit else unit_ref or None

            rows.append(
                FinancialFactRow(
                    accession_number=accession_number,
                    fact_name=name,
                    fact_value=value,
                    unit=unit_label,
                    period_start=ctx.period_start,
                    period_end=ctx.period_end,
                    segment=ctx.segment,
                )
            )

        logger.debug("Extracted %d fact rows from accession %s", len(rows), accession_number)
        return rows


# ---------------------------------------------------------------------------
# Amendment supersession helper
# ---------------------------------------------------------------------------


def apply_amendment_supersession(
    rows: list[FinancialFactRow],
    latest_accession: str,
) -> list[FinancialFactRow]:
    """
    Given rows from multiple accession numbers, keep only the row from the
    *latest_accession* when multiple rows share the same
    (fact_name, period_end, segment) key.

    In practice, the Airflow pipeline resolves amendment order before calling
    the parser, so *latest_accession* is the canonical version.  This helper
    is useful when facts from an original filing and its amendment are merged
    in memory before loading.
    """
    canonical: dict[tuple, FinancialFactRow] = {}
    for row in rows:
        key = (row.fact_name, row.period_end, row.segment)
        existing = canonical.get(key)
        if existing is None:
            canonical[key] = row
        elif row.accession_number == latest_accession:
            # Prefer the latest amendment
            canonical[key] = row
    return list(canonical.values())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _first(
    parent: etree._Element, ns_name: str, local_name: str
) -> etree._Element | None:
    """Return the first child matching namespace-qualified or local-name."""
    results = parent.xpath(ns_name, namespaces=_NSMAP) or parent.xpath(
        f"*[local-name()='{local_name}']"
    )
    return results[0] if results else None


def _parse_date(text: str) -> date | None:
    """Parse an ISO 8601 date string (YYYY-MM-DD)."""
    try:
        from datetime import date as _date

        parts = text.strip().split("-")
        return _date(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError):
        return None


def _measure_label(parent: etree._Element | None) -> str:
    """
    Extract the unit label from a ``<xbrli:measure>`` parent element.

    ``iso4217:USD`` → ``USD``
    ``xbrli:shares`` → ``shares``
    """
    if parent is None:
        return ""
    measure = _first(parent, "xbrli:measure", "measure")
    if measure is None:
        return ""
    text = (measure.text or "").strip()
    # Strip namespace prefix (iso4217:, xbrli:, etc.)
    return text.split(":")[-1] if ":" in text else text


def _parse_fact_value(elem: etree._Element) -> Decimal | None:
    """
    Convert the element's text to a Decimal, applying scale and sign.

    scale  attribute: multiply by 10^scale
    sign   attribute: "-" negates
    Text may contain commas, spaces, or be wrapped in parentheses (negative).
    """
    raw = (elem.text or "").strip()
    # Remove thousands separators / whitespace
    raw = re.sub(r"[\s,]", "", raw)
    # Parentheses denote negative
    if raw.startswith("(") and raw.endswith(")"):
        raw = "-" + raw[1:-1]
    if not raw:
        return None

    try:
        value = Decimal(raw)
    except InvalidOperation:
        logger.warning("Cannot parse fact value %r from element %s", raw, elem.get("name"))
        return None

    scale_attr = elem.get("scale")
    if scale_attr:
        try:
            value = value * Decimal(10) ** int(scale_attr)
        except (ValueError, InvalidOperation):
            pass

    sign_attr = elem.get("sign")
    if sign_attr == "-":
        value = -value

    return value
