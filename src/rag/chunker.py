"""
Filing text chunking for the lookup aid.

Part of a read-only filing-text search tool (``/ask/{ticker}``) that
helps users find and quote relevant passages. This is NOT an extraction
component: it does not produce structured facts, does not write to
``financial_facts``, and is not the narrative extraction system owned by
the Fine-Tuned-SEC-Filing-Extraction-Pipeline repo. See
``docs/BOUNDARY.md`` for the scope contract.

Splits a raw filing HTML/text document into overlapping, metadata-tagged
chunks suitable for indexing. Every chunk carries enough provenance
(accession number, section, source URL) that a retrieved chunk can be
traced back to an exact section of a specific filing -- this is what
makes the citations in ``src/rag/qa.py`` checkable rather than just
plausible-sounding.

Section detection is heuristic: SEC 10-K/10-Q filings conventionally use
"Item N." headings (Item 1. Business, Item 1A. Risk Factors, Item 7.
MD&A, ...). Filings that don't match this pattern fall back to a single
"Document" section rather than failing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator

from lxml import etree

_ITEM_HEADING_RE = re.compile(
    r"(Item\s+\d+[A-Z]?\.?\s*[-\u2013\u2014]?\s*[A-Za-z][^\n]{0,120})",
    re.IGNORECASE,
)

DEFAULT_CHUNK_CHARS = 1200
DEFAULT_OVERLAP_CHARS = 200


@dataclass(frozen=True)
class Chunk:
    """One retrievable unit of filing text plus provenance metadata."""

    chunk_id: str
    accession_number: str
    cik: str
    ticker: str | None
    form_type: str
    section: str
    text: str
    source_url: str | None
    chunk_index: int


def _strip_html(raw: str) -> str:
    """Best-effort HTML -> plain text, tolerant of malformed markup."""
    try:
        tree = etree.fromstring(raw.encode("utf-8"), parser=etree.HTMLParser())
        text = "".join(tree.itertext())
    except Exception:
        text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def _split_into_sections(text: str) -> list[tuple[str, str]]:
    """Split *text* into (section_label, section_body) pairs on 'Item N.' headings."""
    matches = list(_ITEM_HEADING_RE.finditer(text))
    if not matches:
        return [("Document", text)]

    sections: list[tuple[str, str]] = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        label = re.sub(r"\s+", " ", m.group(1)).strip()
        body = text[start:end].strip()
        if body:
            sections.append((label, body))
    return sections


def _split_into_windows(body: str, max_chars: int, overlap_chars: int) -> Iterator[str]:
    if len(body) <= max_chars:
        yield body
        return
    start = 0
    while start < len(body):
        end = min(start + max_chars, len(body))
        yield body[start:end]
        if end == len(body):
            break
        start = end - overlap_chars


def chunk_document(
    raw_html: str,
    *,
    accession_number: str,
    cik: str,
    form_type: str,
    ticker: str | None = None,
    source_url: str | None = None,
    max_chars: int = DEFAULT_CHUNK_CHARS,
    overlap_chars: int = DEFAULT_OVERLAP_CHARS,
) -> list[Chunk]:
    """
    Parse *raw_html* into provenance-tagged, retrieval-sized chunks.

    Sections are detected heuristically via "Item N." headings; each
    section is then split into overlapping fixed-size windows so no
    single chunk is too large for the retriever's similarity comparison.
    """
    plain_text = _strip_html(raw_html)
    chunks: list[Chunk] = []
    index = 0
    for section_label, body in _split_into_sections(plain_text):
        for window in _split_into_windows(body, max_chars, overlap_chars):
            chunks.append(
                Chunk(
                    chunk_id=f"{accession_number}:{index}",
                    accession_number=accession_number,
                    cik=cik,
                    ticker=ticker,
                    form_type=form_type,
                    section=section_label,
                    text=window,
                    source_url=source_url,
                    chunk_index=index,
                )
            )
            index += 1
    return chunks
