"""
TF-IDF retrieval over filing chunks for the lookup aid.

Part of a read-only filing-text search tool (``/ask/{ticker}``). This
is NOT an extraction component — it retrieves existing filing prose for
human review, it does not extract structured facts or write to any
table. See ``docs/BOUNDARY.md`` for the scope contract.

Deterministic and fully offline: no embedding API call or model
download is required, which keeps retrieval reproducible in CI and
free to run at portfolio scale. See AGENTS.md for why TF-IDF was chosen
over a neural embedding index for this project's scope -- the corpus
size here doesn't need it, and it keeps the retrieval step testable
without network access or GPU/torch dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass

from sklearn.feature_extraction import text as sk_text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from src.rag.chunker import Chunk

# SEC filing boilerplate words ("fiscal 2024", "the Company", "this Item")
# occur in nearly every chunk of nearly every filing and carry almost no
# topical signal, but standard English stopword lists don't know that.
# Left in, they dominate cosine similarity for out-of-scope queries that
# happen to mention a fiscal year or say "the company" -- see
# AGENTS.md "Known failure modes" for the concrete case this fixes.
_FILING_BOILERPLATE_STOPWORDS = frozenset(
    {"fiscal", "company", "companys", "item", "year", "quarter", "quarterly", "annual", "corporation"}
)
_STOP_WORDS = frozenset(sk_text.ENGLISH_STOP_WORDS) | _FILING_BOILERPLATE_STOPWORDS


@dataclass(frozen=True)
class ScoredChunk:
    chunk: Chunk
    score: float


class FilingRetriever:
    """A TF-IDF index over a fixed set of chunks, queryable by free text."""

    def __init__(self, chunks: list[Chunk]) -> None:
        if not chunks:
            raise ValueError("FilingRetriever requires at least one chunk")
        self._chunks = chunks
        # No max_df filtering: as a *fraction* it breaks on tiny corpora
        # (sklearn requires max_df * n_docs >= min_df, which fails for a
        # single-chunk corpus -- e.g. a ticker with only one filing
        # ingested so far). Common filing boilerplate is already handled
        # explicitly via _FILING_BOILERPLATE_STOPWORDS instead.
        self._vectorizer = TfidfVectorizer(
            ngram_range=(1, 2),
            stop_words=list(_STOP_WORDS),
            min_df=1,
        )
        self._matrix = self._vectorizer.fit_transform(c.text for c in chunks)

    def query(self, question: str, top_k: int = 5) -> list[ScoredChunk]:
        """Return the *top_k* chunks most similar to *question*, highest score first."""
        query_vec = self._vectorizer.transform([question])
        scores = cosine_similarity(query_vec, self._matrix)[0]
        ranked = sorted(zip(self._chunks, scores), key=lambda pair: pair[1], reverse=True)
        return [ScoredChunk(chunk=c, score=float(s)) for c, s in ranked[:top_k]]
