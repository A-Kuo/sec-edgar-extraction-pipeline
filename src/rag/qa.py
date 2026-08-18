"""
Citation-first answer construction for the filing-text lookup aid.

Part of a read-only filing-text search tool (``/ask/{ticker}``). This
is NOT an extraction component — it quotes existing filing prose for
human review, it does not extract structured facts or write to any
table. See ``docs/BOUNDARY.md`` for the scope contract.

Design intent
-------------
Every answer must be traceable to specific retrieved text. There is no
"trust me" mode: if retrieval doesn't surface anything relevant enough,
the answer says so instead of guessing. For a regulatory/compliance
domain, an ungrounded answer is worse than no answer.

Two answer paths:
  1. Extractive (default, always available) -- deterministic, offline,
     quotes directly from the top retrieved chunk. This is what tests
     and CI exercise, since it requires no external service.
  2. LLM-synthesized (optional) -- if ``OPENAI_API_KEY`` is set, asks a
     model to phrase the answer in prose, constrained to only the
     retrieved chunk text. Falls back to (1) on any error or missing
     key, so the feature never requires network access to function.
     The LLM only phrases the answer; it never extracts or originates
     facts.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass

from src.rag.retrieval import FilingRetriever, ScoredChunk

logger = logging.getLogger(__name__)

MIN_GROUNDING_SCORE = 0.15  # below this, retrieval is treated as "no support found"
REFUSAL_TEXT = "I do not find support for this in the supplied filings."

_WORD_RE = re.compile(r"[a-z0-9]+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


@dataclass(frozen=True)
class Citation:
    accession_number: str
    section: str
    snippet: str
    source_url: str | None
    score: float


@dataclass(frozen=True)
class Answer:
    question: str
    answer_text: str
    citations: list[Citation]
    grounded: bool


def _snippet(text: str, max_chars: int = 280) -> str:
    text = " ".join(text.split())
    return text if len(text) <= max_chars else text[: max_chars - 1].rstrip() + "\u2026"


def _best_sentence_window(chunk_text: str, question: str) -> str:
    """
    Return *chunk_text* starting from the sentence most relevant to
    *question*, rather than always the chunk's first sentence.

    A chunk is one whole filing section (heading + several sentences);
    the specific fact a question asks about is rarely the first
    sentence. Without this, ``_snippet``'s character truncation would
    show the section heading and opening boilerplate while cutting off
    exactly the sentence that supports the answer -- a citation that
    doesn't actually contain the cited claim.
    """
    sentences = [s.strip() for s in _SENTENCE_SPLIT_RE.split(chunk_text) if s.strip()]
    if len(sentences) <= 1:
        return chunk_text

    query_terms = {w for w in _WORD_RE.findall(question.lower()) if len(w) > 2}
    if not query_terms:
        return chunk_text

    def overlap(sentence: str) -> int:
        return len(query_terms & set(_WORD_RE.findall(sentence.lower())))

    scores = [overlap(s) for s in sentences]
    if not any(scores):
        return chunk_text

    best_idx = max(range(len(sentences)), key=lambda i: scores[i])
    return " ".join(sentences[best_idx:])


def _extractive_answer(question: str, top: list[ScoredChunk]) -> str:
    lead = top[0].chunk
    window = _best_sentence_window(lead.text, question)
    return f"Based on {lead.form_type} {lead.accession_number} ({lead.section}): {_snippet(window, 400)}"


def _try_llm_synthesis(question: str, top: list[ScoredChunk]) -> str | None:
    """
    Best-effort LLM paraphrase constrained to retrieved text.

    Never raises: returns None if no API key is configured or the call
    fails for any reason, so callers always have the extractive
    fallback available and tests never depend on network access.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        import requests

        context = "\n\n".join(
            f"[{c.chunk.accession_number} / {c.chunk.section}] {c.chunk.text}" for c in top
        )
        prompt = (
            "Answer the question using ONLY the filing excerpts below. "
            "If the excerpts do not contain the answer, say so explicitly.\n\n"
            f"Excerpts:\n{context}\n\nQuestion: {question}\nAnswer:"
        )
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": os.getenv("RAG_LLM_MODEL", "gpt-4o-mini"),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        logger.warning("LLM synthesis unavailable, falling back to extractive answer: %s", exc)
        return None


def answer_question(
    question: str,
    retriever: FilingRetriever,
    *,
    top_k: int = 5,
    min_score: float = MIN_GROUNDING_SCORE,
    use_llm: bool = True,
) -> Answer:
    """
    Retrieve the most relevant chunks for *question* and construct a
    citation-first answer.

    Returns a refusal (``grounded=False``, no citations) when the best
    retrieval score falls below *min_score* -- i.e. nothing in the
    indexed filings is actually relevant to the question. This is the
    safety behavior that distinguishes a grounded research tool from a
    chatbot that always produces a confident-sounding answer.
    """
    ranked = retriever.query(question, top_k=top_k)

    if not ranked or ranked[0].score < min_score:
        return Answer(question=question, answer_text=REFUSAL_TEXT, citations=[], grounded=False)

    relevant = [r for r in ranked if r.score >= min_score] or ranked[:1]

    answer_text = _try_llm_synthesis(question, relevant) if use_llm else None
    if answer_text is None:
        answer_text = _extractive_answer(question, relevant)

    citations = [
        Citation(
            accession_number=r.chunk.accession_number,
            section=r.chunk.section,
            snippet=_snippet(_best_sentence_window(r.chunk.text, question)),
            source_url=r.chunk.source_url,
            score=round(r.score, 4),
        )
        for r in relevant
    ]

    return Answer(question=question, answer_text=answer_text, citations=citations, grounded=True)
