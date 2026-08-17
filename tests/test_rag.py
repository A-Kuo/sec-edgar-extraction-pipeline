"""
Tests for the retrieval + citation-grounded QA layer (src/rag/).

Uses two narrative filing fixtures (a 10-K and a 10-Q for the same
issuer, distinct accessions and content) to exercise cross-document
retrieval, not just single-document sanity checks.
"""

from __future__ import annotations

import pytest

from src.rag.chunker import chunk_document
from src.rag.evaluation import EvalCase, evaluate
from src.rag.qa import REFUSAL_TEXT, answer_question
from src.rag.retrieval import FilingRetriever

ACCESSION_10K = "0000320193-24-000123"
ACCESSION_10Q = "0000320193-23-000077"


@pytest.fixture()
def corpus_chunks(sample_narrative_10k_html, sample_narrative_10q_html):
    chunks = chunk_document(
        sample_narrative_10k_html,
        accession_number=ACCESSION_10K,
        cik="320193",
        ticker="AAPL",
        form_type="10-K",
        source_url=f"https://www.sec.gov/Archives/edgar/data/320193/{ACCESSION_10K}",
    )
    chunks += chunk_document(
        sample_narrative_10q_html,
        accession_number=ACCESSION_10Q,
        cik="320193",
        ticker="AAPL",
        form_type="10-Q",
        source_url=f"https://www.sec.gov/Archives/edgar/data/320193/{ACCESSION_10Q}",
    )
    return chunks


@pytest.fixture()
def retriever(corpus_chunks) -> FilingRetriever:
    return FilingRetriever(corpus_chunks)


# ---------------------------------------------------------------------------
# Chunker
# ---------------------------------------------------------------------------


class TestChunker:
    def test_detects_item_headings_as_sections(self, sample_narrative_10k_html):
        chunks = chunk_document(
            sample_narrative_10k_html,
            accession_number=ACCESSION_10K,
            cik="320193",
            form_type="10-K",
        )
        sections = {c.section for c in chunks}
        assert any(s.lower().startswith("item 1a") for s in sections)
        assert any(s.lower().startswith("item 7.") or s.lower().startswith("item 7 ") for s in sections)

    def test_every_chunk_carries_accession_and_form_type(self, sample_narrative_10k_html):
        chunks = chunk_document(
            sample_narrative_10k_html,
            accession_number=ACCESSION_10K,
            cik="320193",
            form_type="10-K",
        )
        assert chunks
        assert all(c.accession_number == ACCESSION_10K for c in chunks)
        assert all(c.form_type == "10-K" for c in chunks)

    def test_strips_html_tags(self, sample_narrative_10k_html):
        chunks = chunk_document(
            sample_narrative_10k_html,
            accession_number=ACCESSION_10K,
            cik="320193",
            form_type="10-K",
        )
        joined = " ".join(c.text for c in chunks)
        assert "<p>" not in joined and "<h2>" not in joined

    def test_long_section_is_split_into_overlapping_windows(self):
        long_body = "Risk factor sentence. " * 200  # forces > max_chars
        html = f"<html><body><h2>Item 1A. Risk Factors</h2><p>{long_body}</p></body></html>"
        chunks = chunk_document(
            html,
            accession_number=ACCESSION_10K,
            cik="320193",
            form_type="10-K",
            max_chars=500,
            overlap_chars=100,
        )
        assert len(chunks) > 1
        assert all(c.section.lower().startswith("item 1a") for c in chunks)

    def test_no_headings_falls_back_to_single_document_section(self):
        html = "<html><body><p>Just some plain filing text with no Item headings.</p></body></html>"
        chunks = chunk_document(html, accession_number=ACCESSION_10K, cik="320193", form_type="10-K")
        assert len(chunks) == 1
        assert chunks[0].section == "Document"


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------


class TestFilingRetriever:
    def test_raises_on_empty_corpus(self):
        with pytest.raises(ValueError):
            FilingRetriever([])

    def test_top_result_is_the_most_relevant_chunk(self, retriever):
        results = retriever.query("supply chain risk single-source suppliers", top_k=3)
        assert results
        assert "item 1a" in results[0].chunk.section.lower()
        assert results[0].chunk.accession_number == ACCESSION_10K

    def test_retrieves_correct_accession_across_documents(self, retriever):
        """The 10-Q's quarterly net sales figure should rank above the 10-K's on a query specific to it."""
        results = retriever.query("What were quarterly net sales of $81.8 billion?", top_k=3)
        assert results[0].chunk.accession_number == ACCESSION_10Q

    def test_top_k_is_respected(self, retriever):
        results = retriever.query("net sales", top_k=2)
        assert len(results) <= 2


# ---------------------------------------------------------------------------
# QA / citation-first answering
# ---------------------------------------------------------------------------


class TestAnswerQuestion:
    def test_grounded_answer_cites_the_correct_accession(self, retriever):
        answer = answer_question(
            "What supply chain risk does the company describe?", retriever, use_llm=False
        )
        assert answer.grounded is True
        assert answer.citations
        assert answer.citations[0].accession_number == ACCESSION_10K
        assert "supply chain" in answer.answer_text.lower() or "single-source" in answer.answer_text.lower()

    def test_unrelated_question_is_refused_not_guessed(self, retriever):
        answer = answer_question(
            "What is the company's plan for lunar mining operations?", retriever, use_llm=False
        )
        assert answer.grounded is False
        assert answer.citations == []
        assert answer.answer_text == REFUSAL_TEXT

    def test_llm_synthesis_disabled_without_api_key(self, retriever, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        answer = answer_question("What were net sales in fiscal 2024?", retriever, use_llm=True)
        # Falls back to extractive path; must not raise or hang making a real network call.
        assert answer.grounded is True
        assert answer.answer_text.startswith("Based on")

    def test_every_citation_has_a_traceable_source(self, retriever):
        answer = answer_question("What were net sales in fiscal 2024?", retriever, use_llm=False)
        assert answer.citations
        for c in answer.citations:
            assert c.accession_number in (ACCESSION_10K, ACCESSION_10Q)
            assert c.section
            assert c.snippet


# ---------------------------------------------------------------------------
# Evaluation harness
# ---------------------------------------------------------------------------

EVAL_CASES = [
    EvalCase(
        question="What were Apple's total net sales for fiscal 2024?",
        expect_grounded=True,
        expected_accession=ACCESSION_10K,
        expected_keywords=("391.0 billion",),
    ),
    EvalCase(
        question="What was net income for fiscal 2024?",
        expect_grounded=True,
        expected_accession=ACCESSION_10K,
        expected_keywords=("93.7 billion",),
    ),
    EvalCase(
        question="What supply chain risk does the company disclose in its risk factors?",
        expect_grounded=True,
        expected_accession=ACCESSION_10K,
        expected_keywords=("single-source",),
    ),
    EvalCase(
        question="Why did gross margin change during fiscal 2024?",
        expect_grounded=True,
        expected_accession=ACCESSION_10K,
        expected_keywords=("cost savings",),
    ),
    EvalCase(
        question="What were quarterly net sales for the period ended July 1, 2023?",
        expect_grounded=True,
        expected_accession=ACCESSION_10Q,
        expected_keywords=("81.8 billion",),
    ),
    EvalCase(
        question="Were there material changes to risk factors in the quarterly report?",
        expect_grounded=True,
        expected_accession=ACCESSION_10Q,
        expected_keywords=("no material changes",),
    ),
    # Unsupported cases test topic coverage within THIS indexed corpus --
    # not cross-entity disambiguation. Asking about a different company
    # entirely (e.g. "What was Tesla's revenue?") is handled one layer up,
    # by ticker-scoped indexing in the /ask/{ticker} endpoint, not by this
    # retriever: see AGENTS.md "Known failure modes" for why a lexical
    # retriever alone can't reliably tell "wrong company" from "right
    # company, coincidentally shares financial boilerplate vocabulary".
    EvalCase(question="What is the company's plan for lunar mining operations?", expect_grounded=False),
    EvalCase(
        question="What is the return policy for damaged products purchased in physical retail stores?",
        expect_grounded=False,
    ),
    EvalCase(question="What is the CEO's favorite color?", expect_grounded=False),
    EvalCase(question="How many dogs does the company employ as mascots?", expect_grounded=False),
]


class TestEvaluationHarness:
    def test_eval_report_metrics_are_strong_on_seed_set(self, retriever):
        report = evaluate(EVAL_CASES, retriever, top_k=5)

        assert report.n == len(EVAL_CASES)
        # Seed set is small and deliberately unambiguous; a working retriever+QA
        # layer should score highly on all three axes without being brittle to
        # exact-value assertions.
        assert report.grounding_decision_accuracy >= 0.8
        assert report.retrieval_recall_at_k >= 0.8
        assert report.citation_validity_rate >= 0.6
        assert report.avg_latency_ms >= 0  # sanity: timing was actually recorded

    def test_summary_is_human_readable(self, retriever):
        report = evaluate(EVAL_CASES, retriever)
        summary = report.summary()
        assert "retrieval_recall@k" in summary
        assert "citation_validity" in summary
        assert "grounding_decision_accuracy" in summary
