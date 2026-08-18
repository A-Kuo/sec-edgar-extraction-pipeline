"""
Evaluation harness for the filing-text lookup aid.

Part of a read-only filing-text search tool (``/ask/{ticker}``). This
evaluates the lookup aid's ability to find relevant passages and refuse
gracefully when nothing is relevant — it does not evaluate fact
extraction. See ``docs/BOUNDARY.md`` for the scope contract.

Reports retrieval recall@k, citation validity, and grounding-decision
accuracy on deliberately unsupported questions -- rather than a single
"accuracy" number. "Did it find the right passage" is meaningless here
without also measuring "did it correctly decline when nothing was
relevant."
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from src.rag.qa import Answer, answer_question
from src.rag.retrieval import FilingRetriever


@dataclass(frozen=True)
class EvalCase:
    """
    One evaluation question.

    ``expect_grounded=False`` marks a deliberately unsupported /
    out-of-scope question -- the correct system behavior is to refuse,
    not to guess. ``expected_accession`` and ``expected_keywords`` are
    only meaningful when ``expect_grounded=True``.
    """

    question: str
    expect_grounded: bool
    expected_accession: str | None = None
    expected_keywords: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class CaseResult:
    case: EvalCase
    answer: Answer
    retrieval_hit: bool
    citation_valid: bool
    correct_grounding_decision: bool
    latency_ms: float


@dataclass(frozen=True)
class EvalReport:
    results: list[CaseResult]

    @property
    def n(self) -> int:
        return len(self.results)

    @property
    def retrieval_recall_at_k(self) -> float:
        """Fraction of cases with a known expected accession where a citation from that accession was returned."""
        scoped = [r for r in self.results if r.case.expected_accession is not None]
        if not scoped:
            return float("nan")
        return sum(r.retrieval_hit for r in scoped) / len(scoped)

    @property
    def citation_validity_rate(self) -> float:
        """Fraction of grounded, keyword-checked cases whose citation snippet actually contains the expected keywords."""
        scoped = [r for r in self.results if r.case.expect_grounded and r.case.expected_keywords]
        if not scoped:
            return float("nan")
        return sum(r.citation_valid for r in scoped) / len(scoped)

    @property
    def grounding_decision_accuracy(self) -> float:
        """Fraction of all cases where grounded/refused matched the expected behavior."""
        if not self.results:
            return float("nan")
        return sum(r.correct_grounding_decision for r in self.results) / self.n

    @property
    def avg_latency_ms(self) -> float:
        if not self.results:
            return float("nan")
        return sum(r.latency_ms for r in self.results) / self.n

    def summary(self) -> str:
        return (
            f"cases={self.n}  "
            f"retrieval_recall@k={self.retrieval_recall_at_k:.2f}  "
            f"citation_validity={self.citation_validity_rate:.2f}  "
            f"grounding_decision_accuracy={self.grounding_decision_accuracy:.2f}  "
            f"avg_latency_ms={self.avg_latency_ms:.1f}"
        )


def evaluate(cases: list[EvalCase], retriever: FilingRetriever, *, top_k: int = 5) -> EvalReport:
    """
    Run *cases* against *retriever* and score retrieval/grounding quality.

    Uses ``use_llm=False`` so evaluation is deterministic and requires
    no external API calls, even if ``OPENAI_API_KEY`` happens to be set
    in the environment this runs in.
    """
    results: list[CaseResult] = []
    for case in cases:
        start = time.perf_counter()
        answer = answer_question(case.question, retriever, top_k=top_k, use_llm=False)
        latency_ms = (time.perf_counter() - start) * 1000

        retrieval_hit = case.expected_accession is not None and any(
            c.accession_number == case.expected_accession for c in answer.citations
        )

        if case.expect_grounded and case.expected_keywords:
            citation_valid = any(
                all(kw.lower() in c.snippet.lower() for kw in case.expected_keywords)
                for c in answer.citations
            )
        else:
            citation_valid = True  # not scored for this case; excluded via citation_validity_rate filtering

        results.append(
            CaseResult(
                case=case,
                answer=answer,
                retrieval_hit=retrieval_hit,
                citation_valid=citation_valid,
                correct_grounding_decision=answer.grounded == case.expect_grounded,
                latency_ms=latency_ms,
            )
        )
    return EvalReport(results=results)
