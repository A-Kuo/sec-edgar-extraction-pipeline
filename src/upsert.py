"""
Idempotent database writes: ``INSERT ... ON CONFLICT DO UPDATE`` / ``DO NOTHING``.

Why this exists
----------------
The pipeline used to write facts with ``session.bulk_save_objects(objects)``
— a blind bulk INSERT with no conflict handling. ``financial_facts`` had no
natural-key constraint, only the autoincrement surrogate ``id``, so nothing
stopped a duplicate row. Airflow retries a failed task (``retries=2`` in the
DAG's ``default_args``); if a worker died *after* a batch of INSERTs
committed but *before* the task reported success, the retry would re-run the
same batch and duplicate every fact in it. Landing raw filings used a
check-then-insert (``session.get()`` then a conditional ``add()``) — not
atomic, and a genuine TOCTOU race under concurrent workers.

Every write in this module is instead a single atomic ``INSERT ... ON
CONFLICT`` statement keyed on a stable identity:

* ``financial_facts``   — keyed on ``fact_hash``, a SHA-256 of the fact's
                           natural key (accession, concept, period, segment).
                           A retry recomputes the identical hash for the
                           identical fact and updates the row in place.
* ``filings_raw``        — keyed on the existing ``accession_number`` primary
                           key. ``DO NOTHING``: the raw landing zone for an
                           accession that already landed should not be
                           silently overwritten by a retry racing a
                           legitimate re-ingestion.
* ``model_runs``         — keyed on ``(run_id, model_version)``. A retried
                           scoring task updates the aggregate counts in place
                           rather than deleting and recreating the row, so
                           its ``id`` — and therefore every ``fact_anomalies``
                           row that references it — never churns.
* ``fact_anomalies``     — keyed on ``(model_run_id, accession_number)``,
                           matching ``uq_anomaly_run_accession``.

Portability
-----------
``INSERT ... ON CONFLICT`` is not standard SQL; SQLAlchemy exposes it as a
dialect-specific construct (``sqlalchemy.dialects.postgresql.insert`` /
``sqlalchemy.dialects.sqlite.insert``) with an identical ``.on_conflict_do_
update()`` / ``.on_conflict_do_nothing()`` API on both. ``_insert()`` below
dispatches on the bound engine's dialect name, which is what lets this
module's tests exercise the real conflict-resolution SQL — not a mock of it —
against the SQLite the test suite runs on, while production runs the
identical Python against PostgreSQL.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any, Protocol

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.schema import FactAnomaly, FilingRaw, FinancialFact, ModelRun

logger = logging.getLogger(__name__)


class _SupportsOnConflict(Protocol):
    """Structural type for the subset of the insert() API this module uses."""

    excluded: Any

    def values(self, *args: Any, **kwargs: Any) -> _SupportsOnConflict: ...
    def on_conflict_do_update(self, **kwargs: Any) -> _SupportsOnConflict: ...
    def on_conflict_do_nothing(self, **kwargs: Any) -> _SupportsOnConflict: ...
    def returning(self, *cols: Any) -> _SupportsOnConflict: ...


def _insert(bind: Any) -> Any:
    """
    Return the dialect-appropriate ``insert()`` constructor for *bind*.

    *bind* may be an ``Engine``, a ``Connection``, or a ``Session`` (whose
    ``.get_bind()`` is used). Raises for any dialect without an ON CONFLICT
    implementation wired up here, rather than silently falling back to a
    plain insert that would reintroduce the duplicate-row bug this module
    exists to close.

    Returns ``Any`` rather than a precise callable type deliberately: the
    postgresql and sqlite ``insert()`` constructors return different concrete
    ``Insert`` subclasses, and every caller in this module only uses the
    common ``.values() / .on_conflict_do_update() / .on_conflict_do_nothing()
    / .returning()`` surface both share, matched structurally by
    ``_SupportsOnConflict`` above.
    """
    engine = bind.get_bind() if isinstance(bind, Session) else bind
    dialect = engine.dialect.name

    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        return pg_insert
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        return sqlite_insert
    raise NotImplementedError(
        f"upsert.py has no ON CONFLICT implementation for dialect {dialect!r}. "
        "Production targets PostgreSQL exclusively; SQLite is supported only "
        "so the test suite can exercise real conflict-resolution SQL."
    )


# ---------------------------------------------------------------------------
# financial_facts
# ---------------------------------------------------------------------------


def compute_fact_hash(
    accession_number: str,
    fact_name: str,
    period_start: date | str | None,
    period_end: date | str | None,
    segment: str | None,
) -> str:
    """
    Deterministic SHA-256 identity for one extracted fact.

    The natural key is what makes a fact *this* fact: which filing, which
    concept, which reporting period, and which segment (``None`` for the
    consolidated figure). Two parses of the same filing — the normal case on
    an Airflow retry — produce byte-identical input to this function and
    therefore the identical hash, which is exactly what makes the UPSERT
    converge instead of duplicate.

    Values are normalised before hashing (dates to ISO strings, ``None``
    segment to a literal sentinel) so the hash does not depend on incidental
    representation differences between two extraction runs.
    """
    parts = (
        accession_number,
        fact_name,
        period_start.isoformat() if isinstance(period_start, date) else (period_start or ""),
        period_end.isoformat() if isinstance(period_end, date) else (period_end or ""),
        segment or "",
    )
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8"))
    return digest.hexdigest()


def upsert_financial_facts(session: Session, facts: Sequence[Mapping[str, Any]]) -> int:
    """
    Idempotently write parsed facts.

    Each row's ``fact_hash`` is computed here (not trusted from the caller),
    so a caller cannot accidentally bypass the identity the conflict target
    relies on. A retried batch — full or partial overlap with a batch that
    already landed — converges to one row per fact rather than duplicating.

    Returns the number of input rows processed (inserted or updated).
    """
    if not facts:
        return 0

    insert = _insert(session)
    rows: list[dict[str, Any]] = []
    for fact in facts:
        row = dict(fact)
        row["fact_hash"] = compute_fact_hash(
            accession_number=row["accession_number"],
            fact_name=row["fact_name"],
            period_start=row.get("period_start"),
            period_end=row.get("period_end"),
            segment=row.get("segment"),
        )
        rows.append(row)

    stmt = insert(FinancialFact).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["fact_hash"],
        set_={
            "fact_value": stmt.excluded.fact_value,
            "unit": stmt.excluded.unit,
            "period_start": stmt.excluded.period_start,
            "period_end": stmt.excluded.period_end,
            "segment": stmt.excluded.segment,
            "parsed_at": func.now(),
        },
    )
    session.execute(stmt)

    logger.info("Upserted %d financial facts", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# filings_raw
# ---------------------------------------------------------------------------


def upsert_filing_raw(
    session: Session,
    *,
    accession_number: str,
    cik: str,
    ticker: str | None,
    filing_type: str,
    filing_date: date | str,
    period_of_report: date | str | None,
    raw_html: str | None,
    pipeline_run_id: str | None,
) -> bool:
    """
    Idempotently land a raw filing.

    ``DO NOTHING`` on conflict: the raw landing zone is the source-of-record
    for what was actually downloaded, and a retry re-landing an accession
    that is already present should be a no-op, not a silent overwrite racing
    whatever process ingested it first. Returns ``True`` if this call
    inserted a new row, ``False`` if the accession was already present.
    """
    insert = _insert(session)
    stmt = (
        insert(FilingRaw)
        .values(
            accession_number=accession_number,
            cik=cik,
            ticker=ticker,
            filing_type=filing_type,
            filing_date=filing_date,
            period_of_report=period_of_report,
            raw_html=raw_html,
            pipeline_run_id=pipeline_run_id,
        )
        .on_conflict_do_nothing(index_elements=["accession_number"])
        .returning(FilingRaw.accession_number)
    )
    result = session.execute(stmt)
    inserted = result.first() is not None
    if not inserted:
        logger.debug("Filing %s already landed — no-op", accession_number)
    return inserted


# ---------------------------------------------------------------------------
# model_runs + fact_anomalies
# ---------------------------------------------------------------------------


def upsert_model_run(
    session: Session,
    *,
    run_id: str,
    model_version: str,
    model_sha256: str,
    git_sha: str | None,
    filings_scored: int,
    anomalies_flagged: int,
    threshold: float,
    drift_report: dict[str, Any] | None,
    drift_level: str | None,
) -> int:
    """
    Idempotently write one scoring run's summary, returning its ``id``.

    Keyed on ``(run_id, model_version)``: a retried ``score_anomalies`` task
    updates the same row's aggregate counts instead of deleting and
    recreating it. Keeping ``id`` stable across a retry matters because
    ``fact_anomalies`` rows reference it — deleting and recreating the parent
    (the pipeline's previous approach) is a wider blast radius than a single
    atomic UPSERT needs to be.
    """
    insert = _insert(session)
    stmt = (
        insert(ModelRun)
        .values(
            run_id=run_id,
            model_version=model_version,
            model_sha256=model_sha256,
            git_sha=git_sha,
            filings_scored=filings_scored,
            anomalies_flagged=anomalies_flagged,
            threshold=threshold,
            drift_report=drift_report,
            drift_level=drift_level,
        )
        .on_conflict_do_update(
            index_elements=["run_id", "model_version"],
            set_={
                "model_sha256": model_sha256,
                "git_sha": git_sha,
                "filings_scored": filings_scored,
                "anomalies_flagged": anomalies_flagged,
                "threshold": threshold,
                "drift_report": drift_report,
                "drift_level": drift_level,
            },
        )
        .returning(ModelRun.id)
    )
    model_run_id = session.execute(stmt).scalar_one()
    logger.info("Upserted model_runs id=%s run=%s model=%s", model_run_id, run_id, model_version)
    return model_run_id


def upsert_fact_anomalies(session: Session, model_run_id: int, scores: Sequence[Any]) -> int:
    """
    Idempotently write per-filing anomaly scores for one scoring run.

    Keyed on ``(model_run_id, accession_number)`` — matches
    ``uq_anomaly_run_accession``. Combined with :func:`upsert_model_run`'s
    stable ``id``, a retried scoring task converges to the same final rows
    rather than the previous delete-then-reinsert pattern.
    """
    if not scores:
        return 0

    insert = _insert(session)
    rows = [
        {
            "model_run_id": model_run_id,
            "accession_number": s.accession_number,
            "score": s.score,
            "model_score": s.model_score,
            "rule_score": s.rule_score,
            "is_anomaly": s.is_anomaly,
            "triggered_by": s.triggered_by,
            "reason": s.explain(),
            "rule_violations": [v.as_dict() for v in s.rule_violations],
            "top_contributors": [[n, z] for n, z in s.top_contributors],
        }
        for s in scores
    ]

    stmt = insert(FactAnomaly).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["model_run_id", "accession_number"],
        set_={
            "score": stmt.excluded.score,
            "model_score": stmt.excluded.model_score,
            "rule_score": stmt.excluded.rule_score,
            "is_anomaly": stmt.excluded.is_anomaly,
            "triggered_by": stmt.excluded.triggered_by,
            "reason": stmt.excluded.reason,
            "rule_violations": stmt.excluded.rule_violations,
            "top_contributors": stmt.excluded.top_contributors,
            "scored_at": func.now(),
        },
    )
    session.execute(stmt)

    logger.info("Upserted %d fact_anomalies rows for model_run_id=%s", len(rows), model_run_id)
    return len(rows)
