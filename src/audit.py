"""
Hash-chained, per-accession extraction audit trail.

Closes the gap documented in ``docs/DECISION_LOG.md`` §9: ``pipeline_audit``
is real and append-only by convention, but it is stage-level (one row per
DAG stage per run) and has no ``accession_number``/``system_id`` columns —
"show every extraction attempt against accession X, and prove none of those
records has been altered" was not answerable from it. This module, plus the
``extraction_audit`` table it writes to (``src/schema.py``), answers both.

Two independent guarantees, not one
------------------------------------
1. **A hash chain** (this module). Every row's ``row_hash`` covers its own
   fields *and* the previous row's ``row_hash``. Altering, deleting, or
   reordering any row breaks every hash after it — ``verify_chain()``
   detects and pinpoints exactly that.
2. **A database trigger** (migration ``2cf6396ba519``, PostgreSQL only).
   Blocks ``UPDATE``/``DELETE`` against this table outright, so the guarantee
   does not rest solely on nobody calling those statements.

What this does **not** prove, stated plainly rather than implied
------------------------------------------------------------------
A hash chain proves *internal consistency* — the stored rows are exactly the
rows that were written, in the order they were written. It does **not**
provide non-repudiation against an attacker who has both database write
access and the application's own code, because they could in principle
recompute a fresh, internally-consistent chain from scratch and replace the
whole table. Real non-repudiation needs a secret the writer holds and the
verifier doesn't need write access to (HMAC with a key in a secrets
manager), or an external anchor outside this system's own write path
entirely. That's real future work requiring infrastructure this project
doesn't yet integrate (see ``docs/AUDIT_TRAIL_PLAN.md`` §4) — this chain is
a large, genuine improvement over "nothing stops an UPDATE," not the final
word on it.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.schema import ExtractionAudit

logger = logging.getLogger(__name__)

#: Environment variable naming the identity of whatever is running the
#: worker. No authenticated-identity layer exists anywhere in this pipeline
#: yet, so this is a placeholder that is honest about being one: a real
#: deployment should set it to something a Vault AppRole, a Kubernetes
#: service account, or an Airflow connection's login actually authenticates
#: as. The column exists now so that plumbing has somewhere to land later
#: without another schema migration.
SYSTEM_ID_ENV_VAR = "SYSTEM_ID"
DEFAULT_SYSTEM_ID = "edgar-extraction-pipeline"

_HASH_DELIMITER = "\x1f"  # ASCII unit separator — matches src/upsert.py's convention


def current_system_id() -> str:
    """Return the configured system identity, or the documented default."""
    return os.getenv(SYSTEM_ID_ENV_VAR, DEFAULT_SYSTEM_ID)


# ---------------------------------------------------------------------------
# Hash construction
# ---------------------------------------------------------------------------


def compute_row_hash(
    *,
    prev_row_hash: str | None,
    system_id: str,
    accession_number: str | None,
    run_id: str,
    stage: str,
    extraction_status: str,
    created_at: datetime,
    content_hash: str | None,
    detail: str | None = None,
) -> str:
    """
    Deterministic SHA-256 for one audit row, chained to the previous row.

    Every field that identifies *this* extraction event is included, plus
    the previous row's own hash — that link is what turns a set of per-row
    hashes into a chain: altering row N changes its hash, which no longer
    matches what row N+1 recorded as ``prev_row_hash``, and the break is
    detectable at exactly that point by :func:`verify_chain`.

    ``detail`` is covered by the hash deliberately: it is the one free-text
    field on this table, most often holding an error message — exactly the
    content someone tampering with a record would want to alter, and an
    earlier draft of this function omitted it. Caught by testing the tamper
    path directly rather than only testing that a clean chain verifies:
    mutating ``detail`` alone left the chain reporting VALID, because
    nothing checked it. See ``tests/test_audit.py::TestTamperDetection::
    test_altering_detail_field_alone_is_caught``, which pins exactly this.
    """
    parts = (
        prev_row_hash or "",
        system_id,
        accession_number or "",
        run_id,
        stage,
        extraction_status,
        created_at.isoformat(),
        content_hash or "",
        detail or "",
    )
    return hashlib.sha256(_HASH_DELIMITER.join(parts).encode("utf-8")).hexdigest()


def compute_content_hash(content: str | bytes) -> str:
    """SHA-256 of raw or parsed content, for the ``content_hash`` column."""
    data = content.encode("utf-8") if isinstance(content, str) else content
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def write_extraction_audit(
    session: Session,
    *,
    run_id: str,
    stage: str,
    extraction_status: str,
    accession_number: str | None = None,
    system_id: str | None = None,
    detail: str | None = None,
    content_hash: str | None = None,
) -> ExtractionAudit:
    """
    Append one chained audit row.

    Reads the current chain tail within the caller's transaction, computes
    this row's hash against it, and inserts. Concurrency note: this project
    has one Airflow worker writing at a time, so a plain read-then-write is
    sufficient; if write concurrency ever increases enough to risk two
    transactions reading the same tail before either commits, the fix is a
    ``SELECT ... FOR UPDATE`` on the tail read on PostgreSQL — noted here as
    the documented next step, not built speculatively (see
    ``docs/AUDIT_TRAIL_PLAN.md`` §4).
    """
    resolved_system_id = system_id or current_system_id()
    created_at = datetime.now(UTC)

    tail = session.execute(
        select(ExtractionAudit.row_hash).order_by(ExtractionAudit.id.desc()).limit(1)
    ).scalar_one_or_none()

    row_hash = compute_row_hash(
        prev_row_hash=tail,
        system_id=resolved_system_id,
        accession_number=accession_number,
        run_id=run_id,
        stage=stage,
        extraction_status=extraction_status,
        created_at=created_at,
        content_hash=content_hash,
        detail=detail,
    )

    row = ExtractionAudit(
        run_id=run_id,
        system_id=resolved_system_id,
        accession_number=accession_number,
        stage=stage,
        extraction_status=extraction_status,
        detail=detail,
        content_hash=content_hash,
        prev_row_hash=tail,
        row_hash=row_hash,
        created_at=created_at,
    )
    session.add(row)
    session.flush()
    return row


def write_extraction_audit_batch(
    session: Session,
    *,
    run_id: str,
    stage: str,
    outcomes: dict[str, tuple[str, str | None, str | None]],
    system_id: str | None = None,
) -> list[ExtractionAudit]:
    """
    Write one chained row per accession for a DAG stage.

    ``outcomes`` maps ``accession_number -> (extraction_status, detail,
    content_hash)``. Rows are written in sorted accession order so the chain
    for a given run/stage is reproducible rather than depending on
    iteration order of whatever produced ``outcomes``.
    """
    resolved_system_id = system_id or current_system_id()
    return [
        write_extraction_audit(
            session,
            run_id=run_id,
            stage=stage,
            extraction_status=status,
            accession_number=accession_number,
            system_id=resolved_system_id,
            detail=detail,
            content_hash=content_hash,
        )
        for accession_number, (status, detail, content_hash) in sorted(outcomes.items())
    ]


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


@dataclass
class AuditChainVerification:
    """Result of walking the chain and recomputing every row's hash."""

    valid: bool
    total_rows: int
    broken_at_id: int | None = None
    broken_reason: str | None = None
    checked_ids: list[int] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "total_rows": self.total_rows,
            "broken_at_id": self.broken_at_id,
            "broken_reason": self.broken_reason,
        }

    def summary(self) -> str:
        if self.valid:
            return f"VALID — {self.total_rows} rows, chain intact"
        return (
            f"BROKEN at id={self.broken_at_id} ({self.broken_reason}) — "
            f"{self.total_rows} rows total"
        )


def _row_created_at(row: ExtractionAudit) -> datetime:
    """
    Normalise ``created_at`` for re-hashing.

    SQLite (used in tests) does not round-trip timezone-aware datetimes —
    a value written as UTC comes back naive on read. Re-attaching UTC here
    matches what :func:`write_extraction_audit` fed into the original hash
    (``datetime.now(timezone.utc)``), so verification isn't sensitive to a
    storage detail that has nothing to do with whether the row was tampered
    with. PostgreSQL preserves the timezone natively; this is a no-op there.
    """
    if row.created_at.tzinfo is None:
        return row.created_at.replace(tzinfo=UTC)
    return row.created_at


def verify_chain(session: Session) -> AuditChainVerification:
    """
    Walk ``extraction_audit`` in insertion order and verify every link.

    For each row: recompute its hash from its own stored fields and compare
    against the stored ``row_hash`` (catches a row whose content was
    altered in place), and confirm its ``prev_row_hash`` matches the
    previous row's ``row_hash`` (catches a deleted or reordered row — either
    breaks the link even though no row's own content changed).

    Returns where the chain broke, not just whether it's valid, so a caller
    can point at the specific tampered or missing row rather than having to
    re-derive that themselves.
    """
    rows = (
        session.execute(select(ExtractionAudit).order_by(ExtractionAudit.id.asc())).scalars().all()
    )

    checked_ids: list[int] = []
    expected_prev: str | None = None

    for row in rows:
        checked_ids.append(row.id)

        if row.prev_row_hash != expected_prev:
            return AuditChainVerification(
                valid=False,
                total_rows=len(rows),
                broken_at_id=row.id,
                broken_reason=(
                    f"prev_row_hash mismatch: row records {row.prev_row_hash!r}, "
                    f"chain expected {expected_prev!r} — a row was deleted, "
                    "inserted out of order, or the chain was forked"
                ),
                checked_ids=checked_ids,
            )

        recomputed = compute_row_hash(
            prev_row_hash=row.prev_row_hash,
            system_id=row.system_id,
            accession_number=row.accession_number,
            run_id=row.run_id,
            stage=row.stage,
            extraction_status=row.extraction_status,
            created_at=_row_created_at(row),
            content_hash=row.content_hash,
            detail=row.detail,
        )
        if recomputed != row.row_hash:
            return AuditChainVerification(
                valid=False,
                total_rows=len(rows),
                broken_at_id=row.id,
                broken_reason=(
                    f"row_hash mismatch: stored {row.row_hash!r}, "
                    f"recomputed {recomputed!r} — this row's content was altered "
                    "after it was written"
                ),
                checked_ids=checked_ids,
            )

        expected_prev = row.row_hash

    logger.info("Audit chain verified: %d rows, intact", len(rows))
    return AuditChainVerification(valid=True, total_rows=len(rows), checked_ids=checked_ids)
