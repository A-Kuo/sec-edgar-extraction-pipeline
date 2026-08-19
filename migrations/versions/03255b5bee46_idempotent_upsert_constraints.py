"""idempotent upsert constraints

Adds the identity columns/constraints the UPSERT rewrite in src/upsert.py
needs:

  - financial_facts.fact_hash  — SHA-256 of the fact's natural key
                                  (accession, concept, period, segment),
                                  UNIQUE. The ON CONFLICT target that lets a
                                  retried extraction batch converge onto the
                                  same rows instead of duplicating them.
  - model_runs (run_id, model_version) — UNIQUE. Lets a retried scoring task
                                  update its own row in place rather than
                                  the previous delete-then-recreate pattern.

fact_hash is added nullable, backfilled from any pre-existing rows using the
*same* compute_fact_hash() the application uses at write time (imported, not
reimplemented — a hand-copied version here would silently drift from the
real one the moment either changed), then tightened to NOT NULL. Adding it
directly as NOT NULL would fail outright against a table that already has
rows, on every backend that enforces the standard (PostgreSQL included).

Revision ID: 03255b5bee46
Revises: b17f52f92845
Create Date: 2026-08-12 17:11:23.110809

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "03255b5bee46"
down_revision: str | None = "b17f52f92845"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_BATCH_SIZE = 1000


def upgrade() -> None:
    # batch_alter_table, for every operation here, not just the column-type
    # change: SQLite has no direct `ALTER TABLE ADD CONSTRAINT` at all — even
    # a unique constraint requires the copy-and-move strategy batch mode
    # implements. It is a transparent passthrough to a direct ALTER on
    # backends (PostgreSQL included) that support one natively, so this
    # single code path is correct on both.
    with op.batch_alter_table("financial_facts") as batch_op:
        batch_op.add_column(sa.Column("fact_hash", sa.String(length=64), nullable=True))

    _backfill_fact_hash()

    with op.batch_alter_table("financial_facts") as batch_op:
        batch_op.alter_column("fact_hash", nullable=False)
        batch_op.create_unique_constraint("uq_financial_facts_fact_hash", ["fact_hash"])

    _dedupe_model_runs()

    with op.batch_alter_table("model_runs") as batch_op:
        batch_op.create_unique_constraint(
            "uq_model_runs_run_id_version", ["run_id", "model_version"]
        )


def downgrade() -> None:
    with op.batch_alter_table("model_runs") as batch_op:
        batch_op.drop_constraint("uq_model_runs_run_id_version", type_="unique")

    with op.batch_alter_table("financial_facts") as batch_op:
        batch_op.drop_constraint("uq_financial_facts_fact_hash", type_="unique")
        batch_op.drop_column("fact_hash")


def _backfill_fact_hash() -> None:
    """Compute fact_hash for any rows that predate this migration."""
    from src.upsert import compute_fact_hash

    conn = op.get_bind()
    facts = sa.table(
        "financial_facts",
        sa.column("id", sa.BigInteger),
        sa.column("accession_number", sa.String),
        sa.column("fact_name", sa.String),
        sa.column("period_start", sa.Date),
        sa.column("period_end", sa.Date),
        sa.column("segment", sa.String),
        sa.column("fact_hash", sa.String),
    )

    rows = conn.execute(
        sa.select(
            facts.c.id,
            facts.c.accession_number,
            facts.c.fact_name,
            facts.c.period_start,
            facts.c.period_end,
            facts.c.segment,
        )
    ).fetchall()

    if not rows:
        return

    for offset in range(0, len(rows), _BATCH_SIZE):
        batch = rows[offset : offset + _BATCH_SIZE]
        for row in batch:
            fact_hash = compute_fact_hash(
                accession_number=row.accession_number,
                fact_name=row.fact_name,
                period_start=row.period_start,
                period_end=row.period_end,
                segment=row.segment,
            )
            conn.execute(
                facts.update().where(facts.c.id == row.id).values(fact_hash=fact_hash)
            )


def _dedupe_model_runs() -> None:
    """
    Defensive cleanup before the (run_id, model_version) UNIQUE constraint.

    The application-level invariant (the old delete-then-recreate pattern,
    and now the UPSERT) should already guarantee this, but a migration that
    assumes an invariant instead of enforcing it is exactly the kind of gap
    this audit is about. Keeps the newest row per (run_id, model_version) —
    and its dependent fact_anomalies rows via ON DELETE CASCADE — and drops
    older duplicates, if any exist.
    """
    conn = op.get_bind()
    model_runs = sa.table(
        "model_runs",
        sa.column("id", sa.BigInteger),
        sa.column("run_id", sa.String),
        sa.column("model_version", sa.String),
        sa.column("created_at", sa.DateTime),
    )

    rows = conn.execute(
        sa.select(model_runs.c.id, model_runs.c.run_id, model_runs.c.model_version)
    ).fetchall()

    seen: dict[tuple[str, str], int] = {}
    duplicate_ids: list[int] = []
    for row in rows:
        key = (row.run_id, row.model_version)
        if key in seen:
            # Keep the higher id (inserted later); drop the earlier one.
            duplicate_ids.append(min(seen[key], row.id))
            seen[key] = max(seen[key], row.id)
        else:
            seen[key] = row.id

    if duplicate_ids:
        conn.execute(model_runs.delete().where(model_runs.c.id.in_(duplicate_ids)))
