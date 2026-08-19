"""hash chained extraction audit trail

Creates `extraction_audit` (see src/schema.py::ExtractionAudit and
src/audit.py for the hash-chain construction and verification), and adds a
PostgreSQL trigger blocking UPDATE/DELETE against both `extraction_audit`
and the pre-existing `pipeline_audit`.

Why the trigger matters, not just the table: a hash chain proves internal
consistency (nothing was altered/reordered/deleted without detection), but
by itself it's still application discipline enforcing that nobody calls
UPDATE/DELETE, plus a verification job somebody has to remember to run. The
trigger makes "append-only" a database-level guarantee instead — the same
gap this whole feature exists to close for `pipeline_audit`, which has
claimed "append-only" in its own docstring since it was created but never
had anything actually stopping a write against it.

PostgreSQL-only, explicitly: SQLite (what the unit test suite runs against)
has no equivalent CREATE TRIGGER ... syntax for this, and this migration's
own test coverage is therefore split accordingly — see
docs/AUDIT_TRAIL_PLAN.md §6 and the CI step in .github/workflows/ci.yml
that verifies this trigger actually fires against a live Postgres service
container, which is the only place in this project's test matrix that can.

Revision ID: 2cf6396ba519
Revises: 03255b5bee46
Create Date: 2026-08-16 02:28:18.532696

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2cf6396ba519"
down_revision: str | None = "03255b5bee46"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TRIGGER_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION prevent_audit_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'Table % is append-only: % is not permitted (CMMC AU-12 non-repudiation)',
        TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;
"""

_DROP_TRIGGER_FUNCTION_SQL = "DROP FUNCTION IF EXISTS prevent_audit_mutation();"


def _create_immutability_trigger(table_name: str) -> None:
    trigger_name = f"trg_{table_name}_immutable"
    op.execute(
        f"""
        CREATE TRIGGER {trigger_name}
            BEFORE UPDATE OR DELETE ON {table_name}
            FOR EACH ROW EXECUTE FUNCTION prevent_audit_mutation();
        """
    )


def _drop_immutability_trigger(table_name: str) -> None:
    trigger_name = f"trg_{table_name}_immutable"
    op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};")


def upgrade() -> None:
    op.create_table(
        "extraction_audit",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("system_id", sa.String(length=128), nullable=False),
        sa.Column("accession_number", sa.String(length=25), nullable=True),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("extraction_status", sa.String(length=16), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.String(length=64), nullable=True),
        sa.Column("prev_row_hash", sa.String(length=64), nullable=True),
        sa.Column("row_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_extraction_audit_accession_number"),
        "extraction_audit",
        ["accession_number"],
        unique=False,
    )
    op.create_index(
        op.f("ix_extraction_audit_created_at"), "extraction_audit", ["created_at"], unique=False
    )
    op.create_index(
        op.f("ix_extraction_audit_run_id"), "extraction_audit", ["run_id"], unique=False
    )

    if op.get_bind().dialect.name == "postgresql":
        op.execute(_TRIGGER_FUNCTION_SQL)
        _create_immutability_trigger("pipeline_audit")
        _create_immutability_trigger("extraction_audit")


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        _drop_immutability_trigger("extraction_audit")
        _drop_immutability_trigger("pipeline_audit")
        op.execute(_DROP_TRIGGER_FUNCTION_SQL)

    op.drop_index(op.f("ix_extraction_audit_run_id"), table_name="extraction_audit")
    op.drop_index(op.f("ix_extraction_audit_created_at"), table_name="extraction_audit")
    op.drop_index(op.f("ix_extraction_audit_accession_number"), table_name="extraction_audit")
    op.drop_table("extraction_audit")
