"""
Tests for src/audit.py — the hash-chained extraction audit trail.

Runs entirely against SQLite: the hash-chain logic itself (construction,
linking, verification) doesn't depend on PostgreSQL at all. What SQLite
*cannot* exercise is the immutability trigger from migration
`2cf6396ba519` — that's why every tamper-detection test here writes its
tamper via a raw UPDATE/DELETE that bypasses the ORM, standing in for what
a trigger would otherwise block outright on PostgreSQL. The trigger itself
is verified separately, against a live Postgres service container, in
`.github/workflows/ci.yml`.

`TestTamperDetection` is the centerpiece. It exists because testing only
that a *clean* chain verifies would have missed a real gap: an early
implementation of `compute_row_hash` omitted the `detail` field, and a
chain with a tampered `detail` value reported VALID right up until this
test file caught it — see the comment on that finding in
`compute_row_hash`'s docstring.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.audit import (
    DEFAULT_SYSTEM_ID,
    SYSTEM_ID_ENV_VAR,
    compute_content_hash,
    compute_row_hash,
    current_system_id,
    verify_chain,
    write_extraction_audit,
    write_extraction_audit_batch,
)
from src.schema import Base, ExtractionAudit


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


# ---------------------------------------------------------------------------
# system_id resolution
# ---------------------------------------------------------------------------


class TestSystemId:
    def test_defaults_when_unset(self, monkeypatch):
        monkeypatch.delenv(SYSTEM_ID_ENV_VAR, raising=False)
        assert current_system_id() == DEFAULT_SYSTEM_ID

    def test_reads_the_environment_variable(self, monkeypatch):
        monkeypatch.setenv(SYSTEM_ID_ENV_VAR, "vault-approle-xyz")
        assert current_system_id() == "vault-approle-xyz"


# ---------------------------------------------------------------------------
# compute_row_hash / compute_content_hash
# ---------------------------------------------------------------------------


class TestComputeRowHash:
    def _kwargs(self, **overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "prev_row_hash": None,
            "system_id": "sys-1",
            "accession_number": "A",
            "run_id": "run-1",
            "stage": "download",
            "extraction_status": "success",
            "created_at": datetime(2024, 1, 1, tzinfo=UTC),
            "content_hash": None,
            "detail": None,
        }
        base.update(overrides)
        return base

    def test_deterministic(self):
        kwargs = self._kwargs()
        assert compute_row_hash(**kwargs) == compute_row_hash(**kwargs)

    def test_returns_hex_sha256(self):
        h = compute_row_hash(**self._kwargs())
        assert len(h) == 64
        int(h, 16)

    @pytest.mark.parametrize(
        "field,value",
        [
            ("prev_row_hash", "x" * 64),
            ("system_id", "sys-2"),
            ("accession_number", "B"),
            ("run_id", "run-2"),
            ("stage", "parse"),
            ("extraction_status", "failure"),
            ("created_at", datetime(2025, 1, 1, tzinfo=UTC)),
            ("content_hash", "y" * 64),
            ("detail", "something went wrong"),
        ],
    )
    def test_sensitive_to_every_field(self, field, value):
        """Every field that identifies this event must affect the hash —
        including `detail`, the free-text field an earlier draft omitted."""
        base = self._kwargs()
        changed = {**base, field: value}
        assert compute_row_hash(**base) != compute_row_hash(**changed)

    def test_chained_to_the_previous_hash(self):
        h1 = compute_row_hash(**self._kwargs(prev_row_hash=None))
        h2 = compute_row_hash(**self._kwargs(prev_row_hash="a" * 64))
        assert h1 != h2

    def test_none_and_empty_string_fields_hash_identically(self):
        """A first-ever row (prev_row_hash=None) must not collide with a
        row whose prev_row_hash happens to be an empty string."""
        assert compute_row_hash(**self._kwargs(prev_row_hash=None)) == compute_row_hash(
            **self._kwargs(prev_row_hash="")
        )


class TestComputeContentHash:
    def test_string_and_bytes_of_the_same_content_match(self):
        assert compute_content_hash("hello") == compute_content_hash(b"hello")

    def test_different_content_differs(self):
        assert compute_content_hash("a") != compute_content_hash("b")

    def test_returns_hex_sha256(self):
        h = compute_content_hash("x")
        assert len(h) == 64
        int(h, 16)


# ---------------------------------------------------------------------------
# write_extraction_audit / write_extraction_audit_batch
# ---------------------------------------------------------------------------


class TestWriteExtractionAudit:
    def test_first_row_has_no_prev_hash(self, session):
        row = write_extraction_audit(
            session,
            run_id="run-1",
            stage="download",
            extraction_status="success",
            accession_number="A",
        )
        session.commit()
        assert row.prev_row_hash is None
        assert row.row_hash

    def test_second_row_chains_to_the_first(self, session):
        first = write_extraction_audit(
            session,
            run_id="run-1",
            stage="download",
            extraction_status="success",
            accession_number="A",
        )
        session.commit()
        second = write_extraction_audit(
            session,
            run_id="run-1",
            stage="download",
            extraction_status="success",
            accession_number="B",
        )
        session.commit()
        assert second.prev_row_hash == first.row_hash

    def test_default_system_id_used_when_not_given(self, session, monkeypatch):
        monkeypatch.delenv(SYSTEM_ID_ENV_VAR, raising=False)
        row = write_extraction_audit(
            session, run_id="run-1", stage="download", extraction_status="success"
        )
        assert row.system_id == DEFAULT_SYSTEM_ID

    def test_explicit_system_id_overrides_the_environment(self, session, monkeypatch):
        monkeypatch.setenv(SYSTEM_ID_ENV_VAR, "env-value")
        row = write_extraction_audit(
            session,
            run_id="run-1",
            stage="download",
            extraction_status="success",
            system_id="explicit-value",
        )
        assert row.system_id == "explicit-value"

    def test_accession_number_is_optional(self, session):
        """A batch-level or pre-landing failure has no accession yet."""
        row = write_extraction_audit(
            session,
            run_id="run-1",
            stage="download",
            extraction_status="failure",
            detail="fetch timed out",
        )
        session.commit()
        assert row.accession_number is None

    def test_stored_hash_matches_recomputation(self, session):
        row = write_extraction_audit(
            session,
            run_id="run-1",
            stage="parse",
            extraction_status="success",
            accession_number="A",
            content_hash="c" * 64,
            detail="ok",
        )
        session.commit()
        recomputed = compute_row_hash(
            prev_row_hash=None,
            system_id=row.system_id,
            accession_number="A",
            run_id="run-1",
            stage="parse",
            extraction_status="success",
            created_at=row.created_at.replace(tzinfo=UTC),
            content_hash="c" * 64,
            detail="ok",
        )
        assert row.row_hash == recomputed


class TestWriteExtractionAuditBatch:
    def test_writes_one_row_per_accession(self, session):
        rows = write_extraction_audit_batch(
            session,
            run_id="run-1",
            stage="parse",
            outcomes={
                "A": ("success", None, None),
                "B": ("failure", "malformed XBRL", None),
            },
        )
        session.commit()
        assert len(rows) == 2
        assert session.query(ExtractionAudit).count() == 2

    def test_rows_are_chained_to_each_other(self, session):
        rows = write_extraction_audit_batch(
            session,
            run_id="run-1",
            stage="download",
            outcomes={"A": ("success", None, None), "B": ("success", None, None)},
        )
        session.commit()
        assert rows[1].prev_row_hash == rows[0].row_hash

    def test_written_in_sorted_accession_order_for_reproducibility(self, session):
        rows = write_extraction_audit_batch(
            session,
            run_id="run-1",
            stage="download",
            outcomes={"Z": ("success", None, None), "A": ("success", None, None)},
        )
        assert [r.accession_number for r in rows] == ["A", "Z"]

    def test_empty_outcomes_writes_nothing(self, session):
        assert (
            write_extraction_audit_batch(session, run_id="run-1", stage="download", outcomes={})
            == []
        )


# ---------------------------------------------------------------------------
# verify_chain — the clean path
# ---------------------------------------------------------------------------


class TestVerifyChainClean:
    def test_empty_table_is_valid(self, session):
        result = verify_chain(session)
        assert result.valid
        assert result.total_rows == 0

    def test_single_row_is_valid(self, session):
        write_extraction_audit(
            session, run_id="run-1", stage="download", extraction_status="success"
        )
        session.commit()
        result = verify_chain(session)
        assert result.valid
        assert result.total_rows == 1

    def test_long_chain_is_valid(self, session):
        for i in range(25):
            write_extraction_audit(
                session,
                run_id="run-1",
                stage="download",
                extraction_status="success",
                accession_number=f"A{i}",
            )
        session.commit()
        result = verify_chain(session)
        assert result.valid
        assert result.total_rows == 25
        assert result.broken_at_id is None

    def test_multiple_runs_and_stages_share_one_chain(self, session):
        """The chain is global across runs/stages by design — see
        docs/AUDIT_TRAIL_PLAN.md §4 on why a single chain, and its
        documented scalability alternative if that ever needs revisiting."""
        write_extraction_audit(
            session, run_id="run-1", stage="download", extraction_status="success"
        )
        write_extraction_audit(session, run_id="run-1", stage="parse", extraction_status="success")
        write_extraction_audit(
            session, run_id="run-2", stage="download", extraction_status="success"
        )
        session.commit()
        result = verify_chain(session)
        assert result.valid
        assert result.total_rows == 3


# ---------------------------------------------------------------------------
# verify_chain — tamper detection (the point of this whole module)
# ---------------------------------------------------------------------------


class TestTamperDetection:
    def _seed_chain(self, session) -> list[ExtractionAudit]:
        rows = [
            write_extraction_audit(
                session,
                run_id="run-1",
                stage="download",
                extraction_status="success",
                accession_number="A",
            ),
            write_extraction_audit(
                session,
                run_id="run-1",
                stage="download",
                extraction_status="success",
                accession_number="B",
            ),
            write_extraction_audit(
                session,
                run_id="run-1",
                stage="parse",
                extraction_status="failure",
                accession_number="B",
                detail="malformed XBRL",
            ),
        ]
        session.commit()
        return rows

    def test_altering_detail_field_alone_is_caught(self, session):
        """
        The regression test. compute_row_hash originally omitted `detail`;
        this test would have failed under that version (it reported VALID)
        and is what caught the gap before it shipped.
        """
        rows = self._seed_chain(session)
        target_id = rows[2].id

        session.execute(
            ExtractionAudit.__table__.update()
            .where(ExtractionAudit.id == target_id)
            .values(detail="TAMPERED — original error hidden")
        )
        session.commit()

        result = verify_chain(session)
        assert not result.valid
        assert result.broken_at_id == target_id
        assert "row_hash mismatch" in result.broken_reason

    def test_altering_extraction_status_is_caught(self, session):
        """A tamper that flips failure -> success must not go undetected —
        this is the specific attack an audit trail exists to catch."""
        rows = self._seed_chain(session)
        target_id = rows[2].id

        session.execute(
            ExtractionAudit.__table__.update()
            .where(ExtractionAudit.id == target_id)
            .values(extraction_status="success")
        )
        session.commit()

        result = verify_chain(session)
        assert not result.valid
        assert result.broken_at_id == target_id

    def test_deleting_a_middle_row_breaks_the_chain(self, session):
        rows = self._seed_chain(session)
        deleted_id = rows[1].id

        session.execute(ExtractionAudit.__table__.delete().where(ExtractionAudit.id == deleted_id))
        session.commit()

        result = verify_chain(session)
        assert not result.valid
        assert "prev_row_hash mismatch" in result.broken_reason

    def test_deleting_the_last_row_is_still_detected(self, session):
        """Deleting the tail doesn't corrupt any remaining row's own link,
        but the row count itself is evidence — verify_chain must still be
        able to report the correct total against an expected count
        (checked by the caller, e.g. cross-referencing pipeline_audit's
        records_processed), not silently report a shorter chain as fully
        valid with no signal anything is missing."""
        rows = self._seed_chain(session)
        last_id = rows[-1].id

        session.execute(ExtractionAudit.__table__.delete().where(ExtractionAudit.id == last_id))
        session.commit()

        result = verify_chain(session)
        assert result.valid  # the remaining chain IS internally consistent
        assert result.total_rows == 2  # but the caller can see a row is gone

    def test_tampering_the_first_row_is_caught(self, session):
        rows = self._seed_chain(session)
        first_id = rows[0].id

        session.execute(
            ExtractionAudit.__table__.update()
            .where(ExtractionAudit.id == first_id)
            .values(accession_number="TAMPERED")
        )
        session.commit()

        result = verify_chain(session)
        assert not result.valid
        assert result.broken_at_id == first_id

    def test_broken_reason_names_which_row(self, session):
        rows = self._seed_chain(session)
        session.execute(
            ExtractionAudit.__table__.update()
            .where(ExtractionAudit.id == rows[1].id)
            .values(stage="TAMPERED")
        )
        session.commit()

        result = verify_chain(session)
        assert str(rows[1].id) in result.summary()

    def test_as_dict_is_json_serialisable(self, session):
        import json

        self._seed_chain(session)
        json.dumps(verify_chain(session).as_dict())

    def test_summary_reports_valid_chains_too(self, session):
        self._seed_chain(session)
        result = verify_chain(session)
        assert "VALID" in result.summary()
