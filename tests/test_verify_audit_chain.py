"""
Tests for scripts/verify_audit_chain.py — the CLI wrapper around
src.audit.verify_chain.

Invokes `main()` directly with monkeypatched `sys.argv` and `DATABASE_URL`
against a real file-backed SQLite database, following the pattern in
tests/test_scripts_ml.py and tests/test_dag.py::TestExtractionAuditWiring.
A file-backed (not in-memory) SQLite DB is required here specifically
because the script opens its own fresh engine/session per call — an
in-memory DB would be empty on the second connection.

Tamper detection itself is already covered thoroughly against the hash
chain logic in tests/test_audit.py::TestTamperDetection; these tests only
confirm the CLI correctly surfaces that result (exit code, JSON shape,
report text), not the chain algorithm itself.
"""

from __future__ import annotations

import json

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.audit import write_extraction_audit
from src.schema import Base


@pytest.fixture
def audit_db(tmp_path, monkeypatch):
    db_path = tmp_path / "verify_test.db"
    db_url = f"sqlite:///{db_path}"
    Base.metadata.create_all(create_engine(db_url))
    monkeypatch.setenv("DATABASE_URL", db_url)
    return db_url


def _seed_clean_chain(db_url):
    with Session(create_engine(db_url)) as session:
        write_extraction_audit(
            session,
            run_id="run-1",
            stage="download",
            extraction_status="success",
            accession_number="A",
            content_hash="c" * 64,
        )
        write_extraction_audit(
            session,
            run_id="run-1",
            stage="parse",
            extraction_status="failure",
            accession_number="A",
            detail="malformed XBRL",
        )
        write_extraction_audit(
            session,
            run_id="run-1",
            stage="download",
            extraction_status="success",
            accession_number="B",
        )
        session.commit()


def _tamper_row(db_url, row_id, detail="TAMPERED"):
    with Session(create_engine(db_url)) as session:
        session.execute(
            text("UPDATE extraction_audit SET detail = :detail WHERE id = :id"),
            {"detail": detail, "id": row_id},
        )
        session.commit()


def _run(monkeypatch, args):
    import scripts.verify_audit_chain as verify_audit_chain

    monkeypatch.setattr("sys.argv", ["verify_audit_chain", *args])
    return verify_audit_chain.main()


class TestValidChain:
    def test_clean_chain_exits_zero(self, audit_db, monkeypatch):
        _seed_clean_chain(audit_db)
        assert _run(monkeypatch, []) == 0

    def test_clean_chain_json_output(self, audit_db, monkeypatch, capsys):
        _seed_clean_chain(audit_db)
        exit_code = _run(monkeypatch, ["--json"])
        assert exit_code == 0

        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload == {
            "valid": True,
            "total_rows": 3,
            "broken_at_id": None,
            "broken_reason": None,
        }

    def test_clean_chain_report_text(self, audit_db, monkeypatch, capsys):
        _seed_clean_chain(audit_db)
        _run(monkeypatch, [])
        out = capsys.readouterr().out
        assert "VALID" in out
        assert "Total rows checked : 3" in out

    def test_accession_filter_prints_only_matching_rows(self, audit_db, monkeypatch, capsys):
        _seed_clean_chain(audit_db)
        _run(monkeypatch, ["--accession", "B"])
        out = capsys.readouterr().out
        assert "Rows for accession B" in out
        assert "malformed XBRL" not in out

    def test_accession_filter_still_verifies_full_chain(self, audit_db, monkeypatch, capsys):
        """--accession only filters what's printed — the whole chain (all
        accessions) is still walked and verified, per the flag's help text."""
        _seed_clean_chain(audit_db)
        _run(monkeypatch, ["--accession", "B"])
        out = capsys.readouterr().out
        assert "Total rows checked : 3" in out

    def test_unknown_accession_reports_none_found(self, audit_db, monkeypatch, capsys):
        _seed_clean_chain(audit_db)
        exit_code = _run(monkeypatch, ["--accession", "nonexistent"])
        assert exit_code == 0
        out = capsys.readouterr().out
        assert "(none found)" in out


class TestBrokenChain:
    def test_tampered_row_exits_one(self, audit_db, monkeypatch):
        _seed_clean_chain(audit_db)
        _tamper_row(audit_db, row_id=2)
        assert _run(monkeypatch, []) == 1

    def test_tampered_row_json_output_names_the_row(self, audit_db, monkeypatch, capsys):
        _seed_clean_chain(audit_db)
        _tamper_row(audit_db, row_id=2)
        exit_code = _run(monkeypatch, ["--json"])
        assert exit_code == 1

        payload = json.loads(capsys.readouterr().out)
        assert payload["valid"] is False
        assert payload["broken_at_id"] == 2
        assert payload["broken_reason"]

    def test_tampered_row_report_text(self, audit_db, monkeypatch, capsys):
        _seed_clean_chain(audit_db)
        _tamper_row(audit_db, row_id=2)
        _run(monkeypatch, [])
        out = capsys.readouterr().out
        assert "BROKEN" in out
        assert "Broken at row id   : 2" in out

    def test_tampered_row_still_prints_the_tampered_value(self, audit_db, monkeypatch, capsys):
        _seed_clean_chain(audit_db)
        _tamper_row(audit_db, row_id=2)
        _run(monkeypatch, ["--accession", "A"])
        out = capsys.readouterr().out
        assert "TAMPERED" in out


class TestEmptyChain:
    def test_no_rows_is_valid_and_exits_zero(self, audit_db, monkeypatch):
        assert _run(monkeypatch, []) == 0

    def test_no_rows_json_output(self, audit_db, monkeypatch, capsys):
        exit_code = _run(monkeypatch, ["--json"])
        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload == {
            "valid": True,
            "total_rows": 0,
            "broken_at_id": None,
            "broken_reason": None,
        }
