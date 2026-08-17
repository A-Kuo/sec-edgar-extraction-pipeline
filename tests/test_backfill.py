"""
Tests for the resumable/incremental checkpointing added to scripts/backfill.py.

Covers: checkpoint file round-tripping, --resume skipping already-completed
accessions, and --since-last-run overriding --start-date per CIK. Uses
dry_run=True throughout so no database is touched — these tests only
exercise the checkpoint/filtering logic, not the ingestion write path.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from scripts import backfill


# ---------------------------------------------------------------------------
# Checkpoint persistence
# ---------------------------------------------------------------------------


class TestCheckpointPersistence:
    def test_load_missing_file_returns_empty_dict(self, tmp_path):
        path = tmp_path / "does_not_exist.json"
        assert backfill._load_checkpoint(str(path)) == {}

    def test_save_then_load_round_trips(self, tmp_path):
        path = tmp_path / "checkpoint.json"
        data = {"320193": {"completed": ["0000320193-24-000123"], "last_filing_date": "2024-11-01"}}

        backfill._save_checkpoint(str(path), data)
        loaded = backfill._load_checkpoint(str(path))

        assert loaded == data

    def test_corrupt_file_returns_empty_dict_instead_of_raising(self, tmp_path):
        path = tmp_path / "checkpoint.json"
        path.write_text("{not valid json", encoding="utf-8")

        assert backfill._load_checkpoint(str(path)) == {}

    def test_save_is_atomic_no_tmp_file_left_behind(self, tmp_path):
        path = tmp_path / "checkpoint.json"
        backfill._save_checkpoint(str(path), {"320193": {"completed": [], "last_filing_date": None}})

        assert path.exists()
        assert not (tmp_path / "checkpoint.json.tmp").exists()


# ---------------------------------------------------------------------------
# --resume / --since-last-run filtering (dry-run only, no DB writes)
# ---------------------------------------------------------------------------


_SUBMISSIONS = {
    "tickers": ["AAPL"],
    "filings": {
        "recent": {
            "accessionNumber": [
                "0000320193-24-000123",
                "0000320193-23-000077",
                "0000320193-22-000055",
            ],
            "form": ["10-K", "10-Q", "10-Q"],
            "filingDate": ["2024-11-01", "2023-08-04", "2022-05-02"],
            "reportDate": ["2024-09-28", "2023-07-01", "2022-04-01"],
        }
    },
}


def _fake_client() -> MagicMock:
    client = MagicMock()
    client.get_company_filings.return_value = _SUBMISSIONS
    return client


class TestResumeFiltering:
    def test_without_resume_all_candidates_in_range_are_returned(self):
        checkpoint: dict = {}
        count, facts = backfill._ingest_cik(
            cik="320193",
            start_date="2010-01-01",
            end_date="2024-12-31",
            form_types=["10-K", "10-Q"],
            dry_run=True,
            client=_fake_client(),
            parser=None,
            engine=None,
            cache=None,
            checkpoint=checkpoint,
            checkpoint_path="unused.json",
            resume=False,
            since_last_run=False,
        )
        assert count == 3
        assert facts == 0

    def test_resume_skips_already_completed_accessions(self):
        checkpoint = {
            "320193": {
                "completed": ["0000320193-24-000123", "0000320193-23-000077"],
                "last_filing_date": "2024-11-01",
            }
        }
        count, _ = backfill._ingest_cik(
            cik="320193",
            start_date="2010-01-01",
            end_date="2024-12-31",
            form_types=["10-K", "10-Q"],
            dry_run=True,
            client=_fake_client(),
            parser=None,
            engine=None,
            cache=None,
            checkpoint=checkpoint,
            checkpoint_path="unused.json",
            resume=True,
            since_last_run=False,
        )
        # Only the one accession NOT in "completed" should remain.
        assert count == 1

    def test_since_last_run_overrides_start_date_with_checkpoint(self):
        checkpoint = {
            "320193": {
                "completed": ["0000320193-22-000055"],
                "last_filing_date": "2023-08-04",
            }
        }
        count, _ = backfill._ingest_cik(
            cik="320193",
            start_date="2010-01-01",  # would include all 3 filings on its own
            end_date="2024-12-31",
            form_types=["10-K", "10-Q"],
            dry_run=True,
            client=_fake_client(),
            parser=None,
            engine=None,
            cache=None,
            checkpoint=checkpoint,
            checkpoint_path="unused.json",
            resume=True,
            since_last_run=True,
        )
        # effective_start_date becomes 2023-08-04: excludes the 2022 filing,
        # includes the 2023 and 2024 ones (2023-08-04 itself is inclusive).
        assert count == 2

    def test_unknown_cik_starts_with_empty_checkpoint_state(self):
        """A CIK never seen before must not raise — it just has no prior progress."""
        checkpoint: dict = {}
        count, _ = backfill._ingest_cik(
            cik="320193",
            start_date="2024-01-01",
            end_date="2024-12-31",
            form_types=["10-K"],
            dry_run=True,
            client=_fake_client(),
            parser=None,
            engine=None,
            cache=None,
            checkpoint=checkpoint,
            checkpoint_path="unused.json",
            resume=True,
            since_last_run=True,
        )
        assert count == 1
        assert "320193" in checkpoint  # _ingest_cik initializes state via setdefault
