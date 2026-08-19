"""
Unit tests for the pipeline benchmark runner.

``scripts/benchmark_pipeline.py`` is a developer tool rather than shipped
runtime code, but it sits inside the coverage-gated scope and it drives every
DAG task callable — so a break here is an early warning that a task signature
changed. The end-to-end test runs the real thing in mock mode, redirected at a
temporary working directory so it cannot litter the repo.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from scripts.benchmark_pipeline import (
    MockDAGRun,
    MockTaskInstance,
    _task_timings,
    _time_task,
    benchmark,
)


class TestMockTaskInstance:
    def test_push_then_pull_round_trips(self) -> None:
        ti = MockTaskInstance()
        ti.xcom_push(key="filings", value=[{"accession_number": "0000320193-24-000123"}])
        assert ti.xcom_pull(key="filings") == [{"accession_number": "0000320193-24-000123"}]

    def test_pull_of_unset_key_is_none(self) -> None:
        assert MockTaskInstance().xcom_pull(key="never_pushed") is None

    def test_pull_ignores_task_ids_argument(self) -> None:
        """The real signature accepts task_ids; the mock keys only on `key`."""
        ti = MockTaskInstance()
        ti.xcom_push(key="facts", value=[1, 2, 3])
        assert ti.xcom_pull(key="facts", task_ids="parse_xbrl_facts") == [1, 2, 3]

    def test_push_overwrites_existing_key(self) -> None:
        ti = MockTaskInstance()
        ti.xcom_push(key="k", value="first")
        ti.xcom_push(key="k", value="second")
        assert ti.xcom_pull(key="k") == "second"


class TestMockDAGRun:
    def test_starts_with_no_task_instances(self) -> None:
        assert MockDAGRun().get_task_instances() == []


class TestTimeTask:
    def test_decorator_records_elapsed_time(self) -> None:
        _task_timings.pop("sentinel_task", None)

        @_time_task("sentinel_task")
        def work(**kwargs: object) -> str:
            return "done"

        work()
        assert "sentinel_task" in _task_timings
        assert _task_timings["sentinel_task"] >= 0.0

    def test_timing_is_recorded_even_when_the_task_raises(self) -> None:
        """The decorator times in a finally block — a failing task still reports."""
        _task_timings.pop("failing_task", None)

        @_time_task("failing_task")
        def boom(**kwargs: object) -> None:
            raise ValueError("task blew up")

        with pytest.raises(ValueError, match="task blew up"):
            boom()

        assert "failing_task" in _task_timings


class TestBenchmarkEndToEnd:
    def test_runs_in_mock_mode_and_writes_results(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("MOCK_EDGAR", "true")

        benchmark()

        results_file = tmp_path / "artifacts" / "benchmark_results.json"
        assert results_file.exists(), "benchmark should write artifacts/benchmark_results.json"

        payload = json.loads(results_file.read_text())
        assert payload["mode"] == "mock"
        assert payload["total_seconds"] >= 0.0
        assert isinstance(payload["durations_seconds"], dict)
        assert payload["durations_seconds"], "at least one stage should be timed"

        # Every timed stage must be a real DAG task name, not a typo.
        known_stages = {
            "fetch_new_filings",
            "download_raw_documents",
            "parse_xbrl_facts",
            "validate_quality_gates",
            "score_anomalies",
            "load_to_warehouse",
            "update_audit_trail",
        }
        assert set(payload["durations_seconds"]) <= known_stages

        assert "Timing Breakdown" in capsys.readouterr().out

    def test_forces_mock_mode_even_if_unset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """benchmark() sets MOCK_EDGAR itself — it must never hit the live SEC API."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MOCK_EDGAR", raising=False)

        benchmark()

        assert os.environ["MOCK_EDGAR"] == "true"
