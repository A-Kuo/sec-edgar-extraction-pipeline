"""
Benchmark the SEC EDGAR pipeline end-to-end in mock mode.

Runs the full DAG with MOCK_EDGAR=true, instruments task execution,
and outputs per-stage and total timing.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Task callables will be patched with this decorator
_task_timings: dict[str, float] = {}


def _time_task(task_name: str) -> Callable:
    """Decorator to measure task execution time."""

    def decorator(func: Callable) -> Callable:
        def wrapper(**kwargs: Any) -> Any:
            start = time.time()
            try:
                result = func(**kwargs)
                return result
            finally:
                elapsed = time.time() - start
                _task_timings[task_name] = elapsed

        return wrapper

    return decorator


def benchmark() -> None:
    """Run the pipeline end-to-end in mock mode with timing instrumentation."""
    os.environ["MOCK_EDGAR"] = "true"

    # _task_timings is module-global so the decorator can reach it. Reset it
    # here so a second call in the same interpreter reports only its own run
    # rather than inheriting stale stages from the first.
    _task_timings.clear()

    from dags.edgar_pipeline import (
        download_raw_documents,
        fetch_new_filings,
        load_to_warehouse,
        parse_xbrl_facts,
        score_anomalies,
        update_audit_trail,
        validate_quality_gates,
    )

    print("Benchmarking SEC EDGAR pipeline (mock mode)...\n")

    # Patch task functions with timing decorators
    tasks = [
        ("fetch_new_filings", fetch_new_filings),
        ("download_raw_documents", download_raw_documents),
        ("parse_xbrl_facts", parse_xbrl_facts),
        ("validate_quality_gates", validate_quality_gates),
        ("score_anomalies", score_anomalies),
        ("load_to_warehouse", load_to_warehouse),
        ("update_audit_trail", update_audit_trail),
    ]

    # Create a mock context that mimics Airflow
    mock_context: dict[str, Any] = {
        "run_id": f"benchmark_{datetime.utcnow().isoformat()}",
        "ti": MockTaskInstance(),
        "dag_run": MockDAGRun(),
    }

    overall_start = time.time()
    filings = None

    for task_name, task_func in tasks:
        try:
            start = time.time()
            if task_name == "fetch_new_filings":
                filings = task_func(**mock_context)
            elif task_name == "download_raw_documents":
                mock_context["ti"].xcom_push(key="filings", value=filings)
                download_raw_documents(**mock_context)
            elif task_name == "parse_xbrl_facts":
                mock_context["ti"].xcom_push(
                    key="downloaded_accessions",
                    value=["0000320193-24-000123", "0000320193-23-000077"],
                )
                parse_xbrl_facts(**mock_context)
            elif task_name == "validate_quality_gates":
                mock_context["ti"].xcom_push(
                    key="facts",
                    value=[
                        {
                            "accession_number": "0000320193-24-000123",
                            "fact_name": "us-gaap:Revenues",
                            "fact_value": 391035000000,
                            "unit": "USD",
                            "period_start": "2023-10-01",
                            "period_end": "2024-09-28",
                            "segment": None,
                        },
                        {
                            "accession_number": "0000320193-24-000123",
                            "fact_name": "us-gaap:NetIncomeLoss",
                            "fact_value": 93736000000,
                            "unit": "USD",
                            "period_start": "2023-10-01",
                            "period_end": "2024-09-28",
                            "segment": None,
                        },
                    ],
                )
                validate_quality_gates(**mock_context)
            elif task_name == "score_anomalies":
                score_anomalies(**mock_context)
            elif task_name == "load_to_warehouse":
                load_to_warehouse(**mock_context)
            elif task_name == "update_audit_trail":
                update_audit_trail(**mock_context)

            elapsed = time.time() - start
            _task_timings[task_name] = elapsed
        except Exception as e:
            if "AirflowSkipException" in str(type(e).__name__):
                elapsed = time.time() - start
                _task_timings[task_name] = elapsed
                print(f"  {task_name:<35} {elapsed:>8.3f}s  (skipped)")
            else:
                logger.exception("Task %s failed", task_name)
                print(f"  {task_name:<35} ERROR: {e}")

    overall_elapsed = time.time() - overall_start

    print("\n" + "=" * 50)
    print("Timing Breakdown:")
    print("=" * 50)

    task_display_names = {
        "fetch_new_filings": "fetch_new_filings",
        "download_raw_documents": "download_raw_documents",
        "parse_xbrl_facts": "parse_xbrl_facts",
        "validate_quality_gates": "validate_quality_gates",
        "score_anomalies": "score_anomalies",
        "load_to_warehouse": "load_to_warehouse",
        "update_audit_trail": "update_audit_trail",
    }

    total_staged = 0.0
    for task_name in [t[0] for t in tasks]:
        if task_name in _task_timings:
            duration = _task_timings[task_name]
            total_staged += duration
            print(f"  {task_display_names.get(task_name, task_name):<35} {duration:>8.3f}s")

    print("=" * 50)
    print(f"  {'Total':<35} {overall_elapsed:>8.3f}s")
    print("=" * 50)

    # Write benchmark results to JSON
    benchmark_results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "mode": "mock",
        "durations_seconds": _task_timings,
        "total_seconds": overall_elapsed,
    }

    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    results_file = artifacts_dir / "benchmark_results.json"

    with open(results_file, "w") as f:
        json.dump(benchmark_results, f, indent=2)

    print(f"\nResults written to {results_file}")


class MockTaskInstance:
    """Mock Airflow TaskInstance for local benchmarking."""

    def __init__(self) -> None:
        self.xcom_data: dict[str, Any] = {}

    def xcom_push(self, key: str, value: Any) -> None:
        self.xcom_data[key] = value

    def xcom_pull(self, key: str, task_ids: str | None = None) -> Any:
        return self.xcom_data.get(key)


class MockDAGRun:
    """Mock Airflow DAGRun for local benchmarking."""

    def __init__(self) -> None:
        self.tasks: list[Any] = []

    def get_task_instances(self) -> list[Any]:
        return self.tasks


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s - %(levelname)s - %(message)s",
    )
    try:
        benchmark()
    except Exception:
        logger.exception("Benchmark failed")
        sys.exit(1)
