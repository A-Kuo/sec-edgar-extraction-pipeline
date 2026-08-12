"""
Shared pytest fixtures for the SEC EDGAR extraction pipeline test suite.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def sample_ixbrl_html() -> str:
    """Return the contents of the sample iXBRL fixture file."""
    return (FIXTURES_DIR / "sample_ixbrl.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# ML fixtures
#
# Session-scoped: generating a corpus and fitting a forest costs a second or so,
# and every ML test wants the same one. Nothing mutates them — the corruption
# helpers deep-copy their input — so sharing is safe.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def synthetic_corpus():
    """A small seeded corpus of well-formed filings."""
    from src.ml.synthetic import generate_corpus

    return generate_corpus(n_companies=40, years_per_company=4, seed=7)


@pytest.fixture(scope="session")
def feature_matrix(synthetic_corpus):
    """Features built from ``synthetic_corpus``."""
    from src.ml.features import build_features

    return build_features(synthetic_corpus.facts, synthetic_corpus.filings)


@pytest.fixture(scope="session")
def fitted_detector(feature_matrix):
    """A detector fitted on ``feature_matrix``."""
    from src.ml.model import AnomalyDetector

    return AnomalyDetector(seed=42).fit(feature_matrix)


@pytest.fixture(scope="session")
def sample_ixbrl_bytes(sample_ixbrl_html: str) -> bytes:
    return sample_ixbrl_html.encode("utf-8")


# ---------------------------------------------------------------------------
# Airflow test environment
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def airflow_test_env(tmp_path_factory):
    """
    Configure minimal Airflow environment variables before any test runs.
    Uses a throwaway file-backed SQLite database so no Postgres is needed.

    A true in-memory URL (``sqlite:///:memory:``) looks tempting here but
    Airflow 3 rejects it outright: ``configure_orm()`` requires an absolute
    path and raises ``AirflowConfigException`` before a single test runs. That
    went unnoticed as long as ``tests/test_dag.py`` was the only DAG test,
    because it mocks Airflow away and never calls ``settings.initialize()``.
    ``tests/test_dag_import.py`` spawns a subprocess that imports the real
    ``airflow`` package and inherits this env var, so it is what caught it.
    """
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    os.environ.setdefault("AIRFLOW_HOME", str(airflow_home))
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        f"sqlite:///{airflow_home}/unittests.db",
    )
    os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ["MOCK_EDGAR"] = "true"
    yield
