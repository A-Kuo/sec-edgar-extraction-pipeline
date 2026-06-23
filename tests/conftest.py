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
    Uses an in-memory SQLite database so no Postgres is needed.
    """
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    os.environ.setdefault("AIRFLOW_HOME", str(airflow_home))
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///:memory:"
    )
    os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ["MOCK_EDGAR"] = "true"
    yield
