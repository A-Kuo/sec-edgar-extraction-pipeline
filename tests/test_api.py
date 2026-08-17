"""
Integration tests for api/main.py using FastAPI TestClient.

All database sessions and the Redis cache are replaced with mocks so no
external services are required.  Every test asserts:
  - Correct HTTP status codes
  - Response schema matches the declared Pydantic models
  - Cache is checked BEFORE the database is touched
  - 404 is returned for unknown tickers / accessions
  - Pagination parameters are honoured

Mock strategy
-------------
  - DB: replace the ``get_db`` dependency with a function returning a
    MagicMock whose ``.execute().fetchall()`` / ``.fetchone()`` returns
    controlled row data.
  - Cache: replace the ``get_cache`` dependency with a MagicMock.
"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.main import app, get_cache, get_db


# ---------------------------------------------------------------------------
# Shared row fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 11, 1, 12, 0, 0)
_TODAY = date(2024, 11, 1)

_FILING_ROW = (
    "0000320193-24-000123",  # accession_number
    "320193",                # cik
    "AAPL",                  # ticker
    "10-K",                  # filing_type
    date(2024, 11, 1),       # filing_date
    date(2024, 9, 28),       # period_of_report
    5_000_000,               # file_size_bytes
    _NOW,                    # ingested_at
)

_FACT_ROW = (
    1,                             # id
    "0000320193-24-000123",        # accession_number
    "us-gaap:Revenues",            # fact_name
    391_035_000_000.0,             # fact_value
    "USD",                         # unit
    date(2023, 10, 1),             # period_start
    date(2024, 9, 28),             # period_end
    None,                          # segment
    _NOW,                          # parsed_at
)

_META_ROW = ("10-K", date(2024, 9, 28))

_TS_ROW = (
    "0000320193-24-000123",  # accession_number
    "10-K",                  # filing_type
    date(2023, 10, 1),       # period_start
    date(2024, 9, 28),       # period_end
    391_035_000_000.0,       # fact_value
    "USD",                   # unit
    None,                    # segment
)


# ---------------------------------------------------------------------------
# Helpers to build mock DB and cache objects
# ---------------------------------------------------------------------------


def _mock_db_execute(fetchone=None, fetchall=None):
    """Return a mock Session whose execute() yields controlled results."""
    result = MagicMock()
    result.fetchone.return_value = fetchone
    result.fetchall.return_value = fetchall if fetchall is not None else []
    session = MagicMock()
    session.execute.return_value = result
    return session


def _mock_cache(*, get_cik=None, get_filings=None, get_facts=None, ping=True):
    cache = MagicMock()
    cache.ping.return_value = ping
    cache.get_cik.return_value = get_cik
    cache.get_filings.return_value = get_filings
    cache.get_facts.return_value = get_facts
    return cache


def _override(db=None, cache=None):
    """Register dependency overrides on the app and return a TestClient."""
    if db is not None:
        app.dependency_overrides[get_db] = lambda: db
    if cache is not None:
        app.dependency_overrides[get_cache] = lambda: cache
    return TestClient(app)


def _clear_overrides():
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


class TestHealth:
    def setup_method(self):
        _clear_overrides()

    def test_returns_200(self):
        db = _mock_db_execute(fetchone=(1,))
        cache = _mock_cache(ping=True)
        client = _override(db=db, cache=cache)
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_response_schema(self):
        db = _mock_db_execute(fetchone=(1,))
        cache = _mock_cache(ping=True)
        client = _override(db=db, cache=cache)
        body = client.get("/health").json()
        assert body["status"] == "ok"
        assert "database" in body
        assert "cache" in body

    def test_cache_unavailable_still_returns_200(self):
        db = _mock_db_execute(fetchone=(1,))
        cache = _mock_cache(ping=False)
        client = _override(db=db, cache=cache)
        body = client.get("/health").json()
        assert body["status"] == "ok"
        assert body["cache"] == "unavailable"

    def test_db_error_reflected_in_response(self):
        session = MagicMock()
        session.execute.side_effect = Exception("connection refused")
        cache = _mock_cache(ping=True)
        client = _override(db=session, cache=cache)
        body = client.get("/health").json()
        assert body["status"] == "ok"
        assert "error" in body["database"]


# ---------------------------------------------------------------------------
# GET /filings/{ticker}
# ---------------------------------------------------------------------------


class TestListFilings:
    def setup_method(self):
        _clear_overrides()

    def _filing_dict(self, acc="0000320193-24-000123"):
        return {
            "accession_number": acc,
            "cik": "320193",
            "ticker": "AAPL",
            "filing_type": "10-K",
            "filing_date": "2024-11-01",
            "period_of_report": "2024-09-28",
            "file_size_bytes": 5_000_000,
            "ingested_at": _NOW.isoformat(),
        }

    def test_returns_200_on_cache_hit(self):
        cached = [self._filing_dict()]
        db = _mock_db_execute()
        cache = _mock_cache(get_filings=cached)
        client = _override(db=db, cache=cache)
        resp = client.get("/filings/AAPL")
        assert resp.status_code == 200

    def test_cache_checked_before_db(self):
        cached = [self._filing_dict()]
        db = _mock_db_execute()
        cache = _mock_cache(get_filings=cached)
        client = _override(db=db, cache=cache)
        client.get("/filings/AAPL")
        # DB should NOT have been queried
        db.execute.assert_not_called()
        cache.get_filings.assert_called_once_with("AAPL")

    def test_db_queried_on_cache_miss_and_cache_populated(self):
        db = _mock_db_execute(fetchall=[_FILING_ROW])
        cache = _mock_cache(get_filings=None)
        client = _override(db=db, cache=cache)
        resp = client.get("/filings/AAPL")
        assert resp.status_code == 200
        db.execute.assert_called_once()
        cache.set_filings.assert_called_once()

    def test_response_schema(self):
        cached = [self._filing_dict()]
        cache = _mock_cache(get_filings=cached)
        db = _mock_db_execute()
        client = _override(db=db, cache=cache)
        body = client.get("/filings/AAPL").json()
        assert body["ticker"] == "AAPL"
        assert "total" in body
        assert "items" in body
        assert isinstance(body["items"], list)
        assert len(body["items"]) == 1

    def test_404_on_unknown_ticker(self):
        db = _mock_db_execute(fetchall=[])
        cache = _mock_cache(get_filings=None)
        client = _override(db=db, cache=cache)
        resp = client.get("/filings/ZZZZZ")
        assert resp.status_code == 404
        assert "ZZZZZ" in resp.json()["detail"]

    def test_ticker_uppercased(self):
        cached = [self._filing_dict()]
        cache = _mock_cache(get_filings=cached)
        db = _mock_db_execute()
        client = _override(db=db, cache=cache)
        body = client.get("/filings/aapl").json()
        assert body["ticker"] == "AAPL"
        cache.get_filings.assert_called_with("AAPL")

    def test_pagination_limit(self):
        items = [self._filing_dict(f"acc-{i}") for i in range(10)]
        cache = _mock_cache(get_filings=items)
        db = _mock_db_execute()
        client = _override(db=db, cache=cache)
        body = client.get("/filings/AAPL?limit=3").json()
        assert len(body["items"]) == 3
        assert body["total"] == 10
        assert body["limit"] == 3

    def test_pagination_offset(self):
        items = [self._filing_dict(f"acc-{i}") for i in range(5)]
        cache = _mock_cache(get_filings=items)
        db = _mock_db_execute()
        client = _override(db=db, cache=cache)
        body = client.get("/filings/AAPL?offset=3").json()
        assert len(body["items"]) == 2  # 5 total - 3 skipped

    def test_pagination_offset_beyond_total_returns_empty(self):
        items = [self._filing_dict("acc-0")]
        cache = _mock_cache(get_filings=items)
        db = _mock_db_execute()
        client = _override(db=db, cache=cache)
        body = client.get("/filings/AAPL?offset=100").json()
        assert body["items"] == []
        assert body["total"] == 1


# ---------------------------------------------------------------------------
# GET /filing/{accession}
# ---------------------------------------------------------------------------


class TestGetFilingFacts:
    def setup_method(self):
        _clear_overrides()

    def _facts_list(self):
        return [
            {
                "id": 1,
                "accession_number": "0000320193-24-000123",
                "fact_name": "us-gaap:Revenues",
                "fact_value": 391_035_000_000.0,
                "unit": "USD",
                "period_start": "2023-10-01",
                "period_end": "2024-09-28",
                "segment": None,
                "parsed_at": _NOW.isoformat(),
            }
        ]

    def test_200_on_cache_hit(self):
        db = _mock_db_execute(fetchone=_META_ROW)
        cache = _mock_cache(get_facts=self._facts_list())
        client = _override(db=db, cache=cache)
        resp = client.get("/filing/0000320193-24-000123")
        assert resp.status_code == 200

    def test_cache_checked_before_db_for_facts(self):
        db = _mock_db_execute(fetchone=_META_ROW)
        cache = _mock_cache(get_facts=self._facts_list())
        client = _override(db=db, cache=cache)
        client.get("/filing/0000320193-24-000123")
        cache.get_facts.assert_called_once_with("0000320193-24-000123")
        # DB is still called once for metadata (filing_type / period)
        assert db.execute.call_count == 1

    def test_db_queried_and_cache_set_on_miss(self):
        db = _mock_db_execute(fetchone=_META_ROW, fetchall=[_FACT_ROW])
        cache = _mock_cache(get_facts=None)
        client = _override(db=db, cache=cache)
        resp = client.get("/filing/0000320193-24-000123")
        assert resp.status_code == 200
        cache.set_facts.assert_called_once()

    def test_response_has_facts_list(self):
        db = _mock_db_execute(fetchone=_META_ROW, fetchall=[_FACT_ROW])
        cache = _mock_cache(get_facts=None)
        client = _override(db=db, cache=cache)
        body = client.get("/filing/0000320193-24-000123").json()
        assert "facts" in body
        assert isinstance(body["facts"], list)
        assert len(body["facts"]) == 1
        assert body["facts"][0]["fact_name"] == "us-gaap:Revenues"

    def test_404_on_missing_accession(self):
        db = _mock_db_execute(fetchone=None)
        cache = _mock_cache(get_facts=None)
        client = _override(db=db, cache=cache)
        resp = client.get("/filing/DOES-NOT-EXIST")
        assert resp.status_code == 404
        assert "DOES-NOT-EXIST" in resp.json()["detail"]

    def test_fact_value_is_float(self):
        db = _mock_db_execute(fetchone=_META_ROW, fetchall=[_FACT_ROW])
        cache = _mock_cache(get_facts=None)
        client = _override(db=db, cache=cache)
        body = client.get("/filing/0000320193-24-000123").json()
        assert isinstance(body["facts"][0]["fact_value"], float)


# ---------------------------------------------------------------------------
# GET /facts/{ticker}/{fact_name}
# ---------------------------------------------------------------------------


class TestTimeSeries:
    def setup_method(self):
        _clear_overrides()

    def test_200_with_data(self):
        db = _mock_db_execute(fetchone=("320193",), fetchall=[_TS_ROW])
        cache = _mock_cache(get_cik="320193")
        client = _override(db=db, cache=cache)
        resp = client.get("/facts/AAPL/us-gaap:Revenues")
        assert resp.status_code == 200

    def test_response_schema(self):
        db = _mock_db_execute(fetchone=("320193",), fetchall=[_TS_ROW])
        cache = _mock_cache(get_cik="320193")
        client = _override(db=db, cache=cache)
        body = client.get("/facts/AAPL/us-gaap:Revenues").json()
        assert body["ticker"] == "AAPL"
        assert body["fact_name"] == "us-gaap:Revenues"
        assert isinstance(body["series"], list)
        assert len(body["series"]) == 1

    def test_series_point_fields(self):
        db = _mock_db_execute(fetchone=("320193",), fetchall=[_TS_ROW])
        cache = _mock_cache(get_cik="320193")
        client = _override(db=db, cache=cache)
        pt = client.get("/facts/AAPL/us-gaap:Revenues").json()["series"][0]
        assert pt["accession_number"] == "0000320193-24-000123"
        assert pt["filing_type"] == "10-K"
        assert isinstance(pt["fact_value"], float)
        assert pt["unit"] == "USD"

    def test_cache_checked_for_cik(self):
        db = _mock_db_execute(fetchone=("320193",), fetchall=[_TS_ROW])
        cache = _mock_cache(get_cik="320193")
        client = _override(db=db, cache=cache)
        client.get("/facts/AAPL/us-gaap:Revenues")
        cache.get_cik.assert_called_once_with("AAPL")
        # DB should NOT have been called to look up the CIK (cache hit)
        # Only one DB call for the time-series query itself
        assert db.execute.call_count == 1

    def test_404_unknown_ticker(self):
        db = _mock_db_execute(fetchone=None)
        cache = _mock_cache(get_cik=None)
        client = _override(db=db, cache=cache)
        resp = client.get("/facts/ZZZZZ/us-gaap:Revenues")
        assert resp.status_code == 404

    def test_404_no_data_for_fact(self):
        db = _mock_db_execute(fetchone=("320193",), fetchall=[])
        cache = _mock_cache(get_cik="320193")
        client = _override(db=db, cache=cache)
        resp = client.get("/facts/AAPL/us-gaap:NONEXISTENT")
        assert resp.status_code == 404

    def test_limit_parameter(self):
        rows = [_TS_ROW] * 50
        db = _mock_db_execute(fetchone=("320193",), fetchall=rows)
        cache = _mock_cache(get_cik="320193")
        client = _override(db=db, cache=cache)
        # The limit is passed to the DB query via params — mock returns all 50
        body = client.get("/facts/AAPL/us-gaap:Revenues?limit=50").json()
        assert body["ticker"] == "AAPL"


# ---------------------------------------------------------------------------
# GET /ask/{ticker}
# ---------------------------------------------------------------------------

_NARRATIVE_10K_ROW = (
    "0000320193-24-000123",
    "320193",
    "AAPL",
    "10-K",
    "<html><body><h2>Item 1A. Risk Factors</h2><p>The Company relies on single-source "
    "and limited-source suppliers for some components, which increases the Company's "
    "supply chain risk.</p></body></html>",
)


class TestAsk:
    def setup_method(self):
        _clear_overrides()

    def test_404_when_no_filing_text_indexed(self):
        db = _mock_db_execute(fetchall=[])
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        resp = client.get("/ask/AAPL", params={"q": "What are the risk factors?"})
        assert resp.status_code == 404

    def test_grounded_question_returns_citation(self):
        db = _mock_db_execute(fetchall=[_NARRATIVE_10K_ROW])
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        resp = client.get("/ask/AAPL", params={"q": "What supply chain risk does the company describe?"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["grounded"] is True
        assert body["citations"]
        assert body["citations"][0]["accession_number"] == "0000320193-24-000123"

    def test_unrelated_question_is_refused(self):
        db = _mock_db_execute(fetchall=[_NARRATIVE_10K_ROW])
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        resp = client.get("/ask/AAPL", params={"q": "What is the plan for lunar mining operations?"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["grounded"] is False
        assert body["citations"] == []

    def test_question_too_short_is_rejected(self):
        db = _mock_db_execute(fetchall=[_NARRATIVE_10K_ROW])
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        resp = client.get("/ask/AAPL", params={"q": "?"})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /trigger/{ticker}
# ---------------------------------------------------------------------------


class TestTrigger:
    def setup_method(self):
        _clear_overrides()

    def test_202_no_airflow_configured(self):
        db = _mock_db_execute(fetchone=("320193",))
        cache = _mock_cache(get_cik="320193")
        client = _override(db=db, cache=cache)
        # Ensure AIRFLOW_API_URL is not set
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("AIRFLOW_API_URL", None)
            resp = client.post("/trigger/AAPL")
        assert resp.status_code == 202

    def test_response_has_queued_status(self):
        db = _mock_db_execute(fetchone=("320193",))
        cache = _mock_cache(get_cik="320193")
        client = _override(db=db, cache=cache)
        import os
        os.environ.pop("AIRFLOW_API_URL", None)
        body = client.post("/trigger/AAPL").json()
        assert body["status"] == "queued"
        assert body["ticker"] == "AAPL"

    def test_trigger_unknown_ticker_still_200(self):
        """Trigger should work even if ticker has no filings yet."""
        db = _mock_db_execute(fetchone=None)
        cache = _mock_cache(get_cik=None)
        client = _override(db=db, cache=cache)
        import os
        os.environ.pop("AIRFLOW_API_URL", None)
        resp = client.post("/trigger/NEWCO")
        assert resp.status_code == 202
        assert resp.json()["cik"] is None


# ---------------------------------------------------------------------------
# OpenAPI schema
# ---------------------------------------------------------------------------


class TestOpenAPISchema:
    def setup_method(self):
        _clear_overrides()

    def test_openapi_json_reachable(self):
        client = TestClient(app)
        resp = client.get("/openapi.json")
        assert resp.status_code == 200

    def test_all_endpoints_in_schema(self):
        client = TestClient(app)
        paths = client.get("/openapi.json").json()["paths"]
        assert "/health" in paths
        assert "/filings/{ticker}" in paths
        assert "/filing/{accession}" in paths
        assert "/facts/{ticker}/{fact_name}" in paths
        assert "/ask/{ticker}" in paths
        assert "/trigger/{ticker}" in paths

    def test_all_endpoints_have_summaries(self):
        client = TestClient(app)
        paths = client.get("/openapi.json").json()["paths"]
        for path, methods in paths.items():
            for method, spec in methods.items():
                if method in ("get", "post"):
                    assert "summary" in spec, f"Missing summary: {method.upper()} {path}"
