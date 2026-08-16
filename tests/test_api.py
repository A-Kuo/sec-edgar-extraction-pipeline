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

from fastapi.testclient import TestClient

from api.main import app, get_cache, get_db

# ---------------------------------------------------------------------------
# Shared row fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 11, 1, 12, 0, 0)
_TODAY = date(2024, 11, 1)

_FILING_ROW = (
    "0000320193-24-000123",  # accession_number
    "320193",  # cik
    "AAPL",  # ticker
    "10-K",  # filing_type
    date(2024, 11, 1),  # filing_date
    date(2024, 9, 28),  # period_of_report
    5_000_000,  # file_size_bytes
    _NOW,  # ingested_at
)

_FACT_ROW = (
    1,  # id
    "0000320193-24-000123",  # accession_number
    "us-gaap:Revenues",  # fact_name
    391_035_000_000.0,  # fact_value
    "USD",  # unit
    date(2023, 10, 1),  # period_start
    date(2024, 9, 28),  # period_end
    None,  # segment
    _NOW,  # parsed_at
)

_META_ROW = ("10-K", date(2024, 9, 28))

_TS_ROW = (
    "0000320193-24-000123",  # accession_number
    "10-K",  # filing_type
    date(2023, 10, 1),  # period_start
    date(2024, 9, 28),  # period_end
    391_035_000_000.0,  # fact_value
    "USD",  # unit
    None,  # segment
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
# GET /anomalies/{ticker}
# ---------------------------------------------------------------------------

_ANOMALY_ROW = (
    "0000320193-24-000123",  # accession_number
    "10-K",  # filing_type
    _TODAY,  # filing_date
    0.82,  # score
    0.40,  # model_score
    0.82,  # rule_score
    True,  # is_anomaly
    "rule",  # triggered_by
    "score=0.820 — eps_reconciliation_failed: ...",  # reason
    '[{"rule_id": "eps_reconciliation_failed", "severity": 0.82, "message": "..."}]',
    "v20260101T000000Z-abcd1234",  # model_version
    _NOW,  # scored_at
)


class TestAnomalies:
    def setup_method(self):
        _clear_overrides()

    def test_returns_200_with_rows(self):
        db = _mock_db_execute(fetchall=[_ANOMALY_ROW])
        db.execute.return_value.scalar_one.return_value = 1
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        resp = client.get("/anomalies/AAPL")
        assert resp.status_code == 200

    def test_response_schema(self):
        db = _mock_db_execute(fetchall=[_ANOMALY_ROW])
        db.execute.return_value.scalar_one.return_value = 1
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        body = client.get("/anomalies/AAPL").json()
        assert body["ticker"] == "AAPL"
        assert body["total"] == 1
        item = body["items"][0]
        assert item["accession_number"] == "0000320193-24-000123"
        assert item["is_anomaly"] is True
        assert item["triggered_by"] == "rule"
        assert item["rule_violations"][0]["rule_id"] == "eps_reconciliation_failed"

    def test_404_when_nothing_scored(self):
        db = _mock_db_execute(fetchall=[])
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        resp = client.get("/anomalies/UNSCORED")
        assert resp.status_code == 404

    def test_only_anomalies_filter_is_passed_through(self):
        db = _mock_db_execute(fetchall=[_ANOMALY_ROW])
        db.execute.return_value.scalar_one.return_value = 1
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        resp = client.get("/anomalies/AAPL?only_anomalies=true")
        assert resp.status_code == 200
        # is_anomaly = TRUE must appear in at least one executed query
        queries = [str(call.args[0]) for call in db.execute.call_args_list]
        assert any("is_anomaly = TRUE" in q for q in queries)

    def test_min_score_query_param_bounds(self):
        db = _mock_db_execute(fetchall=[_ANOMALY_ROW])
        db.execute.return_value.scalar_one.return_value = 1
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        assert client.get("/anomalies/AAPL?min_score=1.5").status_code == 422
        assert client.get("/anomalies/AAPL?min_score=-0.1").status_code == 422

    def test_rule_violations_decoded_from_json_string(self):
        """Raw text() queries return JSON as a string on SQLite, unlike PostgreSQL."""
        db = _mock_db_execute(fetchall=[_ANOMALY_ROW])
        db.execute.return_value.scalar_one.return_value = 1
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        body = client.get("/anomalies/AAPL").json()
        assert isinstance(body["items"][0]["rule_violations"], list)

    def test_null_rule_violations_becomes_empty_list(self):
        row = list(_ANOMALY_ROW)
        row[9] = None
        db = _mock_db_execute(fetchall=[tuple(row)])
        db.execute.return_value.scalar_one.return_value = 1
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        body = client.get("/anomalies/AAPL").json()
        assert body["items"][0]["rule_violations"] == []

    def test_pagination_params_accepted(self):
        db = _mock_db_execute(fetchall=[_ANOMALY_ROW])
        db.execute.return_value.scalar_one.return_value = 1
        cache = _mock_cache()
        client = _override(db=db, cache=cache)
        body = client.get("/anomalies/AAPL?limit=10&offset=5").json()
        assert body["limit"] == 10
        assert body["offset"] == 5


# ---------------------------------------------------------------------------
# GET /model/current
# ---------------------------------------------------------------------------


class TestModelInfo:
    def setup_method(self):
        _clear_overrides()

    def _make_registry(self, tmp_path):
        from src.ml.features import build_features
        from src.ml.model import AnomalyDetector
        from src.ml.registry import ModelRegistry

        facts = [
            {
                "accession_number": "A",
                "fact_name": "us-gaap:Revenues",
                "fact_value": 100.0,
                "period_end": "2024-12-31",
            },
            {
                "accession_number": "B",
                "fact_name": "us-gaap:Revenues",
                "fact_value": 200.0,
                "period_end": "2024-12-31",
            },
        ]
        matrix = build_features(facts)
        detector = AnomalyDetector(seed=1).fit(matrix)
        registry = ModelRegistry(tmp_path / "models")
        metadata = registry.register(detector, matrix, metrics={"recall": 0.9})
        registry.promote(metadata.version)
        return registry, metadata

    def test_returns_metadata_of_promoted_model(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MODEL_REGISTRY_ROOT", str(tmp_path / "models"))
        _, metadata = self._make_registry(tmp_path)

        client = TestClient(app)
        resp = client.get("/model/current")
        assert resp.status_code == 200
        body = resp.json()
        assert body["version"] == metadata.version
        assert body["artifact_sha256"] == metadata.artifact_sha256
        assert body["verified"] is True

    def test_404_when_nothing_promoted(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MODEL_REGISTRY_ROOT", str(tmp_path / "empty"))
        client = TestClient(app)
        resp = client.get("/model/current")
        assert resp.status_code == 404

    def test_tampered_artifact_reports_unverified_not_500(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MODEL_REGISTRY_ROOT", str(tmp_path / "models"))
        registry, metadata = self._make_registry(tmp_path)
        artifact = registry.artifact_path(metadata.version)
        artifact.write_bytes(artifact.read_bytes() + b"tampered")

        client = TestClient(app)
        resp = client.get("/model/current")
        assert resp.status_code == 200
        assert resp.json()["verified"] is False


# ---------------------------------------------------------------------------
# GET /audit/{accession}
#
# Backed by a real SQLite session (schema created via Base.metadata.create_all,
# rows written via src.audit.write_extraction_audit) rather than a MagicMock —
# this endpoint reads via a raw text() query, and a MagicMock would only prove
# the endpoint calls .execute() with something, not that the response actually
# round-trips real column types (in particular created_at) through Pydantic.
# ---------------------------------------------------------------------------


class TestAccessionAudit:
    def setup_method(self):
        _clear_overrides()

    def _seeded_session(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as RealSession
        from sqlalchemy.pool import StaticPool

        from src.audit import write_extraction_audit
        from src.schema import Base

        # StaticPool + check_same_thread=False: a plain in-memory SQLite URL
        # gives each new connection a *fresh* empty database, and the default
        # pool would hand out a fresh connection per checkout — the schema
        # created below would vanish before the endpoint's own query ran.
        # TestClient also dispatches the request handler onto a different
        # thread than this one, which check_same_thread=False permits.
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        session = RealSession(engine)
        write_extraction_audit(
            session,
            run_id="run-1",
            stage="download",
            extraction_status="success",
            accession_number="A",
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
        return session

    def test_returns_200_with_history(self):
        cache = _mock_cache()
        client = _override(db=self._seeded_session(), cache=cache)
        resp = client.get("/audit/A")
        assert resp.status_code == 200

    def test_only_the_requested_accessions_rows_are_returned(self):
        cache = _mock_cache()
        client = _override(db=self._seeded_session(), cache=cache)
        body = client.get("/audit/A").json()
        assert body["accession_number"] == "A"
        assert body["total"] == 2
        assert all(item["run_id"] == "run-1" for item in body["items"])

    def test_newest_first(self):
        cache = _mock_cache()
        client = _override(db=self._seeded_session(), cache=cache)
        body = client.get("/audit/A").json()
        assert body["items"][0]["stage"] == "parse"
        assert body["items"][1]["stage"] == "download"

    def test_rows_carry_the_hash_chain_fields(self):
        cache = _mock_cache()
        client = _override(db=self._seeded_session(), cache=cache)
        item = client.get("/audit/A").json()["items"][0]
        assert len(item["row_hash"]) == 64
        assert item["prev_row_hash"] is not None  # second row written, chained to the first

    def test_failure_row_carries_its_detail(self):
        cache = _mock_cache()
        client = _override(db=self._seeded_session(), cache=cache)
        parse_row = next(i for i in client.get("/audit/A").json()["items"] if i["stage"] == "parse")
        assert parse_row["extraction_status"] == "failure"
        assert parse_row["detail"] == "malformed XBRL"

    def test_404_for_an_accession_with_no_audit_history(self):
        cache = _mock_cache()
        client = _override(db=self._seeded_session(), cache=cache)
        resp = client.get("/audit/NEVER-SEEN")
        assert resp.status_code == 404

    def test_does_not_leak_another_accessions_rows(self):
        cache = _mock_cache()
        client = _override(db=self._seeded_session(), cache=cache)
        body = client.get("/audit/B").json()
        assert body["total"] == 1
        assert body["items"][0]["stage"] == "download"


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
        assert "/anomalies/{ticker}" in paths
        assert "/model/current" in paths
        assert "/audit/{accession}" in paths
        assert "/trigger/{ticker}" in paths

    def test_all_endpoints_have_summaries(self):
        client = TestClient(app)
        paths = client.get("/openapi.json").json()["paths"]
        for path, methods in paths.items():
            for method, spec in methods.items():
                if method in ("get", "post"):
                    assert "summary" in spec, f"Missing summary: {method.upper()} {path}"
