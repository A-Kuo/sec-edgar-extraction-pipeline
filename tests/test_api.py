import os
from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

os.environ["MOCK_EDGAR"] = "true"

from api.main import app, get_db
from src.schema import FilingRaw, FinancialFact


@pytest.fixture
def client(db_session):
    """FastAPI test client with mocked database."""

    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)


@pytest.fixture
def sample_filing(db_session):
    """Create a sample filing in the database."""
    filing = FilingRaw(
        accession_number="0000320193-23-000077",
        cik="0000320193",
        ticker="AAPL",
        company_name="Apple Inc.",
        form_type="10-K",
        filing_date=datetime(2023, 10, 27),
        period_end=datetime(2023, 9, 30),
        document_url="https://www.sec.gov/Archives/...",
        raw_html="<html></html>",
        pipeline_run_id="test-run-1",
    )
    db_session.add(filing)
    db_session.commit()
    return filing


@pytest.fixture
def sample_facts(db_session, sample_filing):
    """Create sample facts for a filing."""
    facts = [
        FinancialFact(
            accession_number="0000320193-23-000077",
            fact_name="Revenues",
            unit="USD",
            period_start=None,
            period_end=datetime(2023, 9, 30),
            value=383285000000.0,
            segment="Total",
        ),
        FinancialFact(
            accession_number="0000320193-23-000077",
            fact_name="NetIncomeLoss",
            unit="USD",
            period_start=None,
            period_end=datetime(2023, 9, 30),
            value=96995000000.0,
            segment="Total",
        ),
        FinancialFact(
            accession_number="0000320193-23-000077",
            fact_name="Assets",
            unit="USD",
            period_start=None,
            period_end=datetime(2023, 9, 30),
            value=352755000000.0,
            segment="Total",
        ),
    ]
    for fact in facts:
        db_session.add(fact)
    db_session.commit()
    return facts


class TestHealthEndpoint:
    def test_health_check_success(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data


class TestFilingsEndpoint:
    def test_get_filings_success(self, client, sample_filing, sample_facts):
        response = client.get("/filings/AAPL")
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "AAPL"
        assert len(data["filings"]) > 0
        assert data["filings"][0]["accession_number"] == "0000320193-23-000077"

    def test_get_filings_not_found(self, client):
        response = client.get("/filings/UNKN")
        assert response.status_code == 404
        assert "detail" in response.json()

    def test_get_filings_pagination(self, client, sample_filing, sample_facts, db_session):
        for i in range(15):
            filing = FilingRaw(
                accession_number=f"0000320193-23-{1000 + i:06d}",
                cik="0000320193",
                ticker="AAPL",
                company_name="Apple Inc.",
                form_type="10-K",
                filing_date=datetime(2023, 10, 27),
                period_end=datetime(2023, 9, 30),
                document_url="https://www.sec.gov/Archives/...",
                raw_html="<html></html>",
                pipeline_run_id="test-run-1",
            )
            db_session.add(filing)
        db_session.commit()

        response = client.get("/filings/AAPL?limit=5&offset=0")
        assert response.status_code == 200
        data = response.json()
        assert len(data["filings"]) <= 5
        assert data["limit"] == 5
        assert data["offset"] == 0

    def test_get_filings_cache_hit(self, client, sample_filing, sample_facts):
        response1 = client.get("/filings/AAPL")
        assert response1.status_code == 200

        with patch("api.main.cache.get_filings") as mock_cache:
            mock_cache.return_value = response1.json()["filings"]
            response2 = client.get("/filings/AAPL")
            assert response2.status_code == 200


class TestFilingFactsEndpoint:
    def test_get_filing_facts_success(self, client, sample_filing, sample_facts):
        response = client.get("/filing/0000320193-23-000077")
        assert response.status_code == 200
        data = response.json()
        assert data["accession_number"] == "0000320193-23-000077"
        assert len(data["facts"]) > 0
        assert "Revenues" in [f["fact_name"] for f in data["facts"]]

    def test_get_filing_facts_not_found(self, client):
        response = client.get("/filing/0000000000-00-000000")
        assert response.status_code == 404
        assert "detail" in response.json()

    def test_get_filing_facts_cache_hit(self, client, sample_filing, sample_facts):
        response1 = client.get("/filing/0000320193-23-000077")
        assert response1.status_code == 200

        with patch("api.main.cache.get_facts") as mock_cache:
            mock_cache.return_value = response1.json()["facts"]
            response2 = client.get("/filing/0000320193-23-000077")
            assert response2.status_code == 200


class TestFactTimeSeriesEndpoint:
    def test_get_fact_timeseries_success(self, client, sample_filing, sample_facts):
        response = client.get("/facts/AAPL/Revenues")
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "AAPL"
        assert data["fact_name"] == "Revenues"
        assert len(data["data_points"]) > 0

    def test_get_fact_timeseries_not_found_ticker(self, client):
        response = client.get("/facts/UNKN/Revenues")
        assert response.status_code == 404
        assert "detail" in response.json()

    def test_get_fact_timeseries_not_found_fact(self, client, sample_filing, sample_facts):
        response = client.get("/facts/AAPL/UnknownFact")
        assert response.status_code == 404
        assert "detail" in response.json()

    def test_get_fact_timeseries_multiple_dates(self, client, sample_filing, db_session):
        facts = [
            FinancialFact(
                accession_number="0000320193-23-000077",
                fact_name="Revenues",
                unit="USD",
                period_start=None,
                period_end=datetime(2023, 9, 30),
                value=383285000000.0,
                segment="Total",
            ),
            FinancialFact(
                accession_number="0000320193-23-000078",
                fact_name="Revenues",
                unit="USD",
                period_start=None,
                period_end=datetime(2022, 9, 30),
                value=394328000000.0,
                segment="Total",
            ),
        ]
        for fact in facts:
            db_session.add(fact)

        filing2 = FilingRaw(
            accession_number="0000320193-23-000078",
            cik="0000320193",
            ticker="AAPL",
            company_name="Apple Inc.",
            form_type="10-K",
            filing_date=datetime(2022, 10, 27),
            period_end=datetime(2022, 9, 30),
            document_url="https://www.sec.gov/Archives/...",
            raw_html="<html></html>",
            pipeline_run_id="test-run-1",
        )
        db_session.add(filing2)
        db_session.commit()

        response = client.get("/facts/AAPL/Revenues")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data_points"]) >= 2


class TestTriggerEndpoint:
    def test_trigger_ingestion_success(self, client):
        response = client.post("/trigger/AAPL")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "queued"
        assert "AAPL" in data["message"]
        assert "run_id" in data

    def test_trigger_ingestion_response_format(self, client):
        response = client.post("/trigger/GOOGL")
        assert response.status_code == 200
        data = response.json()
        assert "run_id" in data
        assert "status" in data
        assert "message" in data
