import pytest
from unittest.mock import Mock, patch
import requests
from src.edgar_client import TokenBucket, EdgarClient


class TestTokenBucket:
    def test_acquire_available_tokens(self):
        bucket = TokenBucket(rate=10.0)
        bucket.acquire(5)
        assert bucket.tokens < 10.0

    def test_acquire_blocks_when_empty(self):
        bucket = TokenBucket(rate=0.1)
        bucket.tokens = 0
        import time
        start = time.time()
        bucket.acquire(1)
        elapsed = time.time() - start
        assert elapsed >= 0.01

    def test_token_refill(self):
        import time
        bucket = TokenBucket(rate=100.0)
        bucket.acquire(100)
        initial = bucket.tokens
        time.sleep(0.02)
        bucket.acquire(1)
        assert bucket.tokens > initial


class TestEdgarClient:
    @pytest.fixture
    def client(self):
        return EdgarClient(user_agent="Test Agent")

    def test_get_company_filings_success(self, client):
        with patch('src.edgar_client.requests.Session.request') as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = {
                "filings": {"recent": [{"accessionNumber": "0000320193-23-000077"}]}
            }
            mock_response.headers = {}
            mock_request.return_value = mock_response

            result = client.get_company_filings("0000320193", "10-K")
            assert result is not None
            assert "filings" in result

    def test_get_company_filings_failure(self, client):
        with patch('src.edgar_client.requests.Session.request') as mock_request:
            mock_request.side_effect = requests.exceptions.RequestException()

            result = client.get_company_filings("0000320193", "10-K")
            assert result is None

    def test_user_agent_header(self, client):
        with patch('src.edgar_client.requests.Session.request') as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = {}
            mock_response.headers = {}
            mock_request.return_value = mock_response

            client.get_company_filings("0000320193", "10-K")
            assert "User-Agent" in client.session.headers

    def test_retry_after_header(self, client):
        with patch('src.edgar_client.requests.Session.request') as mock_request:
            with patch('time.sleep'):
                mock_response = Mock()
                mock_response.json.return_value = {}
                mock_response.headers = {"Retry-After": "2"}
                mock_request.return_value = mock_response

                result = client.get_company_filings("0000320193", "10-K")
                assert result is not None

    def test_get_filing_document(self, client):
        with patch('src.edgar_client.requests.Session.request') as mock_request:
            mock_response = Mock()
            mock_response.text = "<html>filing content</html>"
            mock_response.headers = {}
            mock_request.return_value = mock_response

            result = client.get_filing_document("0000320193-23-000077")
            assert result is not None
            assert "filing content" in result
