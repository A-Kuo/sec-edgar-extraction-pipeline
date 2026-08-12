"""
Tests for EdgarClient.

Covers:
  - Correct parsing of submissions JSON (accession numbers, form types)
  - Retry behaviour on HTTP 503 (with exponential backoff + full jitter)
  - Rate limiting (token-bucket enforces ≤10 req/s)
"""

from __future__ import annotations

import random
import time
from unittest.mock import patch

import pytest
import requests

from src.edgar_client import (
    BACKOFF_CAP_SECONDS,
    COMPANY_FACTS_URL,
    SUBMISSIONS_URL,
    EdgarClient,
    _TokenBucket,
    full_jitter_backoff,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_CIK = "320193"  # Apple

SUBMISSIONS_FIXTURE: dict = {
    "cik": "320193",
    "name": "Apple Inc.",
    "tickers": ["AAPL"],
    "exchanges": ["Nasdaq"],
    "filings": {
        "recent": {
            "accessionNumber": [
                "0000320193-24-000123",
                "0000320193-23-000077",
                "0000320193-23-000010",
            ],
            "form": ["10-K", "10-Q", "10-Q"],
            "filingDate": ["2024-11-01", "2023-08-04", "2023-05-05"],
            "reportDate": ["2024-09-28", "2023-07-01", "2023-04-01"],
            "primaryDocument": [
                "aapl-20240928.htm",
                "aapl-20230701.htm",
                "aapl-20230401.htm",
            ],
        }
    },
}

XBRL_FACTS_FIXTURE: dict = {
    "cik": "320193",
    "entityName": "Apple Inc.",
    "facts": {
        "us-gaap": {
            "Revenues": {
                "label": "Revenues",
                "description": "Amount of revenue recognized.",
                "units": {
                    "USD": [
                        {
                            "end": "2024-09-28",
                            "val": 391035000000,
                            "accn": "0000320193-24-000123",
                            "fy": 2024,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2024-11-01",
                        }
                    ]
                },
            }
        }
    },
}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def make_client(**kwargs) -> EdgarClient:
    """Return an EdgarClient with a real requests.Session (intercepted by requests-mock)."""
    return EdgarClient(user_agent="Test Agent test@example.com", **kwargs)


# ---------------------------------------------------------------------------
# 1. Submissions JSON parsing
# ---------------------------------------------------------------------------


class TestGetCompanyFilings:
    def test_returns_parsed_json(self, requests_mock):
        cik = SAMPLE_CIK
        url = SUBMISSIONS_URL.format(cik=cik.lstrip("0"))
        requests_mock.get(url, json=SUBMISSIONS_FIXTURE)

        client = make_client()
        result = client.get_company_filings(cik)

        assert result["cik"] == "320193"
        assert result["name"] == "Apple Inc."
        assert result["tickers"] == ["AAPL"]

    def test_accession_numbers_accessible(self, requests_mock):
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(url, json=SUBMISSIONS_FIXTURE)

        client = make_client()
        data = client.get_company_filings(SAMPLE_CIK)
        accessions = data["filings"]["recent"]["accessionNumber"]

        assert len(accessions) == 3
        assert "0000320193-24-000123" in accessions

    def test_form_types_match(self, requests_mock):
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(url, json=SUBMISSIONS_FIXTURE)

        client = make_client()
        data = client.get_company_filings(SAMPLE_CIK)
        forms = data["filings"]["recent"]["form"]

        assert forms[0] == "10-K"
        assert forms[1] == "10-Q"

    def test_user_agent_header_sent(self, requests_mock):
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(url, json=SUBMISSIONS_FIXTURE)

        client = make_client()
        client.get_company_filings(SAMPLE_CIK)

        assert requests_mock.last_request.headers["User-Agent"] == "Test Agent test@example.com"


# ---------------------------------------------------------------------------
# 2. XBRL facts parsing
# ---------------------------------------------------------------------------


class TestGetXbrlFacts:
    def test_returns_facts_dict(self, requests_mock):
        url = COMPANY_FACTS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(url, json=XBRL_FACTS_FIXTURE)

        client = make_client()
        result = client.get_xbrl_facts(SAMPLE_CIK)

        assert "facts" in result
        assert "us-gaap" in result["facts"]
        assert "Revenues" in result["facts"]["us-gaap"]

    def test_revenue_value_correct(self, requests_mock):
        url = COMPANY_FACTS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(url, json=XBRL_FACTS_FIXTURE)

        client = make_client()
        result = client.get_xbrl_facts(SAMPLE_CIK)
        usd_entries = result["facts"]["us-gaap"]["Revenues"]["units"]["USD"]

        assert usd_entries[0]["val"] == 391_035_000_000
        assert usd_entries[0]["form"] == "10-K"


# ---------------------------------------------------------------------------
# 3. Retry on 503
# ---------------------------------------------------------------------------


class TestFullJitterBackoff:
    """Unit tests for the jitter function in isolation from any HTTP mocking."""

    def test_first_attempt_ceiling_equals_base(self):
        rng = random.Random(0)
        # attempt=0 -> ceiling = base * 2**0 = base
        value = full_jitter_backoff(0, base=1.0, rand=rng)
        assert 0.0 <= value <= 1.0

    def test_ceiling_doubles_each_attempt(self):
        rng = random.Random(0)
        # Force rand.uniform to always return the ceiling itself, so we can
        # read the ceiling back out and confirm the doubling schedule.
        rng.uniform = lambda lo, hi: hi  # type: ignore[method-assign]
        assert full_jitter_backoff(0, base=1.0, cap=1000, rand=rng) == pytest.approx(1.0)
        assert full_jitter_backoff(1, base=1.0, cap=1000, rand=rng) == pytest.approx(2.0)
        assert full_jitter_backoff(2, base=1.0, cap=1000, rand=rng) == pytest.approx(4.0)
        assert full_jitter_backoff(3, base=1.0, cap=1000, rand=rng) == pytest.approx(8.0)

    def test_result_is_never_negative(self):
        rng = random.Random(0)
        for attempt in range(10):
            assert full_jitter_backoff(attempt, rand=rng) >= 0.0

    def test_capped_at_maximum(self):
        rng = random.Random(0)
        rng.uniform = lambda lo, hi: hi  # type: ignore[method-assign]
        # attempt=10 -> base * 2**10 = 1024, far above a small cap.
        assert full_jitter_backoff(10, base=1.0, cap=5.0, rand=rng) == pytest.approx(5.0)

    def test_default_cap_matches_client_constant(self):
        rng = random.Random(0)
        rng.uniform = lambda lo, hi: hi  # type: ignore[method-assign]
        assert full_jitter_backoff(20, rand=rng) == pytest.approx(BACKOFF_CAP_SECONDS)

    def test_same_attempt_produces_varying_delays(self):
        """The entire point: repeated calls must NOT return the same value."""
        rng = random.Random(42)
        values = {full_jitter_backoff(3, rand=rng) for _ in range(20)}
        assert len(values) > 1, "jitter produced identical delays — no randomization occurred"

    def test_uses_module_random_when_no_rand_supplied(self):
        """Without an injected RNG, falls back to the module-level `random`."""
        with patch("src.edgar_client.random.uniform", return_value=2.5) as mock_uniform:
            value = full_jitter_backoff(1, base=1.0)
        mock_uniform.assert_called_once()
        assert value == 2.5


class TestRetryOn503:
    def test_retries_and_succeeds_after_503(self, requests_mock):
        """Client should retry on 503 and eventually succeed."""
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        # First two requests → 503, third → 200
        requests_mock.get(
            url,
            [
                {"status_code": 503},
                {"status_code": 503},
                {"json": SUBMISSIONS_FIXTURE, "status_code": 200},
            ],
        )

        client = make_client(max_retries=5)
        # Patch sleep to avoid actually waiting in tests
        with patch("src.edgar_client.time.sleep"):
            result = client.get_company_filings(SAMPLE_CIK)

        assert result["name"] == "Apple Inc."
        assert requests_mock.call_count == 3

    def test_raises_after_max_retries_503(self, requests_mock):
        """Client should raise after exhausting all retries."""
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(url, status_code=503)

        client = make_client(max_retries=2)
        with patch("src.edgar_client.time.sleep"), pytest.raises(requests.HTTPError):
            client.get_company_filings(SAMPLE_CIK)

        # 1 initial + 2 retries = 3 total
        assert requests_mock.call_count == 3

    def test_retry_after_header_respected(self, requests_mock):
        """
        Client should honor the Retry-After header as a floor, plus a small
        jitter on top (see RETRY_AFTER_JITTER_SECONDS) — many workers all
        given the identical Retry-After would otherwise still wake and retry
        in lockstep at exactly the same instant.
        """
        from src.edgar_client import RETRY_AFTER_JITTER_SECONDS

        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(
            url,
            [
                {"status_code": 503, "headers": {"Retry-After": "5"}},
                {"json": SUBMISSIONS_FIXTURE, "status_code": 200},
            ],
        )

        client = make_client(max_retries=2)
        sleep_calls: list[float] = []
        with patch("src.edgar_client.time.sleep", side_effect=lambda t: sleep_calls.append(t)):
            client.get_company_filings(SAMPLE_CIK)

        assert 5.0 <= sleep_calls[0] <= 5.0 + RETRY_AFTER_JITTER_SECONDS

    def test_retries_on_429(self, requests_mock):
        """Client should also retry on HTTP 429 Too Many Requests."""
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(
            url,
            [
                {"status_code": 429, "headers": {"Retry-After": "1"}},
                {"json": SUBMISSIONS_FIXTURE, "status_code": 200},
            ],
        )

        client = make_client(max_retries=3)
        with patch("src.edgar_client.time.sleep"):
            result = client.get_company_filings(SAMPLE_CIK)

        assert result["cik"] == "320193"

    def test_503_without_retry_after_uses_jittered_backoff_not_bare_sleep(self, requests_mock):
        """
        The core claim under test: consecutive retries must NOT sleep for the
        same deterministic duration. A bare `time.sleep(backoff)` (no jitter)
        would make every one of these sleeps a fixed, predictable value.
        """
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(
            url,
            [
                {"status_code": 503},
                {"status_code": 503},
                {"status_code": 503},
                {"json": SUBMISSIONS_FIXTURE, "status_code": 200},
            ],
        )

        client = make_client(max_retries=5)
        sleep_calls: list[float] = []
        with patch("src.edgar_client.time.sleep", side_effect=lambda t: sleep_calls.append(t)):
            client.get_company_filings(SAMPLE_CIK)

        assert len(sleep_calls) == 3
        assert len(set(sleep_calls)) > 1, (
            f"all retries slept for identical durations {sleep_calls} — "
            "this is the lockstep-retry pattern jitter exists to prevent"
        )
        # Full jitter never sleeps negative or beyond the cap.
        assert all(0.0 <= s <= BACKOFF_CAP_SECONDS for s in sleep_calls)

    def test_two_clients_retrying_simultaneously_do_not_lockstep(self, requests_mock):
        """
        Simulates the actual failure mode: two independent EdgarClient workers
        hit the same 503 at the same moment. Their retry delays must differ —
        that's what stops them from re-hammering SEC EDGAR in unison.
        """
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(
            url,
            [
                {"status_code": 503},
                {"json": SUBMISSIONS_FIXTURE, "status_code": 200},
                {"status_code": 503},
                {"json": SUBMISSIONS_FIXTURE, "status_code": 200},
            ],
        )

        client_a = make_client(max_retries=2)
        client_b = make_client(max_retries=2)

        sleeps_a: list[float] = []
        sleeps_b: list[float] = []

        with patch("src.edgar_client.time.sleep", side_effect=lambda t: sleeps_a.append(t)):
            client_a.get_company_filings(SAMPLE_CIK)
        with patch("src.edgar_client.time.sleep", side_effect=lambda t: sleeps_b.append(t)):
            client_b.get_company_filings(SAMPLE_CIK)

        assert sleeps_a[0] != sleeps_b[0], (
            "two independent clients backing off from the same failure slept "
            "for the identical duration — jitter is not actually randomizing"
        )


# ---------------------------------------------------------------------------
# 4. Token-bucket rate limiting
# ---------------------------------------------------------------------------


class TestTokenBucket:
    def test_does_not_exceed_rate(self):
        """
        Drain the bucket at 10 req/s; elapsed time for N requests
        should be at least (N-1)/rate seconds.
        """
        rate = 10.0
        n_requests = 15
        bucket = _TokenBucket(rate)

        start = time.monotonic()
        for _ in range(n_requests):
            bucket.acquire()
        elapsed = time.monotonic() - start

        # The first ``rate`` requests consume pre-filled tokens instantly;
        # subsequent requests must wait → minimum elapsed ≈ (n - capacity) / rate
        min_expected = (n_requests - rate) / rate
        assert elapsed >= min_expected * 0.9, (
            f"Rate limiting too loose: {elapsed:.3f}s for {n_requests} reqs "
            f"at {rate} rps (expected ≥ {min_expected:.3f}s)"
        )

    def test_refills_over_time(self):
        """After draining the bucket, waiting should replenish tokens."""
        rate = 100.0  # Fast rate so sleep is tiny
        bucket = _TokenBucket(rate)

        # Drain fully
        for _ in range(int(rate)):
            bucket.acquire()

        # Wait long enough to refill 5 tokens
        time.sleep(5 / rate + 0.02)

        # Should be able to acquire 5 more without blocking measurably
        start = time.monotonic()
        for _ in range(5):
            bucket.acquire()
        elapsed = time.monotonic() - start

        assert elapsed < 0.1, f"Refill too slow: took {elapsed:.3f}s"


class TestRateLimitingIntegration:
    def test_client_respects_rate_limit(self, requests_mock):
        """
        Fire N requests quickly; the client's token bucket should
        throttle them so elapsed time is at least (N-capacity)/rate.
        """
        url = SUBMISSIONS_URL.format(cik=SAMPLE_CIK.lstrip("0"))
        requests_mock.get(url, json=SUBMISSIONS_FIXTURE)

        rate = 20.0  # Higher rate for test speed
        n = 25
        client = make_client(rate_limit_rps=rate)

        start = time.monotonic()
        for _ in range(n):
            client.get_company_filings(SAMPLE_CIK)
        elapsed = time.monotonic() - start

        min_expected = (n - rate) / rate
        assert elapsed >= min_expected * 0.85, (
            f"Elapsed {elapsed:.3f}s is less than expected minimum {min_expected:.3f}s"
        )


# ---------------------------------------------------------------------------
# 5. get_filing_document
# ---------------------------------------------------------------------------


class TestGetFilingDocument:
    def test_returns_html_text(self, requests_mock):
        doc_url = (
            "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm"
        )
        requests_mock.get(doc_url, text="<html><body>Annual Report</body></html>")

        client = make_client()
        text = client.get_filing_document(doc_url)

        assert "Annual Report" in text
