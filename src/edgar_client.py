"""
SEC EDGAR API client with token-bucket rate limiting and exponential
backoff with full jitter.

Rate limit: 10 requests/second (SEC policy).
User-Agent is required by the SEC; omitting it causes 403s.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any

import requests
from requests import Response, Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://data.sec.gov"
SUBMISSIONS_URL = BASE_URL + "/submissions/CIK{cik:0>10}.json"
COMPANY_FACTS_URL = BASE_URL + "/api/xbrl/companyfacts/CIK{cik:0>10}.json"
SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

DEFAULT_USER_AGENT = "SEC-EDGAR-Pipeline aus.kuo03@gmail.com"
MAX_RETRIES = 5
RATE_LIMIT_RPS = 10  # requests per second
BACKOFF_CAP_SECONDS = 60.0

# Small jitter added on top of a server-supplied Retry-After value. Retry-After
# is a floor the server actually asked for, so it is honored, not randomized
# away — but many workers all waking at t + Retry-After exactly still
# resynchronizes them into the next lockstep wave. A few seconds of jitter on
# top breaks that up without meaningfully violating what the server asked for.
RETRY_AFTER_JITTER_SECONDS = 3.0


# ---------------------------------------------------------------------------
# Token-bucket rate limiter
# ---------------------------------------------------------------------------


class _TokenBucket:
    """Thread-safe token bucket that enforces *rate* tokens per second."""

    def __init__(self, rate: float) -> None:
        self._rate = rate  # tokens added per second
        self._capacity = rate
        self._tokens = rate
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def acquire(self) -> None:
        """Block until a token is available."""
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                # How long until the next token is available?
                wait = (1 - self._tokens) / self._rate
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Backoff with full jitter
# ---------------------------------------------------------------------------


def full_jitter_backoff(
    attempt: int,
    *,
    base: float = 1.0,
    cap: float = BACKOFF_CAP_SECONDS,
    rand: random.Random | None = None,
) -> float:
    """
    "Full jitter" exponential backoff (AWS Architecture Blog, 2015):
    ``sleep = uniform(0, min(cap, base * 2**attempt))``.

    A deterministic exponential curve is the *cause* of the failure mode it is
    meant to prevent: every client hitting the same rate limit at the same
    moment computes the same delay and retries in lockstep, producing a
    synchronized wave against an endpoint that just told everyone to back off.
    Drawing the actual sleep uniformly from ``[0, ceiling]`` spreads retries
    across the whole window instead, which is what actually protects a
    shared, sensitive endpoint like SEC EDGAR from a retry storm.

    Args:
        attempt: 0-indexed retry attempt number.
        base:    Base delay in seconds before jitter (default 1.0).
        cap:     Maximum ceiling in seconds, before jitter is applied.
        rand:    Injectable ``random.Random`` for deterministic tests;
                 defaults to the module-level ``random`` functions.

    Returns:
        Seconds to sleep, in ``[0, cap]``.
    """
    ceiling = min(cap, base * (2**attempt))
    return rand.uniform(0, ceiling) if rand is not None else random.uniform(0, ceiling)


# ---------------------------------------------------------------------------
# EdgarClient
# ---------------------------------------------------------------------------


class EdgarClient:
    """
    Wrapper around the public SEC EDGAR data APIs.

    All requests pass through a shared token-bucket limiter and retry logic.
    """

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        rate_limit_rps: float = RATE_LIMIT_RPS,
        max_retries: int = MAX_RETRIES,
        session: Session | None = None,
    ) -> None:
        self._user_agent = user_agent
        self._max_retries = max_retries
        self._bucket = _TokenBucket(rate_limit_rps)
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> Response:
        """
        Make a GET request, respecting the rate limit and retrying on
        transient errors (429 Too Many Requests, 503 Service Unavailable).

        Retries use full-jitter exponential backoff (see
        ``full_jitter_backoff``), not a bare ``time.sleep(backoff)`` — a
        deterministic curve means every worker that hits a rate limit at the
        same moment retries in lockstep, which is itself a self-inflicted
        rate-limit-ban risk against a shared endpoint.
        """
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            self._bucket.acquire()
            try:
                resp = self._session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "Request error (attempt %d/%d): %s", attempt + 1, self._max_retries + 1, exc
                )
                if attempt < self._max_retries:
                    wait = full_jitter_backoff(attempt)
                    logger.debug("Backing off %.2fs before retry", wait)
                    time.sleep(wait)
                continue

            if resp.status_code in (429, 503):
                header_value = resp.headers.get("Retry-After")
                if header_value is not None:
                    # The server told us explicitly how long to wait — honor
                    # that as a floor, and add a small jitter on top so many
                    # workers given the identical Retry-After don't all wake
                    # and retry at exactly the same instant.
                    wait = float(header_value) + random.uniform(0, RETRY_AFTER_JITTER_SECONDS)
                else:
                    wait = full_jitter_backoff(attempt)
                logger.warning(
                    "HTTP %d — waiting %.1fs before retry (attempt %d/%d)",
                    resp.status_code,
                    wait,
                    attempt + 1,
                    self._max_retries + 1,
                )
                if attempt < self._max_retries:
                    time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        if last_exc:
            raise last_exc
        raise requests.HTTPError(f"Failed after {self._max_retries} retries: {url}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_company_filings(self, cik: str) -> dict[str, Any]:
        """
        Fetch the submission history for a company.

        Returns the raw JSON from:
          https://data.sec.gov/submissions/CIK{cik}.json

        The response includes company metadata and a ``filings.recent``
        object with arrays of accession numbers, forms, dates, etc.
        """
        url = SUBMISSIONS_URL.format(cik=cik.lstrip("0"))
        logger.debug("get_company_filings cik=%s → %s", cik, url)
        return self._get(url).json()

    def get_xbrl_facts(self, cik: str) -> dict[str, Any]:
        """
        Fetch the full XBRL company-facts JSON for a CIK.

        Returns the raw JSON from:
          https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
        """
        url = COMPANY_FACTS_URL.format(cik=cik.lstrip("0"))
        logger.debug("get_xbrl_facts cik=%s → %s", cik, url)
        return self._get(url).json()

    def get_filing_index(self, cik: str, accession: str) -> list[str]:
        """
        Return a list of document filenames in the filing index for the
        given accession number.

        Index URL:
          https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}
          ...or directly:
          https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/
        """
        acc_nodash = accession.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{acc_nodash}/"
        logger.debug("get_filing_index cik=%s acc=%s → %s", cik, accession, url)
        resp = self._get(url)
        # Parse the HTML index to extract filenames
        from lxml import etree  # lazy import to keep top-level imports clean

        tree = etree.fromstring(resp.content, parser=etree.HTMLParser())
        hrefs: list[str] = tree.xpath("//table//tr/td/a/@href")
        return [h for h in hrefs if not h.startswith("?")]

    def get_filing_document(self, url: str) -> str:
        """
        Download a raw filing document (HTML or XML) and return its text.
        """
        logger.debug("get_filing_document → %s", url)
        return self._get(url).text

    def search_filings(
        self,
        query: str,
        form_type: str = "",
        date_range: tuple[str, str] | None = None,
    ) -> list[str]:
        """
        Search the EDGAR full-text search index.

        Returns a list of accession numbers matching the query.

        Args:
            query:      Free-text search string.
            form_type:  Filter by form type, e.g. "10-K".
            date_range: Optional (start_date, end_date) as "YYYY-MM-DD" strings.
        """
        params: dict[str, str] = {"q": query}
        if form_type:
            params["forms"] = form_type
        if date_range:
            params["dateRange"] = "custom"
            params["startdt"] = date_range[0]
            params["enddt"] = date_range[1]

        logger.debug("search_filings query=%r form=%r date_range=%r", query, form_type, date_range)
        data = self._get(SEARCH_URL, params=params).json()

        hits = data.get("hits", {}).get("hits", [])
        accessions: list[str] = []
        for hit in hits:
            src = hit.get("_source", {})
            acc = src.get("file_date") and src.get("accession_no")
            if not acc:
                acc = hit.get("_id", "")
            if acc:
                accessions.append(acc)
        return accessions
