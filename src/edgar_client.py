import logging
import threading
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class TokenBucket:
    def __init__(self, rate: float = 10.0):
        self.rate = rate
        self.tokens = rate
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

            time.sleep(0.01)


class EdgarClient:
    def __init__(self, user_agent: str, rate_limit: float = 10.0):
        self.user_agent = user_agent
        self.session = requests.Session()
        self.bucket = TokenBucket(rate_limit)

        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 503],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"User-Agent": user_agent})

    def _request(self, method: str, url: str, **kwargs) -> requests.Response | None:
        self.bucket.acquire()
        try:
            response = self.session.request(method, url, **kwargs)
            if "Retry-After" in response.headers:
                wait_time = int(response.headers["Retry-After"])
                logger.warning(f"Received Retry-After: {wait_time}s")
                time.sleep(wait_time)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None

    def get_company_filings(self, cik: str, form_type: str = "10-K") -> dict[str, Any] | None:
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        response = self._request("GET", url)
        if response:
            try:
                return response.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON: {e}")
        return None

    def get_xbrl_facts(self, accession_number: str) -> dict[str, Any] | None:
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={accession_number}&type=10-K&dateb=&owner=exclude&count=100&search_text="
        response = self._request("GET", url)
        if response:
            return {"accession": accession_number, "html": response.text}
        return None

    def get_filing_index(self, cik: str, form_type: str = "10-K") -> dict[str, Any] | None:
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        response = self._request("GET", url)
        if response:
            try:
                return response.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON: {e}")
        return None

    def get_filing_document(self, accession_number: str) -> str | None:
        url = f"https://www.sec.gov/cgi-bin/viewer?action=view&cik={accession_number}&accession_number={accession_number}&xbrl_type=v"
        response = self._request("GET", url)
        if response:
            return response.text
        return None

    def search_filings(self, query: str) -> dict[str, Any] | None:
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?company={query}&action=getcompany"
        response = self._request("GET", url)
        if response:
            return {"query": query, "html": response.text}
        return None
