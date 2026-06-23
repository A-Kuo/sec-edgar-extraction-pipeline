"""
Redis caching layer for the SEC EDGAR extraction pipeline.

Key naming convention (colon-separated hierarchy):
    cik:{ticker}              → CIK string         TTL: 1 h   (3 600 s)
    filings:{cik}             → filing list JSON   TTL: 24 h  (86 400 s)
    facts:{accession_number}  → facts list JSON    TTL: 7 d   (604 800 s)

Design notes
------------
- Uses a shared ``ConnectionPool`` so the process doesn't open a new
  TCP connection for every cache call.
- All values are stored as UTF-8 JSON; the client is initialised with
  ``decode_responses=True`` so string keys/values are returned directly.
- Every ``set`` call includes an explicit ``ex=<TTL>`` so no key ever
  lives indefinitely (see Redis best practices: always set TTL on cache
  keys).
"""

from __future__ import annotations

import json
import logging
from typing import Any

import redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TTL constants (seconds)
# ---------------------------------------------------------------------------

TTL_CIK: int = 3_600       # 1 hour   — ticker→CIK lookups
TTL_FILINGS: int = 86_400  # 24 hours — company filing index
TTL_FACTS: int = 604_800   # 7 days   — parsed XBRL facts (immutable once filed)


# ---------------------------------------------------------------------------
# FilingCache
# ---------------------------------------------------------------------------


class FilingCache:
    """
    Redis-backed cache for EDGAR API data.

    All public methods follow a get/set pattern.  ``get_*`` returns
    the deserialised Python object, or ``None`` on a cache miss.
    ``set_*`` serialises and stores with the appropriate TTL.

    Example::

        cache = FilingCache("redis://localhost:6379/0")

        # CIK lookup
        cik = cache.get_cik("AAPL")
        if cik is None:
            cik = edgar_client.lookup_cik("AAPL")
            cache.set_cik("AAPL", cik)
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        # Connection pool — reused across all calls in this process
        self._pool = redis.ConnectionPool.from_url(
            redis_url,
            decode_responses=True,
            max_connections=20,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _client(self) -> redis.Redis:
        """Return a Redis client bound to the shared pool."""
        return redis.Redis(connection_pool=self._pool)

    def _get_json(self, key: str) -> Any | None:
        """Fetch and deserialise a JSON-encoded key. Returns None on miss."""
        try:
            raw = self._client().get(key)
        except redis.RedisError as exc:
            logger.warning("Redis GET failed for key=%r: %s", key, exc)
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("JSON decode failed for key=%r: %s", key, exc)
            return None

    def _set_json(self, key: str, value: Any, ttl: int) -> None:
        """Serialise *value* to JSON and store with *ttl* seconds expiry."""
        try:
            self._client().set(key, json.dumps(value, default=str), ex=ttl)
            logger.debug("Cache SET key=%r ttl=%ds", key, ttl)
        except redis.RedisError as exc:
            logger.warning("Redis SET failed for key=%r: %s", key, exc)

    # ------------------------------------------------------------------
    # CIK lookups  (TTL: 1 h)
    # ------------------------------------------------------------------

    def get_cik(self, ticker: str) -> str | None:
        """Return the CIK for *ticker*, or None on miss."""
        return self._client().get(f"cik:{ticker.upper()}")

    def set_cik(self, ticker: str, cik: str) -> None:
        """Cache the CIK for *ticker* for 1 hour."""
        try:
            self._client().set(f"cik:{ticker.upper()}", cik, ex=TTL_CIK)
            logger.debug("Cache SET cik:%s ttl=%ds", ticker.upper(), TTL_CIK)
        except redis.RedisError as exc:
            logger.warning("Redis SET cik failed for ticker=%r: %s", ticker, exc)

    def delete_cik(self, ticker: str) -> None:
        """Evict the cached CIK for *ticker*."""
        self._client().delete(f"cik:{ticker.upper()}")

    # ------------------------------------------------------------------
    # Filing index  (TTL: 24 h)
    # ------------------------------------------------------------------

    def get_filings(self, cik: str) -> list[dict] | None:
        """
        Return the cached filing list for *cik*, or None on miss.

        The cached value is a list of filing metadata dicts (as returned
        by ``EdgarClient.get_company_filings``).
        """
        return self._get_json(f"filings:{cik}")

    def set_filings(self, cik: str, filing_list: list[dict]) -> None:
        """Cache the filing index for *cik* for 24 hours."""
        self._set_json(f"filings:{cik}", filing_list, TTL_FILINGS)

    def delete_filings(self, cik: str) -> None:
        """Evict the cached filing list for *cik*."""
        self._client().delete(f"filings:{cik}")

    # ------------------------------------------------------------------
    # Parsed XBRL facts  (TTL: 7 d)
    # ------------------------------------------------------------------

    def get_facts(self, accession_number: str) -> list[dict] | None:
        """
        Return cached parsed facts for *accession_number*, or None on miss.

        Each element in the returned list is a dict matching the
        ``financial_facts`` table schema (without ``id`` / ``parsed_at``).
        """
        return self._get_json(f"facts:{accession_number}")

    def set_facts(self, accession_number: str, facts: list[dict]) -> None:
        """Cache parsed facts for *accession_number* for 7 days."""
        self._set_json(f"facts:{accession_number}", facts, TTL_FACTS)

    def delete_facts(self, accession_number: str) -> None:
        """Evict cached facts for *accession_number*."""
        self._client().delete(f"facts:{accession_number}")

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def ping(self) -> bool:
        """Return True if the Redis server is reachable."""
        try:
            return self._client().ping()
        except redis.RedisError:
            return False

    def ttl(self, key: str) -> int:
        """Return the remaining TTL for *key* in seconds (-2 if not found)."""
        return self._client().ttl(key)
