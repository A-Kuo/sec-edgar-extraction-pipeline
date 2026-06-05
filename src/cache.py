import json
import logging
from typing import Optional, Any
import redis
from redis.connection import ConnectionPool

logger = logging.getLogger(__name__)


class FilingCache:
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        try:
            self.pool = ConnectionPool.from_url(redis_url, max_connections=20, decode_responses=True)
            self.client = redis.Redis(connection_pool=self.pool)
            self.client.ping()
        except Exception as e:
            logger.error(f"Redis connection failed: {e}. Cache will be disabled.")
            self.client = None

    def set_cik_ticker(self, ticker: str, cik: str) -> None:
        if not self.client:
            return
        try:
            self.client.setex(f"cik:{ticker}", 3600, cik)
        except Exception as e:
            logger.error(f"Failed to set cik:{ticker}: {e}")

    def get_cik_ticker(self, ticker: str) -> Optional[str]:
        if not self.client:
            return None
        try:
            return self.client.get(f"cik:{ticker}")
        except Exception as e:
            logger.error(f"Failed to get cik:{ticker}: {e}")
            return None

    def set_filings(self, cik: str, filings: Any) -> None:
        if not self.client:
            return
        try:
            self.client.setex(f"filings:{cik}", 86400, json.dumps(filings))
        except Exception as e:
            logger.error(f"Failed to set filings:{cik}: {e}")

    def get_filings(self, cik: str) -> Optional[Any]:
        if not self.client:
            return None
        try:
            data = self.client.get(f"filings:{cik}")
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Failed to get filings:{cik}: {e}")
            return None

    def set_facts(self, accession_number: str, facts: Any) -> None:
        if not self.client:
            return
        try:
            self.client.setex(f"facts:{accession_number}", 604800, json.dumps(facts))
        except Exception as e:
            logger.error(f"Failed to set facts:{accession_number}: {e}")

    def get_facts(self, accession_number: str) -> Optional[Any]:
        if not self.client:
            return None
        try:
            data = self.client.get(f"facts:{accession_number}")
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Failed to get facts:{accession_number}: {e}")
            return None

    def clear_all(self) -> None:
        if not self.client:
            return
        try:
            self.client.flushdb()
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
