import asyncio
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from typing import Dict

import httpx

from src.config.settings import settings
from src.config.logging import logger


class RobotsCache:
    """
    Fetches and caches robots.txt rules per domain.
    Thread-safe via asyncio lock per domain.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client
        self._cache: Dict[str, RobotFileParser] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def _get_lock(self, domain: str) -> asyncio.Lock:
        async with self._global_lock:
            if domain not in self._locks:
                self._locks[domain] = asyncio.Lock()
            return self._locks[domain]

    async def is_allowed(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            domain = f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            return True

        lock = await self._get_lock(domain)
        async with lock:
            if domain not in self._cache:
                self._cache[domain] = await self._fetch(domain)

        parser = self._cache[domain]
        if parser is None:
            return True  # Failed to fetch — allow by default

        return parser.can_fetch(settings.USER_AGENT, url)

    async def _fetch(self, domain: str) -> RobotFileParser:
        robots_url = f"{domain}/robots.txt"
        parser = RobotFileParser()
        parser.set_url(robots_url)

        try:
            response = await self.client.get(robots_url, timeout=5.0)
            if response.status_code == 200:
                parser.parse(response.text.splitlines())
                logger.debug("robots_fetched", domain=domain)
            else:
                # 404 or other — no restrictions
                parser.parse([])
        except Exception as e:
            logger.debug("robots_fetch_failed", domain=domain, error=str(e))
            parser.parse([])  # Allow all on error

        return parser
