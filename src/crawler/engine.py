import asyncio
import re
from datetime import datetime
from typing import Any, Dict, List, Protocol, Set
from urllib.parse import urljoin

import anyio
import httpx

from src.config.logging import logger
from src.config.settings import settings
from src.crawler.scheduler import AdaptiveScheduler
from src.models.raw_document import RawWebDocument


class DataStorage(Protocol):
    async def save(self, data: Any) -> None: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...


class CrawlerEngine:
    def __init__(self, storage: DataStorage) -> None:
        self.storage = storage
        self.scheduler = AdaptiveScheduler()

        self.client = httpx.AsyncClient(
            timeout=settings.TIMEOUT,
            http2=True,
            headers={"User-Agent": settings.USER_AGENT},
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=settings.MAX_CONNECTIONS,
                max_keepalive_connections=settings.MAX_KEEPALIVE,
            ),
        )
        self.pages_collected = 0
        self.errors = 0
        self.workers_status: dict[int, str] = {}
        self._stop_event = anyio.Event()
        self.done_event = anyio.Event()

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "collected": self.pages_collected,
            "errors": self.errors,
            "queue_size": self.scheduler.qsize(),
            "workers": self.workers_status,
            "is_done": self.done_event.is_set(),
        }

    async def worker(self, worker_id: int) -> None:
        href_re = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
        self.workers_status[worker_id] = "Starting..."
        try:
            while not self._stop_event.is_set():
                try:
                    self.workers_status[worker_id] = "Waiting..."
                    url = await self.scheduler.get_next()

                    if url is None or self._stop_event.is_set():
                        break

                    self.scheduler.set_busy()
                    try:
                        self.workers_status[worker_id] = "Fetching..."
                        response = await self.client.get(url)

                        if response.status_code >= 400:
                            self.errors += 1
                            continue

                        html_text = response.text
                        resp_url = str(response.url)
                        logger.info("page_fetched", url=resp_url)
                        self.pages_collected += 1

                        if self.pages_collected >= settings.MAX_PAGES:
                            self._stop_event.set()

                        doc = RawWebDocument(
                            url=resp_url,
                            timestamp=datetime.utcnow(),
                            status_code=response.status_code,
                            depth=0,
                            html=html_text,
                        )
                        await self.storage.save(doc)

                        self.workers_status[worker_id] = "Extracting..."
                        for match in href_re.finditer(html_text):
                            raw_url = match.group(1)
                            if raw_url.startswith(("javascript:", "mailto:", "tel:", "#")):
                                continue
                            full_url = urljoin(resp_url, raw_url)
                            await self.scheduler.add_task(full_url, priority=10)

                    except Exception as e:
                        self.errors += 1
                        logger.debug("worker_error", error=str(e), url=url)
                    finally:
                        self.scheduler.set_idle()

                    if settings.REQUEST_DELAY > 0:
                        await anyio.sleep(settings.REQUEST_DELAY)

                except Exception:
                    self.errors += 1
        finally:
            self.workers_status[worker_id] = "Finished"

    async def run(self) -> None:
        self.scheduler.clear()

        for url in settings.BASE_URLS:
            await self.scheduler.add_task(url, priority=0)

        try:
            async with anyio.create_task_group() as tg:
                for i in range(settings.CONCURRENCY_LIMIT):
                    tg.start_soon(self.worker, i)
        finally:
            await self.storage.flush()
            await self.storage.close()
            await self.client.aclose()
            self.done_event.set()
            logger.info("crawler_finished", total_pages=self.pages_collected)
