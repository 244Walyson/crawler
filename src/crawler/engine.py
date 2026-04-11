import asyncio
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple
from urllib.parse import urlparse

import anyio
import httpx

from src.config.logging import logger
from src.config.settings import settings
from src.crawler.scheduler import AdaptiveScheduler
from src.models.raw_document import RawWebDocument


class DataStorage(Protocol):
    async def save(self, data: Any) -> None:
        ...
    async def flush(self) -> None:
        ...
    async def close(self) -> None:
        ...


class LinkExtractorProtocol(Protocol):
    def extract_links(self, html: str, base_url: str) -> List[Tuple[int, str]]:
        ...


class CrawlerEngine:
    def __init__(self, extractor: LinkExtractorProtocol, storage: DataStorage) -> None:
        self.extractor = extractor
        self.storage = storage
        self.scheduler = AdaptiveScheduler()
        
        self.client = httpx.AsyncClient(
            timeout=settings.TIMEOUT,
            http2=True,
            headers={
                "User-Agent": settings.USER_AGENT,
            },
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=settings.MAX_CONNECTIONS,
                max_keepalive_connections=settings.MAX_KEEPALIVE
            )
        )
        self.pages_collected = 0
        self.errors = 0
        self.error_counts: Counter[str] = Counter()
        self.workers_status: dict[int, str] = {}
        self._stop_event = anyio.Event()
        self.done_event = anyio.Event()

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "collected": self.pages_collected,
            "errors": self.errors,
            "error_counts": dict(self.error_counts),
            "queue_size": self.scheduler.qsize(),
            "workers": self.workers_status,
            "is_done": self.done_event.is_set(),
        }

    async def worker(self, worker_id: int) -> None:
        self.workers_status[worker_id] = "Starting..."
        try:
            while not self._stop_event.is_set():
                try:
                    self.workers_status[worker_id] = "Waiting..."
                    next_task = await self.scheduler.get_next()
                    
                    if next_task is None or self._stop_event.is_set():
                        break
                        
                    url, depth = next_task

                    self.scheduler.set_busy()
                    try:
                        self.workers_status[worker_id] = f"Fetching..."
                        response = await self.client.get(url)
                        
                        # Only accept successful text/html responses
                        content_type = response.headers.get("content-type", "").lower()
                        if response.status_code != 200 or "text/html" not in content_type:
                            if response.status_code >= 400:
                                self.error_counts[f"HTTP_{response.status_code}"] += 1
                                self.errors += 1
                                logger.warning("http_error", status=response.status_code, url=url)
                            continue

                        logger.info("page_fetched", url=url)
                        self.pages_collected += 1
                        
                        if self.pages_collected >= settings.MAX_PAGES:
                            self._stop_event.set()
                        
                        html_text = response.text
                        resp_url = str(response.url)
                        
                        doc = RawWebDocument(
                            url=resp_url,
                            timestamp=datetime.utcnow(),
                            status_code=response.status_code,
                            depth=depth,
                            html=html_text
                        )
                        await self.storage.save(doc)
                        
                        self.workers_status[worker_id] = "Extracting..."
                        
                        # Offload extraction to avoid blocking the event loop
                        new_urls = await anyio.to_thread.run_sync(
                            self.extractor.extract_links, html_text, resp_url
                        )
                        
                        for priority, link in new_urls:
                            self.scheduler.add_task(link, priority=priority, depth=depth + 1)
                            
                    except Exception as e:
                        error_type = type(e).__name__
                        self.error_counts[error_type] += 1
                        self.errors += 1
                        logger.debug("worker_error", error=str(e), url=url)
                    finally:
                        self.scheduler.set_idle()
                    
                    self.workers_status[worker_id] = "Delay..."
                    await anyio.sleep(settings.REQUEST_DELAY)

                except Exception as e:
                    error_type = type(e).__name__
                    self.error_counts[f"Loop_{error_type}"] += 1
                    self.errors += 1
                    logger.debug("worker_loop_error", error=str(e))
        finally:
            self.workers_status[worker_id] = "Finished"

    async def run(self) -> None:
        self.scheduler.clear()
        
        for url in settings.BASE_URLS:
            self.scheduler.add_task(url, priority=0, depth=0)
        
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
