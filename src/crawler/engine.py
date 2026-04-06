import asyncio
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple
from urllib.parse import urlparse

import anyio
import httpx

from src.config.logging import logger
from src.config.settings import settings
from src.crawler.scheduler import AdaptiveScheduler
from src.models.event import SportEvent
from src.parser.normalizer import Normalizer


class PageParser(Protocol):
    def parse(self, html: str, url: str) -> Tuple[List[Tuple[int, str]], List[SportEvent]]:
        ...


class DataStorage(Protocol):
    async def save(self, data: Any) -> None:
        ...
    async def flush(self) -> None:
        ...
    async def close(self) -> None:
        ...


class CrawlerEngine:
    def __init__(self, parser: PageParser, storage: DataStorage) -> None:
        self.parser = parser
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
        self.unique_events: Set[str] = set()
        self.errors = 0
        self.workers_status: dict[int, str] = {}
        self._stop_event = anyio.Event()
        self.done_event = anyio.Event()

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "collected": self.pages_collected,
            "events": len(self.unique_events),
            "errors": self.errors,
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
                    url = await self.scheduler.get_next()
                    
                    if url is None or self._stop_event.is_set():
                        break

                    self.scheduler.set_busy()
                    try:
                        self.workers_status[worker_id] = f"Fetching..."
                        response = await self.client.get(url)
                        
                        if response.status_code >= 400:
                            continue

                        self.pages_collected += 1
                        if self.pages_collected >= settings.MAX_PAGES:
                            self._stop_event.set()
                        
                        self.workers_status[worker_id] = "Parsing..."
                        
                        # Offload parsing to avoid blocking the event loop
                        html_text = response.text
                        resp_url = str(response.url)
                        new_urls, events = await anyio.to_thread.run_sync(
                            self.parser.parse, html_text, resp_url
                        )
                        
                        for event in events:
                            event = Normalizer.clean_event(event)
                            event.event_id = event.calculate_id()
                            
                            if event.event_id not in self.unique_events:
                                self.unique_events.add(event.event_id)
                                await self.storage.save(event)
                        
                        for priority, link in new_urls:
                            await self.scheduler.add_task(link, priority=priority)
                            
                    except Exception:
                        pass
                    finally:
                        self.scheduler.set_idle()
                    
                    self.workers_status[worker_id] = "Delay..."
                    await anyio.sleep(settings.REQUEST_DELAY)

                except Exception:
                    self.errors += 1
        finally:
            self.workers_status[worker_id] = "Finished"

    async def run(self) -> None:
        self.scheduler.clear()
        self.unique_events.clear()
        
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
