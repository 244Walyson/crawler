from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Tuple

import anyio
import httpx

from src.config.logging import logger
from src.config.settings import settings
from src.crawler.robots import RobotsCache


class DataStorage(Protocol):
    async def save(self, data: Any) -> None: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...


class Scheduler(Protocol):
    async def add_task(self, url: str, priority: int, depth: int) -> None: ...
    async def get_next(self) -> Optional[Tuple[str, int]]: ...
    async def set_idle(self) -> None: ...
    async def increment_total(self) -> int: ...
    async def signal_stop(self) -> None: ...
    async def clear(self) -> None: ...
    def qsize(self) -> int: ...


class LinkExtractorProtocol(Protocol):
    def extract_links(self, html: str, base_url: str) -> List[Tuple[int, str]]: ...


class CrawlerEngine:
    def __init__(
        self,
        extractor: LinkExtractorProtocol,
        storage: DataStorage,
        scheduler: Scheduler,
    ) -> None:
        self.extractor = extractor
        self.storage = storage
        self.scheduler = scheduler

        self.client = httpx.AsyncClient(
            timeout=settings.TIMEOUT,
            http2=False,
            headers={"User-Agent": settings.USER_AGENT},
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=settings.MAX_CONNECTIONS,
                max_keepalive_connections=settings.MAX_KEEPALIVE,
            ),
        )
        self.robots = RobotsCache(self.client)
        self.pages_collected = 0  # Local counter for dashboard
        self.errors = 0
        self.error_counts: Counter[str] = Counter()
        self.workers_status: dict[int, str] = {}
        self._stop_event = anyio.Event()
        self.done_event = anyio.Event()
        self._domain_semaphores: Dict[str, anyio.abc.CapacityLimiter] = {}
        self._semaphore_lock = anyio.Lock()

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

    async def _publish_status(self, worker_id: int, status: str) -> None:
        if hasattr(self.scheduler, 'publish_status'):
            try:
                await self.scheduler.publish_status(worker_id, status)
            except Exception:
                pass

    async def _publish_activity(self, url: str) -> None:
        if hasattr(self.scheduler, 'publish_activity'):
            try:
                await self.scheduler.publish_activity(url)
            except Exception:
                pass

    async def _domain_limiter(self, url: str) -> anyio.abc.CapacityLimiter:
        try:
            domain = url.split('/', 3)[2]
        except IndexError:
            domain = url
        async with self._semaphore_lock:
            if domain not in self._domain_semaphores:
                self._domain_semaphores[domain] = anyio.CapacityLimiter(
                    settings.MAX_CONCURRENCY_PER_DOMAIN
                )
            return self._domain_semaphores[domain]

    async def _publish_error(self, error_type: str) -> None:
        if hasattr(self.scheduler, 'publish_error'):
            try:
                await self.scheduler.publish_error(error_type)
            except Exception:
                pass

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

                    try:
                        if not await self.robots.is_allowed(url):
                            logger.debug("robots_blocked", url=url)
                            continue

                        self.workers_status[worker_id] = "Fetching..."
                        await self._publish_status(worker_id, "Fetching...")
                        limiter = await self._domain_limiter(url)
                        async with limiter:
                            response = await self.client.get(url)

                        content_type = response.headers.get("content-type", "").lower()
                        if response.status_code != 200 or "text/html" not in content_type:
                            if response.status_code >= 400:
                                self.error_counts[f"HTTP_{response.status_code}"] += 1
                                self.errors += 1
                                logger.warning("http_error", status=response.status_code, url=url)
                            continue

                        html_text = response.text
                        resp_url = str(response.url)

                        logger.info("page_fetched", url=resp_url)
                        self.pages_collected += 1
                        await self._publish_activity(resp_url)

                        # Global counter (Redis) — determines stop across all instances
                        total = await self.scheduler.increment_total()
                        if total >= settings.MAX_PAGES:
                            self._stop_event.set()
                            await self.scheduler.signal_stop()

                        from src.models.raw_document import RawWebDocument
                        doc = RawWebDocument(
                            url=resp_url,
                            timestamp=datetime.utcnow(),
                            status_code=response.status_code,
                            depth=depth,
                            html=html_text,
                        )
                        await self.storage.save(doc)

                        self.workers_status[worker_id] = "Extracting..."
                        await self._publish_status(worker_id, "Extracting...")
                        new_urls = self.extractor.extract_links(html_text, resp_url)

                        for priority, link in new_urls:
                            if await self.robots.is_allowed(link):
                                await self.scheduler.add_task(link, priority=priority, depth=depth + 1)

                    except Exception as e:
                        error_type = type(e).__name__
                        self.error_counts[error_type] += 1
                        self.errors += 1
                        logger.debug("worker_error", error=str(e), url=url)
                        await self._publish_error(error_type)
                    finally:
                        await self.scheduler.set_idle()

                    if settings.REQUEST_DELAY > 0:
                        await anyio.sleep(settings.REQUEST_DELAY)

                except Exception as e:
                    error_type = type(e).__name__
                    self.error_counts[f"Loop_{error_type}"] += 1
                    self.errors += 1
                    logger.debug("worker_loop_error", error=str(e))
        finally:
            self.workers_status[worker_id] = "Finished"

    async def run(self) -> None:
        await self.scheduler.clear()
        await self.scheduler.set_start_time()

        for url in settings.BASE_URLS:
            await self.scheduler.add_task(url, priority=0, depth=0)

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
