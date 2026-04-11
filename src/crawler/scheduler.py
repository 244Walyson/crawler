import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple


@dataclass(order=True)
class CrawlTask:
    priority: int
    url: str = field(compare=False)
    depth: int = field(compare=False, default=0)
    next_crawl_time: float = field(default_factory=time.time, compare=False)


class AdaptiveScheduler:
    """In-memory scheduler for single-process mode."""

    def __init__(self) -> None:
        self.queue: asyncio.PriorityQueue[CrawlTask] = asyncio.PriorityQueue()
        self.visited: Set[str] = set()
        self.queued: Set[str] = set()
        self.domain_yields: Dict[str, int] = {}
        self._busy_workers = 0
        self._initialized = False

    async def set_idle(self) -> None:
        self._busy_workers -= 1

    async def add_task(self, url: str, priority: int = 10, depth: int = 0) -> None:
        if url in self.visited or url in self.queued:
            return

        try:
            domain = url.split('/', 3)[2].replace('www.', '')
            boost = -2 if self.domain_yields.get(domain, 0) > 0 else 0
        except IndexError:
            boost = 0

        self.queued.add(url)
        self.queue.put_nowait(CrawlTask(priority=priority + boost, url=url, depth=depth))
        self._initialized = True

    async def get_next(self) -> Optional[Tuple[str, int]]:
        while not self._initialized:
            await asyncio.sleep(0.05)

        consecutive_empty = 0
        while True:
            try:
                task = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                consecutive_empty = 0
                self.visited.add(task.url)
                self.queued.discard(task.url)
                self._busy_workers += 1
                return task.url, task.depth
            except asyncio.TimeoutError:
                if self._busy_workers == 0 and self.queue.empty():
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        return None
                else:
                    consecutive_empty = 0

    async def increment_total(self) -> int:
        return -1

    async def signal_stop(self) -> None:
        pass

    async def publish_status(self, worker_id: int, status: str) -> None:
        pass

    async def publish_activity(self, url: str) -> None:
        pass

    async def publish_error(self, error_type: str) -> None:
        pass

    async def set_start_time(self) -> None:
        pass

    async def publish_log(self, entry: str) -> None:
        pass

    def qsize(self) -> int:
        return self.queue.qsize()

    async def clear(self) -> None:
        self.visited.clear()
        self.queued.clear()
        self.domain_yields.clear()
        self._initialized = False
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
