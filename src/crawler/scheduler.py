import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


@dataclass(order=True)
class CrawlTask:
    priority: int  # Smaller = Higher
    url: str = field(compare=False)
    depth: int = field(compare=False, default=0)
    next_crawl_time: float = field(default_factory=time.time, compare=False)

class AdaptiveScheduler:
    """Simple Priority-based Scheduler with deduplication."""
    def __init__(self) -> None:
        self.queue: asyncio.PriorityQueue[CrawlTask] = asyncio.PriorityQueue()
        self.visited: Set[str] = set()
        self.queued: Set[str] = set()
        self.domain_yields: Dict[str, int] = {}
        self._busy_workers = 0
        self._initialized = False

    def set_busy(self): self._busy_workers += 1
    def set_idle(self): self._busy_workers -= 1

    def update_domain_yield(self, domain: str, count: int = 1):
        self.domain_yields[domain] = self.domain_yields.get(domain, 0) + count

    def add_task(self, url: str, priority: int = 10, depth: int = 0) -> None:
        if url in self.visited or url in self.queued:
            return
        
        try:
            # Fast string split to get domain: 'https://example.com/path' -> 'example.com'
            domain = url.split('/', 3)[2].replace('www.', '')
            boost = -2 if self.domain_yields.get(domain, 0) > 0 else 0
        except IndexError:
            boost = 0
            
        self.queued.add(url)
        self.queue.put_nowait(CrawlTask(priority=priority + boost, url=url, depth=depth))
        self._initialized = True

    async def get_next(self) -> Optional[Tuple[str, int]]:
        # Wait for first tasks
        while not self._initialized:
            await asyncio.sleep(0.1)

        while True:
            if self.queue.empty():
                if self._busy_workers == 0:
                    await asyncio.sleep(1.0)
                    if self.queue.empty() and self._busy_workers == 0:
                        return None
                else:
                    await asyncio.sleep(0.1)
                    continue

            try:
                task = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                continue

            self.visited.add(task.url)
            if task.url in self.queued:
                self.queued.remove(task.url)
            return task.url, task.depth

    def clear(self):
        self.visited.clear()
        self.queued.clear()
        self.domain_yields.clear()
        self._initialized = False
        while not self.queue.empty():
            try: self.queue.get_nowait()
            except: break

    def qsize(self) -> int:
        return self.queue.qsize()
