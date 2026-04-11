from typing import Optional, Tuple
import redis.asyncio as aioredis

QUEUE_KEY = "crawler:queue"   # Sorted set: member=url, score=priority*1000+depth
SEEN_KEY  = "crawler:seen"    # Set: all URLs ever seen (visited + queued)
BUSY_KEY  = "crawler:busy"    # Counter: workers currently fetching (global)
STOP_KEY  = "crawler:stop"    # Flag: set when MAX_PAGES reached
TOTAL_KEY = "crawler:total"   # Counter: total pages collected (global)


class RedisScheduler:
    """
    Distributed scheduler backed by Redis.
    Multiple crawler processes share the same queue and dedup set,
    enabling horizontal scaling across containers.
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self.redis = redis
        self._qsize_cache = 0  # Local approximation for dashboard display

    async def add_task(self, url: str, priority: int = 10, depth: int = 0) -> None:
        # SADD returns 1 if new member, 0 if already exists — atomic dedup
        if not await self.redis.sadd(SEEN_KEY, url):
            return

        # Encode depth into score so we can recover it on dequeue
        score = priority * 1000 + min(depth, 999)
        await self.redis.zadd(QUEUE_KEY, {url: score})
        self._qsize_cache += 1

    async def get_next(self) -> Optional[Tuple[str, int]]:
        consecutive_empty = 0

        while True:
            # Check global stop flag set by any worker instance
            if await self.redis.exists(STOP_KEY):
                return None

            # BZPOPMIN blocks up to 1s — no busy-polling, efficient
            result = await self.redis.bzpopmin(QUEUE_KEY, timeout=1)

            if result is not None:
                consecutive_empty = 0
                _, url, score = result
                url = url.decode() if isinstance(url, bytes) else url
                depth = int(score) % 1000

                await self.redis.incr(BUSY_KEY)
                self._qsize_cache = max(0, self._qsize_cache - 1)
                return url, depth

            # Timed out — check if work is truly done
            queue_size = await self.redis.zcard(QUEUE_KEY)
            busy = int(await self.redis.get(BUSY_KEY) or 0)

            if queue_size == 0 and busy == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    return None
            else:
                consecutive_empty = 0

    async def set_idle(self) -> None:
        await self.redis.decr(BUSY_KEY)

    async def increment_total(self) -> int:
        """Increments global pages counter. Returns new total."""
        return int(await self.redis.incr(TOTAL_KEY))

    async def signal_stop(self) -> None:
        await self.redis.set(STOP_KEY, 1)

    async def publish_status(self, worker_id: int, status: str) -> None:
        import os
        field = f"{os.getpid()}:{worker_id}"
        await self.redis.hset("crawler:workers", field, status)
        await self.redis.expire("crawler:workers", 30)

    async def publish_activity(self, url: str) -> None:
        from datetime import datetime
        entry = f"{datetime.utcnow().strftime('%H:%M:%S')} {url}"
        await self.redis.lpush("crawler:activity", entry)
        await self.redis.ltrim("crawler:activity", 0, 99)

    async def publish_error(self, error_type: str) -> None:
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.incr("crawler:errors")
            pipe.hincrby("crawler:error_types", error_type, 1)
            await pipe.execute()

    async def set_start_time(self) -> None:
        import time
        await self.redis.setnx("crawler:start_time", str(time.time()))

    async def publish_log(self, entry: str) -> None:
        await self.redis.lpush("crawler:logs", entry)
        await self.redis.ltrim("crawler:logs", 0, 499)

    def qsize(self) -> int:
        return self._qsize_cache

    async def clear(self) -> None:
        await self.redis.delete(
            QUEUE_KEY, SEEN_KEY, BUSY_KEY, STOP_KEY, TOTAL_KEY,
            "crawler:workers", "crawler:activity",
            "crawler:errors", "crawler:error_types", "crawler:start_time",
        )
