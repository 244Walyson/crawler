import re
import sys
from datetime import datetime
from urllib.parse import urlparse

import anyio

from src.config.logging import configure_logging, log_queue, logger
from src.config.settings import settings
from src.crawler.engine import CrawlerEngine
from src.parser.link_extractor import LinkExtractor
from src.ui.dashboard import CrawlerDashboard


def is_headless() -> bool:
    return settings.HEADLESS or not sys.stdout.isatty()


async def log_forwarder(scheduler) -> None:
    """Drains the log_queue and forwards entries to Redis in real time."""
    if not hasattr(scheduler, 'publish_log'):
        return

    _event_re = re.compile(r"event='([^']+)'")
    _url_re   = re.compile(r"url='([^']+)'")
    _lvl_re   = re.compile(r"level=(\S+)")

    while True:
        try:
            import queue as _q
            try:
                record = log_queue.get_nowait()
            except _q.Empty:
                await anyio.sleep(0.05)
                continue

            raw = record.getMessage() if hasattr(record, 'getMessage') else str(record)
            level = "info"
            m = _lvl_re.search(raw)
            if m:
                level = m.group(1).lower()

            event_m = _event_re.search(raw)
            url_m   = _url_re.search(raw)
            if event_m:
                event = event_m.group(1)
                url   = url_m.group(1)[:80] if url_m else ""
                msg   = f"{event} {url}".strip()
            else:
                msg = raw[:120]

            ts    = datetime.utcnow().strftime("%H:%M:%S")
            entry = f"{ts}|{level}|{msg}"
            await scheduler.publish_log(entry)

        except Exception:
            await anyio.sleep(0.1)


async def main() -> None:
    configure_logging()

    allowed_domains = {urlparse(url).netloc for url in settings.BASE_URLS}
    extractor = LinkExtractor(allowed_domains=list(allowed_domains))

    # Choose storage backend
    if settings.MONGODB_URI and settings.MONGODB_URI != "mongodb://localhost:27017":
        from src.storage.mongo_storage import MongoStorage
        storage = MongoStorage()
        logger.info("storage_backend", backend="mongodb")
    else:
        # Try MongoDB locally, fallback to file
        try:
            from src.storage.mongo_storage import MongoStorage
            storage = MongoStorage()
            logger.info("storage_backend", backend="mongodb")
        except Exception:
            from src.storage.file_storage import FileStorage
            storage = FileStorage()
            logger.info("storage_backend", backend="file")

    # Choose scheduler backend
    if settings.REDIS_URL and settings.REDIS_URL != "redis://localhost:6379":
        import redis.asyncio as aioredis
        from src.scheduler.redis_scheduler import RedisScheduler
        redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
        scheduler = RedisScheduler(redis_client)
        logger.info("scheduler_backend", backend="redis", url=settings.REDIS_URL)
    else:
        # Try Redis locally, fallback to in-memory
        try:
            import redis.asyncio as aioredis
            from src.scheduler.redis_scheduler import RedisScheduler
            redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
            await redis_client.ping()
            scheduler = RedisScheduler(redis_client)
            logger.info("scheduler_backend", backend="redis")
        except Exception:
            from src.crawler.scheduler import AdaptiveScheduler
            scheduler = AdaptiveScheduler()
            logger.info("scheduler_backend", backend="in-memory")

    engine = CrawlerEngine(extractor=extractor, storage=storage, scheduler=scheduler)

    try:
        if is_headless():
            async with anyio.create_task_group() as tg:
                tg.start_soon(engine.run)
                tg.start_soon(log_forwarder, scheduler)
        else:
            dashboard = CrawlerDashboard(engine)
            async with anyio.create_task_group() as tg:
                tg.start_soon(engine.run)
                tg.start_soon(dashboard.run)
                tg.start_soon(log_forwarder, scheduler)
    except (KeyboardInterrupt, anyio.get_cancelled_exc_class()):
        logger.info("crawler_interrupted")
    except Exception as e:
        if "TaskGroup" not in str(e):
            logger.error("crawler_fatal_error", error=str(e))


if __name__ == "__main__":
    anyio.run(main)
