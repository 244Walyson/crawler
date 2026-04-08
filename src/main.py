import anyio
from urllib.parse import urlparse
from src.crawler.engine import CrawlerEngine
from src.parser.link_extractor import LinkExtractor
from src.storage.file_storage import FileStorage
from src.config.settings import settings
from src.config.logging import configure_logging, logger
from src.ui.dashboard import CrawlerDashboard

async def main() -> None:
    configure_logging()

    allowed_domains = {urlparse(url).netloc for url in settings.BASE_URLS}
    extractor = LinkExtractor(allowed_domains=list(allowed_domains))
    storage = FileStorage()
    engine = CrawlerEngine(extractor=extractor, storage=storage)
    dashboard = CrawlerDashboard(engine)

    logger.info(
        "crawler_starting",
        max_pages=settings.MAX_PAGES,
        concurrency=settings.CONCURRENCY_LIMIT
    )

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(engine.run)
            tg.start_soon(dashboard.run)
    except (KeyboardInterrupt, anyio.get_cancelled_exc_class()):
        logger.info("crawler_interrupted")
    except Exception as e:
        if "TaskGroup" not in str(e):
            logger.error("crawler_fatal_error", error=str(e))

if __name__ == "__main__":
    anyio.run(main)
