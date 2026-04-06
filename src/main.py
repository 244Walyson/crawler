import anyio
from urllib.parse import urlparse
from src.crawler.engine import CrawlerEngine
from src.parser.odds_parser import OddsParser
from src.storage.file_storage import FileStorage
from src.config.settings import settings
from src.config.logging import configure_logging, logger
from src.ui.dashboard import CrawlerDashboard

async def main() -> None:
    # Setup
    configure_logging()
    
    # Extract domains from BASE_URLS for the allowed domains list
    allowed_domains = {urlparse(url).netloc for url in settings.BASE_URLS}
    
    parser = OddsParser(allowed_domains=list(allowed_domains))
    storage = FileStorage()
    engine = CrawlerEngine(parser=parser, storage=storage)
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
        # Check if it's just a combined exception from task group termination
        if "TaskGroup" not in str(e):
            logger.error("crawler_fatal_error", error=str(e))

if __name__ == "__main__":
    anyio.run(main)
