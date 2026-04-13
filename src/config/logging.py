import structlog
import logging
import sys
from logging.handlers import QueueHandler
import queue

# Global queue for logs to be consumed by the UI
log_queue = queue.Queue()

def configure_logging(level=logging.INFO):
    # Standard logging setup
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # ONLY QueueHandler for UI - no more direct StreamHandler to avoid UI breaking
    root_logger.addHandler(QueueHandler(log_queue))

    # Silence noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.UnicodeDecoder(),
            structlog.processors.format_exc_info,
            structlog.processors.LogfmtRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

logger = structlog.get_logger()
