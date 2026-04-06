import structlog
import logging
import sys
from logging.handlers import QueueHandler
import queue

# Global queue for logs to be consumed by the UI
log_queue = queue.Queue()

def configure_logging(level=logging.INFO):
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove all existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    # ONLY QueueHandler for UI - no more direct StreamHandler to avoid UI breaking
    root_logger.addHandler(QueueHandler(log_queue))

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
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
