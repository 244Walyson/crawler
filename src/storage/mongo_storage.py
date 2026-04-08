import asyncio
from typing import Any, List

from motor.motor_asyncio import AsyncIOMotorClient

from src.config.settings import settings
from src.config.logging import logger


class MongoStorage:
    """
    Async MongoDB storage using Motor.
    Buffers documents and bulk-inserts for throughput.
    """

    def __init__(self) -> None:
        self.client = AsyncIOMotorClient(settings.MONGODB_URI)
        self.collection = self.client[settings.DATABASE_NAME][settings.COLLECTION_NAME]
        self.buffer: List[Any] = []
        self._lock = asyncio.Lock()

    async def save(self, event: Any) -> None:
        to_flush: List[Any] = []
        async with self._lock:
            self.buffer.append(event.model_dump())
            if len(self.buffer) >= settings.BUFFER_SIZE:
                to_flush = self.buffer
                self.buffer = []

        if to_flush:
            await self._insert(to_flush)

    async def flush(self) -> None:
        async with self._lock:
            to_flush = self.buffer
            self.buffer = []

        if to_flush:
            await self._insert(to_flush)

    async def _insert(self, docs: List[Any]) -> None:
        try:
            await self.collection.insert_many(docs, ordered=False)
            logger.debug("mongo_written", count=len(docs))
        except Exception as e:
            logger.error("mongo_failed", error=str(e))

    async def close(self) -> None:
        self.client.close()
