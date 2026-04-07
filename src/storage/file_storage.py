import asyncio
from typing import Any, List, Optional

import anyio
from anyio import AsyncFile

from src.config.logging import logger
from src.config.settings import settings


class FileStorage:
    def __init__(self, filename: str = "data.jsonl") -> None:
        self.filename = filename
        self.buffer: List[Any] = []
        self._lock = asyncio.Lock()
        self._file: Optional[AsyncFile] = None

    async def _ensure_file(self) -> AsyncFile:
        if self._file is None:
            self._file = await anyio.open_file(self.filename, mode='a')
        return self._file

    async def save(self, event: Any) -> None:
        to_flush_items = []
        async with self._lock:
            self.buffer.append(event)
            if len(self.buffer) >= settings.BUFFER_SIZE:
                to_flush_items = self.buffer
                self.buffer = []

        if to_flush_items:
            to_flush = await anyio.to_thread.run_sync(
                lambda items: [e.model_dump_json() for e in items],
                to_flush_items
            )
            await self._write_to_disk(to_flush)

    async def flush(self) -> None:
        to_flush_items = []
        async with self._lock:
            if self.buffer:
                to_flush_items = self.buffer
                self.buffer = []

        if to_flush_items:
            to_flush = await anyio.to_thread.run_sync(
                lambda items: [e.model_dump_json() for e in items],
                to_flush_items
            )
            await self._write_to_disk(to_flush)
        
        if self._file:
            await self._file.flush()

    async def _write_to_disk(self, data: List[str]) -> None:
        try:
            f = await self._ensure_file()
            await f.write('\n'.join(data) + '\n')
            # Periodic flush to OS cache
            if len(data) > 10:
                await f.flush()
            logger.debug("storage_written", count=len(data))
        except Exception as e:
            logger.error("storage_failed", error=str(e))

    async def close(self) -> None:
        if self._file:
            await self._file.aclose()
            self._file = None
