import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from redis.asyncio import Redis


@dataclass
class RunMetrics:
    chunk_size: int
    docs_processed: int = 0
    chunks_indexed: int = 0
    tokens_total: int = 0
    elapsed_sec: float = 0.0
    docs_per_sec: float = 0.0
    chunks_per_sec: float = 0.0
    tokens_per_sec: float = 0.0
    redis_used_memory_before: int = 0
    redis_used_memory_after: int = 0
    redis_index_size_bytes: int = 0
    redis_index_size_mb: float = 0.0
    num_terms: int = 0
    avg_postings_len: float = 0.0
    sample_postings: dict[str, int] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


class Timer:
    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed = time.perf_counter() - self._t0


async def redis_used_memory(redis: Redis) -> int:
    info = await redis.info("memory")
    return int(info.get("used_memory", 0))


async def collect_index_metrics(redis: Redis, sample_size: int = 100) -> dict[str, Any]:
    cursor = 0
    term_keys: list[bytes] = []
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match="idx:term:*", count=500)
        term_keys.extend(batch)
        if cursor == 0:
            break
    num_terms = len(term_keys)

    if not num_terms:
        return {
            "num_terms": 0,
            "avg_postings_len": 0.0,
            "redis_index_size_bytes": 0,
            "sample_postings": {},
        }

    sample = term_keys[: min(sample_size, num_terms)]
    pipe = redis.pipeline(transaction=False)
    for key in sample:
        pipe.scard(key)
    sizes = await pipe.execute()
    avg_postings = sum(sizes) / len(sizes) if sizes else 0.0

    pipe = redis.pipeline(transaction=False)
    for key in sample:
        pipe.memory_usage(key)
    mem_per_term = await pipe.execute()
    avg_mem = sum(m for m in mem_per_term if m) / len(mem_per_term)
    estimated_total = int(avg_mem * num_terms)

    sample_postings: dict[str, int] = {}
    for key, size in list(zip(sample, sizes))[:10]:
        k = key.decode() if isinstance(key, bytes) else key
        sample_postings[k] = int(size)

    return {
        "num_terms": num_terms,
        "avg_postings_len": round(avg_postings, 2),
        "redis_index_size_bytes": estimated_total,
        "sample_postings": sample_postings,
    }


def write_metrics(metrics: RunMetrics, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics.to_json(), indent=2, ensure_ascii=False))
