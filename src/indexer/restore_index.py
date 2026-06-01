"""Restore the Redis inverted index from an exported JSONL dump."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from redis.asyncio import Redis

from src.config.settings import settings

BATCH = 2000


async def restore(dump_path: Path, redis_url: str, *, flush: bool) -> tuple[int, int]:
    redis: Redis = Redis.from_url(redis_url, decode_responses=True)
    await redis.ping()

    if flush:
        await redis.flushdb()

    n_terms = n_chunks = 0
    pipe = redis.pipeline(transaction=False)
    ops = 0

    with dump_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            if "term" in obj:
                term = obj["term"]
                for chunk_id in obj["postings"]:
                    pipe.sadd(f"idx:term:{term}", chunk_id)
                    ops += 1
                n_terms += 1

            elif "chunk" in obj:
                chunk_id = obj.pop("chunk")
                pipe.hset(f"chunk:{chunk_id}", mapping=obj)
                ops += 1
                n_chunks += 1

            if ops >= BATCH:
                await pipe.execute()
                pipe = redis.pipeline(transaction=False)
                ops = 0

    if ops:
        await pipe.execute()

    # Reconstruct counters from the data we just loaded
    await redis.set("idx:total_chunks", n_chunks)
    await redis.set("idx:chunk_size", 100)  # default from original indexing run

    await redis.aclose()
    return n_terms, n_chunks


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Restore index from JSONL dump into Redis")
    p.add_argument("--dump", type=Path, default=Path("relatorio/index_dump.jsonl"))
    p.add_argument("--redis-url", type=str, default=settings.REDIS_URL)
    p.add_argument("--no-flush", dest="flush", action="store_false",
                   help="Keep existing Redis data (default: flush first)")
    p.set_defaults(flush=True)
    args = p.parse_args(argv)

    n_terms, n_chunks = asyncio.run(restore(args.dump, args.redis_url, flush=args.flush))
    print(f"Restored {n_terms} terms + {n_chunks} chunks from {args.dump}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
