from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from redis.asyncio import Redis

from src.config.settings import settings


async def export(redis_url: str, out: Path) -> tuple[int, int]:
    redis: Redis = Redis.from_url(redis_url, decode_responses=True)
    await redis.ping()
    out.parent.mkdir(parents=True, exist_ok=True)
    n_terms = 0
    n_chunks = 0
    with out.open("w", encoding="utf-8") as fh:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(
                cursor=cursor, match="idx:term:*", count=500
            )
            for key in keys:
                postings = sorted(await redis.smembers(key))
                fh.write(
                    json.dumps(
                        {"term": key.removeprefix("idx:term:"), "postings": postings},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                n_terms += 1
            if cursor == 0:
                break

        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="chunk:*", count=500)
            for key in keys:
                meta = await redis.hgetall(key)
                meta["chunk"] = key.removeprefix("chunk:")
                fh.write(json.dumps(meta, ensure_ascii=False) + "\n")
                n_chunks += 1
            if cursor == 0:
                break

    await redis.aclose()
    return n_terms, n_chunks


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export Redis inverted index to JSONL")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--redis-url", type=str, default=settings.REDIS_URL)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    n_terms, n_chunks = asyncio.run(export(args.redis_url, args.out))
    print(f"exported {n_terms} terms + {n_chunks} chunks to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
