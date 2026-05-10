from __future__ import annotations

import argparse
import asyncio
import sys

from redis.asyncio import Redis

from src.config.settings import settings
from src.indexer.lang import detect_lang
from src.indexer.nlp import tokenize_and_stem


async def search(query: str, redis_url: str, k: int) -> list[dict]:
    lang = detect_lang("https://example.en/", query)
    stems = tokenize_and_stem(query, lang) or [query.lower()]
    redis: Redis = Redis.from_url(redis_url, decode_responses=True)
    keys = [f"idx:term:{s}" for s in stems]
    chunk_ids = await redis.sinter(*keys) if len(keys) > 1 else await redis.smembers(keys[0])
    chunk_ids = list(chunk_ids)[:k]
    results = []
    for cid in chunk_ids:
        meta = await redis.hgetall(f"chunk:{cid}")
        meta["chunk_id"] = cid
        results.append(meta)
    await redis.aclose()
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("query", nargs="+")
    p.add_argument("-k", type=int, default=5)
    p.add_argument("--redis-url", type=str, default=settings.REDIS_URL)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    q = " ".join(args.query)
    results = asyncio.run(search(q, args.redis_url, args.k))
    print(f"query={q!r}  hits={len(results)}")
    for r in results:
        print(f"- {r.get('chunk_id')}  ({r.get('lang')})  {r.get('doc_url')}")
        text = r.get("text", "")
        print(f"    {text[:160]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
