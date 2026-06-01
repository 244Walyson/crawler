from __future__ import annotations

import math

from redis.asyncio import Redis

K1 = 1.5
B = 0.75
DEFAULT_AVGDL = 100.0


async def _total_chunks(redis: Redis) -> int:
    val = await redis.get("idx:total_chunks")
    if val:
        return max(int(val), 1)
    # Fallback: scan chunk keys (slow, only used if counter was not set)
    cursor, count = 0, 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="chunk:*", count=1000)
        count += len(keys)
        if cursor == 0:
            break
    return max(count, 1)


async def bm25_score(
    redis: Redis,
    query_stems: list[str],
    chunk_ids: list[str],
    *,
    avgdl: float = DEFAULT_AVGDL,
) -> list[tuple[str, float, dict]]:
    """Score chunk_ids with BM25. Returns (chunk_id, score, meta) sorted descending."""
    if not chunk_ids or not query_stems:
        return []

    pipe = redis.pipeline(transaction=False)
    for stem in query_stems:
        pipe.scard(f"idx:term:{stem}")
    dfs = [max(int(v), 1) for v in await pipe.execute()]

    N = await _total_chunks(redis)

    pipe = redis.pipeline(transaction=False)
    for cid in chunk_ids:
        pipe.hgetall(f"chunk:{cid}")
    metas = await pipe.execute()

    scored: list[tuple[str, float, dict]] = []
    for cid, meta in zip(chunk_ids, metas):
        if not meta:
            continue
        # Use stems field for TF; fall back to text for backwards compat
        stem_text = meta.get("stems", meta.get("text", ""))
        stem_list = stem_text.split()
        doc_len = len(stem_list) or avgdl

        score = 0.0
        for stem, df in zip(query_stems, dfs):
            tf = stem_list.count(stem)
            idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
            num = tf * (K1 + 1)
            den = tf + K1 * (1 - B + B * doc_len / avgdl)
            score += idf * (num / max(den, 1e-9))

        scored.append((cid, score, meta))

    return sorted(scored, key=lambda x: x[1], reverse=True)
