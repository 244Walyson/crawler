from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from redis.asyncio import Redis

from src.config.logging import logger
from src.config.settings import settings
from src.indexer.chunker import chunk_tokens
from src.indexer.cleaner import clean_html
from src.indexer.corpus import DEFAULT_DUMP, iter_documents
from src.indexer.inverted_index import index_doc
from src.indexer.lang import detect_lang
from src.indexer.metrics import (
    RunMetrics,
    Timer,
    collect_index_metrics,
    redis_used_memory,
    write_metrics,
)
from src.indexer.nlp import tokenize_and_stem


async def run(
    *,
    chunk_size: int,
    limit: int | None,
    dump_path: Path,
    redis_url: str,
    flush: bool,
    metrics_out: Path | None,
    progress_every: int,
) -> RunMetrics:
    redis: Redis = Redis.from_url(redis_url)
    await redis.ping()
    if flush:
        await redis.flushdb()

    metrics = RunMetrics(chunk_size=chunk_size)
    metrics.redis_used_memory_before = await redis_used_memory(redis)

    with Timer() as t:
        for doc in iter_documents(dump_path, limit=limit):
            doc_id = doc["_id"]
            url = doc["url"]
            html = doc["html"]

            text = clean_html(html)
            if not text:
                continue
            lang = detect_lang(url, text)
            stems = tokenize_and_stem(text, lang)
            if not stems:
                continue

            chunks = chunk_tokens(stems, doc_id, chunk_size)
            await index_doc(
                redis,
                doc_id=doc_id,
                doc_url=url,
                lang=lang,
                chunks=chunks,
                raw_text=text,
            )

            metrics.docs_processed += 1
            metrics.chunks_indexed += len(chunks)
            metrics.tokens_total += len(stems)
            if progress_every and metrics.docs_processed % progress_every == 0:
                logger.info(
                    "indexer_progress",
                    docs=metrics.docs_processed,
                    chunks=metrics.chunks_indexed,
                    tokens=metrics.tokens_total,
                )

    metrics.elapsed_sec = round(t.elapsed, 3)
    metrics.docs_per_sec = round(metrics.docs_processed / max(t.elapsed, 1e-9), 2)
    metrics.chunks_per_sec = round(metrics.chunks_indexed / max(t.elapsed, 1e-9), 2)
    metrics.tokens_per_sec = round(metrics.tokens_total / max(t.elapsed, 1e-9), 2)

    metrics.redis_used_memory_after = await redis_used_memory(redis)
    idx = await collect_index_metrics(redis)
    metrics.num_terms = idx["num_terms"]
    metrics.avg_postings_len = idx["avg_postings_len"]
    metrics.redis_index_size_bytes = idx["redis_index_size_bytes"]
    metrics.redis_index_size_mb = round(idx["redis_index_size_bytes"] / (1024**2), 3)
    metrics.sample_postings = idx["sample_postings"]

    if metrics_out:
        write_metrics(metrics, metrics_out)

    await redis.aclose()
    return metrics


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 2 inverted index builder")
    p.add_argument("--chunk-size", type=int, default=100)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dump", type=Path, default=DEFAULT_DUMP)
    p.add_argument("--redis-url", type=str, default=settings.REDIS_URL)
    p.add_argument(
        "--no-flush",
        dest="flush",
        action="store_false",
        help="Do not FLUSHDB before indexing (default: flush).",
    )
    p.add_argument("--metrics-out", type=Path, default=None)
    p.add_argument("--progress-every", type=int, default=200)
    p.set_defaults(flush=True)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    metrics = asyncio.run(
        run(
            chunk_size=args.chunk_size,
            limit=args.limit,
            dump_path=args.dump,
            redis_url=args.redis_url,
            flush=args.flush,
            metrics_out=args.metrics_out,
            progress_every=args.progress_every,
        )
    )
    print(
        f"\n=== Run summary ===\n"
        f"chunk_size       : {metrics.chunk_size}\n"
        f"docs_processed   : {metrics.docs_processed}\n"
        f"chunks_indexed   : {metrics.chunks_indexed}\n"
        f"tokens_total     : {metrics.tokens_total}\n"
        f"elapsed_sec      : {metrics.elapsed_sec}\n"
        f"docs/sec         : {metrics.docs_per_sec}\n"
        f"chunks/sec       : {metrics.chunks_per_sec}\n"
        f"num_terms        : {metrics.num_terms}\n"
        f"avg_postings_len : {metrics.avg_postings_len}\n"
        f"index_size_MB    : {metrics.redis_index_size_mb}\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
