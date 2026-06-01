from __future__ import annotations

import argparse
import asyncio
import sys

from redis.asyncio import Redis
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from src.config.settings import settings
from src.indexer.nlp import content_words, tokenize_and_stem_bilingual
from src.indexer.ranking import bm25_score

console = Console()


def _dual_stem(query: str) -> tuple[list[str], list[str]]:
    """Stem with both PT+EN (stopwords from both), return (stems, highlight_terms)."""
    stems = tokenize_and_stem_bilingual(query) or [query.lower()]
    highlight = content_words(query)
    return stems, highlight


async def search(
    query: str,
    redis_url: str,
    k: int,
    *,
    mode: str = "and",
) -> tuple[list[dict], str, list[str]]:
    stems, highlight = _dual_stem(query)
    redis: Redis = Redis.from_url(redis_url, decode_responses=True)
    keys = [f"idx:term:{s}" for s in stems]

    if mode == "and" and len(keys) > 1:
        chunk_ids = list(await redis.sinter(*keys))
    else:
        all_ids: set[str] = set()
        for key in keys:
            all_ids.update(await redis.smembers(key))
        chunk_ids = list(all_ids)

    if not chunk_ids and mode == "and":
        all_ids = set()
        for key in keys:
            all_ids.update(await redis.smembers(key))
        chunk_ids = list(all_ids)
        mode = "or (fallback)"

    avgdl = float(await redis.get("idx:chunk_size") or 100)
    scored = await bm25_score(redis, stems, chunk_ids, avgdl=avgdl)
    top = scored[:k]

    results: list[dict] = []
    for cid, score, meta in top:
        meta["chunk_id"] = cid
        meta["score"] = round(score, 4)
        results.append(meta)

    await redis.aclose()
    return results, mode, highlight


def _snippet(meta: dict, max_len: int = 220) -> str:
    text = meta.get("raw") or meta.get("text") or ""
    text = text.replace("\n", " ").strip()
    return text[:max_len] + ("…" if len(text) > max_len else "")


def display_results(query: str, results: list[dict], mode: str) -> None:
    n = len(results)
    header = Text()
    header.append("Query: ", style="bold")
    header.append(query, style="cyan bold")
    header.append(f"  [{mode.upper()}]", style="dim")
    header.append(f"  {n} result{'s' if n != 1 else ''}", style="green" if n else "yellow")
    console.print(Panel(header, style="blue"))

    if not results:
        console.print("[yellow]Nenhum resultado encontrado.[/yellow]\n")
        return

    for i, r in enumerate(results, 1):
        score = r.get("score", 0.0)
        lang = r.get("lang", "?")
        url = r.get("doc_url", "")
        snippet = _snippet(r)

        rank_line = Text()
        rank_line.append(f"#{i}", style="bold green")
        rank_line.append("  score=", style="dim")
        rank_line.append(f"{score:.3f}", style="yellow bold")
        rank_line.append("  lang=", style="dim")
        rank_line.append(lang, style="cyan")
        rank_line.append("  ")
        rank_line.append(url, style="blue underline")

        console.print(rank_line)
        if snippet:
            console.print(f"    [dim]{snippet}[/dim]")
        console.print()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Search the inverted index with BM25 ranking")
    p.add_argument("query", nargs="+", help="Search query")
    p.add_argument("-k", type=int, default=10, help="Number of results (default: 10)")
    p.add_argument("--mode", choices=["and", "or"], default="and",
                   help="Query mode: AND (all terms, default) or OR (any term)")
    p.add_argument("--redis-url", type=str, default=settings.REDIS_URL)
    p.add_argument("--plain", action="store_true", help="Plain text output (no Rich)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    q = " ".join(args.query)
    results, mode, _ = asyncio.run(search(q, args.redis_url, args.k, mode=args.mode))

    if args.plain:
        print(f"query={q!r}  hits={len(results)}  mode={mode}")
        for r in results:
            print(f"  #{r.get('chunk_id')}  score={r['score']}  ({r.get('lang')})  {r.get('doc_url')}")
            print(f"    {_snippet(r)}")
    else:
        display_results(q, results, mode)

    return 0


if __name__ == "__main__":
    sys.exit(main())
