from __future__ import annotations

import argparse
import asyncio
import sys

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.text import Text

from src.config.settings import settings
from src.indexer.search import display_results, search

console = Console()

BANNER = """
  ╔═══════════════════════════════════════╗
  ║   Sports Odds Search  —  BM25 index  ║
  ╚═══════════════════════════════════════╝
  [dim]Comandos:[/dim]
    [cyan]<consulta>[/cyan]           busca em modo AND (todos os termos)
    [cyan]or: <consulta>[/cyan]       busca em modo OR  (algum dos termos)
    [cyan]exit[/cyan] / [cyan]Ctrl-C[/cyan]      sair
"""


async def _repl(redis_url: str, k: int) -> None:
    console.print(Panel(Text.from_markup(BANNER.strip()), style="bold blue", padding=(1, 2)))

    while True:
        try:
            raw = Prompt.ask("\n[bold cyan]busca[/bold cyan]").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Até logo![/dim]")
            break

        if not raw or raw.lower() in {"exit", "quit", "q", "sair"}:
            console.print("[dim]Até logo![/dim]")
            break

        mode = "and"
        query = raw
        if raw.lower().startswith("or:"):
            mode = "or"
            query = raw[3:].strip()

        if not query:
            continue

        try:
            results, actual_mode = await search(query, redis_url, k, mode=mode)
            display_results(query, results, actual_mode)
        except Exception as exc:
            console.print(f"[red bold]Erro:[/red bold] {exc}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Interactive BM25 search REPL")
    p.add_argument("-k", type=int, default=10, help="Max results per query (default: 10)")
    p.add_argument("--redis-url", type=str, default=settings.REDIS_URL)
    args = p.parse_args(argv)
    asyncio.run(_repl(args.redis_url, args.k))
    return 0


if __name__ == "__main__":
    sys.exit(main())
