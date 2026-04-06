import asyncio
import psutil
import queue
import time
import re
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text

from src.config.logging import log_queue
from src.config.settings import settings


class CrawlerDashboard:
    def __init__(self, engine: Any) -> None:
        self.engine = engine
        self.console = Console()
        self.process = psutil.Process()
        # Prime the CPU usage tracker
        self.process.cpu_percent(interval=None)
        self.recent_logs: list[str] = []
        self.start_time = time.time()

    def make_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="logs", size=10),
            Layout(name="footer", size=3)
        )
        layout["main"].split_row(
            Layout(name="stats", ratio=1),
            Layout(name="workers", ratio=2)
        )
        return layout

    def get_stats_table(self) -> Table:
        stats = self.engine.stats
        
        # Calculate run time
        elapsed = time.time() - self.start_time
        mins, secs = divmod(int(elapsed), 60)
        run_time = f"{mins:02d}:{secs:02d}"
        
        # Get process resources
        cpu_percent = self.process.cpu_percent(interval=None)
        memory_mb = self.process.memory_info().rss / (1024 * 1024)
        
        table = Table(title="Crawler Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        table.add_row("Pages Collected", f"[bold green]{stats['collected']}[/]")
        table.add_row("Events Extracted", f"[bold blue]{stats['events']}[/]")
        table.add_row("Errors", f"[bold red]{stats['errors']}[/]")
        table.add_row("Queue Size", str(stats["queue_size"]))
        table.add_row("Run Time", run_time)
        table.add_row("CPU Usage", f"{cpu_percent:.1f}%")
        table.add_row("Memory Usage", f"{memory_mb:.1f} MB")
        return table

    def get_workers_table(self) -> Table:
        stats = self.engine.stats
        table = Table(title="Worker Status")
        table.add_column("ID", style="dim")
        table.add_column("Current Task", style="green")
        for worker_id, status in stats["workers"].items():
            table.add_row(str(worker_id), status)
        return table

    def get_logs_panel(self) -> Panel:
        while True:
            try:
                msg = log_queue.get_nowait()
                formatted = ""
                
                if hasattr(msg, "getMessage"):
                    raw = msg.getMessage()
                    # Clean up logfmt style strings for better UI readability
                    # Example: event='page_fetched' url='http...' -> page_fetched: http...
                    event_match = re.search(r"event='([^']+)'", raw)
                    url_match = re.search(r"url='([^']+)'", raw)
                    
                    if event_match:
                        event_name = event_match.group(1)
                        detail = url_match.group(1) if url_match else ""
                        formatted = f"[{datetime.now().strftime('%H:%M:%S')}] {event_name}: {detail[:60]}"
                    else:
                        formatted = f"[{datetime.now().strftime('%H:%M:%S')}] {raw[:80]}"
                    
                    if formatted:
                        self.recent_logs.append(formatted)
                
                if len(self.recent_logs) > 8:
                    self.recent_logs.pop(0)
            except queue.Empty:
                break
        
        log_text = Text("\n".join(self.recent_logs))
        return Panel(log_text, title="Recent Activity", style="dim white")

    async def run(self) -> None:
        layout = self.make_layout()
        progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
        )
        task_id = progress.add_task("Crawling...", total=settings.MAX_PAGES)
        
        layout["header"].update(Panel(f"Sports Odds Crawler - {datetime.now().strftime('%H:%M:%S')}", style="bold blue"))
        layout["footer"].update(progress)

        with Live(layout, refresh_per_second=4, console=self.console):
            while True:
                try:
                    stats = self.engine.stats
                    progress.update(task_id, completed=stats["collected"])
                    layout["header"].update(Panel(f"Sports Odds Crawler - {datetime.now().strftime('%H:%M:%S')}", style="bold blue"))
                    layout["stats"].update(Panel(self.get_stats_table()))
                    layout["workers"].update(Panel(self.get_workers_table()))
                    layout["logs"].update(self.get_logs_panel())
                    
                    if stats["is_done"]:
                        break
                    
                    if len(stats["workers"]) > 0 and all(s == "Finished" for s in stats["workers"].values()) and stats["queue_size"] == 0:
                         break
                except Exception:
                    # Silence errors during shutdown
                    break
                
                await asyncio.sleep(0.25)
