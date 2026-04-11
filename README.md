# Async Sports Odds Crawler

A high-performance, asynchronous web crawler designed for efficient sports odds data harvesting. Built with modern Python concurrency primitives, it features a real-time terminal dashboard, adaptive scheduling, and buffered asynchronous storage.

## Features

- **High-Concurrency Engine:** Leverages `anyio` and `httpx` for efficient, non-blocking network I/O and worker pool management.
- **Real-Time Monitoring:** Interactive terminal dashboard powered by `rich`, providing live updates on crawl progress, worker status, and data collection rates.
- **Adaptive Scheduler:** Intelligent URL prioritization and deduplication to maximize crawling efficiency.
- **Buffered Async Storage:** High-throughput data persistence to JSONL format using asynchronous file operations.
- **Configurable Architecture:** Fine-tune performance parameters, concurrency limits, and target scopes via environment variables.
- **Distributed Mode:** Scale horizontally with Redis-backed scheduling and MongoDB storage across multiple containers.
- **robots.txt Compliance:** Automatically fetches and respects per-domain robots.txt rules.
- **Web Monitor:** Real-time HTTP dashboard (aiohttp + SSE) for monitoring distributed crawl runs.

## Project Structure

```text
src/
├── config/         # Settings and logging configuration
├── crawler/        # Core engine, scheduling logic, and robots.txt cache
├── models/         # Data models for extracted documents
├── monitor/        # HTTP monitoring server (aiohttp + SSE)
├── parser/         # Link extraction
├── scheduler/      # Redis-backed distributed scheduler
├── storage/        # Async file and MongoDB storage backends
├── ui/             # Terminal-based real-time dashboard
└── main.py         # Application entry point
```

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (recommended) or `pip`
- Docker + Docker Compose (for distributed mode)

### Local Installation

1. Clone the repository and install dependencies:
   ```bash
   git clone <repo-url>
   cd crawler
   uv sync
   ```

2. Create a `.env` file:
   ```bash
   MAX_PAGES=50000
   CONCURRENCY_LIMIT=100
   REQUEST_DELAY=0.0
   MAX_CONCURRENCY_PER_DOMAIN=8
   ```

3. Run the crawler:
   ```bash
   uv run python -m src.main
   ```

### Docker (Distributed Mode)

Start the full stack (Redis + MongoDB + 10 crawler replicas + monitor):

```bash
docker compose up --build
```

- **Monitor dashboard:** http://localhost:8080
- **RedisInsight:** http://localhost:5540
- **Mongo Express:** http://localhost:8081

Scale crawlers:
```bash
docker compose up --scale crawler=20
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `MAX_PAGES` | Total pages limit (global, across all instances) | `50000` |
| `CONCURRENCY_LIMIT` | Workers per crawler instance | `100` |
| `REQUEST_DELAY` | Delay between requests (seconds) | `0.0` |
| `MAX_CONCURRENCY_PER_DOMAIN` | Max simultaneous requests to the same domain | `8` |
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |
| `HEADLESS` | Disable terminal UI (for Docker) | `false` |

## Architecture

The system uses a protocol-based dependency injection design:

1. **Engine:** Orchestrates async workers, respects robots.txt, and uses per-domain rate limiting.
2. **Scheduler:** Pluggable — in-memory `AdaptiveScheduler` for single-node or `RedisScheduler` for distributed runs.
3. **Storage:** Pluggable — `FileStorage` (JSONL) or `MongoStorage` (buffered bulk inserts via Motor).
4. **Monitor:** Standalone aiohttp server reading Redis keys, streaming stats via SSE.

## License

MIT
