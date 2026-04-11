# Async Sports Odds Crawler

A high-performance, asynchronous web crawler designed for efficient sports odds data harvesting. Built with modern Python concurrency primitives, it features a real-time terminal dashboard, adaptive scheduling, and buffered asynchronous storage.

## Features

- **High-Concurrency Engine:** Leverages `anyio` and `httpx` for efficient, non-blocking network I/O and worker pool management.
- **Real-Time Monitoring:** Interactive terminal dashboard powered by `rich`, providing live updates on crawl progress, worker status, and data collection rates.
- **Adaptive Scheduler:** Intelligent URL prioritization and deduplication to maximize crawling efficiency.
- **Buffered Async Storage:** High-throughput data persistence to JSONL format using asynchronous file operations.
- **Configurable Architecture:** Fine-tune performance parameters, concurrency limits, and target scopes via environment variables.

## Project Structure

```text
src/
├── config/         # Settings and logging configuration
├── crawler/        # Core engine and scheduling logic
├── models/         # Data models for extracted documents
├── parser/         # Link extraction and HTML parsing
├── storage/        # Async file-based data persistence
├── ui/             # Terminal-based real-time dashboard
└── main.py         # Application entry point
```

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (recommended) or `pip`

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/async-sports-crawler.git
   cd async-sports-crawler
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

3. Create a `.env` file from the template:
   ```bash
   cp .env.example .env  # If .env.example exists, otherwise create .env
   ```

### Usage

Run the crawler using `uv`:

```bash
uv run src/main.py
```

## Configuration

The application is configured through environment variables or a `.env` file. Key settings include:

| Variable | Description | Default |
|----------|-------------|---------|
| `MAX_CONCURRENT_REQUESTS` | Maximum number of simultaneous worker tasks. | `10` |
| `REQUEST_TIMEOUT` | Network request timeout in seconds. | `30.0` |
| `TARGET_PAGE_COUNT` | Limit the total number of pages to crawl. | `100` |
| `OUTPUT_FILE` | Path to the JSONL output file. | `data/results.jsonl` |

## Technical Architecture

The system is designed around a producer-consumer model:
1. **Engine:** Orchestrates a pool of asynchronous workers.
2. **Workers:** Fetch pages, extract data, and discover new links.
3. **Storage:** Consumes processed data from an internal queue and persists it using buffered writes to minimize disk I/O overhead.
4. **UI:** Periodically polls the engine state to update the terminal dashboard.

## License

MIT
