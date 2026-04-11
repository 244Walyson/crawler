# Project Analysis: Odds Portal Crawler

## 1. Executive Summary
This project is a high-performance, asynchronous web crawler developed in Python to collect sports betting odds from various online sources. It was designed as part of a Web Information Retrieval course, aiming for a scale of 50,000+ pages.

## 2. Technical Architecture

### Core Components
- **`CrawlerEngine` (`src/crawler/engine.py`)**: The central orchestrator. It manages the lifecycle of the crawler, including worker initialization, HTTP client management (using `httpx`), and the main execution loop. It leverages `anyio` for structured concurrency.
- **`AdaptiveScheduler` (`src/crawler/scheduler.py`)**: A priority-based task manager. It handles URL deduplication (preventing redundant visits) and uses an `asyncio.PriorityQueue` to ensure that higher-importance URLs are processed first. It also tracks domain yields to optimize traversal.
- **`OddsParser` (`src/parser/odds_parser.py`)**: A generic, high-speed parser using `selectolax` (Lexbor engine). It extracts match information (teams, sports, tournament) and betting odds from HTML containers. It includes a "Quality Gate" to filter out low-value data.
- **Data Models (`src/models/event.py`)**: Structured data representation using Pydantic, ensuring data integrity for sports events and their respective odds.
- **Storage (`src/storage/file_storage.py`)**: Currently implements a JSONL (JSON Lines) storage strategy, which is ideal for streaming large amounts of data without high memory overhead.

### Key Performance Features
1. **Asynchronous I/O**: Built on `httpx` and `anyio`, allowing hundreds of concurrent connections without blocking.
2. **Fast HTML Parsing**: Uses `selectolax` instead of BeautifulSoup. Lexbor is written in C and is significantly faster, which is critical when parsing tens of thousands of pages.
3. **HTTP/2 Support**: Enabled in the `AsyncClient` for better multiplexing and reduced latency.
4. **Offloaded Parsing**: The engine uses `anyio.to_thread.run_sync` to run the CPU-intensive parsing logic in worker threads, keeping the async event loop responsive for I/O.
5. **Modern Tooling**: Managed by `uv`, ensuring reproducible environments and extremely fast dependency resolution.

## 3. Project Status & Characteristics
- **Complexity**: High. Implements advanced patterns like Protocols for dependency inversion and structured concurrency for reliability.
- **Scalability**: Designed to be horizontally scalable by adjusting concurrency limits and rate-limiting policies via environment variables.
- **Politeness**: Implements `REQUEST_DELAY` and domain-based scheduling to respect target server limits.
- **Current Goal**: Reaching the 50,000-page mark for the academic requirement.

## 4. Recommendations for Improvement
- **Distributed State**: For even larger scales, the `visited` set and `queue` could be moved to Redis to allow multiple crawler instances to work together.
- **Proxy Support**: To avoid IP-based blocking at high volumes, a proxy rotation middleware could be integrated into the `httpx` client.
- **Advanced Normalization**: Implementing fuzzy matching for team names to merge data from different providers more accurately.
