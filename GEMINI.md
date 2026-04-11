# System Instructions: Refactor Crawler for IR Course (Part 1 - Raw Data Collection)

## 🎯 Objective
Refactor and simplify the current crawler architecture. The existing implementation is over-engineered for the current project phase because it parses HTML and extracts structured entities (betting odds, teams). 

For **Part 1** of our academic Web Information Retrieval (IR) assignment, we MUST act strictly as a raw crawler/spider. We only need to discover URLs, download the raw HTML, and store it. All parsing, text extraction, and entity modeling belong to Part 2.

Maximize system effectiveness using this priority:
**Volume (Scale to 50k+ pages) > Politeness (Delays/Robots.txt) > Raw Storage Efficiency**

---

## 🧩 Requirements for Refactoring

### 1. Remove Parsing & Extraction Logic (Strict)
- Completely remove the `OddsParser` and any references to `selectolax` or `BeautifulSoup` from the crawler loop.
- Stop extracting entities (teams, odds, sports, markets).
- Remove quality gates related to missing data fields; the only quality gate now is a successful HTTP 200 response and valid text/html content-type.

### 2. Update Data Models & Storage strategy
- Refactor the `Event` Pydantic models. Replace them with a `RawWebDocument` model.
- The stored JSONL data **must** follow this exact schema, capturing the raw HTTP response:
  ```json
  {
    "url": "[https://example.com](https://example.com)",
    "timestamp": "2026-04-11T00:26:04.152",
    "status_code": 200,
    "depth": 2,
    "html": "<!DOCTYPE html><html>...[entire raw HTML body]...</html>"
  }
