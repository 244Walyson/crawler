from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import ijson
import zstandard as zstd

DEFAULT_DUMP = Path(__file__).resolve().parents[2] / "odds_data.json.zst"


def iter_documents_mongo(
    mongo_uri: str = "mongodb://localhost:27017",
    db: str = "sports_crawler",
    collection: str = "odds_data",
    *,
    limit: int | None = None,
    skip_empty_html: bool = True,
) -> Iterator[dict[str, Any]]:
    """Yield documents directly from a MongoDB collection."""
    from pymongo import MongoClient

    client: MongoClient = MongoClient(mongo_uri)
    col = client[db][collection]
    cursor = col.find({}, {"_id": 1, "url": 1, "html": 1, "depth": 1, "status_code": 1, "timestamp": 1})
    if limit:
        cursor = cursor.limit(limit)
    yielded = 0
    for raw in cursor:
        html = raw.get("html") or ""
        if skip_empty_html and not html.strip():
            continue
        yield {
            "_id": str(raw["_id"]),
            "url": raw.get("url") or "",
            "depth": raw.get("depth", 0),
            "status_code": raw.get("status_code", 0),
            "timestamp": raw.get("timestamp"),
            "html": html,
        }
        yielded += 1
        if limit is not None and yielded >= limit:
            break
    client.close()


def _parse_dt(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        d = raw.get("$date")
        if isinstance(d, str):
            return datetime.fromisoformat(d.replace("Z", "+00:00"))
        if isinstance(d, dict) and "$numberLong" in d:
            return datetime.fromtimestamp(int(d["$numberLong"]) / 1000)
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _doc_id(raw: Any) -> str:
    if isinstance(raw, dict) and "$oid" in raw:
        return str(raw["$oid"])
    return str(raw)


def iter_documents(
    dump_path: Path | str = DEFAULT_DUMP,
    *,
    limit: int | None = None,
    skip_empty_html: bool = True,
) -> Iterator[dict[str, Any]]:
    path = Path(dump_path)
    if not path.exists():
        raise FileNotFoundError(f"Dump not found: {path}")

    dctx = zstd.ZstdDecompressor(max_window_size=2**31)
    yielded = 0
    with path.open("rb") as fh, dctx.stream_reader(fh) as reader:
        for raw in ijson.items(reader, "item", use_float=True):
            html = raw.get("html") or ""
            if skip_empty_html and not html.strip():
                continue
            yield {
                "_id": _doc_id(raw.get("_id")),
                "url": raw.get("url") or "",
                "depth": raw.get("depth", 0),
                "status_code": raw.get("status_code", 0),
                "timestamp": _parse_dt(raw.get("timestamp")),
                "html": html,
            }
            yielded += 1
            if limit is not None and yielded >= limit:
                break
