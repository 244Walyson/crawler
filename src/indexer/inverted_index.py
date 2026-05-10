from redis.asyncio import Redis

PIPELINE_BATCH = 2000


async def index_chunk(
    redis: Redis,
    chunk_id: str,
    stems: list[str],
    *,
    doc_url: str,
    lang: str,
    text_preview: str,
) -> None:
    pipe = redis.pipeline(transaction=False)
    pipe.hset(
        f"chunk:{chunk_id}",
        mapping={
            "doc_url": doc_url,
            "lang": lang,
            "text": text_preview,
        },
    )
    unique = set(stems)
    for stem in unique:
        pipe.sadd(f"idx:term:{stem}", chunk_id)
    await pipe.execute()


async def index_doc(
    redis: Redis,
    *,
    doc_id: str,
    doc_url: str,
    lang: str,
    chunks: list[tuple[str, list[str]]],
    raw_text: str,
) -> int:
    if not chunks:
        return 0
    pipe = redis.pipeline(transaction=False)
    ops = 0
    for chunk_id, stems in chunks:
        offset_start = chunk_id.split(":")[-1]
        text_preview = " ".join(stems)[:400]
        pipe.hset(
            f"chunk:{chunk_id}",
            mapping={
                "doc_url": doc_url,
                "lang": lang,
                "text": text_preview,
                "offset": offset_start,
            },
        )
        pipe.sadd(f"doc:{doc_id}:chunks", chunk_id)
        for stem in set(stems):
            pipe.sadd(f"idx:term:{stem}", chunk_id)
            ops += 1
            if ops >= PIPELINE_BATCH:
                await pipe.execute()
                pipe = redis.pipeline(transaction=False)
                ops = 0
    if ops:
        await pipe.execute()
    return len(chunks)
