def chunk_tokens(
    stems: list[str], doc_id: str, n: int
) -> list[tuple[str, list[str]]]:
    if n <= 0:
        raise ValueError("chunk size must be positive")
    return [(f"{doc_id}:{i // n}", stems[i : i + n]) for i in range(0, len(stems), n)]
