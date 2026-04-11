FROM python:3.12-slim

RUN pip install uv --quiet

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --quiet

COPY src/ src/

ENV PATH="/app/.venv/bin:$PATH"

CMD ["uv", "run", "python", "-m", "src.main"]
