FROM ghcr.io/astral-sh/uv:0.11.8 AS uv
FROM python:3.11.14-slim-bookworm
COPY --from=uv /uv /uvx /bin/
ENV UV_PROJECT_ENVIRONMENT=/opt/venv PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
WORKDIR /app
COPY src/pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/producer.py ./
ENTRYPOINT ["python", "/app/producer.py"]
