FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.8.21 /uv /usr/local/bin/uv
WORKDIR /app
COPY . /app
RUN uv sync --locked --no-dev
EXPOSE 8787
ENTRYPOINT ["/app/.venv/bin/swarm-radio"]
