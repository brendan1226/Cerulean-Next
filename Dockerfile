# ═══════════════════════════════════════════════════
# Cerulean Next — Dockerfile
# ═══════════════════════════════════════════════════

FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

# ── Dev target (hot-reload, no optimisation) ──────
FROM base AS dev
COPY . .
CMD ["uvicorn", "cerulean.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# ── Prod target (no dev deps, non-root user) ──────
FROM base AS prod
COPY . .
RUN useradd -m cerulean && chown -R cerulean:cerulean /app
USER cerulean
CMD ["uvicorn", "cerulean.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
