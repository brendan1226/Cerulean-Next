# Cerulean-Next

A MARC library migration platform that moves bibliographic, patron, and holdings data from legacy ILS (Integrated Library System) systems into **Koha** (open-source ILS). It automates a 6-stage pipeline with AI-assisted field mapping, deduplication, and Koha REST API integration.

## Tech Stack

- **Backend:** Python 3.11, FastAPI, SQLAlchemy 2.0 (async), Pydantic v2
- **Task Queue:** Celery 5.4 with Redis broker
- **Database:** PostgreSQL 15 with Alembic migrations
- **MARC Processing:** pymarc 5.x
- **Frontend:** Vanilla JavaScript SPA (single file, no build step)
- **AI:** Claude API (Anthropic) for field mapping suggestions
- **Monitoring:** Flower (Celery UI), structlog
- **Infrastructure:** Docker Compose

## The 6-Stage Pipeline

| Stage | Name | Description |
|-------|------|-------------|
| 1 | **Data Ingest** | Upload MARC/CSV files, parse, index, detect source ILS |
| 2 | **Analyze & Map** | Create field maps (manual, AI-suggested, or from templates) |
| 3 | **Transform** | Apply approved maps to records, merge files, join items CSV |
| 4 | **Dedup** | Scan for duplicates, resolve clusters, write deduped output |
| 5 | **Push to Koha** | Preflight check, push bibs/patrons/holds/circ via Koha API |
| 6 | **Patron Transform** | Upload patron data, map columns, reconcile values, export |

## Quick Start

### Prerequisites

- [Docker](https://www.docker.com/get-started/) and Docker Compose
- An [Anthropic API key](https://console.anthropic.com/) (for AI mapping features)

### 1. Clone and configure

```bash
git clone https://github.com/your-org/Cerulean-Next.git
cd Cerulean-Next

# Create environment file
cat > .env << 'EOF'
ANTHROPIC_API_KEY=sk-ant-your-key-here
SECRET_KEY=change-me-to-a-random-string
FERNET_KEY=generate-with-python-see-below
EOF
```

Generate a Fernet key for Koha token encryption:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Build and start

```bash
docker compose up -d --build
```

This starts 6 services: **web** (FastAPI), **worker** (Celery), **beat** (scheduler), **flower** (monitoring), **postgres**, and **redis**.

### 3. Run database migrations

```bash
docker compose exec web alembic upgrade head
```

### 4. Open the app

- **Cerulean UI:** http://localhost:8000
- **Flower (task monitor):** http://localhost:5555 (admin/admin)

## Docker Services

| Service | Port | Purpose |
|---------|------|---------|
| web | 8000 | FastAPI app (hot-reload in dev) |
| worker | — | Celery worker (4 concurrent tasks) |
| beat | — | Celery Beat scheduler |
| flower | 5555 | Celery monitoring UI |
| postgres | 5433 | PostgreSQL 15 |
| redis | 6380 | Message broker + result backend |

Optional services (start with `--profile`):

```bash
docker compose --profile search up -d    # Elasticsearch (port 9200)
docker compose --profile minio up -d     # MinIO S3 storage (ports 9000/9001)
```

## Rebuilding After Code Changes

```bash
# API changes only
docker compose up -d --build web

# API + background task changes
docker compose up -d --build web worker

# Everything
docker compose up -d --build
```

## Running Tests

```bash
docker compose exec web pytest tests/ -v
docker compose exec web pytest tests/ --cov=cerulean
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | Claude API key for AI mapping |
| `SECRET_KEY` | Yes | App secret key (JWT signing) |
| `FERNET_KEY` | Recommended | Encrypts Koha API tokens at rest |
| `DATABASE_URL` | No | PostgreSQL connection (default: internal) |
| `REDIS_URL` | No | Redis connection (default: internal) |
| `ELASTICSEARCH_URL` | No | Enable full-text search |
| `FLOWER_USER` / `FLOWER_PASSWORD` | No | Flower UI credentials (default: admin/admin) |

## Documentation

- **[INSTALL.md](INSTALL.md)** — Detailed installation and deployment guide
- **[USER_MANUAL.md](USER_MANUAL.md)** — Complete user manual for all 6 stages
- **[HANDOFF.md](HANDOFF.md)** — Developer handoff document (architecture, models, API reference)

## License

Proprietary. All rights reserved.
