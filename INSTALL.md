# Cerulean-Next — Installation Guide

This guide walks you through setting up Cerulean-Next from scratch on a new machine. The application runs entirely in Docker, so you don't need to install Python, PostgreSQL, or Redis locally.

---

## Prerequisites

### Required

1. **Docker Desktop** (or Docker Engine + Docker Compose)
   - macOS: https://docs.docker.com/desktop/install/mac-install/
   - Windows: https://docs.docker.com/desktop/install/windows-install/
   - Linux: https://docs.docker.com/engine/install/

   Verify installation:
   ```bash
   docker --version          # Docker version 24+ recommended
   docker compose version    # Docker Compose v2+
   ```

2. **Git**
   ```bash
   git --version
   ```

3. **An Anthropic API Key** (for AI-assisted field mapping)
   - Sign up at https://console.anthropic.com/
   - Create an API key under Settings → API Keys
   - This is optional but recommended — without it, AI mapping suggestions won't work

### Optional

- **Python 3.11+** — only needed if you want to run tests or scripts outside Docker
- **A Koha instance** — needed for Stage 5 (Push to Koha). Can be a test instance.

---

## Step 1: Clone the Repository

```bash
git clone https://github.com/your-org/Cerulean-Next.git
cd Cerulean-Next
```

---

## Step 2: Create Environment File

Create a `.env` file in the project root:

```bash
# Required
ANTHROPIC_API_KEY=sk-ant-api03-your-key-here
SECRET_KEY=replace-with-a-long-random-string

# Recommended — encrypts Koha API tokens stored in the database
FERNET_KEY=your-generated-fernet-key

# Optional — Flower monitoring UI credentials
FLOWER_USER=admin
FLOWER_PASSWORD=admin
```

### Generate a SECRET_KEY

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(48))"
```

### Generate a FERNET_KEY

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

If you don't have Python locally, you can generate these after starting the containers:

```bash
docker compose up -d --build
docker compose exec web python -c "import secrets; print(secrets.token_urlsafe(48))"
docker compose exec web python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Then paste the values into your `.env` file and restart:

```bash
docker compose down
docker compose up -d
```

---

## Step 3: Build and Start Services

```bash
docker compose up -d --build
```

This builds the Docker image and starts 6 services:

| Service | Port | What it does |
|---------|------|-------------|
| **web** | http://localhost:8000 | FastAPI application (the UI and API) |
| **worker** | — | Celery background task worker |
| **beat** | — | Celery scheduled task runner |
| **flower** | http://localhost:5555 | Celery task monitoring dashboard |
| **postgres** | localhost:5433 | PostgreSQL 15 database |
| **redis** | localhost:6380 | Message broker and cache |

First build takes 2-3 minutes to download base images and install dependencies.

### Check that everything is running

```bash
docker compose ps
```

You should see all 6 services with status "Up" or "running".

### View logs if something looks wrong

```bash
docker compose logs web        # API server logs
docker compose logs worker     # Background task logs
docker compose logs postgres   # Database logs
```

---

## Step 4: Run Database Migrations

The database schema needs to be created on first run:

```bash
docker compose exec web alembic upgrade head
```

You should see output like:

```
INFO  [alembic.runtime.migration] Running upgrade  -> xxxx, initial schema
INFO  [alembic.runtime.migration] Running upgrade xxxx -> yyyy, ...
```

---

## Step 5: Verify the Installation

1. **Open the UI:** http://localhost:8000
   - You should see the Cerulean dashboard with a "New Project" button

2. **Check the API health endpoint:**
   ```bash
   curl http://localhost:8000/api/health
   ```
   Expected: `{"status":"ok","version":"1.0.0"}`

3. **Open Flower** (task monitor): http://localhost:5555
   - Login with the credentials from your `.env` (default: admin/admin)
   - You should see the worker registered with queues listed

---

## Step 6: Create Your First Project

1. Click **"New Project"** on the dashboard
2. Enter a project code (e.g., `TEST`) and library name
3. Select or type the source ILS (e.g., "SirsiDynix Symphony", "Koha", "Evergreen")
4. Click **Create**

You're now ready to start the migration pipeline. See the [User Manual](USER_MANUAL.md) for guidance on each stage.

---

## Optional Services

### Elasticsearch (full-text record search)

```bash
docker compose --profile search up -d
```

Set the environment variable in `.env`:
```
ELASTICSEARCH_URL=http://elasticsearch:9200
```

Then restart:
```bash
docker compose up -d --build web worker
```

### MinIO (S3-compatible file storage)

```bash
docker compose --profile minio up -d
```

Set in `.env`:
```
S3_ENDPOINT_URL=http://minio:9000
S3_ACCESS_KEY=cerulean
S3_SECRET_KEY=cerulean123
S3_BUCKET=cerulean
```

MinIO console: http://localhost:9001 (cerulean / cerulean123)

---

## Rebuilding After Code Changes

If you pull new code or make changes:

```bash
# API-only changes (routers, schemas, config)
docker compose up -d --build web

# API + background task changes
docker compose up -d --build web worker

# Rebuild everything
docker compose up -d --build
```

The `--build` flag rebuilds the Docker image. Without it, containers use the old image.

**Important:** After rebuilding, hard-refresh your browser (Ctrl+Shift+R / Cmd+Shift+R) to pick up any frontend changes.

---

## Updating the Database Schema

When new Alembic migration files appear (in `alembic/versions/`):

```bash
docker compose exec web alembic upgrade head
```

---

## Running Tests

```bash
# All tests
docker compose exec web pytest tests/ -v

# With coverage report
docker compose exec web pytest tests/ --cov=cerulean

# Specific test file
docker compose exec web pytest tests/tasks/test_transform.py -v
```

---

## Stopping and Restarting

```bash
# Stop all services (preserves data)
docker compose down

# Start again
docker compose up -d

# Stop and DELETE all data (database, redis, files)
docker compose down -v
```

**Warning:** `docker compose down -v` removes all volumes including the database. Only use this if you want a completely fresh start.

---

## Troubleshooting

### "Connection refused" when opening http://localhost:8000

The web server may still be starting. Check logs:
```bash
docker compose logs -f web
```
Wait for `Uvicorn running on http://0.0.0.0:8000`.

### Database migration errors

If you get "relation already exists" or similar:
```bash
docker compose exec web alembic current    # Check current revision
docker compose exec web alembic history    # See all migrations
```

For a fresh start (destroys all data):
```bash
docker compose down -v
docker compose up -d
docker compose exec web alembic upgrade head
```

### Worker not processing tasks

Check the worker is connected to Redis and listening on all queues:
```bash
docker compose logs worker | head -50
```

Look for: `Connected to redis://redis:6379/0` and the queue list.

### "No module named ..." errors

Rebuild the containers to install new dependencies:
```bash
docker compose up -d --build
```

### Port conflicts

If ports 8000, 5433, 5555, or 6380 are already in use on your machine, edit `docker-compose.yml` and change the left-side port numbers under `ports:`.

### File upload issues

Ensure the `cerulean-data` Docker volume exists and the web/worker containers can write to it:
```bash
docker volume ls | grep cerulean
```

---

## Architecture Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Browser    │────▶│   FastAPI     │────▶│  PostgreSQL   │
│  (port 8000) │     │   (web)      │     │  (port 5433)  │
└──────────────┘     └──────┬───────┘     └──────────────┘
                            │
                     ┌──────▼───────┐     ┌──────────────┐
                     │    Redis     │◀────│  Celery       │
                     │  (port 6380) │     │  (worker)     │
                     └──────────────┘     └──────────────┘
```

- **Browser** loads a single-page app served by FastAPI
- **FastAPI** handles API requests and serves the frontend
- **Celery workers** process background tasks (ingest, transform, dedup, push)
- **Redis** acts as the message broker between FastAPI and Celery
- **PostgreSQL** stores all project data, mappings, and audit logs
- File data is stored on a Docker volume (`cerulean-data`) mounted at `/data/projects`
