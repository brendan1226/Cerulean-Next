# Cerulean Next

**ByWater Solutions MARC Migration Platform**

A comprehensive library data migration platform that moves bibliographic, patron, and holdings data from legacy ILS (Integrated Library System) systems into **Koha**, **Aspen Discovery**, and **Evergreen ILS**. Features a 13-step pipeline with AI-assisted field mapping, MARC data quality remediation, batch editing, OAI-PMH harvesting, and multi-user workspaces with Google OAuth.

**Live:** [cerulean-next.gallagher-family-hub.com](https://cerulean-next.gallagher-family-hub.com)

## Features

- **13-step migration pipeline** — Ingest → Config → Quality → Versions → Mapping → Transform → Reconciliation → Patrons → Patron Versions → Load → Holds → Aspen → Evergreen
- **AI-assisted field mapping** — Claude analyzes your MARC data and suggests field mappings with confidence scores
- **MARC Data Quality** — 8-category quality scanner with auto-fix, inline editing, leader byte dropdowns
- **Batch Editing** — MarcEdit-style find/replace, regex, add/delete fields, call number generation
- **RDA Helper** — Auto-generate 336/337/338 fields from leader bytes
- **SQL Explorer** — Query MARC data with SQL-like syntax (`SELECT 001, 245$a WHERE 942$c = 'DVD'`)
- **Data Clustering** — Group records by any field to see value distribution
- **File Management** — Split files by count or field value, join with dedup, MRK export
- **Record Extraction** — Pull records matching criteria into separate files
- **Template System** — Save/load/share field mappings as reusable templates, import from Google Sheets
- **Macros** — Save and replay sequences of batch operations
- **Multi-ILS Push** — Push to Koha (REST API, FastMARCImport, Migration Toolkit), Aspen Discovery (Turbo Migration), Evergreen (direct PostgreSQL)
- **Migration Mode** — Stop Koha daemons + tune MariaDB for 10-50x faster bulk imports
- **Plugin Manager** — Upload, download, auto-install Koha plugins (.kpz)
- **OAI-PMH 2.0** — Harvest records directly from MarcEdit or any OAI client
- **Google OAuth** — Multi-domain authentication (bywatersolutions.com, openfifth.co.uk)
- **Multi-user workspaces** — Private and shared projects, per-user ownership
- **Real-time upload progress** — Speed, ETA, bytes transferred
- **System Logs** — User activity tracking, auth event logging

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Backend** | Python 3.11, FastAPI, SQLAlchemy 2.0 (async), Pydantic v2 |
| **Task Queue** | Celery 5.4 with Redis broker (8 general + 4 push workers) |
| **Database** | PostgreSQL 15 with Alembic migrations |
| **MARC Processing** | pymarc 5.x |
| **Frontend** | Vanilla JavaScript SPA (single file, no build step) |
| **AI** | Claude API (Anthropic) for field mapping and patron column mapping |
| **Auth** | Google OAuth 2.0 via authlib, JWT sessions |
| **Monitoring** | Flower (Celery UI), structlog, System Logs page |
| **Infrastructure** | Docker Compose, nginx reverse proxy, Let's Encrypt SSL |
| **Hosting** | DigitalOcean (production), local Docker (development) |

## The 13-Step Pipeline

| Step | Name | Description |
|------|------|-------------|
| 1 | **Data Ingest** | Upload MARC/CSV files, auto-detect ILS, tag frequency analysis, CSV→MARC converter |
| 2 | **ILS & Config** | Confirm source ILS, set item structure (embedded/separate/none), MARC browser |
| 3 | **Data Quality** | 8-category quality scan, batch edit, clustering, RDA helper, call number generation |
| 4 | **Bib Versions** | Immutable snapshots, version comparison, file diffing |
| 5 | **Field Mapping** | 3-panel editor, AI suggestions, templates, Google Sheets import |
| 6 | **Transform** | Apply mappings, build output, join items CSV |
| 7 | **Reconciliation** | Scan 952 fields, create value mapping rules, apply to MARC data |
| 8 | **Patrons** | Upload CSV/Excel/XML/MARC, AI column mapping, value reconciliation |
| 9 | **Patron Versions** | Snapshot and compare patron data iterations |
| 10 | **Load** | Push to Koha (migration mode, multiple methods, reference data management) |
| 11 | **Holds** | Holds/reserves, circulation history (coming soon) |
| 12 | **Aspen Discovery** | Turbo Migration + Turbo Reindex (parallel workers) |
| 13 | **Evergreen ILS** | Direct PostgreSQL push, trigger control, pingest, metarecord remap |

## MARC Tools

| Tool | Description |
|------|-------------|
| **SQL Explorer** | Query MARC data: `SELECT 001, 245$a WHERE 942$c = 'DVD' LIMIT 100` |
| **Export & Extract** | Export fields to CSV/TSV, extract records by criteria, JSON import/export |
| **File Manager** | Split by count/field value, join with dedup, browse all project files |
| **Macros** | Save and replay batch edit sequences |

## Quick Start (Development)

### Prerequisites

- [Docker](https://www.docker.com/get-started/) and Docker Compose
- An [Anthropic API key](https://console.anthropic.com/) (for AI mapping features)

### 1. Clone and configure

```bash
git clone https://github.com/brendan1226/Cerulean-Next.git
cd Cerulean-Next

# Create environment file
cp .env.example .env   # or create manually:
cat > .env << 'EOF'
DATABASE_URL=postgresql+asyncpg://cerulean:cerulean@postgres:5432/cerulean
REDIS_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1
SECRET_KEY=change-me-to-a-random-string
ANTHROPIC_API_KEY=sk-ant-your-key-here
DATA_ROOT=/data/projects
DEBUG=true
EOF
```

### 2. Build and start

```bash
docker compose up -d --build
```

This starts 8 services: **web** (FastAPI), **worker** (Celery general), **worker-push** (Celery push), **beat** (scheduler), **flower** (monitoring), **postgres**, and **redis**.

### 3. Run database migrations

```bash
docker compose exec web alembic upgrade head
```

### 4. Open the app

- **Cerulean UI:** http://localhost:8000
- **API Docs:** http://localhost:8000/api/docs
- **Flower:** http://localhost:5555 (admin/admin)

> **Note:** Google OAuth is bypassed when `GOOGLE_CLIENT_ID` is not set, so you can use the app without OAuth in development.

## Production Deployment

See [docs/DEPLOY-DIGITALOCEAN.md](docs/DEPLOY-DIGITALOCEAN.md) for full deployment instructions including:
- DigitalOcean Droplet setup
- SSL via Let's Encrypt
- nginx reverse proxy
- Data migration from local to production
- Google OAuth configuration

## Docker Services

| Service | Port | Workers | Purpose |
|---------|------|---------|---------|
| web | 8000 | 4 (dev) / 6 (prod) | FastAPI app |
| worker | — | 8 | Celery general tasks (ingest, analyze, transform, quality) |
| worker-push | — | 4 | Celery push tasks (Koha, Aspen, Evergreen) |
| beat | — | 1 | Celery Beat scheduler |
| flower | 5555 | 1 | Celery monitoring UI |
| postgres | 5433 | — | PostgreSQL 15 |
| redis | 6380 | — | Message broker + result backend |

Optional:
```bash
docker compose --profile search up -d    # Elasticsearch (port 9200)
docker compose --profile minio up -d     # MinIO S3 storage (ports 9000/9001)
```

## API Endpoints

28 router modules providing 150+ endpoints:

| Category | Routers |
|----------|---------|
| **Migration Pipeline** | projects, files, maps, templates, quality, versions, dedup, reconcile, patrons, items, transform, push |
| **ILS Integration** | aspen, evergreen, sandbox |
| **MARC Tools** | batch_edit, marc_sql, marc_export, marc_files, csv_to_marc, rda, macros |
| **Platform** | auth, settings, plugins, suggestions, log, tasks, reference, oai |

Interactive API documentation: `/api/docs` (Swagger) or `/api/redoc`

## Connecting to Koha

### Direct URL
Enter the Koha staff URL in Project Settings (e.g., `https://koha.library.org`).

### Local KTD via SSH Tunnel
For Koha Test Docker running on your workstation:
```bash
ssh -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N
```
Set Koha URL to `http://172.19.0.1:8081` (Docker gateway IP).

See [docs/CONNECT-LOCAL-KOHA.md](docs/CONNECT-LOCAL-KOHA.md) for full instructions (Mac/Windows/Linux).

## Documentation

- **[User Manual (PDF)](docs/Cerulean-Next-User-Manual.pdf)** — Comprehensive guide for migration specialists
- **[User Manual (Markdown)](docs/USER-MANUAL.md)** — Same content in markdown format
- **[Deploy to DigitalOcean](docs/DEPLOY-DIGITALOCEAN.md)** — Production deployment guide
- **[Connect Local Koha](docs/CONNECT-LOCAL-KOHA.md)** — SSH tunnel setup for Mac/Windows/Linux
- **In-app Help** — Searchable interactive help accessible from the sidebar

## Koha Plugins

| Plugin | Purpose |
|--------|---------|
| **Migration Toolkit** (`Koha::Plugin::BWS::MigrationToolkit`) | All-in-one: FastMARCImport + TurboIndex + DB tuning + preflight |
| **Cerulean Endpoints** (`Koha::Plugin::BWS::CeruleanEndpoints`) | Migration mode control, reference data, plugin management |
| **FastMARCImport** | High-performance parallel MARC import |
| **TurboIndex** | Parallel Elasticsearch reindexing |

Upload and auto-install plugins via the Plugin Manager in the sidebar.

## License

Proprietary. ByWater Solutions. All rights reserved.
