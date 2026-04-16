# Architecture & Developer Guide

**Cerulean Next — Technical Reference for Developers**

---

## System Overview

Cerulean Next is a Docker Compose application with 8 services:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   nginx     │───▸│   web (×6)  │───▸│  postgres   │
│  (SSL/proxy)│    │  (FastAPI)  │    │  (PostgreSQL)│
└─────────────┘    └──────┬──────┘    └─────────────┘
                          │
                   ┌──────┴──────┐
                   │   redis     │
                   │  (broker)   │
                   └──────┬──────┘
                          │
              ┌───────────┼───────────┐
              │           │           │
        ┌─────┴─────┐ ┌──┴──┐ ┌─────┴──────┐
        │ worker(×8)│ │beat │ │worker-push │
        │ (general) │ │     │ │    (×4)    │
        └───────────┘ └─────┘ └────────────┘
```

- **nginx**: SSL termination, reverse proxy, static files
- **web**: FastAPI application (6 uvicorn workers in prod, hot-reload in dev)
- **worker**: Celery worker for general tasks (ingest, analyze, transform, quality, etc.) — 8 concurrent
- **worker-push**: Celery worker for long-running push tasks (Koha, Aspen, Evergreen) — 4 concurrent
- **beat**: Celery Beat scheduler (redbeat with Redis backend)
- **postgres**: PostgreSQL 15 database
- **redis**: Message broker + result backend + beat scheduler storage
- **flower**: Celery monitoring UI (port 5555)

## Directory Structure

```
cerulean/
├── main.py                    # FastAPI app factory, middleware, router registration
├── core/
│   ├── config.py              # Settings (pydantic-settings, loaded from .env)
│   ├── database.py            # Async SQLAlchemy engine + session factory
│   ├── auth.py                # OAuth client, JWT creation/decode, domain validation
│   ├── features.py            # AI feature flag registry (source of truth for toggles)
│   ├── preferences.py         # Pref read/write + require_preference dep + pref_enabled_sync
│   ├── logging.py             # structlog configuration
│   └── transform_presets.py   # Built-in transform functions (date, case, clean, extract)
├── models/
│   └── __init__.py            # ALL SQLAlchemy ORM models (25 tables)
├── schemas/
│   ├── projects.py            # ProjectCreate, ProjectOut, MARCFileOut
│   ├── maps.py                # FieldMapOut, MapTemplateOut, TemplateMapEntry
│   ├── events.py              # AuditEventOut, SuggestionOut, CommentOut
│   └── push.py                # BibPushOptions, PushStartRequest
├── api/routers/
│   ├── auth.py                # Google OAuth + user management + server logs
│   ├── preferences.py         # Per-user AI feature flag read/write
│   ├── projects.py            # Project CRUD with ownership/visibility
│   ├── files.py               # File upload, MRK export, record browsing, health-report
│   ├── maps.py                # Field mapping CRUD + AI suggest + AI transform gen/preview
│   ├── templates.py           # Template CRUD + CSV/Google Sheets import
│   ├── quality.py             # Quality scan + clustering
│   ├── batch_edit.py          # Find/replace, regex, add/delete fields, call numbers
│   ├── rda.py                 # RDA 336/337/338 generation
│   ├── versions.py            # Version snapshots + diff
│   ├── transform.py           # Transform pipeline dispatch
│   ├── dedup.py               # Deduplication scan + apply
│   ├── reconcile.py           # Item value reconciliation
│   ├── items.py               # Item column mapping
│   ├── patrons.py             # Patron data import + mapping + reconciliation
│   ├── push.py                # Koha push (REST, FastMARC, Toolkit, migration mode)
│   ├── aspen.py               # Aspen Discovery integration
│   ├── evergreen.py           # Evergreen ILS integration
│   ├── marc_sql.py            # SQL Explorer
│   ├── marc_export.py         # Spreadsheet/JSON export + record extraction
│   ├── marc_files.py          # File split/join
│   ├── csv_to_marc.py         # CSV → MARC converter
│   ├── macros.py              # Macro CRUD + execution
│   ├── plugins.py             # Plugin upload/install
│   ├── settings.py            # System settings GUI
│   ├── suggestions.py         # Suggestions + comments
│   ├── oai.py                 # OAI-PMH 2.0 endpoint
│   ├── sandbox.py             # KTD sandbox provisioning
│   ├── log.py                 # Audit log + SSE stream + export
│   ├── tasks.py               # Task pause/resume/cancel
│   └── reference.py           # Static reference data (Koha fields, presets)
├── tasks/
│   ├── celery_app.py          # Celery app instance, queue routing, config
│   ├── audit.py               # AuditLogger helper for tasks
│   ├── helpers.py             # Shared task helpers (check_paused)
│   ├── ingest.py              # File parsing, ILS detection, tag frequency
│   ├── analyze.py             # AI field mapping, template save/load
│   ├── quality.py             # Quality scan (8 categories) + bulk fix
│   ├── versioning.py          # Version snapshot creation + diff
│   ├── transform.py           # Field map application, merge, build
│   ├── dedup.py               # Dedup scan + apply (5 strategies)
│   ├── reconcile.py           # Item value scan + rule apply
│   ├── items.py               # Item CSV AI mapping
│   ├── patrons.py             # Patron parse, AI map, scan, apply
│   ├── push.py                # Koha push (all methods), migration mode, Docker exec
│   ├── aspen.py               # Aspen Turbo Migration/Reindex
│   ├── evergreen.py           # Evergreen push, pingest, remap, restart
│   └── sandbox.py             # KTD provisioning
frontend/
├── index.html                 # Entire SPA (~12,000 lines)
├── vendor/
│   └── chart.min.js           # Chart.js for tag frequency visualization
└── koha_marc_fields.json      # MARC field reference data
docs/
├── USER-MANUAL.md             # Comprehensive user manual
├── Cerulean-Next-User-Manual.pdf  # Formatted PDF manual
├── API-REFERENCE.md           # All API endpoints
├── DEPLOY-DIGITALOCEAN.md     # Production deployment guide
├── CONNECT-LOCAL-KOHA.md      # SSH tunnel setup
├── ARCHITECTURE.md            # This file
└── (plugin feedback docs)     # Plugin development notes
```

## Database Models (25 tables)

| Model | Table | Purpose |
|-------|-------|---------|
| User | users | Google OAuth user accounts |
| UserPreference | user_preferences | Per-user key/value store — AI feature flags today, future visibility toggles tomorrow |
| Project | projects | Migration projects (owner_id, visibility) |
| MARCFile | marc_files | Uploaded MARC/CSV files with analysis data (incl. `health_report` JSONB + status) |
| FieldMap | field_maps | Source→target MARC field mappings (incl. `ai_prompt` for AI-generated transforms) |
| MapTemplate | map_templates | Reusable mapping sets (JSONB maps array) |
| TransformManifest | transform_manifests | Transform pipeline run tracking |
| DedupRule | dedup_rules | Dedup rule configuration |
| DedupCluster | dedup_clusters | Detected duplicate groups |
| ReconciliationRule | reconciliation_rules | Item value mapping rules |
| ReconciliationScanResult | reconciliation_scan_results | Scanned item values |
| ItemColumnMap | item_column_maps | CSV column→952 subfield mapping |
| PatronFile | patron_files | Uploaded patron data files |
| PatronColumnMap | patron_column_maps | Patron column→Koha header mapping |
| PatronValueRule | patron_value_rules | Patron controlled list rules |
| PatronScanResult | patron_scan_results | Scanned patron categorical values |
| PushManifest | push_manifests | Push operation tracking |
| SandboxInstance | sandbox_instances | KTD container lifecycle |
| AuditEvent | audit_events | Append-only project event log |
| Suggestion | suggestions | Feature requests, bugs, discussions |
| SuggestionVote | suggestion_votes | Upvotes |
| SuggestionComment | suggestion_comments | Threaded comments |
| QualityScanResult | quality_scan_results | MARC quality issues |
| MigrationVersion | migration_versions | Immutable data snapshots |
| Macro | macros | Saved batch edit sequences |
| Plugin | plugins | Uploaded Koha plugin files |
| SystemSetting | system_settings | Key-value platform configuration |

## Celery Task Queues

| Queue | Worker | Tasks |
|-------|--------|-------|
| ingest | worker | File parsing, ILS detection, tag frequency, items CSV parsing |
| analyze | worker | AI field mapping, template save/load, items AI mapping |
| transform | worker | Field map application, merge pipeline, build output |
| dedup | worker | Dedup scan, dedup apply |
| reconcile | worker | Item reconciliation scan + apply |
| patrons | worker | Patron parse, AI map, scan, apply |
| push | worker-push | Koha push (all methods), Aspen migration/reindex, Evergreen push/pingest |
| sandbox | worker | KTD provisioning |
| default | worker | Miscellaneous |

## Authentication Flow

```
Browser → GET /auth/google/login
       → 302 to Google consent screen
       → Google authenticates user
       → 302 to /auth/google/callback
       → Server validates domain, upserts User, creates JWT
       → Returns HTML page that stores JWT in sessionStorage
       → Redirects to /

All subsequent API calls include Authorization: Bearer <JWT>
AuthMiddleware validates JWT, sets request.state.user_id
get_current_user() dependency fetches User from DB
```

When `GOOGLE_CLIENT_ID` is not set, the AuthMiddleware passes all requests through without validation (dev mode).

## Frontend Architecture

The frontend is a single-file vanilla JavaScript SPA (`frontend/index.html`, ~12,000 lines). No build step, no framework, no npm.

**Key patterns:**
- `navigate(view)` — Router function that renders the active view
- Two `<script>` blocks — the second overrides `navigate()` to add stages 3–13
- `window.renderXxx` — Page render functions must use `window.` to be accessible across script blocks
- `window.api(method, path, body)` — Authenticated API calls (includes JWT, handles 401)
- `authDownload(path, filename)` — Authenticated file downloads (JWT in fetch, not in `<a href>`)
- `renderProgressBar(opts)` — Reusable progress bar component
- `pollTaskStatus(projectId, taskId, elementId, label, stageType, onComplete)` — Generic Celery task poller
- `_xhrUpload(url, file, uploadId)` — Upload with real-time progress tracking
- `esc(str)` — XSS-safe HTML escaping (REQUIRED for all server data in innerHTML)

**Important: Route ordering in the renders map matters.** The second `navigate()` function's renders object is the one that actually runs. New pages must be added to BOTH renders maps.

## Docker Exec Pattern

For operations inside Koha/Evergreen containers, Cerulean uses the Docker Engine API via Unix socket (`/var/run/docker.sock`). This avoids requiring the Docker CLI inside the container:

```python
transport = httpx.HTTPTransport(uds="/var/run/docker.sock")
with httpx.Client(transport=transport) as client:
    # Create exec
    create_resp = client.post(f"http://localhost/containers/{container}/exec", json={...})
    exec_id = create_resp.json()["Id"]
    # Start exec
    start_resp = client.post(f"http://localhost/exec/{exec_id}/start", json={...})
    # Parse multiplexed stdout/stderr from Docker frame format
```

## Key Configuration

| Setting | Source | Description |
|---------|--------|-------------|
| DATABASE_URL | .env | PostgreSQL connection (asyncpg for web, psycopg2 for tasks) |
| REDIS_URL | .env | Redis broker URL |
| SECRET_KEY | .env | JWT signing key |
| ANTHROPIC_API_KEY | .env or System Settings | Claude API key |
| GOOGLE_CLIENT_ID | .env or System Settings | OAuth client ID |
| GOOGLE_CLIENT_SECRET | .env or System Settings | OAuth client secret |
| GOOGLE_ALLOWED_DOMAIN | .env or System Settings | Comma-separated allowed email domains |
| DATA_ROOT | .env | Base directory for project files (default: /data/projects) |

System Settings (stored in DB) override environment variables and take effect without restart.

## Per-User Preferences & AI Feature Flags

Every AI-assisted capability (cerulean_ai_spec.md) is **opt-in per user**
and **off by default**. One registry drives the whole surface — schema,
API, UI, and the Celery-side check all read from the same source of
truth.

```
cerulean/core/features.py          FEATURES registry — one Feature() per toggle
cerulean/core/preferences.py       get_user_pref / set_user_pref / pref_enabled_sync
                                    + require_preference(key) FastAPI dep
cerulean/api/routers/preferences   GET/PATCH /users/me/preferences
user_preferences (table)            id, user_id, key, value (JSONB), updated_at
```

**Adding a new toggle:** append a `Feature(key="ai.my_thing", ...)`
entry to `FEATURES`. No schema migration, no UI change — the
Preferences page auto-renders it and `window.hasFeature("ai.my_thing")`
works immediately.

**Dual-layer enforcement (spec §11.5):**
- API routes either declare `dependencies=[Depends(require_preference("..."))]`
  (returns 403 with `feature_key`) or inline-check with `get_user_pref()`
  for a custom error payload.
- Celery tasks call `pref_enabled_sync(session, user_id, key)` at the top
  and early-return `{"skipped": True}` when off. Tasks must be self-gating
  because chain callers (e.g. `ingest_marc_task`) forward `user_id`
  blindly; the flag check is the point where that lookup happens.

**Dev mode:** when `GOOGLE_CLIENT_ID` isn't set, the preferences router
creates a synthetic `dev@localhost` user on first use so the UI works
locally without real OAuth.

## Transform Rule Generation Sandbox

`cerulean/tasks/transform.py` runs AI-generated expressions through
`eval()` against a locked-down globals dict (`_SAFE_GLOBALS`):

- Builtins restricted to pure-value helpers (`len`, `str`, `int`, `re`-
  based idioms, etc.) plus a whitelisted `__import__` that only permits
  the modules in `_SANDBOX_ALLOWED_IMPORTS` (`re`, `datetime`, `time`,
  `_strptime`, `locale`, `encodings`, `string`, `calendar`).
- Expression scope exposes `value` / `v` (the input string) plus `re`,
  `datetime`, `date`, `timedelta`.
- `os`, `subprocess`, `sys`, `socket`, `ctypes`, `importlib`, `pickle`,
  `open`, `exec`, `eval`, `compile`, and `globals()` are all blocked —
  audited by `tests/tasks/test_ai_transform_sandbox.py`.
- `_apply_fn(value, expr)` is the production path (silently returns
  `value` on error). `apply_fn_safe(value, expr)` is the preview path
  (returns `(result, error)` so the UI can show per-row failures).
