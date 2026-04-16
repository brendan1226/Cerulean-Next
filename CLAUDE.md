# Cerulean Next — AI Agent Development Context

This document provides the context an AI coding agent needs to develop for Cerulean Next. Feed this to Claude Code, Cursor, Copilot Workspace, Windsurf, or any AI agent before asking it to make changes.

## What Is Cerulean Next?

Cerulean Next is a **MARC library data migration platform** built by ByWater Solutions. It helps migration specialists move bibliographic, patron, and holdings data from legacy ILS (Integrated Library System) systems into **Koha**, **Aspen Discovery**, and **Evergreen ILS**.

It's a Docker Compose web application with a FastAPI backend, Celery task queue, PostgreSQL database, and a vanilla JavaScript SPA frontend.

**Production URL:** https://cerulean-next.gallagher-family-hub.com

## Tech Stack (DO NOT change these choices)

- **Python 3.11**, **FastAPI**, **SQLAlchemy 2.0 (async with asyncpg)**, **Pydantic v2**
- **Celery 5.4** with **Redis** broker (two worker containers: general + push)
- **PostgreSQL 15** with **Alembic** migrations
- **pymarc 5.x** for MARC record processing
- **Vanilla JavaScript SPA** in a single `frontend/index.html` file (~12,000 lines). No React, no Vue, no build step, no npm.
- **authlib** for Google OAuth, **python-jose** for JWT
- **Docker Compose** for local dev and production

## Critical Architecture Rules

### Backend

1. **All models live in `cerulean/models/__init__.py`** — Do not create separate model files. Import from there.

2. **Routers are in `cerulean/api/routers/`** — One file per feature area. Register new routers in `cerulean/main.py`.

3. **FastAPI route ordering matters** — Specific paths like `/templates/import-csv` MUST be defined BEFORE parameterized paths like `/templates/{template_id}` in the same router file, or FastAPI will match the parameter instead.

4. **Async SQLAlchemy** — The web layer uses `asyncpg`. Use `AsyncSession`, `select()`, `await db.execute()`. For relationships in response serialization, use `selectinload()` to eagerly load — lazy loading fails in async context with `MissingGreenlet` errors.

5. **Celery tasks use sync SQLAlchemy** — Task files use `psycopg2` (sync). The connection URL is rewritten: `settings.database_url.replace("+asyncpg", "+psycopg2")`.

6. **Queue routing** — Tasks are routed to specific queues (ingest, analyze, transform, dedup, reconcile, patrons, push, sandbox, default). Register new task modules in `cerulean/tasks/celery_app.py` (`include` list and `task_routes` dict). The general worker handles all queues except `push`; the `worker-push` container handles only `push`.

7. **Auth middleware** — `AuthMiddleware` in `main.py` validates JWT on all `/api/v1/` paths except those in `_PUBLIC_PATHS` and `/api/v1/oai/`. When `GOOGLE_CLIENT_ID` is empty, auth is bypassed entirely (dev mode). Authenticated user ID is in `request.state.user_id`.

8. **System Settings override env vars** — The `SystemSetting` model stores key-value pairs. At startup, DB values are merged into the `Settings` singleton via `load_settings_from_db()`. The OAuth client is re-registered dynamically when settings change.

9. **HTTP headers must be ASCII** — When setting `Content-Disposition` filenames, strip non-ASCII characters: `name.encode('ascii', 'ignore').decode('ascii')`.

10. **Docker exec via Unix socket** — For running commands inside Koha/Evergreen containers, use `httpx.HTTPTransport(uds="/var/run/docker.sock")` and the Docker Engine API, not the Docker CLI (which isn't available inside the web container).

### Frontend

1. **Single file** — All frontend code is in `frontend/index.html`. Do not create separate JS/CSS files.

2. **Two `<script>` blocks** — The second block overrides `navigate()` to add stages 3–13 and platform pages. New pages MUST be added to BOTH render maps.

3. **Use `window.` prefix** — Functions defined in the second script block that are referenced from HTML onclick attributes or the first script block must use `window.functionName = async function()`.

4. **XSS prevention** — ALL server-supplied data interpolated into `innerHTML` MUST go through `esc()`. Never use raw API values in template strings.

5. **Authenticated downloads** — Never use `<a href="/api/v1/...">` for downloads. Use `authDownload(path, filename)` which fetches with the JWT header and triggers a blob download.

6. **Upload progress** — Use `_xhrUpload(url, file, uploadId)` instead of `fetch()` for file uploads. It provides real-time progress events.

7. **Toast notifications** — Use `toast(message, type)` where type is 'success', 'error', 'info', or 'warn'. Duplicates within 3 seconds are suppressed.

8. **Task polling** — Use `pollTaskStatus(projectId, taskId, statusElId, label, stageType, onComplete)` for Celery task progress tracking.

### Database

1. **Alembic migrations** — Every schema change needs a migration in `alembic/versions/`. Revision IDs follow the pattern `[letter][number][mix]` (e.g., `n4h9i0j1k2l3`). Set `down_revision` to the previous migration's ID.

2. **Existing projects have `owner_id = NULL`** — The `visibility` column defaults to `"private"` but existing (pre-auth) projects were migrated to `"shared"`. Handle `owner_id IS NULL` as "shared/legacy" in access control.

3. **`_uuid()` and `_now()` helpers** — Use these from `cerulean/models/__init__.py` for default primary keys and timestamps.

## File Locations

| What | Where |
|------|-------|
| FastAPI app | `cerulean/main.py` |
| All ORM models | `cerulean/models/__init__.py` |
| API routers | `cerulean/api/routers/*.py` |
| Pydantic schemas | `cerulean/schemas/*.py` |
| Celery tasks | `cerulean/tasks/*.py` |
| Celery config | `cerulean/tasks/celery_app.py` |
| App config | `cerulean/core/config.py` |
| Auth (OAuth/JWT) | `cerulean/core/auth.py` |
| Database setup | `cerulean/core/database.py` |
| Frontend SPA | `frontend/index.html` |
| Alembic migrations | `alembic/versions/*.py` |
| Docker Compose | `docker-compose.yml` |
| Env vars | `.env` |
| User manual | `docs/USER-MANUAL.md` |
| API reference | `docs/API-REFERENCE.md` |
| Architecture | `docs/ARCHITECTURE.md` |

## Common Patterns

### Adding a new API endpoint

```python
# In cerulean/api/routers/my_feature.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from cerulean.core.database import get_db
from cerulean.api.deps import require_project

router = APIRouter(prefix="/projects", tags=["my-feature"])

@router.post("/{project_id}/my-feature/action")
async def my_action(project_id: str, db: AsyncSession = Depends(get_db)):
    project = await require_project(project_id, db)
    # ... do work ...
    return {"result": "ok"}
```

Then register in `cerulean/main.py`:
```python
from cerulean.api.routers import my_feature
app.include_router(my_feature.router, prefix="/api/v1")
```

### Adding a new Celery task

```python
# In cerulean/tasks/my_task.py
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.audit import AuditLogger

@celery_app.task(bind=True, name="cerulean.tasks.my_task.do_something", queue="analyze")
def do_something(self, project_id: str, ...):
    log = AuditLogger(project_id=project_id, stage=3, tag="[my-task]")
    log.info("Starting...")
    # ... do work with sync SQLAlchemy ...
    log.complete("Done")
```

Register in `celery_app.py`:
```python
include=["cerulean.tasks.my_task", ...]
task_routes={"cerulean.tasks.my_task.*": {"queue": "analyze"}, ...}
```

### Adding a new frontend page

```javascript
// 1. Add nav item in sidebar HTML
<div class="nav-item" data-view="my-page" onclick="navigate('my-page')">
  <span class="nav-icon">◈</span> My Page
</div>

// 2. Add to BOTH render maps (there are two!)
'my-page': window.renderMyPage,

// 3. Add to breadcrumb labels
'my-page': 'My Page',

// 4. Add the renderer (in second script block)
window.renderMyPage = async function(el) {
  const p = state.currentProject;
  el.innerHTML = `<div class="page-header">...</div>`;
};
```

### Adding a database table

```python
# 1. Add model to cerulean/models/__init__.py
class MyModel(Base):
    __tablename__ = "my_table"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

# 2. Create migration
# alembic/versions/xxx_add_my_table.py
def upgrade():
    op.create_table("my_table", ...)
def downgrade():
    op.drop_table("my_table")
```

## Cerulean Plugin System

Cerulean ships a `.cpz` plugin system distinct from the `.kpz` Koha
plugin manager. Cerulean plugins extend **Cerulean itself** — transforms
that appear in the Step 5 dropdown, quality checks that run in the
Step 3 scanner, and (Phase B/C) Celery tasks, API endpoints, and UI
tabs. See [docs/PLUGIN-AUTHORING.md](docs/PLUGIN-AUTHORING.md) for the
author-facing guide; the notes below are for engineers maintaining the
plugin loader itself.

### File layout
```
cerulean/core/plugins/
├── __init__.py              # public API re-exports
├── manifest.py              # YAML load + pydantic validation
├── extension_points.py      # TRANSFORM_REGISTRY + QUALITY_CHECK_REGISTRY
├── context.py               # PluginContext passed into python setup()
├── runtime_python.py        # import + call setup(ctx) in-process
├── runtime_subprocess.py    # spawn + timeout + stdin/stdout
└── loader.py                # scan enabled/ on startup, collect errors

cerulean/api/routers/cerulean_plugins.py   # upload/enable/disable/uninstall
alembic/versions/w3q8r9s0t1u2_add_cerulean_plugins.py
examples/plugins/shout-python/             # reference Python plugin
examples/plugins/shout-perl/               # reference subprocess plugin
```

### Storage on disk (under `DATA_ROOT`)
```
/data/projects/plugins/
├── available/          # uploaded .cpz archives — source of truth for rollback
├── enabled/<slug>/     # extracted trees that get scanned at startup
├── disabled/<slug>/    # extracted but dormant (moved here by the disable action)
└── run/<invocation>/   # scratch dirs for subprocess plugin invocations (temp)
```

### Restart-required model (matches Koha's .kpz)
- Plugin hooks register at **process start**: web lifespan + each Celery
  worker via `worker_process_init`. No hot-reload.
- Every install / enable / disable / uninstall response carries
  `restart_required: true` so the UI can remind the operator to run
  `docker compose restart web worker worker-push`.

### Extension points (Phase A — two kinds)
- **Transform** — surfaces in `get_preset_registry()` under category
  `plugin`. Dispatch goes through `apply_preset()` which detects the
  `plugin:<slug>:<key>` prefix and delegates to the plugin runtime.
- **Quality check** — `cerulean/tasks/quality.py` calls `_run_plugin_checks()`
  per record, iterating `all_quality_checks()`. Plugin issues land in the
  same `QualityScanResult` table with `category="plugin:<slug>"`.

### Security model
- Any authenticated user can install. Python plugins are trusted code;
  subprocess plugins run with a minimal env (no `ANTHROPIC_API_KEY`,
  no DB URLs, no Koha tokens) and a default 5-minute timeout.
- Zip extraction refuses path-traversal entries; corrupt archives leave
  nothing on disk.
- Every registration + registration-failure is visible on the **Cerulean
  Plugins** page.

### Adding a new extension-point type (e.g. `push_target` in Phase B)
1. Add the string to `SUPPORTED_EXTENSIONS` in `manifest.py`.
2. Add a new registry dict + register/unregister/get/all helpers in
   `extension_points.py`, plus a matching `register_<type>()` on
   `PluginContext`.
3. In the consumer (the router / task / etc.) call `all_<type>s()` and
   pick the right entry to invoke.
4. Teach `runtime_subprocess.register_subprocess_plugin()` what CLI
   contract the new type uses.
5. Update `docs/PLUGIN-AUTHORING.md` with the callable signature.

## AI-Assisted Features (Phases 1–4)

Cerulean exposes six AI-assisted capabilities. Every one follows the same
pattern — **AI analyzes → human reviews → pipeline executes**. Nothing an
AI produces touches the data without explicit engineer sign-off.

### Per-user feature flags

Every AI feature is opt-in per user via the preference system:

| File | Purpose |
|------|---------|
| `cerulean/core/features.py` | `FEATURES` registry — the single source of truth for every toggle. Add a new `Feature(...)` entry to expose a new flag. |
| `cerulean/core/preferences.py` | Async + sync read/write helpers. `require_preference(key)` is a FastAPI dep that 403s when off. `pref_enabled_sync(session, user_id, key)` is the Celery-side check. |
| `cerulean/api/routers/preferences.py` | `GET/PATCH /api/v1/users/me/preferences`, `POST /reset`. Dev-mode tolerant (synthetic dev user when OAuth not configured). |

**Feature keys (all default OFF per spec §11.3):**

| Key | Phase | Feature |
|-----|-------|---------|
| `ai.data_health_report` | 3 | Auto-analyzes ingested MARC files, produces plain-English briefing |
| `ai.value_aware_mapping` | 2 | Adds a top-50 distinct-value index to the AI Suggest prompt |
| `ai.transform_rule_gen` | 4 | Plain-English → sandboxed Python expression + mandatory before/after preview |
| `ai.code_reconciliation` | 5 (TBD) | Koha authorized-value matching for branch / location / item-type codes |
| `ai.fuzzy_patron_dedup` | 6 (TBD) | AI confidence scoring on probable patron duplicate clusters |
| `ai.record_enrichment` | roadmap | Thin-record enrichment from ISBN / OCLC (not built) |

### Adding a new AI feature

1. **Register in `FEATURES`** — add a `Feature(key=..., default=False, category=CATEGORY_AI, shares_data_with_anthropic=True, ...)` entry. Keys are dot-namespaced (`ai.my_feature`). Defaults MUST be `False`.
2. **Gate the endpoint** — either `dependencies=[Depends(require_preference("ai.my_feature"))]` on the route, or inline `get_user_pref(db, user.id, key)` for custom 403 payloads.
3. **Gate the task** — call `pref_enabled_sync(session, user_id, key)` at the top of the Celery task; return a `{"skipped": True}` no-op when disabled. Tasks must be self-gating so routers can queue them without branching.
4. **Pipe `user_id` through** — the triggering router pulls it from `request.state.user_id` and passes as a kwarg to `apply_async`. Tasks that chain into AI tasks forward `user_id` down.
5. **Hide the UI when off** — frontend helper `window.hasFeature(key)` returns true only when the server-loaded `window.userPrefs.values[key]` is true. Wrap the UI block in `if (!window.hasFeature(...)) return;` or skip the DOM insertion entirely.

### Transform Rule Generation sandbox (Phase 4)

The `fn` transform type runs inside a locked-down `eval()`:

- **Available builtins** (`_SAFE_BUILTINS` in `cerulean/tasks/transform.py`): `len str int float bool list dict tuple set min max abs round sorted reversed enumerate zip map filter isinstance range True False None` + a whitelisted `__import__`.
- **Available modules** (`_SANDBOX_ALLOWED_IMPORTS`): `re _sre datetime _datetime time _strptime locale _locale encodings string calendar`.
- **Available names in expression scope** (`_SAFE_GLOBALS`): `value` / `v` (the input string), `re`, `datetime`, `date`, `timedelta`.
- **BLOCKED**: `open`, `exec`, `eval`, `compile`, `globals`, `os`, `subprocess`, `sys`, `socket`, `ctypes`, `importlib`, `pickle`, `builtins`, and anything else not on the whitelist.

Two entry points:
- `_apply_fn(value, expr)` — production path; swallows errors and returns the input unchanged on failure.
- `apply_fn_safe(value, expr)` — returns `(result, error_or_none)`; used by the preview endpoint so per-row errors surface inline.

Before expanding the whitelist, add a matching `TestSandboxBlocksDangerousEscapes` entry so new modules get audited.

## What NOT to Do

- **Don't create new JS/CSS files** — everything goes in `frontend/index.html`
- **Don't use a JS framework** — stick with vanilla JS, `innerHTML`, and template literals
- **Don't use `<a href>` for API downloads** — use `authDownload()`
- **Don't define routes after `{parameter}` routes** — specific paths first
- **Don't use lazy loading on async relationships** — use `selectinload()`
- **Don't put non-ASCII in HTTP headers** — strip or encode
- **Don't skip Alembic migrations** — every table change needs one
- **Don't restart nginx after web restart** — the resolver config handles DNS re-resolution
- **Don't use the Docker CLI inside containers** — use the Docker Engine API via Unix socket

## Testing Changes

```bash
# Compile check Python
python3 -m py_compile cerulean/path/to/file.py

# Run locally
docker compose up -d --build web worker
docker compose exec web alembic upgrade head

# Check logs
docker compose logs web --tail=20
docker compose logs worker --tail=20
```

## Deploying to Production

```bash
ssh root@cerulean-next.gallagher-family-hub.com
cd /opt/cerulean
git pull
docker compose -f docker-compose.yml -f docker-compose.prod.yml exec web alembic upgrade head
docker compose -f docker-compose.yml -f docker-compose.prod.yml restart web worker worker-push
# nginx does NOT need restart (resolver handles it)
```

## Domain Knowledge

**MARC** (MAchine-Readable Cataloging) is the metadata format used by libraries worldwide. Key concepts:
- **Tags**: 3-digit field identifiers (001=control number, 245=title, 100=author, 952=Koha items)
- **Subfields**: $a, $b, etc. within a tag (245$a=title proper, 245$b=subtitle)
- **Indicators**: Two single-character values per data field
- **Leader**: 24-character fixed field at position 0 of every record
- **ISO 2709**: Binary MARC format (.mrc files)
- **MRK**: Mnemonic text format (=245 10$aTitle$bSubtitle) — MarcEdit's native format
- **MARCXML**: XML representation of MARC

**Koha** is the open-source ILS. Key integration points:
- REST API at `/api/v1/`
- Items stored in 952 fields
- Item types in 942$c
- Plugins are .kpz files (zipped Perl modules)
- Elasticsearch for search indexing

**Evergreen** stores MARC as MARCXML text in PostgreSQL (`biblio.record_entry`). Search indexing via `pingest.pl` populates `metabib.*` tables.

**Aspen Discovery** is a discovery layer that sits in front of Koha/Evergreen. Connects via SystemAPI.
