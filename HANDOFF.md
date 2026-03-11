# Cerulean-Next — Full Project Handoff Document

## What Is This?

**Cerulean-Next** is a MARC library migration platform that moves bibliographic, patron, and holdings data from legacy ILS (Integrated Library System) systems into **Koha** (open-source ILS). It automates a 6-stage pipeline with AI-assisted field mapping, deduplication, and Koha REST API integration.

**Tech Stack:**
- **Backend:** Python 3.14, FastAPI (async), SQLAlchemy 2.0 (async sessions), Pydantic v2
- **Task Queue:** Celery 5.4 with Redis broker (workers are sync, not async)
- **Database:** PostgreSQL 15 with Alembic migrations
- **MARC Processing:** pymarc 5.x (uses `Subfield` namedtuples, not old-style flat lists)
- **Frontend:** Vanilla JavaScript SPA (~4,200 lines, single `frontend/index.html` file, no build step)
- **AI:** Claude API (Anthropic) for field mapping suggestions
- **Monitoring:** Flower (Celery UI), structlog (structured logging)
- **Infrastructure:** Docker Compose (web, worker, beat, flower, postgres, redis, optional elasticsearch + minio)

---

## The 6-Stage Pipeline

```
Stage 1: Data Ingest     → Upload MARC files, parse, index, detect source ILS
Stage 2: Analyze & Map   → Create field maps (manual, AI-suggested, or from templates)
Stage 3: Transform       → Apply approved maps to records, merge files, join items CSV
Stage 4: Dedup           → Scan for duplicates, resolve clusters, write deduped output
Stage 5: Push to Koha    → Preflight check, push bibs/patrons/holds/circ via Koha API
Stage 6: KTD Sandbox     → Spin up Koha Testing Docker for validation
```

Each stage has its own Celery queue, API router, and frontend view.

---

## Database Models

All models live in `cerulean/models/__init__.py`.

### Core Models

| Model | Table | Purpose |
|-------|-------|---------|
| **Project** | `projects` | Top-level migration project (1 library = 1 project). Has `code`, `library_name`, `current_stage` (1–6), `stage_N_complete` booleans, `koha_url`, `koha_token_enc` (Fernet-encrypted), `source_ils`, `archived`, record counts. |
| **MARCFile** | `marc_files` | Uploaded MARC or CSV file. Has `project_id`, `filename`, `file_format` (iso2709/mrk/csv), `record_count`, `tag_frequency` (JSONB histogram), `subfield_frequency` (JSONB), `status` (uploaded→indexing→indexed/error), `sort_order`. |
| **FieldMap** | `field_maps` | Source→target field mapping rule. Has `source_tag/sub`, `target_tag/sub`, `transform_type` (copy/regex/lookup/const/fn/preset), `transform_fn`, `preset_key`, `delete_source`, `approved`, `ai_suggested`, `ai_confidence`, `ai_reasoning`, `source_label` (manual/ai/template:{id}), `sort_order`. |
| **MapTemplate** | `map_templates` | Reusable saved mapping set. Has `name`, `version`, `scope` (project/global), `source_ils`, `maps` (JSONB array of map dicts), `ai_generated`, `reviewed`, `use_count`. |

### Pipeline Tracking

| Model | Table | Purpose |
|-------|-------|---------|
| **TransformManifest** | `transform_manifests` | Tracks a transform or merge run. Has `task_type` (transform/merge), `status` (running/complete/error), `celery_task_id`, `files_processed`, `total_records`, `records_skipped`, `items_joined`, `duplicate_001s` (JSONB), `file_ids` (JSONB). |
| **DedupRule** | `dedup_rules` | Dedup rule config. Has `preset_key` (001/isbn/title_author/oclc), `fields` (JSONB match config), `on_duplicate` strategy, `active`, scan stats. |
| **DedupCluster** | `dedup_clusters` | A group of duplicate records. Has `rule_id`, `match_key`, `records` (JSONB list), `primary_index`, `resolved`. |
| **PushManifest** | `push_manifests` | Tracks a push-to-Koha run. Has `task_type` (preflight/bulkmarc/patrons/holds/circ/reindex), `status`, `dry_run`, record counts, `result_data` (JSONB). |
| **SandboxInstance** | `sandbox_instances` | KTD container lifecycle. Has `container_id`, `status`, `koha_url`, `exposed_port`. |

### Audit & Feedback

| Model | Table | Purpose |
|-------|-------|---------|
| **AuditEvent** | `audit_events` | Append-only project event log. Has `stage` (1–6), `level` (info/warn/error/complete), `tag`, `message`, `record_001`, `extra` (JSONB). Visible in the Project Log UI. |
| **Suggestion** | `suggestions` | Engineer feedback (feature/bug/workflow). Has `type`, `title`, `body`, `status`, `vote_count`. |

---

## Complete API Endpoint List

All endpoints are prefixed with `/api/v1`.

### Projects
```
GET    /projects                              — list projects (exclude archived by default)
POST   /projects                              — create project
GET    /projects/{id}                         — project detail
PATCH  /projects/{id}                         — update (library_name, koha_url, koha_token, source_ils, archived)
```

### Files (Stage 1)
```
POST   /projects/{id}/files                   — upload MARC/CSV file (triggers ingest task)
GET    /projects/{id}/files                   — list files
DELETE /projects/{id}/files/{fid}             — delete file
GET    /projects/{id}/files/{fid}/records/{n} — preview record N (0-indexed)
GET    /projects/{id}/files/{fid}/tags        — tag frequency histogram
GET    /projects/{id}/search?q=...            — full-text search (requires Elasticsearch)
```

### Maps (Stage 2)
```
GET    /projects/{id}/maps?status=...         — list maps (approved|pending|manual)
POST   /projects/{id}/maps                    — create map manually
PATCH  /projects/{id}/maps/{mid}              — edit map
DELETE /projects/{id}/maps/{mid}              — delete map
POST   /projects/{id}/maps/ai-suggest         — dispatch AI analysis task
POST   /projects/{id}/maps/{mid}/approve      — approve single map
POST   /projects/{id}/maps/approve-all        — batch approve by confidence threshold
GET    /projects/{id}/maps/validate           — validate all approved maps (pre-transform check)
```

### Templates
```
GET    /templates?scope=...&source_ils=...    — list templates
POST   /templates?project_id=...              — save project maps as template
GET    /templates/{tid}                       — template detail
PATCH  /templates/{tid}                       — edit metadata
DELETE /templates/{tid}                       — delete
POST   /templates/{tid}/promote               — project→global scope
POST   /projects/{id}/maps/load-template      — apply template to project
```

### Transform (Stage 3)
```
POST   /projects/{id}/transform/start         — dispatch transform task
POST   /projects/{id}/transform/merge         — dispatch merge task
GET    /projects/{id}/transform/status        — poll Celery task status
GET    /projects/{id}/transform/manifest      — list manifests
GET    /projects/{id}/transform/preview       — preview transformed vs original record
POST   /projects/{id}/transform/clear         — delete all transformed files & manifests
POST   /projects/{id}/transform/pause         — pause transform (Redis flag)
POST   /projects/{id}/transform/resume        — resume transform
```

### Dedup (Stage 4)
```
GET    /projects/{id}/dedup/rules             — list rules
POST   /projects/{id}/dedup/rules             — create rule
PATCH  /projects/{id}/dedup/rules/{rid}       — update rule
DELETE /projects/{id}/dedup/rules/{rid}       — delete rule + clusters
POST   /projects/{id}/dedup/presets           — create all 4 preset rules
POST   /projects/{id}/dedup/scan              — dispatch scan task
POST   /projects/{id}/dedup/apply             — dispatch apply task
GET    /projects/{id}/dedup/clusters          — paginated cluster list
PATCH  /projects/{id}/dedup/clusters/{cid}    — resolve cluster
GET    /projects/{id}/dedup/status            — poll task status
```

### Push (Stage 5)
```
POST   /projects/{id}/push/preflight          — dispatch preflight (test Koha connectivity)
POST   /projects/{id}/push/start              — dispatch push tasks (bibs, patrons, holds, circ, reindex)
GET    /projects/{id}/push/status             — poll task status
GET    /projects/{id}/push/manifests          — list push manifests
GET    /projects/{id}/push/log                — alias for manifests (desc order)
GET    /projects/{id}/push/files              — list push-ready MARC files with record counts
GET    /projects/{id}/push/files/download     — download a MARC file
GET    /projects/{id}/push/files/preview      — preview records from a MARC file
```

### Sandbox (Stage 6)
```
POST   /projects/{id}/sandbox/provision       — spin up KTD container
GET    /projects/{id}/sandbox/status          — current sandbox status
POST   /projects/{id}/sandbox/teardown        — stop & remove container
GET    /projects/{id}/sandbox/instances       — list all instances
```

### Audit Log
```
GET    /projects/{id}/log?stage=&level=       — paginated log (filterable)
GET    /projects/{id}/log/stream              — SSE real-time event stream
GET    /projects/{id}/log/export?format=csv   — export as CSV or TXT
```

### Reference
```
GET    /reference/transform-presets           — list available transform presets with descriptions
GET    /reference/koha-marc-fields            — Koha MARC field reference (stub)
```

### Suggestions
```
GET    /suggestions                           — list feedback
POST   /suggestions                           — submit feedback
POST   /suggestions/{id}/vote                 — toggle upvote
PATCH  /suggestions/{id}                      — update status
```

### Health
```
GET    /api/health                            — {"status": "ok", "version": "1.0.0"}
```

---

## Celery Tasks & Queues

Tasks are **synchronous** (Celery workers don't support async). They use `psycopg2` instead of `asyncpg` for database access.

| Queue | Tasks |
|-------|-------|
| **ingest** | `ingest_marc_task`, `detect_ils_task`, `tag_frequency_task` |
| **analyze** | `ai_field_map_task`, `save_template_task`, `load_template_task` |
| **transform** | `transform_pipeline_task`, `merge_pipeline_task` |
| **dedup** | `dedup_scan_task`, `dedup_apply_task` |
| **push** | `push_preflight_task`, `push_bulkmarc_task`, `push_patrons_task`, `push_holds_task`, `push_circ_task`, `es_reindex_task` |
| **sandbox** | `ktd_provision_task`, `ktd_teardown_task` |

### Key Task Details

**Stage 1 — Ingest** (`cerulean/tasks/ingest.py`):
- `ingest_marc_task` validates the file, counts records, marks as "indexed", then chains `detect_ils_task` + `tag_frequency_task`
- `detect_ils_task` samples records, runs heuristics to detect source ILS (e.g. "SirsiDynix Symphony")
- `tag_frequency_task` builds tag + subfield frequency histograms (stored as JSONB)

**Stage 2 — Analyze** (`cerulean/tasks/analyze.py`):
- `ai_field_map_task` collects tag frequencies + sample records, calls Claude API, creates unapproved FieldMap suggestions
- Templates can be saved from / loaded into projects

**Stage 3 — Transform** (`cerulean/tasks/transform.py`):
- `transform_pipeline_task` loads approved FieldMaps, validates them, applies to each record, writes `{stem}_transformed.mrc`
- Transform types: copy, regex (`s/pat/repl/flags`), lookup (JSON dict), const, fn (sandboxed Python eval), preset
- Supports pause/resume via Redis flag `cerulean:pause:{project_id}`
- `delete_source` is deferred until ALL maps finish reading (prevents mid-loop deletion)
- Per-map stats tracking (applied/skipped/errors) logged to audit log
- `merge_pipeline_task` combines transformed files + optional items CSV join (952 fields)

**Stage 4 — Dedup** (`cerulean/tasks/dedup.py`):
- Preset rules: 001, ISBN, title+author, OCLC
- Strategies: keep_first, keep_most_fields, keep_most_items, write_exceptions, merge_holdings
- Writes `merged_deduped.mrc`

**Stage 5 — Push** (`cerulean/tasks/push.py`):
- Preflight: tests Koha connectivity, detects version + search engine
- Bulk MARC import via Koha REST API
- Patron/holds/circ import from CSV

**Stage 6 — Sandbox** (`cerulean/tasks/sandbox.py`):
- Provisions Koha Testing Docker containers for validation

---

## Transform Presets

Defined in `cerulean/core/transform_presets.py`. Each is a pure function `(value: str) -> str`.

| Category | Presets |
|----------|---------|
| **Date** | `date_mdy_to_dmy`, `date_dmy_to_iso`, `date_ymd_to_iso`, `date_marc_to_iso` |
| **Case** | `case_upper`, `case_lower`, `case_title`, `case_sentence` |
| **Text** | `text_trim`, `text_normalize_spaces`, `text_strip_punctuation` |
| **Clean** | `clean_isbn`, `clean_lccn`, `clean_html` |
| **Extract** | `extract_year`, `extract_isbn13` |
| **Const** | `const_value` |

---

## Frontend Architecture

**Single file:** `frontend/index.html` (~4,200 lines of vanilla JS + inline CSS)

### Views (routed via `window.navigate(viewName)`)

| View | Function | Description |
|------|----------|-------------|
| `dashboard` | `renderDashboard()` | Project list with stage progress pips |
| `new-project` | `renderNewProject()` | Create project form |
| `stage1` | `renderStage1()` | File upload, file list, tag frequency viewer |
| `stage2` | `renderStage2()` | Tab UI: Approved Maps, AI Suggestions, Add/Edit modal with unified transform dropdown |
| `stage3` | `renderStage3()` | Sub-tabs: Transform, Merge, Results. Preview with original vs transformed side-by-side |
| `stage4` | `renderStage4()` | Dedup rules, cluster viewer/resolver |
| `stage5` | `renderStage5()` | Push tabs: Preflight, Bibs, Patrons, Holds, Circ. File review with download/preview |
| `stage6` | `renderStage6()` | KTD sandbox provisioning |
| `project-log` | `renderProjectLog()` | Filterable audit log viewer |
| `project-settings` | `renderProjectSettings()` | Edit project name, ILS, Koha URL, API token |
| `templates` | `renderTemplates()` | Template browser |
| `suggestions` | `renderSuggestions()` | Feedback board |

### Key Frontend Patterns

- **API client:** `window.api(method, path, body)` — auto-JSON, error toasts
- **Toast notifications:** `toast(message, type)` — with deduplication (3s window)
- **Task polling:** `pollTaskStatus(projectId, taskId, statusElId, label, stageType, onComplete)` — generic Celery task poller with global progress bar, cancellation support
- **Progress bar:** `renderProgressBar({ percent, label, status, detail })`
- **Stage badges:** `updateStageBadges(project)` — updates nav sidebar indicators
- **Unified transform dropdown:** Combines built-in presets and custom transform types in one `<select>` with `(i)` info button
  - Value encoding: `"copy"`, `"preset:<key>"`, `"custom:regex"`, `"custom:const"`, `"custom:fn"`, `"custom:lookup"`
- **Map sorting:** Maps displayed sorted by source tag (MARC order)
- **Duplicate highlighting:** Duplicate source fields highlighted amber, duplicate targets highlighted red
- **Record preview diff:** Transformed fields highlighted green against original

---

## File Organization

```
cerulean/
├── main.py                          # FastAPI app, router registration, middleware
├── core/
│   ├── config.py                    # Pydantic Settings (env vars)
│   ├── database.py                  # AsyncSession factory, get_db dependency
│   ├── logging.py                   # structlog config
│   ├── search.py                    # Elasticsearch wrapper (optional)
│   └── transform_presets.py         # Preset function registry
├── models/__init__.py               # ALL 12 SQLAlchemy models in one file
├── schemas/                         # Pydantic v2 request/response schemas
│   ├── projects.py, maps.py, transform.py, dedup.py, push.py, sandbox.py, events.py
├── api/routers/                     # FastAPI route handlers
│   ├── projects.py, files.py, maps.py, templates.py, transform.py,
│   ├── dedup.py, push.py, sandbox.py, log.py, suggestions.py, reference.py
├── tasks/                           # Celery task modules
│   ├── celery_app.py                # Celery config + task routing
│   ├── audit.py                     # AuditLogger class
│   ├── ingest.py, analyze.py, transform.py, dedup.py, push.py, sandbox.py
└── utils/
    └── marc.py                      # iter_marc, get_001, write_marc, is_valid_marc

frontend/
├── index.html                       # Complete SPA (~4,200 lines)
├── vendor/                          # Vendored JS libraries
└── koha_marc_fields.json            # Static Koha MARC field reference

alembic/versions/                    # 4 migration files
tests/tasks/                         # 82+ tests (transform, dedup, push, ingest)
docker-compose.yml                   # 6-8 services
```

---

## File Storage Layout

```
{DATA_ROOT}/{project_id}/
├── raw/                             # Uploaded source files
│   └── MARCExport.mrc
├── transformed/                     # Stage 3 output
│   └── MARCExport_transformed.mrc
├── merged.mrc                       # Stage 3 merge output
├── merged_deduped.mrc               # Stage 4 dedup output
├── patrons.csv                      # Patron data for push
├── holds.csv                        # Holds data for push
└── items.csv                        # Items CSV for 952 join
```

---

## Docker Services

| Service | Port | Purpose |
|---------|------|---------|
| **web** | 8000 | FastAPI app (uvicorn, hot-reload) |
| **worker** | — | Celery worker (concurrency=4, all queues) |
| **beat** | — | Celery Beat scheduler |
| **flower** | 5555 | Celery monitoring UI |
| **postgres** | 5433 | PostgreSQL 15 |
| **redis** | 6380 | Broker + result backend |
| **elasticsearch** | 9200 | Optional (`--profile search`) |
| **minio** | 9000/9001 | Optional S3 (`--profile minio`) |

**Rebuild commands:**
```bash
docker compose up -d --build web          # API changes only
docker compose up -d --build web worker   # API + task code changes
docker compose up -d --build              # Everything
```

---

## Key Patterns & Conventions

### Async API / Sync Tasks Split
- **API endpoints** use `AsyncSession` (asyncpg) via `Depends(get_db)`
- **Celery tasks** use sync `Session` (psycopg2) — created per-task with `Session(_engine)`
- Never mix: tasks cannot use async sessions

### AuditLogger (visible in Project Log UI)
```python
log = AuditLogger(project_id=pid, stage=3, tag="[transform]")
log.info("Starting")       # → AuditEvent row + structlog
log.warn("Issue found")
log.error("Failed: reason")
log.complete("Done")        # MUST call before returning success
```
- Python's `logger.warning()` goes to Docker container logs only (NOT visible in UI)
- `AuditLogger` writes to both DB (visible in UI) AND structlog

### MARC Processing (pymarc 5.x)
```python
# Creating fields:
field = pymarc.Field(tag="952", indicators=[" ", " "],
                     subfields=[Subfield(code="a", value="LIB")])
# Adding subfields to existing field:
field.add_subfield("b", "MAIN")
# Iterating subfields:
for sf in field.subfields:  # sf.code, sf.value
```

### Transform Flow (per record)
1. Read record from source file via `_iter_marc()`
2. `_apply_maps_to_record(record, maps_data, stats)` — loops ALL maps, each in try/except
3. `_sort_record(record)` — sort fields by tag, subfields by 0-9 then a-z
4. `record.as_marc()` → write bytes to output file
5. Deferred deletes: `delete_source` tags collected during step 2, removed AFTER all maps finish

### Field Map Rules
- Only `approved=True` maps are used in transforms
- `source_label` tracks origin: `"manual"`, `"ai"`, `"template:{id}"`
- Editing an AI-suggested map sets `source_label="manual"` (preserves `ai_suggested=True` for audit)
- `transform_type="preset"` requires `preset_key` (e.g. `"clean_isbn"`)
- `transform_type="fn"` runs sandboxed `eval()` with limited builtins

### Pause/Resume
- Transform checks Redis key `cerulean:pause:{project_id}` between records
- Set: `POST /transform/pause` → `REDIS SET cerulean:pause:{pid} 1`
- Clear: `POST /transform/resume` → `REDIS DEL cerulean:pause:{pid}`

### Token Security
- Koha API tokens encrypted with Fernet before storage
- `FERNET_KEY` env var required
- Encrypted in `projects.py` router, decrypted in `push.py` tasks

---

## Current State & Known Issues

### Working
- Full ingest pipeline (upload, parse, index, detect ILS, tag frequency)
- AI field mapping suggestions (Claude API)
- Manual map creation/editing with unified dropdown
- Map templates (save, load, promote)
- Transform pipeline with all transform types
- Merge pipeline with items CSV join
- Dedup scan & apply with 4 preset strategies
- Push preflight (Koha connectivity check)
- Audit logging with SSE streaming
- Project settings page
- File review & download in Stage 5

### Known Issue: Transform "Only 2 Maps Applied"
- User reports only 2 of 15 maps produce visible results
- Root cause most likely: source MARC records don't contain the tags that most maps reference
- Per-map stats tracking was added but **requires worker container rebuild** (`docker compose up -d --build worker`)
- After rebuild, the Project Log will show per-map applied/skipped/errors counts
- First-record diagnostic will log which tags exist vs which maps expect

### Recently Fixed
- Toast notification loop (pollTaskStatus catch block rescheduling after SUCCESS)
- `_sort_record` crash on `None` subfield codes from corrupt MARC data
- "Skipped" records were still being written to output (missing `continue`)
- Old transformed files not cleaned up before new transform run
- `delete_source` removing fields mid-loop (now deferred)
- Preview not showing original record after first record
- Scroll position jumping when navigating records in preview

### Not Yet Built/Validated
- End-to-end push to a real Koha instance (preflight works, bulkmarcimport untested)
- KTD sandbox provisioning (Docker-in-Docker)
- Authentication/authorization (currently open)
- Performance with 100k+ record files
- Elasticsearch full-text search integration

---

## Environment Variables

```bash
DATABASE_URL=postgresql+asyncpg://cerulean:cerulean@postgres:5432/cerulean
REDIS_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1
SECRET_KEY=<long random string>
FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-4-20250514
DATA_ROOT=/data/projects
DEBUG=true
LOG_LEVEL=info
# Optional:
ELASTICSEARCH_URL=http://elasticsearch:9200
FLOWER_USER=admin
FLOWER_PASSWORD=admin
```

---

## Running Tests

```bash
pytest tests/ -v                    # All tests
pytest tests/tasks/test_transform.py -v  # Transform tests only (48 tests)
pytest tests/ --cov=cerulean        # With coverage
```

82+ tests currently passing.
