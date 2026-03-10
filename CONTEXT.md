# Cerulean-Next — Context for New Chats

Copy-paste this into a new AI chat to give it full context on the project.

---

## What is Cerulean-Next?

Cerulean-Next is a **MARC library migration platform** that moves bibliographic data from legacy ILS systems into Koha. It has a 6-stage pipeline:

| Stage | Name | What it does |
|-------|------|-------------|
| 1 | Data Ingest | Upload MARC/CSV files, auto-detect source ILS, index & build tag frequency |
| 2 | Analyze & Map | Create field mappings (source tag → Koha tag), AI-suggest maps, approve |
| 3 | Transform & Merge | Apply approved maps to MARC records, merge all files + optional items CSV join |
| 4 | Validate & Dedup | Scan for duplicates (by 001, ISBN, title/author, OCLC), resolve clusters, write deduped output |
| 5 | Push to Koha | Preflight connectivity check, push bibs/patrons/holds/circ via Koha REST API |
| 6 | KTD Sandbox | Provision a Koha Testing Docker container for validation |

Each stage sets `stage_N_complete=True` and advances `current_stage` when done.

---

## Tech Stack

- **Backend:** Python 3.11, FastAPI (async), SQLAlchemy 2.0 (async sessions), Pydantic v2
- **Task Queue:** Celery 5.4 with Redis broker, tasks use sync psycopg2 (not async)
- **Database:** PostgreSQL 15, Alembic migrations
- **MARC Processing:** pymarc 5.1.2 (uses `Subfield` objects, NOT old-style alternating lists)
- **Frontend:** Single vanilla JS SPA (`frontend/index.html`, ~2,880 lines), no build step, served from FastAPI at `GET /`
- **Infrastructure:** Docker Compose (web, worker, beat, flower, postgres, redis, optional minio)

---

## Project Structure

```
cerulean/
  main.py                    # FastAPI app factory, all router registration
  core/
    config.py                # Pydantic Settings (DATABASE_URL, REDIS_URL, DATA_ROOT, etc.)
    database.py              # AsyncSession factory, Base declarative class
    logging.py               # structlog configuration
  models/__init__.py         # ALL 12 SQLAlchemy models in one file
  schemas/
    projects.py, maps.py, events.py, transform.py, dedup.py, push.py, sandbox.py
  api/routers/
    projects.py, files.py, maps.py, templates.py, transform.py,
    dedup.py, push.py, sandbox.py, log.py, suggestions.py
  tasks/
    celery_app.py            # Celery app + config
    audit.py                 # AuditLogger (writes AuditEvent rows from tasks)
    ingest.py                # Stage 1 tasks
    analyze.py               # Stage 2 tasks (ILS detection, AI map suggest)
    transform.py             # Stage 3 tasks
    dedup.py                 # Stage 4 tasks
    push.py                  # Stage 5 tasks
    sandbox.py               # Stage 6 tasks
  utils/
    marc.py                  # Shared helpers: iter_marc(), get_001(), write_marc()
  services/                  # (empty — reserved for future business logic)
frontend/
  index.html                 # Complete SPA — two <script> blocks, inline CSS
tests/
  tasks/
    test_transform.py        # 48 tests — transform helpers
    test_dedup.py            # 27 tests — dedup helpers
    test_push.py             # 7 tests — push helpers + model imports
alembic/
  versions/                  # 2 migrations (transform_manifests, push/sandbox tables)
docker-compose.yml
Dockerfile
requirements.txt
```

---

## Key Models (all in `cerulean/models/__init__.py`)

| Model | Table | Key Fields |
|-------|-------|-----------|
| Project | projects | code, library_name, current_stage, stage_1-6_complete, koha_url, koha_token_enc |
| MARCFile | marc_files | project_id, filename, file_format, record_count, tag_frequency (JSONB), status |
| FieldMap | field_maps | project_id, source_tag/sub, target_tag/sub, transform_type, approved |
| MapTemplate | map_templates | name, source_ils, scope, maps (JSONB) |
| TransformManifest | transform_manifests | project_id, task_type, status, output_path, files_processed |
| DedupRule | dedup_rules | project_id, preset_key, fields (JSONB), on_duplicate, active |
| DedupCluster | dedup_clusters | rule_id, match_key, records (JSONB), resolved, primary_index |
| PushManifest | push_manifests | project_id, task_type, dry_run, records_total/success/failed |
| SandboxInstance | sandbox_instances | project_id, container_id, status, koha_url, exposed_port |
| AuditEvent | audit_events | project_id, stage, event_type, message, detail (JSONB) |
| Suggestion | suggestions | project_id, title, body, category, vote_score |
| SuggestionVote | suggestion_votes | suggestion_id, direction |

---

## API Endpoints (all under `/api/v1`)

**Projects:** GET/POST/PATCH `/projects`
**Files:** POST/GET `/projects/{id}/files`, GET `.../files/{fid}/records/{n}`, GET `.../files/{fid}/tags`
**Maps:** GET/POST/PATCH/DELETE `/projects/{id}/maps`, POST `.../maps/ai-suggest`, POST `.../maps/approve-all`
**Templates:** CRUD `/templates`, POST `.../promote`, POST `/projects/{id}/maps/load-template`
**Transform:** POST `.../transform/start`, POST `.../transform/merge`, GET `.../transform/status`, GET `.../transform/manifest`
**Dedup:** CRUD `.../dedup/rules`, POST `.../dedup/presets`, POST `.../dedup/scan`, POST `.../dedup/apply`, GET `.../dedup/clusters`, PATCH `.../dedup/clusters/{cid}`, GET `.../dedup/status`
**Push:** POST `.../push/preflight`, POST `.../push/start`, GET `.../push/status`, GET `.../push/manifests`
**Sandbox:** POST `.../sandbox/provision`, GET `.../sandbox/status`, POST `.../sandbox/teardown`, GET `.../sandbox/instances`
**Log:** GET `.../log` (paginated), GET `.../log/stream` (SSE), GET `.../log/export`
**Suggestions:** GET/POST/PATCH `/suggestions`, POST `.../vote`
**Health:** GET `/api/health`

---

## Celery Task Queues

| Queue | Tasks |
|-------|-------|
| ingest | ingest_marc_task |
| analyze | detect_ils_task, tag_frequency_task, ai_map_suggest_task |
| transform | transform_pipeline_task, merge_pipeline_task |
| dedup | dedup_scan_task, dedup_apply_task |
| push | push_preflight_task, push_bulkmarc_task, push_patrons_task, push_holds_task, push_circ_task, es_reindex_task |
| sandbox | ktd_provision_task, ktd_teardown_task |

---

## Current Status

- All backend endpoints implemented and syntactically correct
- All 82 unit tests passing
- Docker Compose runs (web, worker, postgres, redis)
- Frontend serves at `http://localhost:8000/` with all 6 stage views
- File upload works (fixed file_format NOT NULL, broadened accepted extensions)
- **NOT yet validated:** End-to-end workflow through all 6 stages
- **Workflow sequencing** (stage preconditions, what triggers next stage) needs specification

---

## Patterns to Follow

- **Routers** use `AsyncSession` via `Depends(get_db)` — all async
- **Tasks** use sync `create_engine` with psycopg2 — Celery workers are not async
- **AuditLogger** writes `AuditEvent` rows from within tasks for traceability
- **pymarc 5.x**: Use `Subfield(code=..., value=...)` objects, iterate with `for sf in field.subfields` (NOT `zip(subfields[::2], subfields[1::2])`)
- **Shared MARC helpers** live in `cerulean/utils/marc.py` — don't duplicate in task files
- **Frontend** uses `window.api(method, path, body)` for all API calls, `window.navigate(view)` for routing
