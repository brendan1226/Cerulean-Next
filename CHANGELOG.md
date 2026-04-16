# Changelog

All notable changes to Cerulean Next are documented here.

## [2.1.0] — 2026-04-15 — AI-Assisted Data Manipulation (Phases 1–4)

Implements the first four of six AI-assisted capabilities described in
`cerulean_ai_spec.md`. Every feature follows the same pattern: **AI
analyzes → human reviews → pipeline executes**. Nothing AI produces
touches the data without explicit engineer sign-off.

All features are **off by default** and gated per user via the new
preference system. Existing functionality is unchanged — everything is
additive.

### Phase 1 — User Preferences Foundation

- New `user_preferences` table (generic key/value per user) backs AI
  feature flags today and future granular visibility toggles tomorrow.
- `cerulean/core/features.py` registry is the single source of truth for
  every toggleable preference. Adding a new toggle is one dict entry.
- `require_preference(key)` FastAPI dependency → 403 when disabled.
  `pref_enabled_sync()` helper for Celery tasks.
- `GET/PATCH /api/v1/users/me/preferences` + `POST /reset`.
- New **My Preferences** page in the sidebar, auto-rendered from the
  server-side registry with per-feature toggles, "Enable all AI features"
  shortcut, and reset-to-defaults.
- Dev-mode tolerant: synthetic dev user is created when OAuth isn't
  configured so the UI still works locally.

### Phase 2 — Value-Aware Field Mapping

- When `ai.value_aware_mapping` is on, the AI Suggest task now sends
  Claude a top-50 distinct-value index per subfield (capped at 50k
  records per file) alongside the tag frequency report. Suggestions
  cite specific values ("values match known branch codes MAIN,
  BRANCH1, BOOKMOBILE") instead of reasoning from tag names alone.
- Field Mapping step (Step 5) now shows **High / Med / Low** confidence
  badges on every AI suggestion row (thresholds 0.85 / 0.60).
- Hover-reveal **ⓘ info tooltip** on AI rows shows the reasoning inline.
- Low-confidence rows get an amber tint and stay pending when **Approve
  All** runs (threshold bumped 0.70 → 0.60 to match the Med/Low cutoff).

### Phase 3 — Data Health Report

- New `marc_files.health_report` JSONB column + status metadata.
- `data_health_report_task` (analyze queue) runs on every newly-ingested
  MARC file when `ai.data_health_report` is on. Sends a stratified
  sample (first 100 + random 400) to Claude, stores the JSON report.
- New **◈ AI Data Health Report** collapsible panel on Step 1, hidden
  entirely when the feature is off. Summary paragraph, ILS origin chip
  + confidence, sample / file size badges, findings cards grouped
  action_required → warning → info.
- `GET /api/v1/projects/{pid}/files/{fid}/health-report` and
  `POST /.../health-report/run` endpoints.

### Phase 4 — Transform Rule Generation

- New `field_maps.ai_prompt` column stores the original plain-English
  description alongside the generated expression.
- **Describe transform** panel in the map edit and new modals (hidden
  when `ai.transform_rule_gen` is off). User types intent, clicks
  Generate, Claude writes a sandboxed Python expression and a
  before/after preview on 10 real sample values renders inline.
- **Mandatory preview gate** (spec §5.4): the Approve checkbox can't
  be saved until a clean preview has rendered. Editing the expression
  manually re-engages the gate until Preview is clicked.
- `fn` transform sandbox expanded so spec examples (`re.sub`,
  `datetime.strptime` + `strftime`) actually run. `__import__` is
  whitelisted against `re / datetime / time / _strptime / locale /
  encodings / string / calendar` only — `os`, `subprocess`, `sys`,
  `socket`, `ctypes`, `importlib`, `pickle` stay blocked.
- `apply_fn_safe(value, expr) → (result, error)` surfaces per-row
  failures so the preview table can show errors inline instead of
  silently passing the input through.
- `POST /api/v1/projects/{pid}/maps/ai-transform/generate` and
  `POST /.../ai-transform/preview` endpoints (both gated).

### Tests

- 69 new unit tests (161 total, up from 92). Coverage:
  - `tests/core/test_features.py` — registry defaults, AI-off invariants, payload shape.
  - `tests/core/test_preferences.py` — sync helpers, unknown-key validation.
  - `tests/tasks/test_ai_value_aware.py` — `_build_value_index`, `_format_value_index`.
  - `tests/tasks/test_ai_health_report.py` — report parsing, stratified sampling.
  - `tests/tasks/test_ai_transform_sandbox.py` — spec examples run, dangerous escapes blocked (13 attack vectors), whitelist pinned.

### Bug Fixes

- `_build_value_index` no longer counts whitespace-only subfield values
  (they collapsed to empty strings and polluted the AI prompt).

### Migrations

- `t0n5o6p7q8r9` — create `user_preferences` table.
- `u1o6p7q8r9s0` — add `health_report` / `health_report_status` /
  `health_report_error` / `health_report_generated_at` to `marc_files`.
- `v2p7q8r9s0t1` — add `ai_prompt` to `field_maps`.

---

## [2.0.0] — 2026-04-14

### Major Features

#### MARC Tools Suite
- **SQL Explorer** — Query MARC data with SQL-like syntax (`SELECT 001, 245$a WHERE 942$c = 'DVD'`). Supports =, !=, CONTAINS, LIKE, EXISTS, MISSING, AND/OR, ORDER BY, LIMIT. Schema browser, example queries, CSV export.
- **Export & Extract** — Export selected MARC fields as CSV/TSV spreadsheet. Extract records by criteria into separate .mrc files. JSON import/export.
- **File Manager** — Split files by record count or by field value. Join multiple files with optional 001 deduplication. Browse all project .mrc files.
- **Macros** — Save and replay sequences of batch edit operations (find/replace, regex, add/delete fields). Reusable across projects.
- **Clustering** — Group records by any field/subfield to see value distribution with counts, percentages, and visual bars. Export as CSV.
- **CSV to MARC** — Convert spreadsheet data into MARC bib records by mapping columns to tags/subfields.

#### Batch Editing (MarcEdit-style)
- **Find & Replace** — Text substitution across all records, filter by tag/subfield, case-sensitive and whole-field options.
- **Regex Substitution** — Pattern-based changes with capture groups.
- **Add Field** — Add new MARC fields to all records with conditional logic.
- **Delete Field** — Remove fields/subfields with optional content filtering.
- **Call Number Generation** — Copy classification (082/050/090) into call number field with author cutter.

#### RDA Helper
- Scan records for missing 336/337/338 fields.
- Auto-generate RDA content/media/carrier type fields from MARC leader bytes.
- Reference table showing complete leader-to-RDA mapping.

#### MarcEdit Integration
- **MRK Export** — Download any MARC file as .mrk (MarcEdit's native text format).
- **OAI-PMH 2.0 Endpoint** — MarcEdit can harvest records directly via `GET /oai/{project_id}`. Supports Identify, ListRecords, GetRecord, ListMetadataFormats with marcxml output and pagination.

#### Authentication & Multi-User
- **Google OAuth** login for `@bywatersolutions.com` and `@openfifth.co.uk` domains.
- **Multi-domain support** — Comma-separated allowed domains in System Settings.
- **Per-user workspaces** — Private and shared project visibility.
- **User model** with Google profile (name, email, picture, last login).
- **Auth middleware** gates all API endpoints (bypassed when OAuth not configured for dev).

#### System Administration
- **System Settings GUI** — Configure OAuth, JWT, AI settings from the browser. Changes take effect immediately.
- **System Logs** — View registered users, authentication events, and per-user action history.
- **Plugin Manager** — Upload, download, auto-install Koha plugins (.kpz) via REST API or Docker fallback.

#### Template System Enhancements
- **Google Sheets Import** — Paste a Google Sheets URL to create a template (sheet must be shared).
- **CSV Import/Export** — Download templates as CSV, edit in spreadsheets, re-import.
- **Template Preview** — Click any template to see its full mapping table.
- **Sample Template** — One-click "Symphony → Koha" demo template with 31 mappings.

#### Suggestions & Feedback
- **Comments** — Threaded comment discussion on each suggestion.
- **Editing** — Edit your own suggestions (title, body, type).
- **Expanded Statuses** — Open, Confirmed, In Progress, Fixed, Shipped, Future Dev, Won't Fix, System Action, Closed.
- **Two-panel layout** — List on left, detail + comments on right.

#### Reference Data Management (Load Setup)
- **CSV Download** — Export reference data (libraries, item types, etc.) as spreadsheet.
- **SQL Download** — Export as `INSERT IGNORE` statements for direct Koha DB loading.
- **CSV Upload** — Import edited descriptions back into the grid.

#### Upload Experience
- **Real-time progress** — Bytes transferred, upload speed (MB/s), elapsed time, ETA.
- **XHR-based upload** — Replaced fetch() with XMLHttpRequest for progress events.

### Pipeline & Workflow
- **Reordered pipeline** — Reconcile → Patrons → Patron Versions now come before Load (logical data preparation order).
- **Renamed "Stage" to "Step"** — All headers, breadcrumbs, and help text use consistent "Step N" numbering.
- **Clickable pipeline bar** — Click any step in the pipeline bar to navigate directly.
- **Reconciliation explanation** — Info banner explaining when to use vs. skip reconciliation.
- **Koha connection help** — `?` buttons on Koha URL fields with popup explaining direct URL vs. SSH tunnel.

### Infrastructure
- **DigitalOcean deployment** — Production at cerulean-next.gallagher-family-hub.com.
- **SSL via Let's Encrypt** — Auto-renewal via certbot.
- **nginx reverse proxy** — Dynamic resolver to prevent 502 after web restarts.
- **Dedicated push worker** — Separate Celery container for long-running Koha/Aspen/Evergreen tasks.
- **Scaled workers** — 8 general + 4 push concurrent Celery tasks.
- **Timezone** — Set to America/Los_Angeles (Portland, OR).

### Documentation
- **User Manual** — Comprehensive PDF and markdown manual covering all 23 feature areas.
- **Interactive Help** — Searchable, collapsible help page in the app with 18 sections.
- **Deploy Guide** — Step-by-step DigitalOcean deployment instructions.
- **Local Koha Guide** — SSH tunnel setup for Mac, Windows, and Linux.
- **README** — Complete project overview with hosting recommendations.

### Bug Fixes
- Fixed OAuth redirect_uri behind reverse proxy (http:// → https://).
- Fixed suggestion vote endpoint (pre-auth leftover requiring user_email query param).
- Fixed suggestion comments lazy-load in async SQLAlchemy (MissingGreenlet).
- Fixed template route ordering (specific paths before {template_id}).
- Fixed JS syntax error in template preview (broken escaped quotes).
- Fixed non-ASCII characters in Content-Disposition headers (Unicode → ASCII strip).
- Fixed all download links site-wide to use authenticated fetch (JWT token).
- Fixed reconciliation scan results disappearing on tab switch.
- Fixed settings module name collision with config settings in main.py.
- Fixed System Logs to read from file + Docker socket instead of Docker CLI.

---

## [1.0.0] — 2026-04-05

### Initial Release
- **Stage 12 — Aspen Discovery**: Turbo Migration + Turbo Reindex with parallel workers.
- **Stage 13 — Evergreen ILS**: Direct PostgreSQL push with trigger control (all_on/indexing_only/all_off), pingest execution via Docker exec.
- **Evergreen metarecord remap** — Auto-populate metabib.metarecord_source_map after bulk insert.
- **Evergreen service restart** — Restart memcached + osrf_control + apache2 via Docker exec.

---

## [0.9.0] — 2026-03-31

### Core Pipeline
- 11-stage architecture: Ingest → Config → Quality → Versions → Mapping → Transform → Load → Reconciliation → Patrons → Patron Versions → Holds.
- Quality scanning with 8 check categories and auto-fix.
- AI-assisted field mapping via Claude API.
- Migration Mode for Koha (daemon control + DB tuning).
- Multiple push methods: REST API, FastMARCImport, Migration Toolkit, Bulk API.
- TurboIndex parallel Elasticsearch reindexing.
- Item reconciliation with scan/rules/apply workflow.
- Patron data import with CSV/Excel/XML/MARC parsing, AI column mapping, value reconciliation.
- Version snapshots with diff comparison.
- Live import operations panel with Koha job direct polling.
