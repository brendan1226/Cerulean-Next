# Changelog

All notable changes to Cerulean Next are documented here.

## [2.9.0] — 2026-04-29 — Remove all AI features over patron data

Patron records are PII. Cerulean now categorically excludes patron data
from any AI/LLM processing, regardless of safeguards.

### Removed

- **Patron AI column mapping** — `POST /projects/{id}/patrons/maps/ai-suggest`,
  `GET .../ai-suggest/status`, and the `patron_ai_map_task` Celery task.
  The "AI Suggest Mappings" button is gone from Step 8 → Patron Mapping.
- **Fuzzy Patron Deduplication (Phase 6)** — `patron_fuzzy_dedup_task`,
  the `/patrons/fuzzy-dedup/*` endpoints, the `PatronDedupCluster` model
  and `patron_dedup_clusters` table, the `ai.fuzzy_patron_dedup` feature
  flag, and the Stage 8 "Fuzzy Dedup" tab.
- The `projects.patron_ai_task_id` column.
- The `tests/tasks/test_patron_fuzzy_dedup.py` test module.

### Schema

- Migration `a7u2v3w4x5y6_remove_patron_ai_features` drops
  `patron_dedup_clusters` (and its index) plus
  `projects.patron_ai_task_id`.

### Spec & docs

- `cerulean_ai_spec.md` Feature 5 (Fuzzy Patron Dedup) is marked
  REMOVED and excluded from the implementation order, feature-key
  table, and summary.
- `CLAUDE.md` — patron-PII exclusion is documented inline alongside
  the AI feature key table; the Phase 6 implementation notes are gone.
- `tests/core/test_features.py` — `ai.fuzzy_patron_dedup` removed from
  the required-keys set; new `test_no_patron_ai_feature_keys` guard
  prevents reintroduction of any AI feature targeting patron records.

## [2.8.0] — 2026-04-16 — System Status Dashboard

Admin-only dashboard for monitoring system health, active tasks, errors,
and worker status across all projects.

### Backend (`cerulean/api/routers/system_status.py`)

- **`GET /system/status`** — aggregated health snapshot: worker health
  (Celery inspect ping/stats/active), queue depths (Redis LLEN), Redis
  memory, active task count, 24h error count, computed alerts, and
  recommended actions.
- **`GET /system/status/tasks`** — all running tasks across every project,
  enriched with Celery state + duration. Includes tasks from
  TransformManifest, PushManifest, and live worker inspection.
- **`GET /system/status/errors`** — recent errors from AuditEvent table,
  grouped by tag with frequency summary.
- **`POST /system/status/actions/revoke-task`** — terminate a specific
  Celery task (for hung processes).
- **`POST /system/status/actions/purge-queue`** — clear a stuck queue.

### Alert detection

- **No workers** (critical) — no Celery workers responding to ping.
- **Queue backlog** (warn) — more than 50 tasks pending in a queue.
- **Hung task** (critical) — task running for > 1 hour.
- **Long-running** (info) — task running for > 30 minutes.

Each alert generates a recommended action: restart workers, revoke hung
task, or scale workers.

### Frontend (System Status page)

- Sidebar nav item: "System Status" with heartbeat icon.
- **Alerts banner** — green "all healthy" or amber with alert list.
- **Worker cards** — per-worker health: online status, active tasks,
  pool size.
- **Queue depth bars** — visual bar chart, color-coded (green < 10,
  blue < 50, amber > 50).
- **Redis metrics** — memory usage, peak, connected clients.
- **Active tasks table** — all projects, sortable, with duration
  highlighting (amber for > 30m, bold amber for > 1h) and inline
  Revoke button.
- **Error feed** — last 24h errors with tag-grouped frequency badges,
  expandable table.
- **Recommended actions** — actionable cards with execute buttons for
  revoke-task, manual instructions for restart/scale.
- **Auto-refresh** every 30 seconds while on the page.

## [2.7.0] — 2026-04-16 — Plugin System Phase C: UI Tab Injection

Plugins can now contribute tabs to stage pages in the frontend.

### `ui_tab` extension point (Python only)

Declare a `ui_tab` in the manifest with `metadata.context` (list of page
identifiers) and `metadata.api_endpoint` (key of a sibling `api_endpoint`
extension point that serves the tab content). The frontend discovers
registered tabs via `GET /cerulean-plugins/ui-tabs` and dynamically
injects tab buttons into matching stage pages after each navigation.

Context values:
- `"project"` — every project page (all stages)
- `"stage:<n>"` — specific stage (e.g. `"stage:3"`, `"stage:7"`)
- `"admin"` — admin/platform pages

Content delivery: plugin API endpoint returns HTML (rendered directly)
or JSON with an `html` key. Fetch failures degrade gracefully with an
error message in the tab panel.

### Backend
- `manifest.py`: `EXT_UI_TAB = "ui_tab"` added to SUPPORTED_EXTENSIONS;
  Python-only (rejected for subprocess manifests).
- `extension_points.py`: `PluginUITab` dataclass + `_UI_TAB_REGISTRY` +
  register/get/all/unregister helpers; `clear_all()` resets all 6.
- `context.py`: `ctx.register_ui_tab(key, context, api_endpoint, icon)`.
- `cerulean_plugins.py`: `GET /cerulean-plugins/ui-tabs` discovery endpoint.

### Frontend
- `_loadPluginTabs()` fetches tabs at login alongside `loadUserPrefs()`.
- `_injectPluginTabs(view)` runs after every `navigate()` call (both
  script blocks) — scans for `.tab-bar` elements and appends buttons.
- `_pluginTabLoad()` fetches content from the plugin's API endpoint.
- Plugin tab buttons show a ◈ prefix + the plugin's configured icon.

### Tests
7 new tests (4 manifest + 3 registry). Full suite now at **370 passing**.

## [2.6.0] — 2026-04-16 — Plugin System Phase B: Tasks, Endpoints, DB Store

Three new extension-point types for the Cerulean Plugin System:

### `celery_task`
Plugins can register Celery task callables (Python or subprocess).
Subprocess tasks are wrapped in a thunk that runs the entry binary with
config.json input and reads a JSON result from output.json.

### `api_endpoint` (Python only)
Plugins provide a FastAPI `APIRouter`; mounted at
`/api/v1/plugins/<slug>/<key>` during the web lifespan startup.

### `db_store` (Python only)
Plugins provide a SQLAlchemy declarative model; tables auto-created via
`create_all(checkfirst=True)`. Enforced `cpz_<slug>_` table name prefix.

### Validation
- `api_endpoint` and `db_store` require `runtime: python` — rejected at
  manifest parse time for subprocess plugins.
- `db_store` table name prefix enforced in `PluginContext.register_db_store`.

### Infrastructure
- `extension_points.py`: 3 new registries + dataclasses + CRUD helpers,
  `clear_all()` resets all 5 registries.
- `manifest.py`: `ExtensionPoint.type` Literal expanded, `metadata` dict
  added (queue hints, etc.).
- `runtime_subprocess.py`: `_make_celery_task_thunk` + `run_subprocess_task`.
- `main.py`: `_mount_plugin_endpoints` + `_create_plugin_db_tables` in
  lifespan, after plugin load.
- `context.py`: 3 new registration methods.
- `__init__.py`: all new types exported.

### Tests
16 new tests across manifest (9) and registry (7). Full suite 363 passing.

### Docs
`docs/PLUGIN-AUTHORING.md` updated with Phase B section — manifest
examples, Python setup() patterns, and DB access patterns for all
three new types.

## [2.5.0] — 2026-04-16 — AI Phase 6: Fuzzy Patron Deduplication

Sixth and final near-term AI feature from `cerulean_ai_spec.md`. Patron
records from legacy ILS exports often contain near-duplicates — same
person entered twice with a slightly different name, reformatted DOB, or
abbreviated address. This phase pre-filters on blocking keys + edit
distance then hands Claude the hard calls.

### Feature flag

`ai.fuzzy_patron_dedup` — default off. When enabled, a "Fuzzy Dedup"
tab appears on Step 8 → Patrons.

### Backend

- **`POST /projects/{id}/patrons/fuzzy-dedup`** — gated by
  `require_preference("ai.fuzzy_patron_dedup")`. 409 when no parsed CSV.
- **`GET .../fuzzy-dedup/status?task_id=…`** — stateless polling.
- **`GET .../fuzzy-dedup/clusters`** — list clusters for review,
  ordered by confidence descending. Optional `?resolved=` filter.
- **`PATCH .../fuzzy-dedup/clusters/{id}`** — resolve or dismiss.
- **`patron_fuzzy_dedup_task`** (Celery, `patrons` queue):
  * Reads combined patron CSV.
  * Blocks on `(surname[0], birth_year)`.
  * Generates pairs within each block where Levenshtein on surname ≤ 3.
  * Batches 30 pairs per Claude call, capped at 5,000 total pairs.
  * Only stores pairs Claude flags as `is_duplicate=true` with
    `confidence ≥ 50`.
  * Re-running clears and rebuilds from scratch.
- **Migration `y5s0t1u2v3w4_add_patron_dedup_clusters.py`** — new
  `patron_dedup_clusters` table, separate from the MARC-oriented
  `dedup_clusters`.

### Frontend (Step 8 → Patrons → Fuzzy Dedup tab)

- Card-per-cluster layout with side-by-side field comparison table.
- Differing values highlighted in amber for quick triage.
- Confidence badge (High/Med/Low) + AI reasoning text.
- "Keep Primary" and "Not a Duplicate" buttons per cluster.
- Live progress during scoring — shows batch count + clusters found.

### Tests

Added `tests/tasks/test_patron_fuzzy_dedup.py` — 48 tests covering
Levenshtein, blocking keys, field extraction, prompt shape, Claude
response parsing, confidence clamping, system-prompt invariants, and
config sanity. Full suite now at **347 passing**.

## [2.4.0] — 2026-04-16 — AI Phase 5: Code Reconciliation

Fifth of six AI-assisted features from `cerulean_ai_spec.md`. Matching
source item codes (branch, location, item type, collection, status) to
Koha authorized values is one of the most time-consuming manual tasks
in every migration; this hands Claude the first pass.

### Feature flag

`ai.code_reconciliation` — registered in `cerulean/core/features.py`,
default off, per-user opt-in. Shown on the Rules tab of Step 7 as
`✦ AI Suggest Matches` when enabled AND the project has a Koha URL +
token configured.

### Backend

- **`POST /projects/{id}/reconcile/ai-suggest`** — gated by
  `require_preference("ai.code_reconciliation")`. Returns 409 when Koha
  config is missing or when no scan rows exist.
- **`GET /projects/{id}/reconcile/ai-suggest/status?task_id=…`** —
  stateless polling. Returns PROGRESS meta with
  `{categories_done, categories_total, current_category}` for UI
  progress rendering.
- **`ai_reconciliation_suggest_task`** (Celery, `reconcile` queue) —
  walks every vocab category that has scan rows, fetches the matching
  Koha authorized values list via REST, asks Claude one category at a
  time, and persists the response as inactive `ReconciliationRule`
  rows. Re-running is idempotent:
  * Active (engineer-approved) rules are never touched.
  * Existing inactive ai_suggested rules refresh in place.
  * Null-match ("no Koha value found") suggestions still seed a row so
    the AI's reasoning surfaces in the UI instead of silently vanishing.
- **Migration `x4r9s0t1u2v3_add_recrule_ai_fields.py`** adds
  `ai_suggested BOOLEAN`, `ai_confidence FLOAT`, `ai_reasoning TEXT` to
  `reconciliation_rules`.

### Frontend (Step 7 → Rules tab)

- `✦ AI Suggest Matches` button, hidden unless the feature flag is on.
  Disabled with a tooltip when Koha is unconfigured.
- Rules table gets a Confidence column and an "AI" badge on
  ai_suggested rows.
- Low-confidence rows get an amber row tint so the engineer visually
  triages them before bulk-approving.
- Hover tooltip on the confidence badge shows Claude's reasoning.
- Inactive ai_suggested rows get an inline `Approve` button; approval
  flips `active=true` via the existing PATCH endpoint and drops the row
  into the pipeline — no separate approval flow.

### Tests

Added `tests/tasks/test_ai_code_reconciliation.py` — 48 unit tests
covering the Koha endpoint mapper, Koha-value summarisation across the
three API shapes (item_types, libraries, authorised_values), prompt
shape, JSON parsing (good + malformed + hallucinated types), confidence
clamping, and system-prompt invariants. Full suite now at **299
passing**.

## [2.3.0] — 2026-04-16 — Patron fast path: skip Column Mapping + SQL export

Two-part quality-of-life improvement for Step 8 (Patrons) aimed at
migrations where the source file is already Koha-shaped or where the
operator just wants the INSERT statements for their controlled values.

### "File uses Koha headers" upload checkbox

New checkbox on the patron upload dialog (and the "Select from Stage 1
uploads" path). When ticked:

- The parse task auto-creates **and auto-approves** `PatronColumnMap`
  rows for every source column whose name is a case-insensitive exact
  match for a standard Koha borrower header (`cardnumber`, `surname`,
  `firstname`, `email`, `categorycode`, `branchcode`, `dateofbirth`, …
  the full list in `KOHA_BORROWER_HEADERS`).
- Controlled-list headers (`categorycode`, `branchcode`, `title`,
  `lost`) also get `is_controlled_list=True` so the **Scan Controlled
  Values** button on the Value Reconciliation tab unblocks immediately.
- Unmatched columns still appear on Column Mapping for manual review.
- Idempotent: re-uploading a file with the checkbox ticked never
  duplicates existing column maps.

### `GET /projects/{id}/patrons/scan-sql`

New endpoint that renders `INSERT IGNORE` statements for the distinct
controlled values found by the most recent scan. Output is plain text
suitable for `mysql koha_db < file.sql` on the Koha host. Coverage:

| Header | Target table | Columns written |
|--------|-------------|-----------------|
| `categorycode` | `categories` | categorycode, description, category_type, enrolmentperiod |
| `branchcode` | `branches` | branchcode, branchname |
| `title` | `authorised_values` (category `BOR_TITLES`) | category, authorised_value, lib |
| `lost` | `authorised_values` (category `LOST`) | category, authorised_value, lib |

Each row carries a `-- N patron(s)` inline comment so the operator can
see volumes at a glance. Single quotes and backslashes in source values
are escaped so the output is safe to pipe into `mysql` straight away.

### Frontend

- **Download SQL** button on each Value Reconciliation category panel —
  single-header export (`...-patron-categorycode-values.sql`).
- **Download SQL (all controlled values)** button at the top of the
  panel — combined export (`...-patron-controlled-values.sql`).

### Tests

- 28 new unit tests (`tests/tasks/test_patron_fastpath.py`): the header
  matcher (every known header self-matches, case-insensitivity,
  whitespace handling, unknown headers rejected) plus the SQL generator
  (shape per controlled header, single-quote escaping, backslash
  escaping, empty-value skipping, unknown-header silent ignore). 251
  total, up from 223.

### Backward compatibility

Untouched when the checkbox is unticked — existing Column Mapping +
approval flow works exactly as before. Nothing removed, no schema
changes.

---

## [2.2.0] — 2026-04-16 — Cerulean Plugin System (Phase A)

First-party plugin platform that lets any migration specialist extend
Cerulean with custom transforms and quality checks, in Python or any
language via a subprocess contract. Distinct from the existing Koha
`.kpz` plugin manager — these are `.cpz` archives that extend Cerulean
itself, not plugins pushed AT Koha.

### What shipped

- **Manifest spec** (`manifest.yaml`, version 1) — slug validation,
  runtime-specific rules, permission whitelist, placeholder checking
  for subprocess args. See [docs/PLUGIN-AUTHORING.md](docs/PLUGIN-AUTHORING.md).
- **Two runtimes**:
  - `python` — plugin imported in-process, `setup(ctx)` registers hooks.
  - `subprocess` — any-language executable invoked per call with
    `{input_path} / {output_path} / {config_json}` placeholders; stderr
    captured, 5-minute default timeout, minimal env (no host secrets).
- **Two extension points**: `transform` (surfaces in Step 5 dropdown
  under the **Plugin** category) and `quality_check` (runs in Step 3
  scanner, issues land in the existing `QualityScanResult` table).
- **Installer API** under `/api/v1/cerulean-plugins`:
  upload / list / enable / disable / uninstall. Archives stay in
  `available/` for rollback.
- **Sidebar page** "Cerulean Plugins" with drag-and-drop upload,
  per-row enable / disable / uninstall, error display, and a
  "restart required" banner after any state change.
- **Reference plugins** under `examples/plugins/`: `shout-python`
  (Python) and `shout-perl` (subprocess).
- **Authoring guide** at `docs/PLUGIN-AUTHORING.md`, served from
  `/help/plugin-authoring`.

### Schema (migration `w3q8r9s0t1u2`)

- New `cerulean_plugins` table: `slug` (unique), `name`, `version`,
  `author`, `description`, `runtime`, full parsed `manifest` (JSONB),
  `install_path`, `archive_filename`, `status` (enabled / disabled /
  error), `error_message`, `installed_by`, `installed_at`, `updated_at`.

### Tests

- **52 new tests** (223 total, up from 171):
  - `tests/core/test_plugin_manifest.py` — 37 cases covering manifest
    validation, malformed YAML, runtime-specific rules, extension-point
    duplicates, unknown fields.
  - `tests/core/test_plugin_registry.py` — registry register / lookup /
    re-register / unregister / isolation between plugins.
  - `tests/core/test_plugin_loader.py` — end-to-end python plugin load
    on disk, broken-plugin error isolation, rollback of partial
    registrations, error-path coverage.
  - `tests/core/test_plugin_runtime_subprocess.py` — real shell script
    plugin (happy path, non-zero exit, timeout, missing executable,
    non-executable entry, env-leak regression).

### Restart-required model

Installing / upgrading / toggling a plugin requires
`docker compose restart web worker worker-push` — same model Koha uses
for its `.kpz` plugins. No hot-reload magic; Celery workers and the web
process each run `load_all_enabled_plugins()` on startup.

### Security model

- Any authenticated user can install (same trust boundary as existing
  Koha `.kpz` uploads).
- Python plugins are trusted in-process code.
- Subprocess plugins run in a scratch directory with a minimal env —
  host secrets like `ANTHROPIC_API_KEY` don't leak.
- Every state change writes an AuditEvent.

---

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
