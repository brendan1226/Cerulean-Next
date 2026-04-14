# Changelog

All notable changes to Cerulean Next are documented here.

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
