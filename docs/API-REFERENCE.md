# API Reference

**Cerulean Next REST API**
Base URL: `/api/v1`

Interactive documentation: `/api/docs` (Swagger UI) or `/api/redoc`

All endpoints require JWT authentication via `Authorization: Bearer <token>` header unless noted as public.

---

## Authentication

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/auth/google/login` | Public | Redirect to Google OAuth consent screen |
| GET | `/auth/google/callback` | Public | OAuth callback — exchanges code for JWT |
| GET | `/auth/me` | Required | Return current user info |
| GET | `/auth/users` | Required | List all registered users |
| GET | `/auth/server-logs` | Required | Read server logs (filter: auth/error) |

---

## User Preferences (AI Feature Flags)

Per-user toggles backed by a single `user_preferences` table. The
`registry` in the GET response is the source of truth for which keys
exist and their defaults — the frontend auto-renders the My Preferences
page from it. All AI features default **OFF** for every user.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/users/me/preferences` | Return `{registry, values}` — registry metadata + caller's current values |
| PATCH | `/users/me/preferences` | Set one preference: body `{key, value}`. Unknown keys → 404; wrong type → 400 |
| POST | `/users/me/preferences/reset` | Delete every stored preference for the caller |
| GET | `/users/{user_id}/preferences` | Read-only view of another user's values |

**Feature keys (all default `false`):**
`ai.data_health_report`, `ai.value_aware_mapping`, `ai.transform_rule_gen`,
`ai.code_reconciliation`, `ai.record_enrichment`. Patron data is PII and
is never sent to an AI/LLM, so no AI feature targets the patron pipeline.

AI endpoints return **403** with `{error: "PREFERENCE_DISABLED",
feature_key: "..."}` when the caller's flag for that feature is off.

---

## Projects

| Method | Path | Description |
|--------|------|-------------|
| GET | `/projects` | List projects (filtered by owner/visibility) |
| POST | `/projects` | Create project |
| GET | `/projects/{id}` | Get project detail |
| PATCH | `/projects/{id}` | Update project settings |

---

## Files (Step 1 — Ingest)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/files` | Upload MARC/CSV file |
| GET | `/projects/{id}/files` | List files for project |
| DELETE | `/projects/{id}/files/{fid}` | Delete a file |
| GET | `/projects/{id}/files/{fid}/records/{n}` | Fetch record N (0-indexed) |
| GET | `/projects/{id}/files/{fid}/tags` | Tag frequency histogram |
| GET | `/projects/{id}/files/{fid}/export-mrk` | Export file as MRK format |
| POST | `/projects/{id}/files/{fid}/mark-ready` | Mark file as push-ready (skip stages 3-6) |
| POST | `/projects/{id}/files/{fid}/recategorize` | Recategorize file (marc ↔ items) |

### AI Data Health Report (gated: `ai.data_health_report`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/projects/{id}/files/{fid}/health-report` | Current report + status (always available read-only) |
| POST | `/projects/{id}/files/{fid}/health-report/run` | Trigger a fresh analysis. Requires the feature flag on |

Response shape on GET:
```json
{
  "file_id": "…",
  "filename": "…",
  "status": "running" | "ready" | "error" | null,
  "report": { "summary": "…", "ils_origin": {...}, "findings": [...], ... },
  "error": "…" | null,
  "generated_at": "ISO8601" | null
}
```

The health task also runs automatically after every MARC ingest when the
uploading user has `ai.data_health_report` enabled.

---

## CSV to MARC (Step 1)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/csv-to-marc/preview` | Upload CSV and preview columns |
| POST | `/projects/{id}/csv-to-marc/convert` | Convert CSV to MARC with column→tag mapping |

---

## Quality (Step 3)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/quality/scan` | Dispatch quality scan |
| GET | `/projects/{id}/quality/scan/status` | Poll scan task status |
| GET | `/projects/{id}/quality/summary` | Issue counts by category |
| GET | `/projects/{id}/quality/issues` | Paginated issue list |
| GET | `/projects/{id}/quality/issues/{iid}` | Single issue detail |
| POST | `/projects/{id}/quality/fix/{iid}` | Manually fix one issue |
| POST | `/projects/{id}/quality/bulk-fix` | Bulk auto-fix a category |
| GET | `/projects/{id}/quality/bulk-fix/preview` | Preview bulk fix changes |
| POST | `/projects/{id}/quality/ignore/{iid}` | Ignore one issue |
| POST | `/projects/{id}/quality/ignore-category` | Ignore all in a category |
| POST | `/projects/{id}/quality/ignore-record/{n}` | Ignore all issues for a record |
| GET | `/projects/{id}/quality/record/{n}` | Get record with issue annotations |
| POST | `/projects/{id}/quality/record/{n}/edit` | Edit a field in a record |
| POST | `/projects/{id}/quality/approve` | Approve quality pass (gate) |
| POST | `/projects/{id}/quality/clear` | Clear all scan results |
| POST | `/projects/{id}/quality/cluster` | Build clusters by field value |

---

## Batch Edit (Step 3)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/batch-edit/find-replace` | Find and replace text |
| POST | `/projects/{id}/batch-edit/add-field` | Add a field to all records |
| POST | `/projects/{id}/batch-edit/delete-field` | Delete a field from all records |
| POST | `/projects/{id}/batch-edit/regex` | Regex substitution |
| POST | `/projects/{id}/batch-edit/call-numbers` | Generate call numbers |

---

## RDA Helper (Step 3)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/rda/scan` | Scan for missing 336/337/338 fields |
| POST | `/projects/{id}/rda/apply` | Auto-generate RDA fields |

---

## Versions (Step 4)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/versions/create` | Create version snapshot |
| GET | `/projects/{id}/versions` | List versions (filter by data_type) |
| GET | `/projects/{id}/versions/{vid}` | Version detail |
| PATCH | `/projects/{id}/versions/{vid}` | Update label |
| DELETE | `/projects/{id}/versions/{vid}` | Delete version |
| POST | `/projects/{id}/versions/diff` | Dispatch diff computation |
| GET | `/projects/{id}/versions/diff/status` | Poll diff task |

---

## Field Mapping (Step 5)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/projects/{id}/maps` | List field maps |
| POST | `/projects/{id}/maps` | Create map manually |
| PATCH | `/projects/{id}/maps/{mid}` | Edit map |
| DELETE | `/projects/{id}/maps/{mid}` | Delete map |
| POST | `/projects/{id}/maps/ai-suggest` | Trigger AI analysis (value-aware when `ai.value_aware_mapping` is on) |
| POST | `/projects/{id}/maps/{mid}/approve` | Approve single map |
| POST | `/projects/{id}/maps/approve-all` | Batch approve (min_confidence defaults to 0.60 → excludes Low-confidence AI rows) |
| POST | `/projects/{id}/maps/load-template` | Load template into project |

### AI Transform Rule Generation (gated: `ai.transform_rule_gen`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/maps/ai-transform/generate` | Describe a transform in plain English → sandboxed Python expression + reasoning + 10-row before/after preview |
| POST | `/projects/{id}/maps/ai-transform/preview` | Re-run preview for an edited expression (no AI call) |

Both endpoints pull sample values directly from the project's indexed
MARC files; no sample values are accepted from the client. The expression
is executed against the production `fn` sandbox so what the preview shows
is exactly what the transform pipeline will produce.

Generate request body:
```json
{ "description": "Strip the trailing period and comma",
  "source_tag": "245", "source_sub": "a" }
```

Generate response body:
```json
{
  "expression": "re.sub(r'[.,]+$', '', value.strip())",
  "reasoning": "Strips any run of trailing periods or commas after trimming whitespace.",
  "preview": [
    { "before": "Science, technology, and society.",
      "after":  "Science, technology, and society", "error": null }
  ]
}
```

Approved AI-generated rules are stored with the user's original
description in `field_maps.ai_prompt` for traceability.

---

## Items (Step 5)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/projects/{id}/items/maps` | List item column maps |
| POST | `/projects/{id}/items/maps` | Create item column map |
| PATCH | `/projects/{id}/items/maps/{mid}` | Edit item map |
| DELETE | `/projects/{id}/items/maps/{mid}` | Delete item map |
| POST | `/projects/{id}/items/maps/{mid}/approve` | Approve item map |
| POST | `/projects/{id}/items/maps/approve-all` | Batch approve |
| POST | `/projects/{id}/items/maps/ai-suggest` | AI suggest item mappings |
| GET | `/projects/{id}/items/preview` | Preview items data |
| POST | `/projects/{id}/items/rename-column` | Rename a CSV column |
| GET/PUT | `/projects/{id}/items/match-config` | Get/set match key config |

---

## Transform (Step 6)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/transform/start` | Dispatch transform task |
| POST | `/projects/{id}/transform/merge` | Dispatch merge task |
| POST | `/projects/{id}/build` | Combined build (transform + merge) |
| GET | `/projects/{id}/transform/status` | Poll task status |
| GET | `/projects/{id}/transform/manifest` | List manifests |
| GET | `/projects/{id}/transform/preview` | Preview transformed record |
| POST | `/projects/{id}/transform/clear` | Clear output files |

---

## Reconciliation (Step 7)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/reconcile/confirm-source` | Set source file |
| POST | `/projects/{id}/reconcile/scan` | Dispatch scan task |
| GET | `/projects/{id}/reconcile/scan/status` | Poll scan status |
| GET | `/projects/{id}/reconcile/values` | Query values by vocab category |
| GET | `/projects/{id}/reconcile/koha-list` | Fetch Koha controlled vocab |
| GET | `/projects/{id}/reconcile/rules` | List rules |
| POST | `/projects/{id}/reconcile/rules` | Create rule |
| PATCH | `/projects/{id}/reconcile/rules/{rid}` | Update rule |
| DELETE | `/projects/{id}/reconcile/rules/{rid}` | Delete rule |
| GET | `/projects/{id}/reconcile/validate` | Validate rules completeness |
| POST | `/projects/{id}/reconcile/apply` | Apply rules to MARC data |
| GET | `/projects/{id}/reconcile/report` | Get apply report |
| GET | `/projects/{id}/reconcile/files` | List controlled value CSVs |
| GET | `/projects/{id}/reconcile/files/{name}` | Preview a controlled value file |
| PUT | `/projects/{id}/reconcile/descriptions/{cat}` | Save value descriptions |
| GET | `/projects/{id}/reconcile/sample` | Sample records for a value |
| POST | `/projects/{id}/reconcile/clear` | Clear scan results |

---

## Patrons (Step 8)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/patrons/upload` | Upload patron file |
| POST | `/projects/{id}/patrons/select-file` | Select Stage 1 file as patron source |
| GET | `/projects/{id}/patrons/files` | List patron files |
| DELETE | `/projects/{id}/patrons/files/{fid}` | Delete patron file |
| DELETE | `/projects/{id}/patrons/file` | Delete all patron files |
| POST | `/projects/{id}/patrons/reparse` | Re-parse all patron files |
| GET | `/projects/{id}/patrons/available-files` | List Stage 1 files available for patron use |
| GET | `/projects/{id}/patrons/preview` | Preview combined parsed data |
| GET | `/projects/{id}/patrons/maps` | List column maps |
| POST | `/projects/{id}/patrons/maps` | Create column map |
| PATCH | `/projects/{id}/patrons/maps/{mid}` | Edit column map |
| DELETE | `/projects/{id}/patrons/maps/{mid}` | Delete column map |
| POST | `/projects/{id}/patrons/maps/{mid}/approve` | Approve column map |
| POST | `/projects/{id}/patrons/maps/approve-all` | Batch approve |
| POST | `/projects/{id}/patrons/scan` | Scan controlled values |
| GET | `/projects/{id}/patrons/scan/status` | Poll scan status |
| GET | `/projects/{id}/patrons/values` | Query scanned values |
| GET | `/projects/{id}/patrons/rules` | List value rules |
| DELETE | `/projects/{id}/patrons/rules/{rid}` | Delete value rule |
| GET | `/projects/{id}/patrons/validate` | Validate rules |
| POST | `/projects/{id}/patrons/apply` | Apply transformations |
| GET | `/projects/{id}/patrons/report` | Get apply report |
| GET | `/projects/{id}/patrons/output-files` | List output files |
| GET | `/projects/{id}/patrons/output-files/{name}` | Preview output file |
| GET | `/projects/{id}/patrons/output-files/{name}/download` | Download output file |
| POST | `/projects/{id}/patrons/clear` | Clear output data |

---

## Push / Load (Step 10)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/push/preflight` | Dispatch preflight check |
| POST | `/projects/{id}/push/start` | Dispatch push tasks (bibs/patrons/holds/reindex) |
| GET | `/projects/{id}/push/status` | Poll Celery task status |
| GET | `/projects/{id}/push/manifests` | List push manifests |
| GET | `/projects/{id}/push/manifests/{mid}` | Get manifest detail |
| GET | `/projects/{id}/push/koha-refs` | Fetch Koha reference data |
| GET | `/projects/{id}/push/needed-refs` | Scan project for needed ref values |
| POST | `/projects/{id}/push/libraries` | Push missing libraries to Koha |
| POST | `/projects/{id}/push/patron-categories` | Push missing patron categories |
| POST | `/projects/{id}/push/item-types` | Push missing item types |
| POST | `/projects/{id}/push/authorised-values` | Push missing authorised values |
| POST | `/projects/{id}/push/scan-marc` | Scan MARC file for controlled values |
| GET | `/projects/{id}/push/files` | List push-ready files |
| GET | `/projects/{id}/push/files/download` | Download a push-ready file |
| GET | `/projects/{id}/push/files/preview` | Preview a record from push file |
| GET | `/projects/{id}/push/migration-mode` | Check daemon status |
| POST | `/projects/{id}/push/migration-mode/enable` | Enable migration mode |
| POST | `/projects/{id}/push/migration-mode/disable` | Disable migration mode |
| GET | `/projects/{id}/push/utf8-scan` | Scan for UTF-8 issues |
| POST | `/projects/{id}/push/utf8-repair` | Repair UTF-8 issues |
| GET | `/projects/{id}/push/koha-job/{jid}` | Direct Koha job polling |
| POST | `/projects/{id}/push/toolkit-reindex` | Dispatch toolkit reindex |
| POST | `/projects/{id}/push/turboindex` | Dispatch TurboIndex reindex |
| GET | `/projects/{id}/push/cerulean/refdata` | Fetch via Cerulean plugin |
| POST | `/projects/{id}/push/cerulean/truncate` | Truncate Koha data |
| GET | `/projects/{id}/push/cerulean/db-stats` | Koha DB statistics |
| GET | `/projects/{id}/push/cerulean/es-status` | Elasticsearch status |

---

## Aspen Discovery (Step 12)

| Method | Path | Description |
|--------|------|-------------|
| PATCH | `/projects/{id}/aspen/config` | Set Aspen URL |
| GET | `/projects/{id}/aspen/counts` | Get Koha + Aspen record counts |
| POST | `/projects/{id}/aspen/turbo-migration` | Start Turbo Migration |
| POST | `/projects/{id}/aspen/turbo-reindex` | Start Turbo Reindex |
| GET | `/projects/{id}/aspen/status` | Poll background process |
| POST | `/projects/{id}/aspen/stop-indexers` | Stop Aspen indexers |
| POST | `/projects/{id}/aspen/start-indexers` | Start Aspen indexers |

---

## Evergreen ILS (Step 13)

| Method | Path | Description |
|--------|------|-------------|
| PATCH | `/projects/{id}/evergreen/config` | Set Evergreen DB connection |
| GET | `/projects/{id}/evergreen/test` | Test DB connection |
| GET | `/projects/{id}/evergreen/counts` | Get record counts |
| POST | `/projects/{id}/evergreen/push-bibs` | Push MARC records |
| POST | `/projects/{id}/evergreen/pingest` | Run parallel ingest |
| POST | `/projects/{id}/evergreen/remap` | Metarecord remap |
| POST | `/projects/{id}/evergreen/restart-services` | Restart OpenSRF stack |

---

## MARC Tools

### SQL Explorer

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/marc-sql/query` | Run SQL-like query |
| GET | `/projects/{id}/marc-sql/schema` | Get available tags/subfields |

### Export & Extract

| Method | Path | Description |
|--------|------|-------------|
| POST | `/projects/{id}/marc-export/to-spreadsheet` | Export fields as CSV/TSV |
| POST | `/projects/{id}/marc-export/extract-records` | Extract records by criteria |
| POST | `/projects/{id}/marc-export/to-json` | Export as JSON |
| POST | `/projects/{id}/marc-export/from-json` | Import from JSON |

### File Manager

| Method | Path | Description |
|--------|------|-------------|
| GET | `/projects/{id}/marc-files/list` | List all .mrc files |
| POST | `/projects/{id}/marc-files/split` | Split file by count or field |
| POST | `/projects/{id}/marc-files/join` | Merge files with optional dedup |

### Macros

| Method | Path | Description |
|--------|------|-------------|
| GET | `/macros` | List macros |
| POST | `/macros` | Create macro |
| GET | `/macros/{id}` | Get macro detail |
| PATCH | `/macros/{id}` | Update macro |
| DELETE | `/macros/{id}` | Delete macro |
| POST | `/projects/{id}/macros/{id}/run` | Run macro on project |

---

## Templates

| Method | Path | Description |
|--------|------|-------------|
| GET | `/templates` | List templates |
| POST | `/templates` | Save current maps as template |
| POST | `/templates/import-csv` | Import from CSV file |
| POST | `/templates/import-google-sheet` | Import from Google Sheets URL |
| POST | `/templates/seed-sample` | Create sample template |
| GET | `/templates/{id}` | Template detail |
| GET | `/templates/{id}/csv` | Download as CSV |
| PATCH | `/templates/{id}` | Edit template |
| DELETE | `/templates/{id}` | Delete template |
| POST | `/templates/{id}/promote` | Promote to global scope |

---

## Plugins

| Method | Path | Description |
|--------|------|-------------|
| GET | `/plugins` | List uploaded plugins |
| POST | `/plugins/upload` | Upload .kpz file |
| GET | `/plugins/{id}/download` | Download plugin file |
| DELETE | `/plugins/{id}` | Delete plugin |
| POST | `/projects/{id}/plugins/{id}/install` | Auto-install to Koha |

---

## Suggestions

| Method | Path | Description |
|--------|------|-------------|
| GET | `/suggestions` | List (filter by type/status) |
| POST | `/suggestions` | Submit suggestion |
| GET | `/suggestions/{id}` | Get with comments |
| PATCH | `/suggestions/{id}` | Edit / update status |
| DELETE | `/suggestions/{id}` | Delete (owner only) |
| POST | `/suggestions/{id}/vote` | Toggle upvote |
| POST | `/suggestions/{id}/comments` | Add comment |
| GET | `/suggestions/{id}/comments` | List comments |
| DELETE | `/suggestions/{id}/comments/{cid}` | Delete comment |

---

## System Settings

| Method | Path | Description |
|--------|------|-------------|
| GET | `/settings` | List settings (secrets masked) |
| PUT | `/settings` | Update settings |

---

## OAI-PMH (Public)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/oai/{project_id}?verb=Identify` | Public | Repository info |
| GET | `/oai/{project_id}?verb=ListMetadataFormats` | Public | Available formats |
| GET | `/oai/{project_id}?verb=ListRecords&metadataPrefix=marcxml` | Public | Harvest records |
| GET | `/oai/{project_id}?verb=GetRecord&identifier=...&metadataPrefix=marcxml` | Public | Get single record |

Only shared projects are harvestable.

---

## Utility

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/api/health` | Public | Health check |
| GET | `/reference/koha-fields` | Required | Koha MARC field reference |
| GET | `/reference/transform-presets` | Required | Available transform presets |
| GET | `/projects/{id}/tasks/active` | Required | List running tasks |
| POST | `/projects/{id}/tasks/{tid}/pause` | Required | Pause task |
| POST | `/projects/{id}/tasks/{tid}/resume` | Required | Resume task |
| POST | `/projects/{id}/tasks/{tid}/cancel` | Required | Cancel task |
| GET | `/projects/{id}/log` | Required | Audit log (paginated) |
| GET | `/projects/{id}/log/stream` | Required | SSE real-time log stream |
| GET | `/projects/{id}/log/export` | Required | Export log as CSV/TXT |
| GET | `/projects/{id}/search` | Required | Full-text search (requires ES) |
