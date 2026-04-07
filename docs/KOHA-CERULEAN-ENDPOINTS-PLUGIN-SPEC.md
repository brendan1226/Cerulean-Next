# Koha Cerulean-Endpoints Plugin — Full API Spec

**Plugin name:** `Koha::Plugin::BWS::CeruleanEndpoints`
**Namespace:** `cerulean`
**Base path:** `/api/v1/contrib/cerulean`

This plugin provides REST endpoints for everything Cerulean needs during
a library migration that Koha's core API doesn't offer. Designed to
eliminate all SQL-direct-insert hacks, Docker exec commands, and
workarounds currently in Cerulean.

---

## 1. Reference Data Management

### 1.1 GET /refdata — List All Reference Data

```
GET /api/v1/contrib/cerulean/refdata
```

Returns the full list of every reference data type with codes + descriptions.

**Response (200):**
```json
{
  "libraries": [
    { "branchcode": "MAIN", "branchname": "Main Library", "branchaddress1": "..." }
  ],
  "item_types": [
    { "itemtype": "BOOK", "description": "Book", "notforloan": 0 }
  ],
  "patron_categories": [
    { "categorycode": "ADULT", "description": "Adult Patron", "category_type": "A", "enrolmentperiod": 99 }
  ],
  "authorised_values": {
    "LOC": [{ "authorised_value": "ADULT", "lib": "Adult Section" }],
    "CCODE": [{ "authorised_value": "FIC", "lib": "Fiction" }],
    "NOT_LOAN": [{ "authorised_value": "1", "lib": "Not for loan" }],
    "LOST": [{ "authorised_value": "1", "lib": "Lost" }],
    "DAMAGED": [{ "authorised_value": "1", "lib": "Damaged" }],
    "WITHDRAWN": [{ "authorised_value": "1", "lib": "Withdrawn" }]
  },
  "frameworks": [
    { "frameworkcode": "", "frameworktext": "Default" },
    { "frameworkcode": "FA", "frameworktext": "Fast add" }
  ],
  "matching_rules": [
    { "matcher_id": 1, "code": "ISBN", "description": "ISBN matcher" }
  ]
}
```

**Why Cerulean needs this:** Stage 7 Setup tab compares MARC file values
against what Koha has. Currently requires 6+ separate GET calls to
different core API endpoints.

### 1.2 POST /refdata — Bulk Upsert Reference Data

```
POST /api/v1/contrib/cerulean/refdata
Content-Type: application/json
```

**Request:**
```json
{
  "libraries": [
    { "branchcode": "NORTH", "branchname": "North Branch" }
  ],
  "item_types": [
    { "itemtype": "DVD", "description": "DVD" }
  ],
  "patron_categories": [
    { "categorycode": "STUDENT", "description": "Student", "enrolmentperiod": 12, "category_type": "C" }
  ],
  "authorised_values": {
    "LOC": [
      { "authorised_value": "ADULT", "lib": "Adult Section" },
      { "authorised_value": "CHILD", "lib": "Children's Section" }
    ],
    "CCODE": [
      { "authorised_value": "FIC", "lib": "Fiction" }
    ]
  }
}
```

All sections optional — send only what you need.

**Response (200):**
```json
{
  "libraries": { "created": 1, "existed": 0, "errors": [] },
  "item_types": { "created": 1, "existed": 0, "errors": [] },
  "patron_categories": { "created": 1, "existed": 0, "errors": [] },
  "authorised_values": {
    "LOC": { "created": 2, "existed": 0, "errors": [] },
    "CCODE": { "created": 1, "existed": 0, "errors": [] }
  }
}
```

**Behavior:** INSERT IGNORE — existing values skipped, not updated.

---

## 2. Database Management

### 2.1 POST /db/truncate — Clear Migration Data

```
POST /api/v1/contrib/cerulean/db/truncate
Content-Type: application/json
```

**Request:**
```json
{
  "tables": ["biblio", "items"],
  "confirm": "YES_DELETE_ALL_DATA"
}
```

Supported tables:
- `biblio` (also clears `biblioitems`, `biblio_metadata`)
- `items`
- `import_batches` (also clears `import_records`, `import_items`, `import_biblios`)

**Response (200):**
```json
{
  "truncated": ["biblio", "biblioitems", "biblio_metadata", "items"],
  "messages": ["Truncated 4 tables", "Reset AUTO_INCREMENT"]
}
```

**Why:** Before re-importing, Cerulean needs to clear old data. Currently
done via direct SQL from the worker container.

### 2.2 POST /db/tuning — MariaDB Tuning (already in MigrationToolkit)

Already exists in the Migration Toolkit plugin. Include it here too for
completeness:

```
POST /api/v1/contrib/cerulean/db/tuning
Body: { "action": "apply" }   →  flush=2, pool=2GB
Body: { "action": "restore", "saved_flush": 1, "saved_pool": 134217728 }
```

### 2.3 GET /db/stats — Database Statistics

```
GET /api/v1/contrib/cerulean/db/stats
```

**Response (200):**
```json
{
  "biblios": 470000,
  "items": 598000,
  "patrons": 15000,
  "holds": 2300,
  "checkouts": 45000,
  "import_batches": 3,
  "background_jobs_pending": 0,
  "background_jobs_running": 1,
  "innodb_buffer_pool_size_mb": 2048,
  "innodb_flush_log_at_trx_commit": 2,
  "database_size_mb": 1250
}
```

**Why:** Cerulean needs these counts for verification after import,
for the preflight check, and for the dashboard.

---

## 3. Elasticsearch Management

### 3.1 GET /es/status — ES Cluster Status

```
GET /api/v1/contrib/cerulean/es/status
```

**Response (200):**
```json
{
  "cluster_status": "green",
  "index_name": "koha_kohadev_biblios",
  "doc_count": 470000,
  "index_size_mb": 850,
  "active_shards": 5,
  "node_count": 1,
  "search_engine": "Elasticsearch"
}
```

**Why:** Cerulean verifies ES doc count matches DB biblio count after
reindex. Currently we curl ES directly from the worker container.

### 3.2 DELETE /es/index — Drop ES Index

```
DELETE /api/v1/contrib/cerulean/es/index
```

**Response (200):**
```json
{ "deleted": "koha_kohadev_biblios", "acknowledged": true }
```

**Why:** Before TurboIndex with reset=true, the ES index must be
deleted. Currently done via curl to ES from the worker container.
The plugin has direct access to ES config from koha-conf.xml.

---

## 4. Daemon Management

### 4.1 GET /daemons — List Daemon Status

```
GET /api/v1/contrib/cerulean/daemons
```

**Response (200):**
```json
{
  "instance": "kohadev",
  "daemons": {
    "es-indexer": { "running": true, "pid": 3242 },
    "indexer": { "running": true, "pid": 3200 },
    "zebra": { "running": true, "pid": 3005 },
    "sip": { "running": true, "pid": 3038 },
    "z3950": { "running": true, "pid": 3122 },
    "worker-default": { "running": true, "pid": 3148 },
    "worker-long_tasks": { "running": true, "pid": 3175 },
    "plack": { "running": true, "pid": 3092 }
  }
}
```

### 4.2 POST /daemons — Start/Stop Daemons

```
POST /api/v1/contrib/cerulean/daemons
Content-Type: application/json
```

**Request:**
```json
{
  "action": "stop",
  "daemons": ["es-indexer", "indexer", "zebra", "sip", "z3950"]
}
```

Or to restart everything:
```json
{
  "action": "start",
  "daemons": ["es-indexer", "indexer", "zebra", "sip", "z3950"]
}
```

**Response (200):**
```json
{
  "results": {
    "es-indexer": { "action": "stop", "success": true, "message": "Stopped" },
    "indexer": { "action": "stop", "success": true, "message": "Stopped" },
    "zebra": { "action": "stop", "success": true, "message": "Stopped" },
    "sip": { "action": "stop", "success": true, "message": "Stopped" },
    "z3950": { "action": "stop", "success": true, "message": "Stopped" }
  }
}
```

**Why:** Cerulean's Migration Mode currently stops daemons via Docker
exec → daemon CLI. Having a REST endpoint means Cerulean doesn't need
Docker socket access at all — everything goes through the API.

### 4.3 POST /daemons/migration-mode — One-Click Toggle

```
POST /api/v1/contrib/cerulean/daemons/migration-mode
Body: { "action": "enable" }   →  stops non-essential daemons + tunes DB
Body: { "action": "disable" }  →  restarts daemons + restores DB
```

**Response (200):**
```json
{
  "mode": "migration",
  "daemons_stopped": ["es-indexer", "indexer", "zebra", "sip", "z3950"],
  "db_tuning": { "action": "applied", "saved_flush": 1, "saved_pool": 134217728 }
}
```

This combines daemon management + DB tuning into a single call —
the recommended pre-import setup.

---

## 5. File Management (for large file handling)

### 5.1 POST /upload/marc — Chunked MARC Upload

```
POST /api/v1/contrib/cerulean/upload/marc
Content-Type: application/marc
x-file-name: records.mrc
```

Accepts raw MARC bytes. Writes to Koha's temp directory and returns
a file reference that can be passed to the import endpoint.

**Response (201):**
```json
{
  "file_id": "tmp_abc123",
  "filepath": "/var/lib/koha/kohadev/tmp/cerulean_abc123.mrc",
  "size_bytes": 1312456789,
  "record_count_estimate": 470000
}
```

**Why:** For very large files (1GB+), separating the upload from the
import dispatch avoids HTTP timeout issues. The import endpoint can
then reference the file by `file_id` instead of streaming it again.

### 5.2 POST /upload/marc/{file_id}/import — Import Pre-uploaded File

```
POST /api/v1/contrib/cerulean/upload/marc/{file_id}/import
Content-Type: application/json
Body: { "encoding": "UTF-8", "comments": "Akron migration" }
```

Uses the file already uploaded via 5.1. Returns `{ "job_id": N }`.

---

## 6. Patron Management

### 6.1 POST /patrons/bulk — Bulk Patron Import

```
POST /api/v1/contrib/cerulean/patrons/bulk
Content-Type: application/json
```

**Request:**
```json
{
  "patrons": [
    {
      "surname": "Smith",
      "firstname": "John",
      "cardnumber": "12345",
      "categorycode": "ADULT",
      "branchcode": "MAIN",
      "email": "john@example.com"
    }
  ],
  "on_duplicate": "skip"
}
```

`on_duplicate`: `skip` | `update` | `error`

**Response (200):**
```json
{
  "created": 14500,
  "updated": 0,
  "skipped": 500,
  "errors": 3,
  "error_details": [
    { "cardnumber": "99999", "reason": "Missing required field: surname" }
  ]
}
```

**Why:** Koha's core `POST /patrons` is one-at-a-time. For 15k+ patrons,
Cerulean needs a bulk endpoint. Currently we POST each patron individually
which takes ~30 minutes for 15k records.

### 6.2 POST /patrons/bulk-from-csv — CSV Patron Import

```
POST /api/v1/contrib/cerulean/patrons/bulk-from-csv
Content-Type: text/csv
```

Body is raw CSV with headers matching Koha borrower field names.
Returns same response as 6.1.

---

## 7. Holds Management

### 7.1 POST /holds/bulk — Bulk Holds Import

```
POST /api/v1/contrib/cerulean/holds/bulk
Content-Type: application/json
```

**Request:**
```json
{
  "holds": [
    {
      "patron_id": 42,
      "biblio_id": 1001,
      "pickup_library_id": "MAIN",
      "notes": "Migrated hold"
    }
  ]
}
```

**Response (200):**
```json
{ "created": 2300, "errors": 5, "error_details": [...] }
```

**Why:** Core `POST /holds` is one-at-a-time. Bulk is needed for
migration of holds queues.

---

## 8. Circulation History

### 8.1 POST /circulation/bulk-history — Import Historical Checkouts

```
POST /api/v1/contrib/cerulean/circulation/bulk-history
Content-Type: application/json
```

**Request:**
```json
{
  "checkouts": [
    {
      "borrowernumber": 42,
      "itemnumber": 1001,
      "issuedate": "2024-01-15",
      "returndate": "2024-02-01"
    }
  ]
}
```

Inserts into `old_issues` (historical checkouts table).

**Response (200):**
```json
{ "imported": 45000, "errors": 12, "error_details": [...] }
```

**Why:** There's no Koha API for importing historical checkout data.
Currently requires manual SQL INSERT into `old_issues`. This is one
of the most requested migration features.

---

## 9. System Information

### 9.1 GET /system/info — Koha System Info

```
GET /api/v1/contrib/cerulean/system/info
```

**Response (200):**
```json
{
  "koha_version": "24.05.06.000",
  "instance_name": "kohadev",
  "search_engine": "Elasticsearch",
  "es_version": "8.12.0",
  "db_version": "10.11.6-MariaDB",
  "perl_version": "5.36.0",
  "os": "Debian 12",
  "plugins_installed": [
    { "name": "BWS::MigrationToolkit", "version": "1.0.0" },
    { "name": "BWS::CeruleanEndpoints", "version": "1.0.0" }
  ],
  "koha_conf": {
    "intranetdir": "/usr/share/koha/intranet/cgi-bin",
    "opacdir": "/usr/share/koha/opac/cgi-bin"
  }
}
```

**Why:** Cerulean's preflight needs to know Koha version, search engine
type, and available plugins to configure the push correctly.

---

## Priority Order for Implementation

1. **POST /refdata** (bulk upsert) — eliminates 6 SQL hack tasks in Cerulean
2. **GET /refdata** — eliminates 6 separate GET calls
3. **POST /daemons/migration-mode** — eliminates Docker socket requirement
4. **GET /db/stats** — eliminates direct DB queries for verification
5. **GET /es/status** — eliminates direct ES curl calls
6. **POST /patrons/bulk** — speeds up patron import 100x
7. **POST /holds/bulk** — enables holds migration
8. **POST /circulation/bulk-history** — enables circ history migration
9. **GET /system/info** — nice-to-have for dashboard/debugging
10. **POST /upload/marc** + import — nice-to-have for very large files
11. **POST /db/truncate** — nice-to-have for re-import scenarios
12. **DELETE /es/index** — nice-to-have (TurboIndex reset handles this)

---

## Cerulean Integration Pattern

Once this plugin exists, Cerulean's push flow becomes entirely REST-based
with zero Docker socket access, zero direct SQL, zero container exec:

```
1. GET  /cerulean/system/info          → detect Koha version + plugins
2. GET  /cerulean/refdata              → get all reference data
3. POST /cerulean/refdata              → push missing branches/itypes/locs
4. POST /cerulean/daemons/migration-mode {enable}
5. POST /migrationtoolkit/import       → fast MARC import
6. POST /migrationtoolkit/reindex      → TurboIndex ES rebuild
7. POST /cerulean/daemons/migration-mode {disable}
8. GET  /cerulean/db/stats             → verify counts
9. GET  /cerulean/es/status            → verify ES index
```

Everything over HTTP. Works with any Koha installation (packages,
Docker, manual) as long as the plugins are installed.

---

*Spec from Cerulean Next, April 2026.*
