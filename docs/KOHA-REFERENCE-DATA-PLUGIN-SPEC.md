# Koha Reference Data Management — Plugin API Spec for Cerulean

**Purpose:** Give Cerulean (or any migration platform) REST endpoints to
create/manage ALL Koha reference data types needed before a MARC import.
Today Koha's core REST API has endpoints for some types (libraries,
patron_categories) but is missing many others.

## What Cerulean Needs

Before importing MARC records, the migration platform must ensure that
every controlled value referenced in the 952 item fields exists in Koha.
If they don't exist, the import fails silently or creates items without
the correct metadata.

### Reference data types

| Type | 952 subfield | Koha table | Core REST API? | Need from plugin |
|------|-------------|------------|----------------|------------------|
| **Libraries (branches)** | `$a`, `$b` | `branches` | POST /libraries | Already works |
| **Item types** | `$y` | `itemtypes` | GET only, POST returns 400 | **Need POST** |
| **Locations (LOC)** | `$c` | `authorised_values` | GET only, no POST | **Need POST** |
| **Collection codes (CCODE)** | `$8` | `authorised_values` | GET only, no POST | **Need POST** |
| **Not-for-loan statuses** | `$7` | `authorised_values` | GET only, no POST | **Need POST** |
| **Lost statuses (LOST)** | `$1` | `authorised_values` | GET only, no POST | **Need POST** |
| **Damaged statuses** | `$4` | `authorised_values` | GET only, no POST | **Need POST** |
| **Withdrawn statuses** | `$0` | `authorised_values` | GET only, no POST | **Need POST** |
| **Patron categories** | n/a (patrons) | `categories` | Inconsistent | **Need POST** |
| **Frameworks** | n/a (biblio) | `biblio_framework` | None | Nice-to-have |
| **MARC modification templates** | n/a | `marc_modification_templates` | None | Nice-to-have |

## Proposed Endpoints

All under `/api/v1/contrib/migrationtoolkit` (or a separate refdata plugin).

### 1. Bulk Reference Data Upsert

```
POST /api/v1/contrib/migrationtoolkit/refdata
Content-Type: application/json
```

**Permission:** `parameters: manage_itemtypes` or superlibrarian

**Request:**
```json
{
  "item_types": [
    { "code": "BOOK", "description": "Book" },
    { "code": "DVD", "description": "DVD" }
  ],
  "authorised_values": {
    "LOC": [
      { "value": "ADULT", "description": "Adult Section" },
      { "value": "CHILD", "description": "Children's Section" }
    ],
    "CCODE": [
      { "value": "FIC", "description": "Fiction" }
    ],
    "NOT_LOAN": [
      { "value": "1", "description": "Not for loan" }
    ],
    "LOST": [
      { "value": "1", "description": "Lost" },
      { "value": "2", "description": "Long overdue" }
    ],
    "DAMAGED": [
      { "value": "1", "description": "Damaged" }
    ],
    "WITHDRAWN": [
      { "value": "1", "description": "Withdrawn" }
    ]
  },
  "libraries": [
    { "branchcode": "MAIN", "branchname": "Main Library" },
    { "branchcode": "NORTH", "branchname": "North Branch" }
  ],
  "patron_categories": [
    { "categorycode": "ADULT", "description": "Adult", "enrolmentperiod": 99, "category_type": "A" }
  ]
}
```

All fields are optional — send only what you need to create.

**Response (200):**
```json
{
  "item_types": { "created": 2, "skipped": 0, "errors": 0 },
  "authorised_values": {
    "LOC": { "created": 2, "skipped": 0, "errors": 0 },
    "CCODE": { "created": 1, "skipped": 0, "errors": 0 },
    "NOT_LOAN": { "created": 1, "skipped": 0, "errors": 0 },
    "LOST": { "created": 2, "skipped": 0, "errors": 0 },
    "DAMAGED": { "created": 1, "skipped": 0, "errors": 0 },
    "WITHDRAWN": { "created": 1, "skipped": 0, "errors": 0 }
  },
  "libraries": { "created": 2, "skipped": 0, "errors": 0 },
  "patron_categories": { "created": 1, "skipped": 0, "errors": 0 },
  "errors": []
}
```

**Behavior:**
- Uses INSERT IGNORE / upsert semantics — existing values are skipped, not updated
- `skipped` = values that already existed (not an error)
- `errors` array contains any failures with reason text
- One call handles everything — no need for multiple round-trips

### 2. Reference Data Summary (already in toolkit as /preflight, extend it)

```
GET /api/v1/contrib/migrationtoolkit/refdata
```

**Response (200):**
```json
{
  "item_types": [
    { "code": "BOOK", "description": "Book" },
    { "code": "DVD", "description": "DVD" }
  ],
  "authorised_values": {
    "LOC": [
      { "value": "ADULT", "description": "Adult Section" }
    ],
    "CCODE": [],
    "NOT_LOAN": [{ "value": "1", "description": "Not for loan" }],
    "LOST": [],
    "DAMAGED": [],
    "WITHDRAWN": []
  },
  "libraries": [
    { "branchcode": "MAIN", "branchname": "Main Library" }
  ],
  "patron_categories": [
    { "categorycode": "ADULT", "description": "Adult" }
  ]
}
```

Returns the **full list** of each type (not just counts like /preflight).
This lets Cerulean do the diff client-side: compare what's in the MARC
file vs what Koha has, identify the gaps, then POST only the missing ones.

### 3. Individual Type Endpoints (alternative to bulk)

If bulk is too complex to implement first, these individual endpoints
would also work:

```
POST /api/v1/contrib/migrationtoolkit/refdata/item-types
Body: [{ "code": "BOOK", "description": "Book" }]

POST /api/v1/contrib/migrationtoolkit/refdata/authorised-values/{category}
Body: [{ "value": "ADULT", "description": "Adult Section" }]

POST /api/v1/contrib/migrationtoolkit/refdata/libraries
Body: [{ "branchcode": "MAIN", "branchname": "Main Library" }]

POST /api/v1/contrib/migrationtoolkit/refdata/patron-categories
Body: [{ "categorycode": "ADULT", "description": "Adult", "enrolmentperiod": 99, "category_type": "A" }]
```

Each returns: `{ "created": N, "skipped": N, "errors": [...] }`

## How Cerulean Would Use This

```python
# Step 1: Scan MARC file for needed values
scan = scan_marc_952_values(marc_file)
# → {"homebranch": ["MAIN","NORTH"], "itype": ["BOOK","DVD"], "loc": ["ADULT","CHILD"], ...}

# Step 2: Get what Koha has
koha_refs = GET /api/v1/contrib/migrationtoolkit/refdata

# Step 3: Compute the diff
missing_branches = [b for b in scan["homebranch"] if b not in koha_branches]
missing_itypes = [i for i in scan["itype"] if i not in koha_itypes]
missing_locs = [l for l in scan["loc"] if l not in koha_locs]
# etc.

# Step 4: Push all missing in one call
POST /api/v1/contrib/migrationtoolkit/refdata
{
  "libraries": [{"branchcode": b, "branchname": b} for b in missing_branches],
  "item_types": [{"code": i, "description": i} for i in missing_itypes],
  "authorised_values": {
    "LOC": [{"value": l, "description": l} for l in missing_locs],
  }
}

# Step 5: Verify
preflight = GET /api/v1/contrib/migrationtoolkit/preflight
assert preflight["branches"] >= expected
assert preflight["itemtypes"] >= expected
```

## Implementation Notes for the Plugin

### Database tables involved

| Type | Table | Key column | Description column |
|------|-------|-----------|-------------------|
| Item types | `itemtypes` | `itemtype` (varchar 10) | `description` (longtext) |
| Authorised values | `authorised_values` | composite: `category` + `authorised_value` | `lib` (varchar 200) |
| Libraries | `branches` | `branchcode` (varchar 10) | `branchname` (longtext) |
| Patron categories | `categories` | `categorycode` (varchar 10) | `description` (longtext) |

### SQL patterns

```sql
-- Item types
INSERT IGNORE INTO itemtypes (itemtype, description) VALUES (?, ?);

-- Authorised values
INSERT IGNORE INTO authorised_values (category, authorised_value, lib)
VALUES (?, ?, ?);

-- Libraries
INSERT IGNORE INTO branches (branchcode, branchname) VALUES (?, ?);

-- Patron categories
INSERT IGNORE INTO categories (categorycode, description, enrolmentperiod, category_type)
VALUES (?, ?, ?, 'A');
```

`INSERT IGNORE` is the right approach — if the value already exists, skip it
silently. No need for UPDATE semantics during migration setup.

### Permissions

The plugin should require `parameters: manage_itemtypes` or superlibrarian.
This is a setup-phase operation, not a day-to-day one.

### Error handling

- Unknown category in `authorised_values` → create the category first
  (INSERT INTO `authorised_value_categories`) then insert values
- `branchcode` too long (>10 chars) → return error with the specific value
- Duplicate within the same request → skip (idempotent)

## Priority

If only ONE endpoint gets built:

> **POST /refdata with the bulk upsert** — one call to push everything.

This eliminates 6+ separate REST calls and handles the full pre-import
setup in a single request.

---

*Spec from Cerulean Next, April 2026.*
