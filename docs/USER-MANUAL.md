# Cerulean Next User Manual

**ByWater Solutions MARC Migration Platform**
*Version 1.0 — April 2026*

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Dashboard & Projects](#dashboard--projects)
3. [Step 1 — Data Ingest](#step-1--data-ingest)
4. [Step 2 — ILS Detection & Migration Config](#step-2--ils-detection--migration-config)
5. [Step 3 — MARC Data Quality](#step-3--marc-data-quality)
6. [Step 4 — Bib Versions](#step-4--bib-versions)
7. [Step 5 — Field Mapping](#step-5--field-mapping)
8. [Step 6 — Transform](#step-6--transform)
9. [Step 7 — Item & Bib Reconciliation](#step-7--item--bib-reconciliation)
10. [Step 8 — Patron Data](#step-8--patron-data)
11. [Step 9 — Patron Versions](#step-9--patron-versions)
12. [Step 10 — Load (Push to Koha)](#step-10--load-push-to-koha)
13. [Step 11 — Holds & Remaining Data](#step-11--holds--remaining-data)
14. [Step 12 — Aspen Discovery](#step-12--aspen-discovery)
15. [Step 13 — Evergreen ILS](#step-13--evergreen-ils)
16. [Templates](#templates)
17. [Plugins](#plugins)
18. [System Settings](#system-settings)
19. [Project Log & Audit Trail](#project-log--audit-trail)
20. [Suggestions & Feedback](#suggestions--feedback)
21. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Signing In

Cerulean Next uses Google OAuth for authentication. Click **Sign in with Google** and use your `@bywatersolutions.com` account. Only ByWater Solutions team members can access the platform. Sessions last 8 hours by default (configurable in System Settings).

After signing in, your name and avatar appear in the sidebar footer. Click **Sign out** to end your session.

### Navigation

The left sidebar provides access to all areas:

- **Workspace** — Dashboard and New Project
- **Project** — Steps 1-13 (Ingest through Evergreen), Project Log, Settings (visible when a project is selected)
- **Platform** — Help, Templates, Suggestions, Plugins, System Settings

The **pipeline bar** at the top of each stage page shows your progress through all stages. Completed stages show a checkmark.

### Creating a Project

1. Click **New Project** in the sidebar
2. Enter a **Project Code** (short identifier, e.g., `westfield_2025`)
3. Enter the **Library Name**
4. Optionally enter the **Koha URL** and **API Token**
5. Set **Visibility**: Private (only you) or Shared (all team members)
6. Click **Create Project**

---

## Dashboard & Projects

The dashboard shows all projects you have access to:

- **Your projects** (any visibility)
- **Shared projects** from other team members
- **Legacy projects** (created before multi-user was enabled)

Each project row shows the project code, library name, source ILS, stage progress pips, current stage badge, and a "Shared" badge for shared projects.

**Actions:**
- Click a project to open it
- Click **Edit** to access project settings
- Click **Archive** to hide a project (use "Show archived" toggle to restore)

---

## Step 1 — Data Ingest

**Purpose:** Upload source MARC and item data files, then let the system analyze them.

### Uploading MARC Files

Drag and drop `.mrc`, `.marc`, `.mrk`, `.csv`, or `.xml` files onto the upload area, or click to browse. Files up to 2 GB are supported.

### What Happens Behind the Scenes

When you upload a file, several automated tasks run:

1. **Format Detection** — Cerulean checks the file structure to determine whether it is ISO 2709 (binary MARC), MRK (text MARC), CSV, or MARCXML. It validates the MARC leader and record structure to confirm the file is well-formed.

2. **Record Counting** — Every record in the file is iterated to get an exact count.

3. **ILS Detection** — Cerulean samples up to 200 records and checks for ILS-specific MARC fields:
   - **SirsiDynix Symphony**: Looks for "Sirsi" in 035$a or heavy use of 999 fields
   - **Innovative Millennium/Sierra**: Detects 907 (bib record number) or 945 (item) local fields
   - **Polaris**: Detects 949 fields with $t subfield
   - **Koha**: Looks for 952 (items) or 942 (item type) fields
   - A confidence score (0-100%) is calculated as the ratio of matching records to sampled records

4. **Tag Frequency Analysis** — Every MARC tag across all records is counted, producing a histogram showing which fields are present and how often (e.g., "245: 42,118 records, 852: 41,900 records"). Subfield-level frequency is also calculated.

5. **Elasticsearch Indexing** — If Elasticsearch is configured, records are indexed for full-text search (non-blocking; the migration proceeds even if this step fails).

### Uploading Item CSV Files

If items/holdings are in a separate CSV file (not embedded in MARC 952 fields):

1. Upload the CSV file
2. Cerulean auto-detects the delimiter (comma, tab, pipe, or semicolon) using Python's csv.Sniffer
3. The header row is auto-detected (first line with 3+ unique, identifier-like fields)
4. If no header is detected, columns are named `col_1`, `col_2`, etc.
5. For each column, Cerulean calculates the unique value count and collects up to 20 sample values
6. You can recategorize any file between "MARC" and "Items CSV" if auto-detection was wrong

### What to Look For

- **Record count** — Does it match what the source library provided?
- **ILS signal** — Is the detected source ILS correct?
- **Tag frequency** — Are expected fields present? Missing 245 (title) is a red flag. High 852/952 counts indicate embedded items.

---

## Step 2 — ILS Detection & Migration Config

**Purpose:** Confirm the source ILS and configure how items are structured.

### ILS Confirmation

Cerulean displays the detected ILS and confidence score. You can override this if the detection was incorrect — the ILS setting influences which field mapping templates are suggested in Stage 5.

### Item Structure

Select how item/holdings data is organized:

| Option | When to Use |
|--------|-------------|
| **Embedded** | Items are in MARC 952 (Koha), 949 (Polaris), 945 (Millennium), or 999 (Symphony) fields within each bib record |
| **Separate** | Items are in a separate CSV file uploaded in Stage 1 |
| **None** | No item data to migrate (bibs only) |

### MARC Record Browser

Browse individual records to verify data quality:
- Navigate with arrow buttons or jump to a specific record number
- View each field with tag, indicators, and subfield breakdown
- Item tags (952, 949, 999, 852) are automatically highlighted

### Quick Path

For simple migrations where the data is already clean and properly mapped (e.g., Koha-to-Koha), click **Mark as Ready to Load** to skip stages 3-6 and go directly to Stage 7. The file will be pushed as-is without field mapping, deduplication, or reconciliation.

---

## Step 3 — MARC Data Quality

**Purpose:** Scan records for quality issues, understand what the system finds, and fix problems before loading into the target ILS.

### Running a Scan

1. Select which files to scan using the checkboxes
2. Click **Run Quality Scan**
3. Wait for the async task to complete (progress shown in the task bar)

### What the System Checks

The quality scanner performs **8 categories** of checks on every record:

#### 1. Encoding Issues
The system tests every field (control fields and data fields) for valid UTF-8 encoding. Characters that cannot be properly encoded/decoded as UTF-8 are flagged as errors. This catches:
- Latin-1 or Windows-1252 characters that weren't properly converted
- MARC-8 encoded data that wasn't converted to UTF-8
- Corrupted byte sequences from file transfers or system conversions

**Severity:** Error

#### 2. Diacritics
The system checks for two diacritics problems:
- **Orphan combining characters** — Unicode combining marks (accents, tildes, umlauts) that appear at the beginning of a subfield value without a base character to attach to
- **Decomposed Unicode** — Characters that are stored in decomposed form (base character + separate combining mark) instead of the preferred composed form (single precomposed character). For example, "e" + combining acute accent instead of "e-acute". The system suggests NFC normalization.

**Severity:** Warning / Info

#### 3. Indicator Values
MARC indicators must be a space or a digit 0-9. The system checks both indicator 1 and indicator 2 on every data field. Common problems include:
- Letters used as indicators (often from data conversion errors)
- Special characters or control characters
- The system suggests replacing invalid indicators with a space

**Severity:** Warning

#### 4. Subfield Structure
The system checks for required subfields in critical fields:
- **245 (Title)** — Must have $a. Records without a title subfield are flagged as errors.
- **100/110/111 (Author)** — Should have $a (main entry). Missing author subfields are flagged as warnings.

**Severity:** Error (245$a) / Warning (1xx$a)

#### 5. Field Length
MARC fields have a maximum length of 9,999 bytes. The system calculates the total byte length of each field (including subfield codes) and flags any that exceed this limit. Oversized fields will cause import failures in most ILS systems.

**Severity:** Error

#### 6. Leader & 008 Validation
The **MARC leader** is a 24-character fixed field at the start of every record. The system validates specific byte positions:

| Position | Name | Valid Values |
|----------|------|-------------|
| 5 | Record status | a (increase), c (corrected), d (deleted), n (new), p (increase from prepub) |
| 6 | Type of record | a (language material), c/d/i/j (music), e/f (cartographic), g (projected), k (2D graphic), m (computer file), n/o/p/r/t (others) |
| 7 | Bibliographic level | a (component), b (component serial), c (collection), d (subunit), i (integrating), m (monograph), s (serial) |
| 8 | Type of control | space or a (archival) |
| 9 | Character coding scheme | space (MARC-8) or a (UTF-8) |
| 17 | Encoding level | space, 1-8, u (unknown), z (not applicable) |
| 18 | Descriptive cataloging form | space, a (AACR2), c (ISBD), i (ISBD), n (not applicable), u (unknown) |
| 19 | Multipart resource record level | space, a, b, c |

The **008 field** (fixed-length data) must be exactly 40 characters. Records with shorter 008 fields are padded with fill characters (|), and longer ones are truncated.

**Severity:** Error (wrong length) / Warning (invalid byte values)

#### 7. Blank / Empty Fields
The system flags:
- Control fields with blank or whitespace-only data
- Data fields where all subfields are empty
- Records missing a 245 field entirely (no title)

**Severity:** Warning / Error (missing 245)

#### 8. Duplicate Detection
After scanning individual records, the system performs two duplicate checks:
- **001 duplicates** — Tracks all 001 (control number) values. If two or more records share the same 001, they are flagged. This often indicates a data export error.
- **ISBN duplicates** — Extracts 020$a values, strips prefixes and qualifiers, and flags records sharing the same ISBN. This may indicate legitimate duplicate records that should be merged.

**Severity:** Warning (001) / Info (ISBN)

### Fixing Issues

- **Auto-fix** — The system can automatically fix certain issues:
  - Invalid leader bytes are replaced with safe defaults (e.g., position 5 → 'n', position 6 → 'a', position 7 → 'm')
  - Short 008 fields are padded to 40 characters with fill characters
  - Invalid indicators are replaced with spaces
  - Decomposed Unicode is composed via NFC normalization
  - The fix is applied in-memory, then the entire file is rewritten to disk

- **Manual edit** — Click an issue to see the full MARC record. Click any field value to edit it inline. Changes are saved immediately.

- **Ignore** — Mark issues as "ignored" if they are acceptable for your migration

- **Bulk actions** — "Ignore all" or "Auto-fix all" per category to handle large volumes efficiently

---

## Step 4 — Bib Versions

**Purpose:** Create immutable snapshots of your bibliographic data at different stages of the migration pipeline.

### Why Version?

Migrations are iterative. You might run the transform, discover a mapping error, fix it, and re-run. Versions let you:
- Track what changed between iterations
- Verify that a transform produced the expected results
- Roll back if something went wrong

### Creating a Snapshot

Click **Create Version** to capture:
- A frozen copy of the current MARC file
- All field mappings at the time of creation
- Quality scan results summary
- Record count

### Comparing Versions

1. Select two versions from the dropdowns
2. Click **Compare**
3. View the differences: added/removed/changed records and fields

---

## Step 5 — Field Mapping

**Purpose:** Define how source MARC fields map to Koha's MARC structure.

### Three-Panel Layout

| Panel | Purpose |
|-------|---------|
| **Left** | File selector and tag frequency browser — see which fields exist and how often |
| **Center** | Record inspector — navigate individual records and view field details |
| **Right** | Field mapping editor — create, edit, approve mappings |

### Creating Mappings Manually

1. Click **+ Add** in the right panel
2. Select the **source** MARC tag and subfield (e.g., 852$h)
3. Select the **target** Koha tag and subfield (e.g., 952$o for call number)
4. Optionally add a **transform**:
   - **Copy** — Direct passthrough (most common)
   - **Regex** — Perl-style substitution (e.g., `s/^REF /R/i`)
   - **Lookup** — JSON table mapping (e.g., `{"JUV": "JUVENILE", "REF": "REFERENCE"}`)
   - **Constant** — Fixed value for every record (e.g., always set 942$c to "BK")
   - **Function** — Sandboxed Python expression (safe builtins only: len, str, int, float — no file access or imports)
   - **Preset** — Built-in transforms (see list below)
5. Click **Save**

### Available Presets

| Preset | What It Does |
|--------|-------------|
| date_mdy_to_dmy | Convert MM/DD/YYYY to DD/MM/YYYY |
| date_dmy_to_iso | Convert DD/MM/YYYY to YYYY-MM-DD |
| date_ymd_to_iso | Convert YYYYMMDD to YYYY-MM-DD |
| date_marc_to_iso | Convert MARC date format to ISO |
| case_upper | UPPERCASE |
| case_lower | lowercase |
| case_title | Title Case |
| case_sentence | Sentence case |
| text_trim | Strip leading/trailing whitespace |
| text_normalize_spaces | Collapse multiple spaces to one |
| text_strip_punctuation | Remove punctuation characters |
| clean_isbn | Remove hyphens and qualifier text from ISBNs |
| clean_lccn | Normalize LCCN format |
| clean_html | Strip HTML tags |
| extract_year | Extract 4-digit year from text |
| extract_isbn13 | Extract 13-digit ISBN |

### AI-Assisted Mapping

Click **AI Suggest** to have Claude analyze your data and generate mapping suggestions:

1. The system sends Claude the tag frequency report (top 100 tags) and up to 50 sample MARC records
2. Claude analyzes the field structure and suggests mappings based on known ILS patterns
3. Each suggestion includes a confidence score (0-100%) and reasoning
4. Suggestions appear in the mapping editor with `AI Suggested` labels
5. **You must approve each suggestion** — nothing is applied automatically

The AI is particularly good at identifying:
- Location codes (852$a/b, 949$l, 945$l → 952$a/b)
- Call numbers (092, 099, 852$h+i, 949$a → 952$o)
- Barcodes (876$p, 949$i, 945$i → 952$p)
- Item types (source-specific fields → 942$c)

### Templates

- **Save as Template** — Save your current approved mappings as a reusable template. Choose project-scoped (this project only) or global (shared across all projects and users).
- **Load Template** — Apply a previously saved template. In "merge" mode, existing approved mappings are preserved; in "replace" mode, all mappings are replaced.

Templates are especially valuable when migrating multiple libraries from the same source ILS.

---

## Step 6 — Transform

**Purpose:** Apply your field mappings to the MARC data and build the output file.

### What Happens During a Build

1. **Validation** — Each approved field map is validated:
   - Source and target tags must be 3-digit numeric
   - Transform type must be one of: copy, regex, lookup, const, fn, preset
   - Regex expressions must be valid
   - Lookup tables must be valid JSON
   - Preset keys must exist in the presets library

2. **Per-Record Transformation** — For each record in each source file:
   - Maps are grouped by (source_tag, target_tag) to preserve repeating field relationships
   - For each instance of a source field, the system builds one target field
   - Transforms are applied to each subfield value
   - If a source field is mapped to multiple targets, all mappings are applied before the source is deleted
   - Per-map statistics are tracked: how many records had the source field, how many were transformed successfully, how many had errors

3. **Items CSV Joining** (if "Include items data" is checked):
   - Loads approved ItemColumnMap rows (from Stage 5)
   - Reads the items CSV file into a lookup table keyed by the match column
   - For each MARC record, extracts the match value (e.g., 001) and finds matching CSV rows
   - Each matching CSV row becomes a 952 field, with subfields populated according to the column map

4. **Output** — Transformed records are written to `output.mrc` (or per-file `*_transformed.mrc` files and a merged `merged.mrc`)

### Diagnostic Logging

The first record is fully logged to the audit trail showing which source tags existed, which were mapped, and which were missing. This helps catch mapping errors early.

---

## Step 7 — Item & Bib Reconciliation

**Purpose:** Map source item field values to Koha's controlled vocabularies. Source ILS systems use different codes than Koha — this stage creates the translation rules.

### Vocabulary Categories

The system scans MARC 952 fields for values in these subfields:

| Category | 952 Subfield | Koha Field | Example Source Values | Example Koha Values |
|----------|-------------|------------|----------------------|---------------------|
| itype | $y | Item type | "BK", "DVD", "JBOOK" | "BK", "DVD", "JBK" |
| loc | $c | Shelving location | "ADULT", "CHILD", "REF" | "ADULT", "CHILD", "REF" |
| ccode | $8 | Collection code | "FICTION", "NONFIC" | "FIC", "NF" |
| not_loan | $7 | Not for loan | "1", "REFERENCE" | "1", "-1" |
| withdrawn | $0 | Withdrawn status | "LOST", "DISCARD" | "1" |
| damaged | $4 | Damaged status | "WATER", "TORN" | "1" |
| homebranch | $a | Home library | "MAIN", "BR1", "BR2" | "MAIN", "BRANCH1" |
| holdingbranch | $b | Current holding library | Same as homebranch | Same as homebranch |

### How the Scan Works

1. The system reads every record in your output MARC file
2. For each 952 field, it extracts the value from each mapped subfield
3. It builds a frequency table: how many records contain each unique value per category
4. Results are displayed as sortable lists showing each source value and its record count

### Creating Rules

For each source value, create a rule:

| Operation | What It Does |
|-----------|-------------|
| **Rename** | Change one value to another (e.g., "JUV" → "JUVENILE") |
| **Merge** | Combine multiple source values into one target (e.g., "JV", "JUV", "JUVEN" all → "JUVENILE") |
| **Split** | Conditionally assign different target values based on other fields in the same record (e.g., if 952$a = "MAIN" then "ADULT", else "CHILD") |
| **Delete** | Remove the subfield ("subfield" mode) or the entire 952 field ("field" mode) |

### Applying Rules

When you click Apply:
1. Rules are sorted by `sort_order` and applied in sequence
2. For split operations, conditions are evaluated against other fields in the record
3. The modified records are written to `Biblios-mapped-items.mrc`
4. Per-category CSV files are generated (e.g., `items/items_itype.csv`) for reference

---

## Step 8 — Patron Data

**Purpose:** Import patron/borrower records into Koha from various source formats.

### Supported Input Formats

| Format | Detection |
|--------|-----------|
| **CSV** | Auto-detects delimiter (comma, tab, pipe, semicolon) and encoding (via chardet) |
| **TSV** | Tab-separated values |
| **Excel** (.xlsx, .xls) | Reads via openpyxl/xlrd with configurable sheet name and header row |
| **XML** | Flattens repeating elements — auto-detects the most common element as the "row" |
| **MARC** (.mrc) | Flattens each record to key-value pairs (control fields as tag, data fields as tag$subcode) |

### What Happens During Parsing

1. **Encoding detection** — The system uses chardet to detect the file's character encoding (UTF-8, Latin-1, Windows-1252, etc.) and converts to UTF-8
2. **Delimiter detection** — For CSV files, Python's csv.Sniffer samples the file to determine the delimiter
3. **Header detection** — The first row with 3+ unique, non-numeric values is treated as the header row
4. **Data extraction** — All rows are parsed and written to a normalized `parsed.csv`

### Column Mapping

Map each source column to a Koha borrower field. Cerulean supports all 78 standard Koha borrower fields:

- **Identity**: cardnumber, surname, firstname, title, othernames, initials, pronouns
- **Contact**: email, emailpro, phone, phonepro, mobile, fax
- **Address**: address, address2, city, state, zipcode, country (+ B_address fields for alternate address)
- **Library**: branchcode, categorycode, sort1, sort2
- **Dates**: dateofbirth, dateenrolled, dateexpiry
- **Status**: lost, debarred, debarredcomment, gonenoaddress
- **Auth**: userid, password
- **Other**: borrowernotes, opacnote, contactnote, patron_attributes

### AI-Assisted Column Mapping

Click **AI Suggest** to have Claude analyze your column headers and sample data. The AI:

1. Receives your column headers and up to 20 sample rows
2. Suggests mappings with confidence scores
3. Identifies columns needing transforms (e.g., a "Name" column that needs splitting into surname + firstname)
4. Flags controlled list columns (categorycode, branchcode, title, lost) for value reconciliation

**Name Splitting**: The AI auto-detects name formats:
- "Last, First" → surname + firstname
- "Last, First Middle" → surname + firstname + middlename
- "First Last" → firstname + surname
- "First Middle Last" → firstname + middlename + surname

### Available Transforms

| Transform | What It Does |
|-----------|-------------|
| copy | Direct passthrough |
| name_split | Split full name into parts (surname, firstname, middlename) |
| split | Split by delimiter and take a specific index |
| date_mdy_to_iso | MM/DD/YYYY → YYYY-MM-DD |
| date_dmy_to_iso | DD/MM/YYYY → YYYY-MM-DD |
| date_ymd_to_iso | YYYYMMDD → YYYY-MM-DD |
| case_upper / lower / title | Change letter case |
| gender_normalize | Normalize gender values (M/F/X) |
| first_char | Extract first character |
| const | Fixed value for every patron |
| regex | Pattern-based substitution |

### Value Reconciliation

After column mapping, scan for controlled list values that need translation:

| Koha Header | Validated Against |
|-------------|------------------|
| categorycode | Koha patron categories (`/api/v1/patron_categories`) |
| branchcode | Koha libraries (`/api/v1/libraries`) |
| title | Koha authorized values (TITLE category) |
| lost | Koha authorized values (LOST category) |

Create rename, merge, split, or delete rules (same as Stage 8 item reconciliation).

### Apply & Push

When you click Apply:
1. Column mappings are applied to every row
2. Transforms are executed (name splitting, date conversion, etc.)
3. Value rules translate controlled list codes
4. Output is written to `patrons_transformed.csv` with Koha borrower headers
5. Delete rules in "exclude_row" mode skip entire patron records; "blank_field" mode clears just the field

---

## Step 9 — Patron Versions

**Purpose:** Snapshot patron data at different stages (same concept as Stage 4 for bibs).

Create version snapshots after each iteration of patron loading. Compare versions to verify that mapping and rule changes produce the expected results.

---

## Step 10 — Load (Push to Koha)

**Purpose:** Push your prepared MARC and patron data into the target Koha instance. This is the final action step — all data preparation (mapping, reconciliation, patrons) should be complete before pushing.

### Setup Tab

Before pushing records, verify your environment:

| Action | What It Does |
|--------|-------------|
| **Scan MARC** | Analyzes the output file for controlled values (item types, locations, collection codes) — helps identify values that need to exist in Koha before import |
| **Fetch Reference Data** | Pulls current reference data from Koha via the REST API: libraries, patron categories, item types, authorized values |
| **Verify Counts** | Checks how many bibs, items, and patrons currently exist in Koha |
| **DB Stats** | Shows MariaDB buffer pool usage and Elasticsearch index status |

### Migration Mode

For large migrations (10,000+ records), enabling **Migration Mode** dramatically improves import speed by:

**Stopping non-essential daemons:**

| Daemon | Purpose | Why Stop It |
|--------|---------|-------------|
| koha-es-indexer | Elasticsearch incremental indexer | Biggest resource hog — tries to index every record as it's inserted |
| koha-indexer | Zebra incremental indexer | Legacy search indexer |
| koha-zebra | Zebra search server | Not needed during bulk load |
| koha-sip | SIP2 circulation protocol | No patrons checking out during migration |
| koha-z3950-responder | Z39.50 search responder | No external catalog searches during migration |

**Daemons kept running:**
- koha-worker (needed for FastMARCImport and background jobs)
- koha-plack (needed for REST API access)

**Database tuning applied:**
- `innodb_flush_log_at_trx_commit = 2` (async flush — 10-50x faster writes, safe for bulk load since we'll re-import if it crashes)
- `innodb_buffer_pool_size` increased (more RAM for InnoDB caching)

**Important:** Always disable Migration Mode after import completes. This restores daemons and safe database settings.

### Push Methods

| Method | Speed | Requirements | Best For |
|--------|-------|-------------|----------|
| **REST API** | Slow (1 record/sec) | No plugins needed | Small imports (<1,000 records), testing |
| **FastMARCImport Plugin** | Fast (parallel workers) | FastMARCImport plugin | Medium imports |
| **Migration Toolkit Plugin** | Fastest (all-in-one) | MigrationToolkit plugin | Large migrations — includes DB tuning, parallel import, and reindex |

#### REST API Method
- Stages each record individually via `POST /api/v1/biblios`
- Commits in batches
- Stops after 10 consecutive failures if no records have succeeded (prevents spinning on auth errors or bad data)

#### FastMARCImport Plugin Method
- Copies the MARC file into the Koha Docker container
- Runs `bulkmarcimport.pl` with configured options (match field, overlay action, worker count)
- Parses stdout for success/failure counts
- 600-second timeout per file

#### Migration Toolkit Plugin Method
- Single REST API call: `POST /api/v1/contrib/migrationtoolkit/import`
- Handles DB tuning, parallel import, and reindexing in one workflow
- Configurable worker count (1-16) and batch size

### Reindex Tab

After loading records, Koha's Elasticsearch index must be rebuilt for records to appear in OPAC search:

- **Full reindex** — Rebuilds the entire search index via Koha's background job system
- **TurboIndex Plugin** — Parallel reindex using multiple workers (much faster for large catalogs)

### Manifests Tab

Every push operation creates a manifest recording:
- Method used (REST, FastMARC, Toolkit)
- Start/end time
- Records total, succeeded, and failed
- Error messages (if any)
- Celery task ID for debugging

---

## Step 11 — Holds & Remaining Data

**Purpose:** Migrate holds/reserves, circulation history, and other remaining data.

*Coming soon.* This stage will support:

- Active holds/reserves (linking patrons to bibliographic records)
- Checkout history
- Fines and fees
- Reading history

---

## Step 12 — Aspen Discovery

**Purpose:** Bulk migrate data from Koha into an Aspen Discovery instance.

### Configuration

Enter the Aspen Discovery URL (e.g., `http://aspen:85`). Cerulean authenticates to Aspen's SystemAPI using session-based login and injects a `Host: localhost` header for Apache vhost compatibility.

### Turbo Migration

High-performance parallel extraction from Koha's database directly into Aspen's database:

1. Set the number of **workers** (1-16) — more workers = faster but more CPU/memory
2. Set the **batch size** — records per batch
3. Click **Start Turbo Migration**
4. Monitor progress via auto-refreshing record count cards

Typical performance: ~940,000 records in 8 minutes with 8 workers.

### Turbo Reindex

After migration, rebuild Aspen's Solr search index:

1. Optionally check **Clear Solr first** for a clean rebuild
2. Set workers and batch size
3. Click **Start Turbo Reindex**

Typical performance: ~940,000 records in 25 minutes.

### Indexer Control

- **Stop Indexers** — Pause Aspen's background indexing during bulk load (prevents wasted CPU on partial data)
- **Start Indexers** — Resume indexing after load completes

---

## Step 13 — Evergreen ILS

**Purpose:** Bulk migrate MARC records into an Evergreen ILS instance via direct PostgreSQL access.

### How Evergreen Stores Data

Unlike Koha (which stores ISO 2709 binary MARC), Evergreen stores records as **MARCXML text** in the `biblio.record_entry` PostgreSQL table. Cerulean converts each record from ISO 2709 → MARCXML using pymarc before insertion.

Search indexing is handled by `pingest.pl` which populates the `metabib.*` tables (title, author, subject, keyword, identifier indexes).

### Database Connection

Configure the Evergreen PostgreSQL connection directly — Cerulean connects via psycopg2 (not through Evergreen's APIs):
- Host, Port, Database name (default: `evergreen`), Username, Password
- Click **Test Connection** to verify connectivity and see current bib counts

### 1. Push Bibs

Insert MARCXML records into `biblio.record_entry`:

- **Batch Size** — Records per INSERT batch (default: 500). Larger batches = fewer commits but more memory.
- **Trigger Mode** — Controls which PostgreSQL triggers fire during insert:

| Mode | Speed | What's Disabled | Post-Insert Work |
|------|-------|-----------------|-----------------|
| Skip indexing only (recommended) | Fast | `aaa_indexing_ingest_or_delete` (full metabib reingest) and `bbb_simple_rec_trigger` (simple record cache) | Run pingest + metarecord remap |
| All triggers on | Slowest | Nothing | None — indexes build as records insert |
| All triggers off | Fastest | ALL triggers on `biblio.record_entry` | Run pingest + metarecord remap + rebuild everything |

- **Remap metarecords** (checkbox, on by default) — After inserting records, the system calls `metabib.remap_metarecord_for_bib(id, fingerprint, deleted)` for every new bib in 5,000-id chunks. This is **critical**: without it, records are invisible in OPAC search because the `metabib.metarecord_source_map` table (which the search visibility filter joins against) is not populated by the disabled triggers.

### 2. Parallel Ingest (pingest)

After bulk loading, run `pingest.pl` inside the Evergreen container to build search indexes:

- **Max Child** — Parallel worker processes (default: 2 — small values prevent PostgreSQL connection exhaustion)
- **Batch Size** — Records per pingest batch (default: 25 — small batches are actually faster due to PostgreSQL query plan caching)
- The command runs via Docker Engine API (exec into the container)

### 3. Metarecord Remap (repair)

If records were pushed without the remap checkbox, or if search results are incomplete:

- Set **Start bib id** (0 = remap all non-deleted bibs)
- This calls `SELECT metabib.remap_metarecord_for_bib(id, fingerprint, deleted)` for every matching bib
- Progress is reported in 5,000-id chunks

### 4. Restart OpenSRF Stack

**Required after a metarecord remap.** OpenSRF and mod_perl cache metarecord state in memory. Until these services are restarted, the OPAC will return stale search results.

The system restarts three services inside the Evergreen container:
1. **memcached** — Clears cached metarecord data
2. **osrf_control --restart-all** — Restarts all OpenSRF services
3. **apache2** — Restarts mod_perl workers

The OPAC will be briefly unavailable during the restart (typically 10-30 seconds).

---

## Templates

**Purpose:** Save and reuse field mapping configurations across projects.

### Template Types

| Type | Scope | Use Case |
|------|-------|----------|
| **Global** | All projects, all users | Standard mapping for a specific ILS (e.g., "Symphony to Koha") |
| **Project** | Single project | Project-specific customizations |

### Managing Templates

Navigate to **Templates** in the sidebar to:
- View all templates with source ILS, map count, and usage statistics
- **Promote** a project template to global scope (makes it available to all users)
- **Delete** templates no longer needed
- Load templates into a project from Stage 5 (with merge or replace mode)

---

## Plugins

**Purpose:** Upload and manage Koha plugins (.kpz files).

### Uploading a Plugin

1. Navigate to **Plugins** in the sidebar
2. Click **Choose File** and select a `.kpz` file
3. Click **Upload** — the file is stored on the Cerulean server

### Installing to Koha

With a project selected that has a Koha URL configured:

1. Click **Install to Koha** next to the plugin
2. Confirm the installation
3. Cerulean attempts two installation methods:
   - **REST API** (preferred) — Uploads via the Cerulean Endpoints plugin's `/plugins/upload` endpoint, then restarts Plack
   - **Docker fallback** — Copies the .kpz into the Koha container via Docker API, runs `install_plugins.pl`, then restarts Plack workers

If auto-install fails, use the **Download** link to install the plugin manually via Koha's admin interface (Administration > Plugins > Upload plugin).

### Available Plugins

| Plugin | Purpose |
|--------|---------|
| **Migration Toolkit** (`Koha::Plugin::BWS::MigrationToolkit`) | All-in-one: FastMARCImport + TurboIndex + DB tuning + preflight checks + reference data management |
| **Cerulean Endpoints** (`Koha::Plugin::BWS::CeruleanEndpoints`) | Migration mode control, reference data API, plugin management, system info |
| **FastMARCImport** | High-performance parallel MARC import (standalone) |
| **TurboIndex** | Parallel Elasticsearch reindexing (standalone) |

---

## System Settings

**Purpose:** Configure platform-wide settings without editing environment files or restarting containers.

Navigate to **System Settings** in the sidebar:

### Google OAuth
| Setting | Description |
|---------|-------------|
| Client ID | From Google Cloud Console (APIs & Services > Credentials) |
| Client Secret | From Google Cloud Console |
| Allowed Domain | Email domain for login (default: `bywatersolutions.com`) |

### Authentication
| Setting | Description |
|---------|-------------|
| JWT Expiry | How long login sessions last in minutes (default: 480 = 8 hours) |

### AI
| Setting | Description |
|---------|-------------|
| Anthropic API Key | Required for AI-assisted field mapping and patron column mapping |
| Claude Model | Which Claude model to use (default: `claude-sonnet-4-20250514`) |

Settings saved here override environment variables and take effect immediately. The OAuth client is re-registered dynamically — no container restart needed.

---

## Project Log & Audit Trail

**Purpose:** View a chronological log of every significant operation performed on a project.

Every stage writes audit events as operations proceed. Each event includes:
- **Timestamp** — When the event occurred (UTC)
- **Stage** — Which pipeline stage generated it
- **Level** — Severity (see below)
- **Tag** — Short label like `[ingest]`, `[ai-map]`, `[push]`, `[evergreen-push]`
- **Message** — Human-readable description

### Log Levels

| Level | Color | Meaning |
|-------|-------|---------|
| **Info** | Blue | Normal operation milestone (e.g., "Indexed 42,118 records") |
| **Warning** | Yellow | Non-blocking issue (e.g., "Record 5,231: invalid indicator value") |
| **Error** | Red | Failed operation (e.g., "Push failed: connection refused") |
| **Complete** | Green | Stage or task completed successfully |

### Filtering & Export

- **Stage filter** — Show events from a specific stage only
- **Level filter** — Show only errors, warnings, etc.
- **Export CSV** — Download as spreadsheet-compatible CSV
- **Export TXT** — Download as plain text log

---

## Suggestions & Feedback

**Purpose:** Submit and vote on feature requests, bug reports, and workflow ideas.

### Submitting

1. Click **Submit** in the Suggestions page
2. Choose type: Feature, Bug, Workflow, or Discussion
3. Enter a title and description
4. Submit

### Voting

Click the upvote arrow on any suggestion to signal priority. Higher-voted items are addressed first.

### Statuses

| Status | Meaning |
|--------|---------|
| Open | Submitted, awaiting review |
| Confirmed | Reviewed and accepted |
| In Progress | Actively being worked on |
| Shipped | Deployed and available |
| Closed | Not planned or resolved |

---

## Troubleshooting

### Common Issues

**"Internal Server Error" on a page**
- Check the **Project Log** for error details
- Verify the Koha URL is reachable from the Cerulean server
- Ensure database migrations are up to date: run `alembic upgrade head` in the web container

**File upload fails**
- Maximum file size is 2 GB
- Ensure the file is a valid MARC format (.mrc, .marc, .mrk, .xml, .csv)
- Check available disk space on the server

**ILS detection shows wrong ILS or low confidence**
- This is normal for files with few ILS-specific local fields
- Override the ILS in Stage 2 — it only affects which templates are suggested

**Quality scan finds thousands of issues**
- This is common for older catalogs. Focus on **errors** first (red), then **warnings** (yellow).
- Use "Auto-fix all" on categories where the fix is safe (e.g., leader byte defaults, indicator spaces)
- **Encoding errors** often mean the source data is MARC-8 or Latin-1, not UTF-8. Re-export from the source ILS with UTF-8 encoding if possible.

**Push to Koha fails**
- Verify Koha URL and credentials in Project Settings
- Test the connection from Stage 7 Setup tab
- Check that required plugins are installed (Migration Toolkit recommended for large imports)
- Ensure the Koha API is accessible from the Cerulean container (network connectivity)
- For Docker-based Koha: verify the Koha container is on the same Docker network as Cerulean

**Push succeeds but records not in OPAC search**
- **Koha**: Run an Elasticsearch reindex from Stage 7 Reindex tab. If using Migration Mode, make sure you disabled it (restarts the ES indexer daemon).
- **Evergreen**: Run Metarecord Remap (Stage 13 card 3), then Restart OpenSRF Stack (card 4). Without the remap, `metabib.metarecord_source_map` is empty and the search visibility filter hides all records.

**Google OAuth not working**
- Verify Client ID and Secret in System Settings
- Check that the redirect URI in Google Cloud Console matches exactly: `https://your-domain/api/v1/auth/google/callback`
- Ensure the Authorized JavaScript Origins includes your domain
- If using "Internal" app type, only users in your Google Workspace org can sign in

**AI mapping suggestions are empty or low quality**
- Verify the Anthropic API key in System Settings
- Ensure MARC files have been uploaded and analyzed (Stage 1 must complete)
- More records = better suggestions. The AI works best with 20+ sample records.
- If the source ILS uses unusual local field numbering, AI may miss them — create those mappings manually

**Task stuck in "running" state**
- Check the Celery worker logs for errors
- The task bar shows active tasks — click to expand details
- If a worker crashed mid-task, the task may remain in "running" state. Restart the worker container to reset.

### Getting Help

- Check the **Project Log** for detailed error messages and timestamps
- Use the **Help** page (sidebar) for quick reference on any stage
- Submit a **Suggestion** with type "Bug" for platform issues
- Contact the development team for urgent migration support
