# Cerulean Next User Manual

**ByWater Solutions MARC Migration Platform**
*Version 2.0 — April 2026*

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Dashboard & Projects](#dashboard--projects)
3. [Step 1 — Data Ingest](#step-1--data-ingest)
4. [Step 2 — ILS Detection & Migration Config](#step-2--ils-detection--migration-config)
5. [Step 3 — MARC Data Quality & Remediation](#step-3--marc-data-quality--remediation)
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
16. [MARC Tools](#marc-tools)
17. [Templates](#templates)
18. [Plugins](#plugins)
19. [Suggestions & Feedback](#suggestions--feedback)
20. [System Administration](#system-administration)
21. [OAI-PMH Harvesting](#oai-pmh-harvesting)
22. [Connecting to a Local Koha](#connecting-to-a-local-koha)
23. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Signing In

Cerulean Next uses Google OAuth for authentication. Click **Sign in with Google** and use your organizational Google account. The platform supports multiple domains — currently `@bywatersolutions.com` and `@openfifth.co.uk` accounts are authorized. Additional domains can be added via System Settings (comma-separated).

Sessions last 8 hours by default (configurable in System Settings). After signing in, your name and avatar appear in the sidebar footer. Click **Sign out** to end your session.

If this is your first time signing in, Google may show an "unverified app" warning. Click **Advanced → Go to Cerulean Next** to proceed. This is expected for internal tools.

### Navigation

The left sidebar provides access to all areas:

- **Workspace** — Dashboard and New Project
- **Project** — Steps 1–13, Project Log, Settings (visible when a project is selected)
- **MARC Tools** — SQL Explorer, Export & Extract, File Manager, Macros (visible when a project is selected)
- **Platform** — Help, Templates, Suggestions, Plugins, System Logs, System Settings

The **pipeline bar** at the top of each step page shows your progress through all steps. Completed steps show a checkmark. **Click any step in the pipeline bar to navigate directly to it.**

### Creating a Project

1. Click **New Project** in the sidebar
2. Enter a **Project Code** (short identifier, e.g., `westfield_2025`)
3. Enter the **Library Name**
4. Optionally enter the **Koha URL** and **API Token** (click the `?` button for connection help)
5. Set **Visibility**: Private (only you) or Shared (all team members)
6. Click **Create Project**

---

## Dashboard & Projects

The dashboard shows all projects you have access to:

- **Your projects** (any visibility)
- **Shared projects** from other team members (marked with a "Shared" badge)
- **Legacy projects** (created before multi-user was enabled)

Each project row shows the project code, library name, source ILS, stage progress pips, and current stage badge.

**Actions:**
- Click a project to open it
- Click **Edit** to access project settings (Koha URL, auth type, visibility)
- Click **Archive** to hide a project (use "Show archived" toggle to restore)

---

## Step 1 — Data Ingest

**Purpose:** Upload source MARC and item data files, then let the system analyze them.

### Uploading MARC Files

Drag and drop `.mrc`, `.marc`, `.mrk`, `.csv`, or `.xml` files onto the upload area, or click to browse. Files up to 2 GB are supported. The upload shows **real-time progress** including:

- Bytes transferred / total (e.g., "245.3 MB / 1.2 GB")
- Upload speed in MB/s
- Elapsed time and estimated time remaining (ETA)
- Percentage bar

### What Happens Behind the Scenes

When you upload a file, several automated tasks run in sequence:

1. **Format Detection** — Cerulean validates the MARC leader and record structure to determine if the file is ISO 2709 (binary MARC), MRK (mnemonic text), CSV, or MARCXML.

2. **Record Counting** — Every record in the file is iterated for an exact count.

3. **ILS Detection** — Cerulean samples up to 200 records and checks for ILS-specific MARC fields:
   - **SirsiDynix Symphony**: Looks for "Sirsi" in 035$a or heavy use of 999 fields
   - **Innovative Millennium/Sierra**: Detects 907 (bib record number) or 945 (item) local fields
   - **Polaris**: Detects 949 fields with $t subfield
   - **Koha**: Looks for 952 (items) or 942 (item type) fields
   - A confidence score (0–100%) is calculated as the ratio of matching records to sampled records.

4. **Tag Frequency Analysis** — Every MARC tag across all records is counted, producing a histogram showing which fields are present and how often (e.g., "245: 42,118 records, 852: 41,900 records"). Subfield-level frequency is also calculated.

5. **Elasticsearch Indexing** — If Elasticsearch is configured, records are indexed for full-text search (non-blocking).

### Uploading Item CSV Files

If items/holdings are in a separate CSV file (not embedded in MARC 952 fields):

1. Upload the CSV file using the Items upload area
2. Cerulean auto-detects the delimiter (comma, tab, pipe, or semicolon) using Python's csv.Sniffer
3. The header row is auto-detected (first line with 3+ unique, identifier-like fields)
4. If no header is detected, columns are named `col_1`, `col_2`, etc. — click any column name to rename it
5. For each column, Cerulean calculates the unique value count and collects up to 20 sample values

### MRK Export

Each indexed MARC file has a **↓ MRK** button that exports the file in MarcEdit's mnemonic text format. This is useful for:
- Opening files directly in MarcEdit for detailed editing
- Human-readable viewing of MARC data
- Round-trip editing: download MRK → edit in MarcEdit → save → re-upload

### CSV to MARC Converter

The **Convert CSV to MARC** card at the bottom of Step 1 lets you create MARC bibliographic records from a spreadsheet:

1. Upload a CSV file
2. Preview columns and sample data
3. Map each CSV column to a MARC tag and subfield (e.g., "Title" → 245$a, "Author" → 100$a)
4. Click **Convert** to generate a `.mrc` file
5. The resulting file is automatically registered and indexed

This is useful for creating bibs from acquisitions spreadsheets, donation lists, or other tabular data.

### What to Look For

- **Record count** — Does it match what the source library provided?
- **ILS signal** — Is the detected source ILS correct?
- **Tag frequency** — Are expected fields present? Missing 245 (title) is a red flag. High 852/952 counts indicate embedded items.

---

## Step 2 — ILS Detection & Migration Config

**Purpose:** Confirm the source ILS and configure how items are structured.

### ILS Confirmation

Cerulean displays the detected ILS and confidence score. You can override this if the detection was incorrect — the ILS setting influences which field mapping templates are suggested in Step 5.

### Item Structure

Select how item/holdings data is organized:

| Option | When to Use |
|--------|-------------|
| **Embedded** | Items are in MARC 952 (Koha), 949 (Polaris), 945 (Millennium), or 999 (Symphony) fields within each bib record |
| **Separate** | Items are in a separate CSV file uploaded in Step 1 |
| **None** | No item data to migrate (bibs only) |

### MARC Record Browser

Browse individual records to verify data quality:
- Navigate with arrow buttons or jump to a specific record number
- View each field with tag, indicators, and subfield breakdown
- Item tags (952, 949, 999, 852) are automatically highlighted with a detection banner

### Quick Path

For simple migrations where the data is already clean and properly mapped (e.g., Koha-to-Koha), click **Mark as Ready to Load** to skip steps 3–6 and go directly to Step 10. The file will be pushed as-is without field mapping, deduplication, or reconciliation.

---

## Step 3 — MARC Data Quality & Remediation

**Purpose:** Scan records for quality issues, fix problems, perform batch edits, analyze data distribution, and ensure RDA compliance.

Step 3 has five tabs: **Summary**, **Issues**, **Batch Edit**, **Clusters**, and **RDA Helper**.

### Summary Tab

Shows the results of the quality scan with issue counts grouped by category.

### Running a Quality Scan

1. Select which files to scan using the checkboxes
2. Click **Run Quality Scan**
3. Wait for the async task to complete (progress shown in the task bar)

### What the System Checks (8 Categories)

#### 1. Encoding Issues
Tests every field for valid UTF-8 encoding. Catches Latin-1/Windows-1252 characters that weren't converted, MARC-8 data, and corrupted byte sequences from file transfers or system conversions.

**Severity:** Error

#### 2. Diacritics
Detects **orphan combining characters** (Unicode combining marks at the start of a value without a base character) and **decomposed Unicode** (characters stored as separate base + combining mark instead of a single precomposed character). The system suggests NFC normalization.

**Severity:** Warning / Info

#### 3. Indicator Values
MARC indicators must be a space or a digit 0–9. Both indicators on every data field are checked. Common problems include letters used as indicators (often from data conversion errors) or special characters.

**Severity:** Warning

#### 4. Subfield Structure
- **245 (Title)** must have $a — records without a title subfield are flagged as errors
- **100/110/111 (Author)** should have $a — missing author subfields are flagged as warnings

#### 5. Field Length
MARC fields have a maximum length of 9,999 bytes. The system calculates the total byte length of each field (including subfield codes) and flags any that exceed this limit. Oversized fields cause import failures in most ILS systems.

**Severity:** Error

#### 6. Leader & 008 Validation
The MARC leader is a 24-character fixed field. Validated byte positions include:

| Position | Name | Valid Values |
|----------|------|-------------|
| 5 | Record status | a (increase), c (corrected), d (deleted), n (new), p (increase from prepub) |
| 6 | Type of record | a (language material), c/d (music), e/f (cartographic), g (projected), i/j (sound), k (graphic), m (computer), r (3D) |
| 7 | Bibliographic level | a (component), b (component serial), c (collection), d (subunit), i (integrating), m (monograph), s (serial) |
| 9 | Character coding scheme | space (MARC-8) or a (UTF-8) |
| 17 | Encoding level | space (full), 1–8, u (unknown), z (not applicable) |

The **008 field** must be exactly 40 characters. Records with shorter 008 fields are padded with fill characters (|), and longer ones are truncated.

#### 7. Blank / Empty Fields
Flags control fields with whitespace-only data, data fields with all-empty subfields, and records missing a 245 (title) field entirely.

#### 8. Duplicate Detection
- **001 duplicates** — Flags records sharing the same control number (often a data export error)
- **ISBN duplicates** — Extracts 020$a values, strips hyphens and qualifiers, flags records sharing the same ISBN (may indicate records to merge)

### Fixing Issues

- **Auto-fix** — The system applies safe automatic corrections: invalid leader bytes replaced with safe defaults (e.g., position 5 → 'n', position 6 → 'a'), short 008 fields padded to 40 characters, invalid indicators replaced with spaces, decomposed Unicode composed via NFC normalization. The fix is applied in-memory, then the entire file is rewritten to disk.
- **Manual edit** — Click an issue to see the full MARC record. Click any field value to edit it inline. For leader positions, a dropdown shows all valid values with descriptions.
- **Ignore** — Mark issues as "ignored" if they are acceptable for your migration.
- **Bulk actions** — "Ignore all" or "Auto-fix all" per category for high-volume processing.

### Issues Tab

Two-panel view: issue list on the left (sortable by record, tag, severity), full MARC record detail on the right. Click any issue to view the record with problematic fields highlighted. Edit fields inline.

### Batch Edit Tab

MarcEdit-style batch editing operations that apply changes across all records in your MARC file. All operations create a new `_edited.mrc` file — your original is always preserved.

#### Find & Replace
- Search for text across all records (or filtered by tag/subfield)
- Case-sensitive option
- Whole-field replacement option (replaces entire field value, not just the substring)
- Shows before/after preview for the first 5 changes

#### Regex Substitution
- Apply a regular expression substitution (e.g., `s/c(\d{4}).*/\1/` to extract a 4-digit year from 260$c)
- Supports capture groups (\1, \2)
- Case-insensitive flag
- Requires a tag; optionally filter to a specific subfield

#### Add Field
- Add a new MARC field to all records
- Configure tag, indicators, and one or more subfields (entered as `code=value`, one per line)
- Optional condition: only add if another tag exists or is missing (e.g., "add 590 only if 852 exists")

#### Delete Field
- Remove a field from all records by tag
- Optionally delete only a specific subfield (leaves the rest of the field intact)
- Optional filter: only delete if the field contains specific text (e.g., "delete 852 only if it contains 'DISCARD'")

#### Generate Call Numbers
- Copy classification data (082$a Dewey, 050$a LC, or 090$a local) into a target call number field (default: 952$o)
- Automatically appends an author cutter (first letter of 100$a surname)
- "Skip if exists" option to avoid overwriting records that already have call numbers
- Useful for libraries whose source data has classification but no formatted call numbers

### Clusters Tab

Group records by any MARC field or subfield to see the distribution of values. This is invaluable for data review before reconciliation.

1. Enter a field (e.g., `852$a` for branch codes, `942$c` for item types, `650$a` for subjects)
2. Click **Build Clusters** (or use a quick-access button for common fields)
3. View the frequency table showing each unique value, record count, percentage, and a visual distribution bar
4. Export clusters as CSV for offline analysis

Use cases:
- **Before reconciliation**: "How many unique branch codes are there? What are they?" → `852$a`
- **Item type distribution**: "What item types exist in this data?" → `942$c` or `999$a`
- **Subject analysis**: "What are the most common subjects?" → `650$a`
- **Data cleanup**: "Are there variant spellings?" → cluster by author or publisher

### RDA Helper Tab

Auto-generate RDA-compliant 336 (Content Type), 337 (Media Type), and 338 (Carrier Type) fields based on each record's MARC leader bytes.

#### Scan
Click **Scan** to check how many records are missing each RDA field. Shows counts for missing 336, 337, and 338 separately.

#### Apply
Select which fields to generate (336, 337, 338 — all checked by default), optionally check "Overwrite existing RDA fields," and click **Apply RDA**. The system:

1. Reads leader position 6 (Type of record) to determine content type (text, performed music, cartographic image, etc.)
2. Maps to the appropriate media type (unmediated, audio, video, computer)
3. Uses leader positions 6+7 to determine carrier type (volume, audio disc, online resource, etc.)
4. Creates properly formatted 336/337/338 fields with $a (term), $b (code), and $2 (source vocabulary)

A reference table at the bottom shows the complete leader-to-RDA mapping.

**Example**: A record with leader position 6 = "a" (language material), position 7 = "m" (monograph) will get:
- 336 $a text $b txt $2 rdacontent
- 337 $a unmediated $b n $2 rdamedia
- 338 $a volume $b nc $2 rdacarrier

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

The **Compare Versions** tab lets you select two version snapshots and diff them. The system shows:
- Records added, removed, changed, and unchanged
- Field-level differences for changed records

### Comparing Files

The **Compare Files** tab lets you diff any two `.mrc` files in the project (not just version snapshots). Select two files from the dropdown and click Compare to see record count differences.

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
2. Select the **source** MARC tag and subfield (e.g., 852$h) from a datalist populated with your file's actual tags
3. Select the **target** Koha tag and subfield (e.g., 952$o for call number) from a datalist of Koha MARC fields
4. Optionally add a **transform**:
   - **Copy** — Direct passthrough (most common)
   - **Regex** — Perl-style substitution (e.g., `s/^REF /R/i`)
   - **Lookup** — JSON table mapping (e.g., `{"JUV": "JUVENILE", "REF": "REFERENCE"}`)
   - **Constant** — Fixed value for every record (e.g., always set 942$c to "BK")
   - **Function** — Sandboxed Python expression (safe builtins only: len, str, int, float — no file access or imports)
   - **Preset** — Built-in transforms (see below)
5. Optionally check **Move** (delete source field after copy)
6. Click **Save**

### Available Presets

| Preset | What It Does |
|--------|-------------|
| date_mdy_to_dmy | Convert MM/DD/YYYY to DD/MM/YYYY |
| date_dmy_to_iso | Convert DD/MM/YYYY to YYYY-MM-DD |
| date_ymd_to_iso | Convert YYYYMMDD to YYYY-MM-DD |
| date_marc_to_iso | Convert MARC date format to ISO |
| case_upper / lower / title / sentence | Change letter case |
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

1. The system sends Claude the tag frequency report (top 100 tags) and up to 50 sample MARC records in text format
2. Claude analyzes the field structure and suggests mappings based on known ILS patterns
3. Each suggestion includes a confidence score (0–100%) and reasoning
4. Suggestions appear with `AI Suggested` labels — **you must approve each one individually**

The AI is particularly good at identifying:
- Location codes (852$a/b, 949$l, 945$l → 952$a/b)
- Call numbers (092, 099, 852$h+i, 949$a → 952$o)
- Barcodes (876$p, 949$i, 945$i → 952$p)
- Item types (source-specific fields → 942$c)

### Templates

Templates save a set of field mappings for reuse across projects.

#### Saving Templates
Click **Save Template** to save your current approved mappings. Choose:
- **Project scope** — available only in this project
- **Global scope** — shared across all projects and users

#### Loading Templates
Click **Load Template** to apply a previously saved template. Choose:
- **Merge** mode — adds template maps without overwriting existing approved maps
- **Replace** mode — removes all existing maps and applies the template

#### Importing Templates from CSV or Google Sheets
Click **↑ Import CSV** to create a template from a spreadsheet. Two import methods:

1. **Upload CSV** — Select a CSV file with columns: `source_tag, source_sub, target_tag, target_sub, transform_type, transform_fn, preset_key, delete_source, notes`
2. **Google Sheets URL** — Paste a Google Sheets link (the sheet must be shared as "Anyone with the link can view"). Cerulean fetches the CSV directly — no download step needed.

Only `source_tag` and `target_tag` are required columns. Everything else defaults to `copy` transform.

#### Exporting Templates as CSV
Click **↓ CSV** on any template card to download it as a CSV. Open in Google Sheets or Excel, edit, and re-import.

#### Template Preview
Click any template in the Templates page to preview its full mapping table with source/target fields, transform badges, MOVE indicators, and notes.

#### Sample Template
Click **+ Sample Template** to create a demonstration "Symphony → Koha" template with 31 mappings including bibliographic fields, item field moves (852→952), item type lookups (999→942), and location code mappings.

---

## Step 6 — Transform

**Purpose:** Apply your field mappings to the MARC data and build the output file.

### What Happens During a Build

1. **Validation** — Each approved field map is validated: source/target tags must be 3-digit numeric, transform type must be valid, regex expressions tested, lookup tables parsed as JSON, preset keys verified.

2. **Per-Record Transformation** — For each record in each source file:
   - Maps are grouped by (source_tag, target_tag) to preserve repeating field relationships
   - For each instance of a source field, one target field is built
   - Transforms are applied to each subfield value
   - Source fields are deleted only after all maps finish reading them (prevents data loss when multiple maps read the same source)
   - Per-map statistics track: records with the source field, successful transforms, errors

3. **Items CSV Joining** (if "Include items data" is checked):
   - Loads approved ItemColumnMap rows (from Step 5)
   - Reads the items CSV into a lookup table keyed by the match column
   - For each MARC record, extracts the match value (e.g., 001) and finds matching CSV rows
   - Each matching row becomes a 952 field with subfields populated from the column map

4. **Output** — Transformed records are written to `output.mrc`. The first record is fully logged to the audit trail showing which source tags existed, which were mapped, and which were missing.

---

## Step 7 — Item & Bib Reconciliation

**Purpose:** Map source item field values to Koha's controlled vocabularies. Source ILS systems use different codes than Koha — this step creates the translation rules and rewrites the values in your MARC file.

**When to skip:** If your source data already uses the exact same codes as Koha (e.g., Koha-to-Koha migration), you can skip this step and go straight to Load. The Load Setup tab will still show you what reference data Koha needs — but it won't change your file's values.

**When to use:** If migrating from Symphony, Polaris, Millennium, or any non-Koha ILS where item codes differ (e.g., "JUV" → "JUVENILE", "MAIN" → "MAINLIB").

### Vocabulary Categories

The system scans MARC 952 fields for values in these subfields:

| Category | 952 Subfield | Koha Field | Example Source | Example Koha |
|----------|-------------|------------|---------------|-------------|
| itype | $y | Item type | "BK", "DVD", "JBOOK" | "BK", "DVD", "JBK" |
| loc | $c | Shelving location | "ADULT", "CHILD" | "ADULT", "CHILD" |
| ccode | $8 | Collection code | "FICTION", "NONFIC" | "FIC", "NF" |
| not_loan | $7 | Not for loan | "1", "REFERENCE" | "1", "-1" |
| withdrawn | $0 | Withdrawn status | "LOST", "DISCARD" | "1" |
| damaged | $4 | Damaged status | "WATER", "TORN" | "1" |
| homebranch | $a | Home library | "MAIN", "BR1" | "MAIN", "BRANCH1" |
| holdingbranch | $b | Current holding | Same as homebranch | Same |

### Workflow

1. **Source** — Confirm the MARC file to scan
2. **Scan** — Extract unique values per category (results persist — no need to re-scan unless the file changes)
3. **Rules** — Create mapping rules for each source value
4. **Validate** — Check rules for completeness
5. **Apply** — Rewrite the MARC file with rules applied → produces `Biblios-mapped-items.mrc`
6. **Report** — Review results with per-category CSV exports

### Rule Operations

| Operation | What It Does |
|-----------|-------------|
| **Rename** | Change one value to another (e.g., "JUV" → "JUVENILE") |
| **Merge** | Combine multiple source values into one target (e.g., "JV", "JUV", "JUVEN" all → "JUVENILE") |
| **Split** | Conditionally assign different targets based on other fields in the same record (e.g., if 952$a = "MAIN" then "ADULT", else "CHILD") |
| **Delete** | Remove the subfield ("subfield" mode) or the entire 952 field ("field" mode) |

---

## Step 8 — Patron Data

**Purpose:** Import patron/borrower records into Koha from various source formats.

### Supported Formats

| Format | Detection |
|--------|-----------|
| **CSV** | Auto-detects delimiter (comma, tab, pipe, semicolon) and encoding (via chardet) |
| **TSV** | Tab-separated values |
| **Excel** (.xlsx, .xls) | Reads via openpyxl/xlrd with configurable sheet name and header row |
| **XML** | Flattens repeating elements — auto-detects the most common element as the "row" |
| **MARC** (.mrc) | Flattens each record to key-value pairs (tag$subcode: value) |

### Column Mapping

Map each source column to a Koha borrower field. Cerulean supports all 78 standard Koha borrower fields including:
- **Identity**: cardnumber, surname, firstname, title, othernames, initials, pronouns
- **Contact**: email, emailpro, phone, phonepro, mobile, fax
- **Address**: address, address2, city, state, zipcode, country (+ B_ fields for alternate)
- **Library**: branchcode, categorycode, sort1, sort2
- **Dates**: dateofbirth, dateenrolled, dateexpiry
- **Status**: lost, debarred, debarredcomment, gonenoaddress
- **Auth**: userid, password
- **Other**: borrowernotes, opacnote, patron_attributes

### AI-Assisted Column Mapping

Click **AI Suggest** to have Claude analyze column headers and up to 20 sample rows. The AI:
- Suggests mappings with confidence scores
- Identifies columns needing transforms (e.g., splitting a "Name" column into surname + firstname)
- Flags controlled list columns (categorycode, branchcode, title, lost)
- Auto-detects name formats: "Last, First" / "First Last" / "First Middle Last"

### Available Transforms

| Transform | What It Does |
|-----------|-------------|
| copy | Direct passthrough |
| name_split | Split full name into parts (surname, firstname, middlename) |
| split | Split by delimiter and take a specific index |
| date_mdy_to_iso | MM/DD/YYYY → YYYY-MM-DD |
| date_dmy_to_iso | DD/MM/YYYY → YYYY-MM-DD |
| case_upper / lower / title | Change letter case |
| gender_normalize | Normalize gender values (M/F/X) |
| first_char | Extract first character |
| const | Fixed value for every patron |
| regex | Pattern-based substitution |

### Value Reconciliation

After column mapping, scan for controlled list values that need translation. Create rename, merge, split, or delete rules (same as Step 7).

---

## Step 9 — Patron Versions

Same concept as Step 4 (Bib Versions) but for patron records. Create snapshots after each iteration of patron loading and compare versions.

---

## Step 10 — Load (Push to Koha)

**Purpose:** Push your prepared MARC and patron data into the target Koha instance. This is the final action step — all data preparation should be complete before pushing.

### Setup Tab

| Action | What It Does |
|--------|-------------|
| **Scan MARC** | Analyzes the output file for controlled values — identifies values that need to exist in Koha before import |
| **Fetch Reference Data** | Pulls current libraries, patron categories, item types, and authorized values from Koha via REST API |
| **Verify Counts** | Shows current bib/item/patron counts in Koha's MariaDB and Elasticsearch |
| **Clear Koha Data** | Truncates bibs/items (destructive — for re-migration only) |

#### Reference Data Management

The comparison grid shows what your MARC data references vs. what exists in Koha. For each category (Libraries, Patron Categories, Item Types, Locations, Collection Codes):

- Edit descriptions inline
- **↓ CSV** — Download as a spreadsheet for offline editing
- **↓ SQL** — Download as `INSERT IGNORE` statements for direct Koha database loading
- **↑ Upload CSV** — Import edited descriptions back
- **Push Missing** — Create missing values in Koha via the REST API

### Migration Mode

For large migrations (10,000+ records), enabling **Migration Mode** dramatically improves import speed by:

**Stopping non-essential daemons:**

| Daemon | Purpose | Why Stop It |
|--------|---------|-------------|
| koha-es-indexer | Elasticsearch incremental indexer | Biggest resource hog |
| koha-indexer | Zebra incremental indexer | Legacy search indexer |
| koha-zebra | Zebra search server | Not needed during bulk load |
| koha-sip | SIP2 circulation protocol | No patrons checking out |
| koha-z3950-responder | Z39.50 search responder | No external searches |

**Database tuning:** `innodb_flush_log_at_trx_commit = 2` (async flush, 10–50x faster writes) and increased buffer pool.

**Important:** Always disable Migration Mode after import completes.

### Push Methods

| Method | Speed | Requirements | Best For |
|--------|-------|-------------|----------|
| **REST API** | ~1 rec/sec | No plugins | Small imports, testing |
| **FastMARCImport** | Parallel workers | Plugin required | Medium imports |
| **Migration Toolkit** | Fastest (all-in-one) | Plugin required | Large migrations |

### Reindex Tab

After loading, run an Elasticsearch reindex to make records searchable in the OPAC. Use **TurboIndex Plugin** for parallel reindexing on large catalogs.

---

## Step 11 — Holds & Remaining Data

*Coming soon.* Will support: active holds/reserves, checkout history, fines and fees, reading history.

---

## Step 12 — Aspen Discovery

**Purpose:** Bulk migrate data from Koha into an Aspen Discovery instance.

- **Turbo Migration** — Parallel extraction from Koha's DB directly into Aspen. Typical: ~940K records in 8 minutes (8 workers).
- **Turbo Reindex** — Rebuild Aspen's Solr index. Typical: ~940K records in 25 minutes.
- **Indexer Control** — Stop/start Aspen's background indexers during bulk load.

---

## Step 13 — Evergreen ILS

**Purpose:** Bulk migrate MARC records into Evergreen via direct PostgreSQL access.

Evergreen stores records as **MARCXML text** in `biblio.record_entry`. Cerulean converts from ISO 2709 automatically.

### Push Bibs — Trigger Modes

| Mode | Speed | What's Disabled | Post-Insert Work |
|------|-------|-----------------|-----------------|
| Skip indexing only | Fast | `aaa_indexing_ingest_or_delete` + `bbb_simple_rec_trigger` | pingest + metarecord remap |
| All triggers on | Slowest | Nothing | None |
| All triggers off | Fastest | ALL triggers | pingest + remap + everything |

### Critical: Metarecord Remap

When triggers are disabled, `metabib.metarecord_source_map` is NOT populated. Records are **invisible to OPAC search** until `metabib.remap_metarecord_for_bib()` is called. The remap checkbox (on by default) handles this automatically in 5,000-id chunks.

### Restart OpenSRF

After remap: restart memcached + osrf_control + apache2 inside the Evergreen container. Required because OpenSRF and mod_perl cache metarecord state.

---

## MARC Tools

The MARC Tools section appears in the sidebar when a project is selected. It provides powerful data analysis and manipulation tools.

### SQL Explorer

Query your MARC data using SQL-like syntax:

```sql
SELECT 001, 245$a, 852$a WHERE 942$c = 'DVD' LIMIT 100
SELECT 001, 245$a WHERE 020$a EXISTS
SELECT * WHERE 245$a LIKE '%journal%' LIMIT 10
SELECT 001, 100$a, 245$a ORDER BY 100$a LIMIT 50
```

**Supported operators:**
- `=`, `!=` — exact match
- `CONTAINS`, `NOT CONTAINS` — substring search
- `LIKE` — SQL LIKE with % and _ wildcards
- `STARTS WITH` — prefix match
- `EXISTS`, `MISSING` — field presence check
- `AND`, `OR` — combine conditions
- `ORDER BY ... ASC/DESC` — sort results
- `LIMIT` — cap result count (default 500)

**Features:**
- **Schema browser** — click "Show Schema" to see all available tags/subfields from your data, clickable to insert into the query
- **Example queries** — quick-access buttons for common queries
- **Export results** — download query results as CSV

### Export & Extract

#### Export to Spreadsheet
Select specific MARC fields to export as columns in a CSV or TSV file. For example: export `001, 245$a, 852$a, 952$p` to get a spreadsheet of control numbers, titles, branches, and barcodes. Optionally filter by tag/value (e.g., "only records where 942$c = DVD").

#### Extract Records
Pull records matching criteria into a separate `.mrc` file. Supports multiple operators (equals, contains, not contains, starts with, exists, missing, regex) with AND/OR logic. Output is a new MARC file that can be processed independently.

#### JSON Export/Import
- **Export as JSON** — Download all records as JSON (pymarc dict format) for API integrations or data exchange
- **Import from JSON** — Upload a JSON file to create MARC records, automatically registered and indexed

### File Manager

#### List Files
Browse all `.mrc` files in the project directory tree with sizes.

#### Split Files
- **By record count** — Break a large file into chunks (e.g., 10,000 records per file). Useful for parallel processing or testing with subsets.
- **By field value** — Create separate files per unique value in a field (e.g., split by 852$a to get one file per branch). Each file is named with the field value.

#### Join Files
Merge multiple `.mrc` files into one. Optionally deduplicate by 001 control number (skips records with duplicate 001 values).

### Macros

Save and replay sequences of batch edit operations. Macros are reusable across projects.

#### Creating a Macro
1. Click **+ Create Macro**
2. Give it a name and description
3. Define the operations as a JSON array:

```json
[
  {"type": "find_replace", "find": "old text", "replace": "new text", "tag": "245"},
  {"type": "delete_field", "tag": "9XX"},
  {"type": "regex", "tag": "260", "subfield": "c", "pattern": "c(\\d{4}).*", "replacement": "\\1"},
  {"type": "add_field", "tag": "590", "subfields": [{"code": "a", "value": "Migrated from Symphony"}]}
]
```

**Operation types:** `find_replace`, `regex`, `add_field`, `delete_field`

#### Running a Macro
Click **Run** on any macro to execute all operations in sequence on the current project's MARC data. Results show per-operation statistics.

---

## Templates

**Purpose:** Save and reuse field mapping configurations across projects.

### Template Types

| Type | Scope | Use Case |
|------|-------|----------|
| **Global** | All projects, all users | Standard mapping for a specific ILS (e.g., "Symphony to Koha") |
| **Project** | Single project | Project-specific customizations |

### Managing Templates

- **Preview** — Click any template to see its full mapping table
- **↓ CSV** — Download as a spreadsheet
- **Load** — Apply to current project (merge or replace mode)
- **↑ Global** — Promote a project template to global scope
- **Delete** — Remove a template
- **↑ Import** — Create from CSV file or Google Sheets URL

---

## Plugins

**Purpose:** Upload and manage Koha plugins (.kpz files).

### Uploading
Select a `.kpz` file and click Upload. The file is stored on the Cerulean server.

### Installing to Koha
With a project selected that has a Koha URL, click **Install to Koha**. Cerulean tries:
1. **REST API** (preferred) — uploads via the Cerulean Endpoints plugin
2. **Docker fallback** — copies into the container via Docker API, runs `install_plugins.pl`

Both methods automatically restart Plack workers after installation.

### Available Plugins

| Plugin | Purpose |
|--------|---------|
| **Migration Toolkit** | All-in-one: FastMARCImport + TurboIndex + DB tuning + preflight |
| **Cerulean Endpoints** | Migration mode control, reference data, plugin management |
| **FastMARCImport** | High-performance parallel MARC import (standalone) |
| **TurboIndex** | Parallel Elasticsearch reindexing (standalone) |

---

## Suggestions & Feedback

**Purpose:** Submit, discuss, and track feature requests, bug reports, and workflow ideas.

### Two-Panel Layout
- **Left** — List of suggestions with vote counts, type badges, and comment counts
- **Right** — Detail view with full description, status, and threaded comments

### Features
- **Submit** new suggestions (Feature, Bug, Workflow, or Discussion)
- **Vote** — Click the upvote arrow to signal priority
- **Comment** — Post threaded comments on any suggestion for team discussion
- **Edit** — Modify your own suggestion's title, body, and type
- **Delete** — Remove your own suggestions
- **Status management** — Change status via dropdown: Open, Confirmed, In Progress, Fixed, Shipped, Future Dev, Won't Fix, System Action, Closed

---

## System Administration

### System Settings
Configure platform-wide settings from the browser (no server restart needed):

| Setting | Description |
|---------|-------------|
| Google OAuth Client ID/Secret | From Google Cloud Console |
| Allowed Domains | Comma-separated email domains (e.g., `bywatersolutions.com,openfifth.co.uk`) |
| JWT Expiry | Login session duration in minutes (default: 480 = 8 hours) |
| Anthropic API Key | Required for AI field mapping and patron column mapping |
| Claude Model | Which model to use for AI features |

### System Logs
View authentication events and user activity:

- **Registered Users** table — name, email, avatar, last login, join date, status
- **Server Logs** viewer with filters:
  - **All** — every logged event
  - **Authentication** — logins, rejections, new users
  - **Errors** — server errors and tracebacks

Every significant user action (POST/PATCH/PUT/DELETE requests) is logged with the user's email address, so you can see who did what and when.

### Plugins
Upload, download, and auto-install Koha plugins. See the [Plugins](#plugins) section for details.

---

## OAI-PMH Harvesting

Cerulean provides an OAI-PMH 2.0 endpoint that allows MarcEdit and other OAI clients to harvest MARC records directly from a project.

### Base URL

```
https://cerulean-next.gallagher-family-hub.com/api/v1/oai/{project_id}
```

### Supported Verbs

| Verb | Description |
|------|-------------|
| `Identify` | Repository description |
| `ListMetadataFormats` | Available formats (marcxml) |
| `ListRecords` | Harvest all records (paginated, 100 per batch) |
| `GetRecord` | Retrieve a single record by identifier |

### Using with MarcEdit

1. Open MarcEdit → **Tools → OAI Harvester**
2. Enter the base URL with your project ID
3. Set metadata format to `marcxml`
4. Click **Harvest**

### Access Control

Only **shared** and legacy (no owner) projects are harvestable. Private projects return an error. No authentication is required for OAI access — the endpoint is public for shared projects.

---

## Connecting to a Local Koha

If your Koha instance runs locally (KTD on your workstation), use an SSH tunnel to forward the port to the Cerulean server. Full instructions for Mac, Windows, and Linux are available via **Help → Local Koha Guide (.md)** or the `?` button next to the Koha URL field.

**Quick start (Mac/Linux):**
```bash
ssh -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N
```

Then set the Koha URL in Project Settings to `http://172.19.0.1:8081` (Docker gateway IP).

Ask Brendan for the server password if you don't have it.

---

## Troubleshooting

### Quality scan finds thousands of issues
Focus on **errors** (red) first, then **warnings** (yellow). Use "Auto-fix all" for safe categories (leader defaults, indicator spaces, NFC normalization). **Encoding errors** often mean the source data is MARC-8 or Latin-1 — re-export from the source ILS with UTF-8 if possible.

### Push succeeds but records not in OPAC search
- **Koha**: Run Elasticsearch reindex (Step 10 Reindex tab). If Migration Mode is on, disable it first.
- **Evergreen**: Run Metarecord Remap + Restart OpenSRF (Step 13 cards 3 & 4).

### File upload fails or is slow
- Maximum file size: 2 GB (configurable via nginx `client_max_body_size`)
- The upload progress bar shows speed and ETA
- If upload completes but file doesn't appear: check the Celery worker is running (`docker compose logs worker`)

### AI mapping suggestions are empty
- Verify the Anthropic API key in System Settings
- Ensure Step 1 completed (records analyzed)
- More records = better suggestions (20+ sample records ideal)

### Push to Koha fails
- Verify URL & credentials in Project Settings (click `?` for connection help)
- Test from Step 10 Setup → Run Preflight
- For Docker-based Koha: verify containers are on the same Docker network or use SSH tunnel

### Task stuck in "running" state
Check the Celery worker logs. If a worker crashed, restart it. The task bar shows active tasks — click to expand details.

### OAuth not working
- Check Client ID/Secret in System Settings
- Redirect URI must match exactly: `https://cerulean-next.gallagher-family-hub.com/api/v1/auth/google/callback`
- For multi-domain: ensure the app type is **External** in Google Cloud Console
- Multi-domain allowed domains are comma-separated in System Settings

### Getting Help
- Check the **Project Log** for detailed error messages and timestamps
- Use the **Help** page (sidebar) for quick reference on any step
- Submit a **Suggestion** with type "Bug" for platform issues
- View **System Logs** for authentication and user activity history
- Contact the development team for urgent migration support
