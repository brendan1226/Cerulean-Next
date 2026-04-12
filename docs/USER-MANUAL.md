# Cerulean Next User Manual

**ByWater Solutions MARC Migration Platform**
*Version 1.0 — April 2026*

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Dashboard & Projects](#dashboard--projects)
3. [Stage 1 — Data Ingest](#stage-1--data-ingest)
4. [Stage 2 — ILS Detection & Migration Config](#stage-2--ils-detection--migration-config)
5. [Stage 3 — MARC Data Quality](#stage-3--marc-data-quality)
6. [Stage 4 — Bib Versions](#stage-4--bib-versions)
7. [Stage 5 — Field Mapping](#stage-5--field-mapping)
8. [Stage 6 — Transform](#stage-6--transform)
9. [Stage 7 — Load (Push to Koha)](#stage-7--load-push-to-koha)
10. [Stage 8 — Item & Bib Reconciliation](#stage-8--item--bib-reconciliation)
11. [Stage 9 — Patron Data](#stage-9--patron-data)
12. [Stage 10 — Patron Versions](#stage-10--patron-versions)
13. [Stage 11 — Holds & Remaining Data](#stage-11--holds--remaining-data)
14. [Stage 12 — Aspen Discovery](#stage-12--aspen-discovery)
15. [Stage 13 — Evergreen ILS](#stage-13--evergreen-ils)
16. [Templates](#templates)
17. [Plugins](#plugins)
18. [System Settings](#system-settings)
19. [Project Log & Audit Trail](#project-log--audit-trail)
20. [Suggestions & Feedback](#suggestions--feedback)
21. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Signing In

Cerulean Next uses Google OAuth for authentication. Click **Sign in with Google** and use your `@bywatersolutions.com` account. Only ByWater Solutions team members can access the platform.

After signing in, your name and avatar appear in the sidebar footer. Click **Sign out** to end your session.

### Navigation

The left sidebar provides access to all areas:

- **Workspace** — Dashboard and New Project
- **Project** — Stages 1-13, Project Log, Settings (visible when a project is selected)
- **Platform** — Templates, Suggestions, Plugins, System Settings

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

Each project row shows the project code, library name, source ILS, stage progress pips, and current stage badge.

**Actions:**
- Click a project to open it
- Click **Edit** to access project settings
- Click **Archive** to hide a project (use "Show archived" toggle to restore)

---

## Stage 1 — Data Ingest

**Purpose:** Upload your source MARC and item data files.

### Uploading MARC Files

1. Drag and drop `.mrc`, `.marc`, `.mrk`, `.csv`, or `.xml` files onto the upload area, or click to browse
2. Files up to 2 GB are supported
3. After upload, Cerulean automatically:
   - Detects the file format (ISO 2709, MRK, CSV, MARCXML)
   - Counts records
   - Analyzes tag frequency (which MARC fields are present and how often)
   - Attempts to detect the source ILS

### Uploading Item CSV Files

If items/holdings are in a separate CSV file (not embedded in MARC 952 fields):

1. Upload the CSV file
2. Cerulean detects it as an items file based on column headers
3. You can recategorize any file between "MARC" and "Items CSV" if auto-detection was wrong

### What to Look For

- **Record count** — verify it matches your expectations
- **ILS signal** — the detected source ILS (Symphony, Polaris, Millennium, etc.)
- **Tag frequency** — check that expected fields (245, 001, 852/952) are present

---

## Stage 2 — ILS Detection & Migration Config

**Purpose:** Confirm the source ILS and configure how items are structured.

### ILS Confirmation

Cerulean displays the detected ILS and confidence score. You can override this if the detection was incorrect.

### Item Structure

Select how item/holdings data is organized:

- **Embedded** — Items are in MARC 952 (or similar) fields within each bib record
- **Separate** — Items are in a separate CSV file (uploaded in Stage 1)
- **None** — No item data to migrate

### MARC Record Browser

Browse individual records to verify data quality:

- Use arrow buttons to navigate between records
- View each field with tag, indicators, and subfield breakdown
- Identify item tags (952, 949, 999, 852) automatically

### Quick Path

If you don't need field mapping, dedup, or reconciliation (e.g., a simple Koha-to-Koha migration), click **Mark as Ready to Load** to skip directly to Stage 7.

---

## Stage 3 — MARC Data Quality

**Purpose:** Scan records for quality issues and fix them before loading.

### Running a Scan

1. Select which files to scan using the checkboxes
2. Click **Run Quality Scan**
3. Wait for the async task to complete (progress shown in the task bar)

### Reviewing Issues

Issues are grouped by category:

- **Encoding** — Character set problems (non-UTF-8 data)
- **Diacritics** — Missing or malformed diacritical marks
- **Indicators** — Invalid indicator values
- **Subfield structure** — Missing or duplicate subfields
- **Field length** — Fields exceeding MARC limits
- **Leader/008** — Invalid leader bytes or control field values
- **Duplicates** — Duplicate records detected
- **Blank fields** — Empty fields that should have content

Each category card shows counts by severity (error/warning/info) and how many can be auto-fixed.

### Fixing Issues

- **Auto-fix** — Click the auto-fix button on a category to apply all safe automatic corrections
- **Manual edit** — Click an issue to see the full MARC record. Click any field value to edit it inline.
- **Ignore** — Mark issues as "ignored" if they are acceptable
- **Bulk actions** — "Ignore all" or "Auto-fix all" per category

---

## Stage 4 — Bib Versions

**Purpose:** Create snapshots of your bibliographic data at different stages of the migration.

### Creating a Snapshot

Click **Create Version** to capture the current state of your MARC data. Each version records:

- Record count
- All field mappings at the time of creation
- Quality scan results
- A frozen copy of the MARC file

### Comparing Versions

1. Select two versions from the dropdowns
2. Click **Compare**
3. View the differences between the two snapshots

Use versions to track your progress and verify that transformations produce the expected results.

---

## Stage 5 — Field Mapping

**Purpose:** Define how source MARC fields map to Koha fields.

### Three-Panel Layout

- **Left panel** — File selector and tag frequency browser
- **Center panel** — Record inspector (navigate and view individual records)
- **Right panel** — Field mapping editor

### Creating Mappings

1. Click **+ Add** in the right panel
2. Select the source MARC tag and subfield
3. Select the target Koha field
4. Optionally add transform rules (copy, regex, constant, lookup)
5. Click **Save**

### AI-Assisted Mapping

Click **AI Suggest** to have Claude analyze your MARC data and suggest field mappings automatically. Review each suggestion and approve or reject.

### Templates

- **Save as Template** — Save your current mappings as a reusable template
- **Load Template** — Apply a previously saved template
- Templates can be **project-scoped** (this project only) or **global** (shared across all projects)

---

## Stage 6 — Transform

**Purpose:** Apply your field mappings and build the output MARC file.

### Building Output

1. Select the MARC files to transform
2. Optionally check **Include items data** if you have a separate items CSV
3. If including items, configure the **key column** (CSV) and **match tag** (MARC) for joining
4. Click **Build Output**
5. Wait for the async task to complete

The transform creates a new MARC file with all mappings applied, ready for loading.

### Results

View the transformation manifest showing:
- Records processed
- Records successfully transformed
- Records with errors
- Output file location

---

## Stage 7 — Load (Push to Koha)

**Purpose:** Push your prepared MARC data into the target Koha instance.

### Setup Tab

Before pushing records, verify your environment:

1. **Scan MARC** — Analyze the output file for controlled values (item types, locations, etc.)
2. **Fetch Reference Data** — Pull current reference data from Koha (libraries, patron categories, item types, authorized values)
3. **Verify Counts** — Check existing record counts in Koha
4. **DB Stats** — View MariaDB and Elasticsearch statistics

### Migration Mode

For large migrations, enable **Migration Mode** to:
- Stop background daemons (indexer, SIP, Z39.50)
- Apply database tuning (larger buffer pool, relaxed flush settings)
- Dramatically speed up import

Remember to **disable Migration Mode** after the import completes.

### Bibs Tab

Push bibliographic records using one of several methods:

- **REST API** — Standard Koha staging + commit (works with any Koha version)
- **FastMARCImport Plugin** — High-performance parallel import
- **Migration Toolkit Plugin** — All-in-one import with DB tuning and reindexing

### Reindex Tab

After loading records, trigger an **Elasticsearch reindex** to make records searchable in the OPAC. Options:

- **Full reindex** — Rebuild the entire search index
- **TurboIndex Plugin** — Parallel reindex for faster completion

### Manifests Tab

View the audit trail of all push operations with status, record counts, and timing.

---

## Stage 8 — Item & Bib Reconciliation

**Purpose:** Map source item field values to Koha's controlled vocabularies.

### Vocabulary Categories

Cerulean scans your MARC 952 fields for values in these categories:

| Category | Koha Field | Description |
|----------|------------|-------------|
| itype | 952$y | Item type (book, DVD, audiobook, etc.) |
| loc | 952$c | Shelving location |
| ccode | 952$8 | Collection code |
| not_loan | 952$7 | Not for loan status |
| withdrawn | 952$0 | Withdrawn status |
| damaged | 952$4 | Damaged status |
| homebranch | 952$a | Home library branch code |
| holdingbranch | 952$b | Current holding branch code |

### Workflow

1. **Source** — Confirm the MARC file to scan
2. **Scan** — Run the scan to extract unique values per category
3. **Rules** — Create mapping rules (e.g., "JUV" -> "JUVENILE", "REF" -> "REF")
4. **Validate** — Check rules for completeness and conflicts
5. **Apply** — Apply rules to transform item data
6. **Report** — Review results

---

## Stage 9 — Patron Data

**Purpose:** Import patron/borrower records into Koha.

### Uploading Patron Files

Upload patron data in CSV, TSV, Excel (.xlsx/.xls), XML, or MARC format. Cerulean auto-detects the format and parses column headers.

### Column Mapping

Map each column in your patron file to a Koha borrower field:

- surname, firstname, email, phone, address, city, state, zipcode
- categorycode, branchcode, cardnumber, dateenrolled, dateexpiry
- And many more Koha borrower fields

Use **AI Suggest** for automatic column mapping based on header names and sample data.

### Value Reconciliation

Map source patron category codes, library codes, and other controlled values to Koha equivalents:

- **Category codes** — Adult, Juvenile, Staff, etc.
- **Branch codes** — Main, Branch1, Branch2, etc.
- **Title codes** — Mr., Mrs., Ms., Dr., etc.

### Apply & Push

Apply the column mappings and value rules, then push patron records to Koha.

---

## Stage 10 — Patron Versions

**Purpose:** Snapshot patron data at different stages (same as Stage 4 for bibs).

Create version snapshots after each iteration of patron loading. Compare versions to verify changes.

---

## Stage 11 — Holds & Remaining Data

**Purpose:** Migrate holds/reserves, circulation history, and other remaining data.

*Coming soon.* This stage will support:

- Active holds/reserves (linking patrons to bibliographic records)
- Checkout history
- Fines and fees
- Reading history

---

## Stage 12 — Aspen Discovery

**Purpose:** Bulk migrate data from Koha into an Aspen Discovery instance.

### Configuration

Enter the Aspen Discovery URL (e.g., `http://aspen:85`).

### Turbo Migration

High-performance parallel extraction from Koha's database directly into Aspen's database:

1. Set the number of **workers** (1-16)
2. Set the **batch size**
3. Click **Start Turbo Migration**
4. Monitor progress via the auto-refreshing record count cards

### Turbo Reindex

After migration, rebuild Aspen's Solr search index:

1. Optionally check **Clear Solr first**
2. Set workers and batch size
3. Click **Start Turbo Reindex**

### Indexer Control

- **Stop Indexers** — Pause Aspen's background indexing during bulk load
- **Start Indexers** — Resume indexing after load completes

---

## Stage 13 — Evergreen ILS

**Purpose:** Bulk migrate MARC records into an Evergreen ILS instance via direct PostgreSQL access.

### Database Connection

Configure the Evergreen PostgreSQL connection:
- Host, Port, Database name, Username, Password
- Click **Test Connection** to verify

### 1. Push Bibs

Insert MARCXML records into `biblio.record_entry`:

- **Batch Size** — Records per INSERT batch (default: 500)
- **Trigger Mode**:
  - *Skip indexing only* (recommended) — Disables heavy indexing triggers for speed
  - *All triggers on* — Slowest but safest
  - *All triggers off* — Fastest, requires full pingest afterward
- **Remap metarecords** — Checkbox to populate `metabib.metarecord_source_map` after insert (required for search visibility)

### 2. Parallel Ingest (pingest)

Build search indexes after bulk loading:

- **Max Child** — Parallel worker processes (default: 2)
- **Batch Size** — Records per pingest batch (default: 25)

### 3. Metarecord Remap (repair)

If records are invisible in OPAC search after a push, run a standalone metarecord remap:

- Set **Start bib id** (0 = all bibs)
- This calls `metabib.remap_metarecord_for_bib()` for each record

### 4. Restart OpenSRF Stack

After a metarecord remap, restart Evergreen's service stack:
- Restarts **memcached**, **osrf_control --restart-all**, and **apache2**
- Required because OpenSRF caches metarecord state

---

## Templates

**Purpose:** Save and reuse field mapping configurations across projects.

### Template Types

- **Global** — Available to all projects and users
- **Project** — Scoped to a single project

### Managing Templates

Navigate to **Templates** in the sidebar to:
- View all templates with source ILS, map count, and usage statistics
- **Promote** a project template to global scope
- **Delete** templates no longer needed
- Load templates into a project from Stage 5

---

## Plugins

**Purpose:** Upload and manage Koha plugins (.kpz files).

### Uploading a Plugin

1. Navigate to **Plugins** in the sidebar
2. Click **Choose File** and select a `.kpz` file
3. Click **Upload**

### Installing to Koha

With a project selected that has a Koha URL configured:

1. Click **Install to Koha** next to the plugin
2. Confirm the installation
3. Cerulean uploads the plugin and restarts Plack workers automatically

If auto-install fails, use the **Download** link to install the plugin manually via Koha's admin interface.

### Available Plugins

| Plugin | Purpose |
|--------|---------|
| **Migration Toolkit** | All-in-one: FastMARCImport + TurboIndex + DB tuning + preflight |
| **Cerulean Endpoints** | Migration mode control, reference data, plugin management |
| **FastMARCImport** | High-performance parallel MARC import |
| **TurboIndex** | Parallel Elasticsearch reindexing |

---

## System Settings

**Purpose:** Configure platform-wide settings (admin).

Navigate to **System Settings** in the sidebar to manage:

### Google OAuth
- **Client ID** — From Google Cloud Console
- **Client Secret** — From Google Cloud Console
- **Allowed Domain** — Email domain for login (default: `bywatersolutions.com`)

### Authentication
- **JWT Expiry** — How long login sessions last (default: 480 minutes / 8 hours)

### AI
- **Anthropic API Key** — Required for AI-assisted field mapping
- **Claude Model** — Which Claude model to use for AI features

Settings saved here override environment variables and take effect immediately without a container restart.

---

## Project Log & Audit Trail

**Purpose:** View a chronological log of all operations performed on a project.

### Filtering

- **Stage filter** — Show events from a specific stage
- **Level filter** — Show only errors, warnings, info, or completion events

### Exporting

Click **Export CSV** or **Export TXT** to download the log for external analysis or record-keeping.

### Log Levels

| Level | Meaning |
|-------|---------|
| **Info** | Normal operation milestone |
| **Warning** | Non-blocking issue that may need attention |
| **Error** | Failed operation |
| **Complete** | Stage or task completed successfully |

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
- Check the Project Log for error details
- Verify the Koha URL is reachable from the Cerulean server
- Ensure database migrations are up to date: `alembic upgrade head`

**File upload fails**
- Maximum file size is 2 GB
- Ensure the file is a valid MARC format (.mrc, .marc, .mrk, .xml, .csv)
- Check available disk space on the server

**Push to Koha fails**
- Verify Koha URL and credentials in Project Settings
- Test the connection from Stage 7 Setup tab
- Check that required plugins are installed (Migration Toolkit recommended)
- Ensure the Koha API is accessible from the Cerulean container

**Records not appearing in OPAC search after Koha push**
- Run an Elasticsearch reindex from Stage 7 Reindex tab
- For Evergreen: run Metarecord Remap + Restart OpenSRF Stack

**Google OAuth not working**
- Verify Client ID and Secret in System Settings
- Check that the redirect URI matches exactly in Google Cloud Console
- Ensure the allowed domain matches your email domain

**AI mapping suggestions are empty**
- Verify the Anthropic API key in System Settings
- Check that MARC files have been uploaded and analyzed (Stage 1)

### Getting Help

- Check the **Project Log** for detailed error messages
- Submit a **Suggestion** with type "Bug" for platform issues
- Contact the development team for urgent migration support
