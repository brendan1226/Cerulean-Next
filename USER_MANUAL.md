# Cerulean-Next — User Manual

Cerulean-Next guides you through migrating library data from a legacy ILS into Koha. The workflow is divided into 6 stages, each building on the previous one.

---

## Table of Contents

1. [Dashboard & Projects](#1-dashboard--projects)
2. [Stage 1: Data Ingest](#2-stage-1-data-ingest)
3. [Stage 2: Analyze & Map](#3-stage-2-analyze--map)
4. [Stage 3: Transform](#4-stage-3-transform)
5. [Stage 4: Deduplication](#5-stage-4-deduplication)
6. [Stage 5: Push to Koha](#6-stage-5-push-to-koha)
7. [Stage 6: Patron Data Transformation](#7-stage-6-patron-data-transformation)
8. [Project Log](#8-project-log)
9. [Templates](#9-templates)
10. [Tips & Best Practices](#10-tips--best-practices)

---

## 1. Dashboard & Projects

### Creating a Project

1. Click **"New Project"** on the dashboard
2. Fill in:
   - **Project Code** — a short identifier (e.g., `MYLIB`)
   - **Library Name** — the full library name
   - **Source ILS** — the system you're migrating from (e.g., "SirsiDynix Symphony", "Koha", "Evergreen", "CARL.X")
3. Click **Create**

Each project tracks one library migration. The dashboard shows all projects with colored stage-progress indicators.

### Project Settings

Click the gear icon on any project to edit:
- Library name and source ILS
- **Koha URL** — the base URL of your target Koha instance (e.g., `https://koha.mylibrary.org`)
- **Koha API Token** — your Koha REST API token (stored encrypted)
- **Archive** — hide completed projects from the dashboard

---

## 2. Stage 1: Data Ingest

**Goal:** Upload your source MARC or CSV files so Cerulean can analyze them.

### Uploading Files

1. Navigate to **Stage 1** from the sidebar
2. Click **"Choose file"** and select a MARC file (.mrc, .iso2709, .mrk) or CSV file
3. Click **Upload** — the file is uploaded and a background task begins processing it

Processing includes:
- Counting records
- Building a tag frequency histogram (which MARC tags appear and how often)
- Detecting the source ILS from MARC data patterns

### Viewing Files

The file list shows:
- Filename, format, record count, and status
- Click a file to expand the **tag frequency** viewer — this shows which MARC fields are present and how many records contain each

### Record Preview

Click the **preview** icon on any file to browse individual records. Use the left/right arrows to navigate between records.

### When to Proceed

Move to Stage 2 once all your files are uploaded and showing status **"indexed"**.

---

## 3. Stage 2: Analyze & Map

**Goal:** Create field mappings that tell Cerulean how to convert your source MARC tags into Koha-compatible MARC fields.

### Understanding Field Maps

A field map is a rule that says: "Take data from source tag X, subfield Y and put it in target tag A, subfield B, applying transform Z."

For example:
- Source `099$a` → Target `952$o` (copy call number to Koha items)
- Source `260$c` → Target `264$c` with date extraction transform

### AI-Assisted Mapping

1. Click **"AI Suggest"** to dispatch an AI analysis task
2. Claude analyzes your tag frequencies and sample records
3. Suggestions appear with confidence scores (0-100%)
4. Review each suggestion — click **Approve** to accept, or edit before approving

### Manual Mapping

1. Click **"Add Map"** to create a mapping manually
2. Select the **source tag/subfield** from the dropdown (populated from your uploaded data)
3. Select the **target tag/subfield** (Koha MARC fields)
4. Choose a **transform type**:
   - **copy** — direct copy, no modification
   - **regex** — regular expression substitution
   - **const** — constant value (ignores source, writes a fixed value)
   - **preset** — built-in transform (date conversion, case change, ISBN cleaning, etc.)
   - **fn** — custom Python expression (advanced)
   - **lookup** — JSON lookup table
5. Click **Save**

### Approving Maps

Only **approved** maps are used in Stage 3 (Transform). You can:
- Approve maps one at a time
- Use **"Approve All"** with a confidence threshold (e.g., approve all AI suggestions with 85%+ confidence)

### Validation

Click **"Validate"** to check your approved maps for issues:
- Duplicate target fields
- Missing required Koha fields
- Invalid tag/subfield combinations

### Loading Templates

If you've migrated from the same ILS before, you can load a saved template:
1. Go to the **Templates** section
2. Select a template matching your source ILS
3. Click **"Load"** — this creates maps from the template (still need approval)

---

## 4. Stage 3: Transform

**Goal:** Apply your approved field maps to convert source records into Koha-compatible MARC.

### Running a Transform

1. Navigate to **Stage 3**
2. Click **"Start Transform"**
3. The progress bar shows records processed in real-time
4. When complete, each source file has a corresponding `_transformed.mrc` file

### Preview

Use the **Preview** tab to compare original vs. transformed records side-by-side. Transformed fields are highlighted in green.

### Merge

After transforming, use the **Merge** tab to:
1. Combine multiple transformed files into a single `merged.mrc`
2. Optionally join an **items CSV** — this adds 952 (items) fields to records based on a CSV file you upload

### Pause/Resume

Long transforms can be paused and resumed. The worker saves its position and continues from where it stopped.

### Re-running

If you change your maps, click **"Clear"** to remove transformed files, then run the transform again.

---

## 5. Stage 4: Deduplication

**Goal:** Find and resolve duplicate records in your merged file.

### Dedup Rules

Cerulean provides 4 preset duplicate-detection rules:
- **001 match** — records with identical control numbers
- **ISBN match** — records sharing the same ISBN
- **Title + Author** — fuzzy matching on title and author fields
- **OCLC number** — records with the same OCLC number

Click **"Create Presets"** to add all four, or create custom rules.

### Scanning

1. Click **"Scan"** to search for duplicates using active rules
2. Results appear as **clusters** — groups of records that match each other

### Resolving Clusters

For each cluster, choose a resolution strategy:
- **Keep first** — keep the first record, discard others
- **Keep most fields** — keep the record with the most data
- **Keep most items** — keep the record with the most 952 fields
- **Merge holdings** — combine item records from all duplicates into one bib
- **Write exceptions** — flag for manual review

### Applying

Click **"Apply"** to write the deduplicated output file (`merged_deduped.mrc`).

---

## 6. Stage 5: Push to Koha

**Goal:** Send your processed records to a Koha instance.

### Setup

Before pushing, configure your Koha connection in **Project Settings**:
- **Koha URL** — e.g., `https://koha.mylibrary.org`
- **Koha API Token** — generated from Koha's administration panel

### Preflight Check

1. Click **"Run Preflight"**
2. Cerulean tests connectivity, detects the Koha version, and checks the search engine
3. If preflight passes, you're ready to push

### Push Types

| Tab | What it does |
|-----|-------------|
| **Bibs** | Push bibliographic MARC records via Koha's bulkmarcimport |
| **Patrons** | Import patron CSV data |
| **Holds** | Import hold requests |
| **Circ** | Import circulation (checkout) data |

### File Review

Before pushing, review your files in the **Files** tab:
- Preview individual records
- Download MARC files for manual inspection

### Push Manifests

Each push creates a manifest tracking:
- Records attempted, succeeded, and failed
- Error details for failed records
- Dry-run mode (test without actually importing)

---

## 7. Stage 6: Patron Data Transformation

**Goal:** Transform patron data (CSV, TSV, TXT, XML) into Koha-compatible borrower format.

Stage 6 has its own sub-pipeline with four tabs:

### Upload & Parse

1. Click **"Upload Patron File"** and select a file (CSV, TSV, TXT, or XML)
2. Cerulean auto-detects the format, delimiter, and encoding
3. Once parsed, a **Data Preview** table shows the first 30 rows with pagination
4. You can upload multiple patron files — they're combined for mapping

**Alternatively**, if your patron data was already uploaded in Stage 1 as a MARC file, click **"Select Existing File"** to reuse it.

### Column Mapping

Map source columns from your patron data to Koha borrower fields:

1. The table shows each source column with:
   - **Koha Header** — dropdown of all 77 Koha borrower fields (cardnumber, surname, firstname, dateofbirth, categorycode, branchcode, etc.)
   - **Transform** — how to modify the data (copy, split, date conversion, case change, regex, constant)
   - **Controlled** — checkbox for fields that use controlled value lists
   - **Status** — Approved/Pending (click to toggle)

2. **AI Suggest** — click to have Claude analyze your columns and suggest mappings automatically

3. **Splitting fields** — if a source column contains combined data (e.g., "Smith, John"):
   - Click the **Split** button on that row
   - Choose a delimiter (comma, space, pipe, etc.)
   - Preview shows how the data splits
   - Map each part to a Koha header

4. **Regex/Const transforms** — click the config button (shown as a summary like `/pattern/repl/` or `="value"`) to enter:
   - **Regex:** pattern and replacement string
   - **Const:** a fixed value to write for every record
   - **Split:** delimiter and part index

5. **Adding mappings** — click **"+ Add Mapping"** to create additional column maps. The source column dropdown is populated from your uploaded file headers.

6. **Approve All** — batch-approve all AI suggestions meeting a confidence threshold

### Value Reconciliation

For controlled-list columns (categorycode, branchcode, etc.), reconcile source values to valid Koha values:

1. Click **"Scan Values"** — Cerulean reads the mapped data and extracts all unique values for controlled columns
2. The scanned values appear in a table showing each unique value and its record count
3. Create **rules** to transform values:
   - **Rename** — change "Adult" to "ADULT"
   - **Merge** — combine "Jr" and "Junior" into "JR"
   - **Split** — split "Adult/Child" into separate categories based on conditions
   - **Delete** — exclude rows with certain values, or blank the field
4. Click **"Validate"** to check for unmatched values (source values with no rule)

### Apply & Report

1. Click **"Apply"** to run the full patron transformation pipeline
2. Cerulean applies column maps, transforms, and value rules to produce a Koha-ready CSV
3. The **Report** shows:
   - Total records processed
   - Records included/excluded
   - Per-column transform results
4. Download the output CSV for import into Koha

---

## 8. Project Log

Every action in Cerulean is logged. Access the log from the sidebar:

- **Filter by stage** (1-6) or **level** (info, warning, error, complete)
- **Live streaming** — events appear in real-time via SSE
- **Export** — download the log as CSV or TXT

The log is especially useful for:
- Tracking transform progress (per-map applied/skipped/error counts)
- Debugging failed tasks
- Auditing what was done and when

---

## 9. Templates

Templates save your field mappings so you can reuse them across projects.

### Saving a Template

1. Go to Stage 2 of a project with approved maps
2. Click **"Save as Template"**
3. Name it (e.g., "SirsiDynix Symphony → Koha 23.05")

### Loading a Template

1. Open a new project's Stage 2
2. Click **"Load Template"**
3. Select a template — filter by source ILS
4. Maps are created as unapproved suggestions — review and approve them

### Promoting Templates

Templates start as **project-scoped**. Click **"Promote"** to make them **global** (available to all projects).

---

## 10. Tips & Best Practices

### General Workflow

1. Upload all source files before starting mapping
2. Use AI suggestions as a starting point, then refine manually
3. Always validate maps before transforming
4. Preview transformed records to spot issues early
5. Run dedup scan before pushing to Koha

### Large Files

- Files with 100k+ records may take several minutes to process
- Use the progress bar and Flower dashboard to monitor long-running tasks
- Transform supports pause/resume for very large jobs

### Patron Data

- Upload a small sample file first to test your column mappings
- Use the Data Preview to verify the source data looks correct before mapping
- For name fields that combine first and last names, use the Split feature
- Always scan and reconcile values for controlled-list fields before applying

### Troubleshooting

- **Task stuck at "PENDING"** — check that the Celery worker is running: look at Flower (http://localhost:5555) or `docker compose logs worker`
- **"Error" status on a file** — check the Project Log for the error message
- **Transform produces unexpected results** — use the Preview tab to compare original vs. transformed records. Check that the correct maps are approved.
- **AI suggestions seem wrong** — the AI works best with complete tag frequency data. Make sure files are fully indexed before requesting suggestions.

### Keyboard Shortcuts

- **Hard refresh** the browser (Ctrl+Shift+R / Cmd+Shift+R) after code updates to pick up frontend changes
- The UI auto-refreshes task status every 2 seconds while tasks are running
