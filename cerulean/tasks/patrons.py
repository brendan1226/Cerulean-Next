"""
cerulean/tasks/patrons.py
─────────────────────────────────────────────────────────────────────────────
Stage 6 Celery tasks: Patron Data Transformation.

    patron_parse_task    — parse uploaded patron file into normalised CSV
    patron_ai_map_task   — AI-suggest column mappings to Koha borrower headers
    patron_scan_task     — scan parsed data for controlled list column values
    patron_apply_task    — apply mappings + rules, write patrons_transformed.csv

All tasks write AuditEvent rows via AuditLogger.
"""

import csv
import json
import os
import uuid
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)

_PROGRESS_INTERVAL = 500

# ── Koha borrower controlled list headers ──────────────────────────────

PATRON_CONTROLLED_HEADERS: dict[str, dict] = {
    "categorycode": {
        "koha_endpoint": "/api/v1/patron_categories",
        "description": "Borrower type (required)",
    },
    "branchcode": {
        "koha_endpoint": "/api/v1/libraries",
        "description": "Home library (required). Cross-validate with Stage 5.",
    },
    "title": {
        "koha_endpoint": "/api/v1/authorised_value_categories/TITLE/authorised_values",
        "description": "Salutation",
    },
    "lost": {
        "koha_endpoint": "/api/v1/authorised_value_categories/LOST/authorised_values",
        "description": "Lost card status",
    },
}

# ── All standard Koha borrower CSV headers ─────────────────────────────

KOHA_BORROWER_HEADERS: list[str] = [
    "cardnumber", "surname", "firstname", "title", "othernames",
    "initials", "streetnumber", "streettype", "address", "address2",
    "city", "state", "zipcode", "country", "email", "phone",
    "mobile", "fax", "emailpro", "phonepro", "B_streetnumber",
    "B_streettype", "B_address", "B_address2", "B_city", "B_state",
    "B_zipcode", "B_country", "B_email", "B_phone", "dateofbirth",
    "branchcode", "categorycode", "dateenrolled", "dateexpiry",
    "gonenoaddress", "lost", "debarred", "debarredcomment",
    "contactname", "contactfirstname", "contacttitle", "borrowernotes",
    "relationship", "sex", "password", "flags", "userid",
    "opacnote", "contactnote", "sort1", "sort2",
    "altcontactfirstname", "altcontactsurname", "altcontactaddress1",
    "altcontactaddress2", "altcontactaddress3", "altcontactstate",
    "altcontactzipcode", "altcontactcountry", "altcontactphone",
    "smsalertnumber", "sms_provider_id", "privacy", "privacy_guarantor_checkouts",
    "checkprevcheckout", "updated_on", "lastseen", "lang",
    "login_attempts", "overdrive_auth_token", "anonymized",
    "autorenew_checkouts", "patron_attributes",
]


# ══════════════════════════════════════════════════════════════════════════
# PATRON PARSE TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.patrons.patron_parse_task",
    max_retries=0,
    queue="patrons",
)
def patron_parse_task(self, project_id: str, patron_file_id: str) -> dict:
    """Parse uploaded patron file and write normalised parsed.csv.

    Supports CSV, TSV, Excel (.xlsx/.xls), fixed-width, XML, and Patron MARC.
    Auto-detects format from extension and content sniffing.

    Args:
        project_id: UUID of the project.
        patron_file_id: UUID of the PatronFile row.

    Returns:
        dict with row_count, columns, format.
    """
    from cerulean.models import PatronFile

    log = AuditLogger(project_id=project_id, stage=6, tag="[patron-parse]")
    log.info("Patron file parse starting")

    try:
        with Session(_engine) as db:
            pf = db.get(PatronFile, patron_file_id)
            if not pf:
                log.error(f"PatronFile {patron_file_id} not found")
                return {"error": "patron_file_not_found"}
            filename = pf.filename
            file_format = pf.file_format
            parse_settings = pf.parse_settings or {}
            pf.status = "parsing"
            db.commit()

        project_dir = Path(settings.data_root) / project_id / "patrons"
        raw_dir = project_dir / "raw"
        raw_path = raw_dir / filename

        if not raw_path.is_file():
            log.error(f"Raw file not found: {raw_path}")
            with Session(_engine) as db:
                pf = db.get(PatronFile, patron_file_id)
                if pf:
                    pf.status = "error"
                    db.commit()
            return {"error": "file_not_found"}

        # Parse based on format
        rows: list[dict] = []
        detected_format = file_format

        ext = raw_path.suffix.lower()

        if ext in (".csv", ".tsv", ".txt", ".dat", ".prn"):
            rows, detected_format = _parse_csv_tsv(raw_path, parse_settings, ext)
        elif ext in (".xlsx", ".xls"):
            rows, detected_format = _parse_excel(raw_path, parse_settings, ext)
        elif ext == ".xml":
            rows, detected_format = _parse_xml(raw_path, parse_settings)
        elif ext in (".mrc", ".marc"):
            rows, detected_format = _parse_patron_marc(raw_path, parse_settings)
        else:
            # Try CSV as default
            rows, detected_format = _parse_csv_tsv(raw_path, parse_settings, ext)

        if not rows:
            log.warn("Parsed 0 rows — check file format and settings")
            # Write empty per-file CSV so concat still works
            project_dir.mkdir(parents=True, exist_ok=True)
            per_file_csv = project_dir / f"parsed_{patron_file_id}.csv"
            per_file_csv.write_text("")
            with Session(_engine) as db:
                pf = db.get(PatronFile, patron_file_id)
                if pf:
                    pf.status = "parsed"
                    pf.row_count = 0
                    pf.column_headers = []
                    pf.detected_format = detected_format
                    db.commit()
            _concat_patron_files(project_id)
            return {"row_count": 0, "columns": [], "format": detected_format}

        # Normalise: write per-file parsed_{patron_file_id}.csv
        # Compute column union preserving order (handles rows with different keys)
        columns = []
        seen_cols = set()
        for row in rows:
            for key in row:
                if key not in seen_cols:
                    columns.append(key)
                    seen_cols.add(key)
        project_dir.mkdir(parents=True, exist_ok=True)
        per_file_csv = project_dir / f"parsed_{patron_file_id}.csv"

        with open(str(per_file_csv), "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        # Update PatronFile record
        with Session(_engine) as db:
            pf = db.get(PatronFile, patron_file_id)
            if pf:
                pf.status = "parsed"
                pf.row_count = len(rows)
                pf.column_headers = columns
                pf.detected_format = detected_format
                db.commit()

        # Concatenate all per-file CSVs into parsed.csv
        _concat_patron_files(project_id)

        log.complete(
            f"Patron file parsed — {len(rows):,} rows, "
            f"{len(columns)} columns, format={detected_format}"
        )
        return {
            "row_count": len(rows),
            "columns": columns,
            "format": detected_format,
        }

    except Exception as exc:
        log.error(f"Patron parse failed: {exc}")
        with Session(_engine) as db:
            pf = db.get(PatronFile, patron_file_id)
            if pf:
                pf.status = "error"
                db.commit()
        raise


# ══════════════════════════════════════════════════════════════════════════
# PATRON AI MAP TASK
# ══════════════════════════════════════════════════════════════════════════


_PATRON_AI_SYSTEM_PROMPT = """You are an expert library systems migration specialist.
You will be given column headers and sample rows from a patron/borrower data export.
Your task is to suggest mappings from the source columns to Koha borrower CSV headers.

KOHA BORROWER HEADERS (most important):
- cardnumber: Library card number / barcode
- surname, firstname: Name fields
- title: Salutation (Mr, Mrs, Dr, etc.) — CONTROLLED LIST
- address, address2, city, state, zipcode, country: Primary address
- email, phone, mobile: Contact
- dateofbirth: Date of birth (YYYY-MM-DD format in Koha)
- branchcode: Home library code — CONTROLLED LIST, REQUIRED
- categorycode: Patron category/type — CONTROLLED LIST, REQUIRED
- dateenrolled, dateexpiry: Enrollment dates (YYYY-MM-DD)
- lost: Lost card status — CONTROLLED LIST
- borrowernotes: Staff notes
- sex: M or F
- patron_attributes: Extended attributes (pipe-separated)
- userid: Login username
- password: Hashed or plaintext password

AVAILABLE TRANSFORMS:
- copy: Direct copy (default)
- split: Split a field and take one part (provide delimiter and index in transform_config).
  Example: full name "Smith, John" → surname with {delimiter:", ",index:0}, firstname with {delimiter:", ",index:1}.
  Use index 0 for first part, 1 for second, -1 for last. A single source column can be mapped multiple times with different split configs.
- date_mdy_to_iso: Convert MM/DD/YYYY to YYYY-MM-DD
- date_dmy_to_iso: Convert DD/MM/YYYY to YYYY-MM-DD
- date_ymd_to_iso: Normalise YYYY-MM-DD
- date_marc_to_iso: Convert YYYYMMDD to YYYY-MM-DD
- case_upper, case_lower, case_title: Case transforms
- const: Constant value (provide in transform_config.value)
- regex: Regex substitution (provide pattern/replacement in transform_config)

RULES:
1. Only suggest mappings you are confident about.
2. For each suggestion provide: source_column, target_header, transform_type,
   transform_config (object or null), is_controlled_list (boolean),
   confidence (0.0–1.0), reasoning (1–2 sentences).
3. Mark columns that map to categorycode, branchcode, title, or lost as is_controlled_list=true.
4. If a column should be ignored (internal IDs, timestamps), suggest target_header=null with reasoning.

Return ONLY a JSON array of suggestion objects. No preamble, no markdown fences."""


@celery_app.task(
    bind=True,
    name="cerulean.tasks.patrons.patron_ai_map_task",
    max_retries=0,
    queue="patrons",
)
def patron_ai_map_task(self, project_id: str) -> dict:
    """Call Claude API for patron column mapping suggestions.

    Reads parsed patron data, sends column headers + sample rows to Claude,
    writes PatronColumnMap rows with approved=False.

    Args:
        project_id: UUID of the project.

    Returns:
        dict with suggestions_written, suggestions_replaced, suggestions_skipped.
    """
    from cerulean.models import PatronColumnMap

    log = AuditLogger(project_id=project_id, stage=6, tag="[patron-ai]")
    log.info("Patron AI column mapping starting")

    try:
        # Read parsed CSV headers + sample rows
        parsed_csv = Path(settings.data_root) / project_id / "patrons" / "parsed.csv"
        if not parsed_csv.is_file():
            log.error("parsed.csv not found — run parse first")
            return {"error": "parsed_csv_not_found"}

        headers = []
        sample_rows = []
        with open(str(parsed_csv), "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            for i, row in enumerate(reader):
                if i >= 20:
                    break
                sample_rows.append(row)

        if not headers:
            log.error("No headers found in parsed.csv")
            return {"error": "no_headers"}

        log.info(f"Sending {len(headers)} columns, {len(sample_rows)} sample rows to Claude")

        # Build prompt
        header_list = "\n".join(f"  - {h}" for h in headers)
        sample_text = json.dumps(sample_rows[:10], indent=2, default=str)

        user_message = (
            f"SOURCE COLUMN HEADERS:\n{header_list}\n\n"
            f"SAMPLE ROWS ({len(sample_rows)} rows, first 10 shown):\n{sample_text}"
        )

        # Call Claude API
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        response = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=4096,
            system=_PATRON_AI_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        raw_text = response.content[0].text.strip()
        suggestions = _parse_ai_suggestions(raw_text)
        log.info(f"Claude returned {len(suggestions)} suggestions")

        # Write PatronColumnMap rows
        written = 0
        skipped = 0
        replaced = 0

        with Session(_engine) as db:
            existing = db.execute(
                select(PatronColumnMap).where(PatronColumnMap.project_id == project_id)
            ).scalars().all()

            approved_keys = {m.source_column for m in existing if m.approved}
            unapproved_by_key = {
                m.source_column: m for m in existing if not m.approved
            }

            for s in suggestions:
                src_col = s.get("source_column", "").strip()
                if not src_col:
                    continue

                # Never override an approved map
                if src_col in approved_keys:
                    skipped += 1
                    continue

                # Replace existing unapproved map
                existing_map = unapproved_by_key.get(src_col)
                if existing_map:
                    existing_map.target_header = s.get("target_header")
                    existing_map.transform_type = s.get("transform_type", "copy")
                    existing_map.transform_config = s.get("transform_config")
                    existing_map.is_controlled_list = bool(s.get("is_controlled_list", False))
                    existing_map.ai_confidence = float(s.get("confidence", 0.5))
                    existing_map.ai_reasoning = s.get("reasoning")
                    existing_map.ai_suggested = True
                    existing_map.source_label = "ai"
                    replaced += 1
                    continue

                row = PatronColumnMap(
                    id=str(uuid.uuid4()),
                    project_id=project_id,
                    source_column=src_col,
                    target_header=s.get("target_header"),
                    ignored=s.get("target_header") is None,
                    transform_type=s.get("transform_type", "copy"),
                    transform_config=s.get("transform_config"),
                    is_controlled_list=bool(s.get("is_controlled_list", False)),
                    approved=False,
                    ai_suggested=True,
                    ai_confidence=float(s.get("confidence", 0.5)),
                    ai_reasoning=s.get("reasoning"),
                    source_label="ai",
                )
                db.add(row)
                written += 1

            db.commit()

        log.complete(
            f"Patron AI mapping complete — {written} new, {replaced} updated, "
            f"{skipped} skipped (approved). Engineer review required."
        )
        return {
            "suggestions_written": written,
            "suggestions_replaced": replaced,
            "suggestions_skipped": skipped,
        }

    except Exception as exc:
        log.error(f"Patron AI map task failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# PATRON SCAN TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.patrons.patron_scan_task",
    max_retries=0,
    queue="patrons",
)
def patron_scan_task(self, project_id: str) -> dict:
    """Scan parsed patron data and extract unique values for controlled list columns.

    Identifies controlled list columns from approved PatronColumnMap entries
    where is_controlled_list=True. Deletes existing scan results before scanning.

    Args:
        project_id: UUID of the project.

    Returns:
        dict with rows_scanned and categories breakdown.
    """
    from cerulean.models import PatronColumnMap, PatronScanResult

    log = AuditLogger(project_id=project_id, stage=6, tag="[patron-scan]")
    log.info("Patron scan starting")

    try:
        # Load approved controlled list column maps
        with Session(_engine) as db:
            maps = db.execute(
                select(PatronColumnMap).where(
                    PatronColumnMap.project_id == project_id,
                    PatronColumnMap.approved == True,  # noqa: E712
                    PatronColumnMap.is_controlled_list == True,  # noqa: E712
                    PatronColumnMap.target_header.isnot(None),
                )
            ).scalars().all()

            # Build {source_column: koha_header}
            controlled_columns = {
                m.source_column: m.target_header for m in maps
            }

        if not controlled_columns:
            log.warn("No approved controlled list column mappings found")
            return {"rows_scanned": 0, "categories": {}}

        log.info(f"Scanning {len(controlled_columns)} controlled list columns: {list(controlled_columns.values())}")

        # Read parsed CSV
        parsed_csv = Path(settings.data_root) / project_id / "patrons" / "parsed.csv"
        if not parsed_csv.is_file():
            log.error("parsed.csv not found — run parse first")
            return {"error": "parsed_csv_not_found"}

        # Delete existing scan results
        with Session(_engine) as db:
            db.execute(
                delete(PatronScanResult)
                .where(PatronScanResult.project_id == project_id)
            )
            db.commit()

        # Scan: accumulate {(koha_header, value): count}
        value_counts: dict[tuple[str, str], int] = defaultdict(int)
        rows_scanned = 0

        with open(str(parsed_csv), "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows_scanned += 1
                for src_col, koha_hdr in controlled_columns.items():
                    val = row.get(src_col, "").strip()
                    if val:
                        value_counts[(koha_hdr, val)] += 1

                if rows_scanned % _PROGRESS_INTERVAL == 0:
                    self.update_state(state="PROGRESS", meta={
                        "rows_scanned": rows_scanned,
                    })

        # Bulk-insert scan results
        now = datetime.utcnow()
        with Session(_engine) as db:
            for (koha_hdr, value), count in value_counts.items():
                db.add(PatronScanResult(
                    project_id=project_id,
                    koha_header=koha_hdr,
                    source_value=value,
                    record_count=count,
                    scanned_at=now,
                ))
            db.commit()

        # Build category summary
        categories: dict[str, int] = defaultdict(int)
        for (hdr, _val), _count in value_counts.items():
            categories[hdr] += 1

        log.complete(
            f"Patron scan complete — {rows_scanned:,} rows, "
            f"{len(value_counts)} distinct values across {len(categories)} categories"
        )
        return {
            "rows_scanned": rows_scanned,
            "distinct_values": len(value_counts),
            "categories": dict(categories),
        }

    except Exception as exc:
        log.error(f"Patron scan failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# PATRON APPLY TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.patrons.patron_apply_task",
    max_retries=0,
    queue="patrons",
)
def patron_apply_task(self, project_id: str) -> dict:
    """Apply approved column mappings and value rules, write patrons_transformed.csv.

    Reads parsed patron data, applies column name mappings, inline transforms,
    and value reconciliation rules for controlled list columns. Writes output
    CSV with Koha borrower headers.

    Args:
        project_id: UUID of the project.

    Returns:
        dict with stats: rows_in, rows_out, rules_applied, etc.
    """
    from cerulean.models import PatronColumnMap, PatronValueRule, Project

    log = AuditLogger(project_id=project_id, stage=6, tag="[patron-apply]")
    log.info("Patron apply starting")

    try:
        # Load approved column maps
        with Session(_engine) as db:
            maps = db.execute(
                select(PatronColumnMap).where(
                    PatronColumnMap.project_id == project_id,
                    PatronColumnMap.approved == True,  # noqa: E712
                    PatronColumnMap.ignored == False,  # noqa: E712
                    PatronColumnMap.target_header.isnot(None),
                )
                .order_by(PatronColumnMap.sort_order)
            ).scalars().all()

            # Snapshot maps
            column_maps = []
            for m in maps:
                column_maps.append({
                    "source_column": m.source_column,
                    "target_header": m.target_header,
                    "transform_type": m.transform_type or "copy",
                    "transform_config": m.transform_config or {},
                    "is_controlled_list": m.is_controlled_list,
                })

            # Load active value rules
            rules = db.execute(
                select(PatronValueRule).where(
                    PatronValueRule.project_id == project_id,
                    PatronValueRule.active == True,  # noqa: E712
                )
                .order_by(PatronValueRule.sort_order)
            ).scalars().all()

            rules_data = []
            for r in rules:
                rules_data.append({
                    "koha_header": r.koha_header,
                    "operation": r.operation,
                    "source_values": set(r.source_values) if r.source_values else set(),
                    "target_value": r.target_value,
                    "split_conditions": r.split_conditions,
                    "delete_mode": r.delete_mode or "blank_field",
                })

        if not column_maps:
            log.error("No approved column mappings found")
            return {"error": "no_approved_maps"}

        # Build value rule lookup: {(koha_header, source_value): rule}
        rule_lookup: dict[tuple[str, str], dict] = {}
        for rule in rules_data:
            for sv in rule["source_values"]:
                rule_lookup[(rule["koha_header"], sv)] = rule

        log.info(f"Loaded {len(column_maps)} column maps, {len(rules_data)} value rules")

        # Read parsed CSV
        parsed_csv = Path(settings.data_root) / project_id / "patrons" / "parsed.csv"
        if not parsed_csv.is_file():
            log.error("parsed.csv not found")
            return {"error": "parsed_csv_not_found"}

        project_dir = Path(settings.data_root) / project_id / "patrons"
        output_path = project_dir / "patrons_transformed.csv"
        controlled_dir = project_dir / "controlled"
        controlled_dir.mkdir(parents=True, exist_ok=True)

        # Determine output headers from mappings
        output_headers = []
        seen_headers = set()
        for cm in column_maps:
            hdr = cm["target_header"]
            if hdr and hdr not in seen_headers:
                output_headers.append(hdr)
                seen_headers.add(hdr)

        rows_in = 0
        rows_out = 0
        rows_excluded = 0
        rules_applied = 0

        # Track final values for controlled list columns
        final_values: dict[tuple[str, str], int] = defaultdict(int)

        with open(str(parsed_csv), "r", encoding="utf-8", errors="replace") as in_f:
            reader = csv.DictReader(in_f)

            with open(str(output_path), "w", newline="", encoding="utf-8") as out_f:
                writer = csv.DictWriter(out_f, fieldnames=output_headers)
                writer.writeheader()

                for row in reader:
                    rows_in += 1
                    out_row = {}
                    exclude_row = False

                    for cm in column_maps:
                        src = cm["source_column"]
                        tgt = cm["target_header"]
                        raw_val = row.get(src, "").strip()

                        # Apply inline transform
                        val = _apply_transform(raw_val, cm["transform_type"], cm["transform_config"])

                        # Apply value rule if controlled list
                        if cm["is_controlled_list"] and val:
                            rule = rule_lookup.get((tgt, val))
                            if rule:
                                op = rule["operation"]
                                if op in ("rename", "merge"):
                                    val = rule["target_value"] or ""
                                    rules_applied += 1
                                elif op == "split":
                                    new_val = _evaluate_patron_split(row, column_maps, rule)
                                    if new_val is not None:
                                        val = new_val
                                    rules_applied += 1
                                elif op == "delete":
                                    if rule["delete_mode"] == "exclude_row":
                                        exclude_row = True
                                        rules_applied += 1
                                        break
                                    else:
                                        val = ""
                                        rules_applied += 1

                        out_row[tgt] = val

                        # Track controlled list values
                        if cm["is_controlled_list"] and val:
                            final_values[(tgt, val)] += 1

                    if exclude_row:
                        rows_excluded += 1
                        continue

                    writer.writerow(out_row)
                    rows_out += 1

                    if rows_in % _PROGRESS_INTERVAL == 0:
                        self.update_state(state="PROGRESS", meta={
                            "rows_processed": rows_in,
                            "rules_applied": rules_applied,
                        })

        # Write per-category controlled value CSVs
        # Clean old CSVs
        for old in controlled_dir.glob("patron_*.csv"):
            old.unlink()

        cats_with_values: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for (hdr, val), count in final_values.items():
            cats_with_values[hdr].append((val, count))

        category_files = {}
        for cat, entries in cats_with_values.items():
            entries.sort(key=lambda x: (-x[1], x[0]))
            csv_path = controlled_dir / f"patron_{cat}.csv"
            with open(str(csv_path), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["value", "record_count"])
                for val, count in entries:
                    writer.writerow([val, count])
            category_files[cat] = f"patrons/controlled/{csv_path.name}"

        log.info(
            f"Wrote {len(category_files)} controlled value files: "
            + ", ".join(f"{c} ({len(cats_with_values[c])} values)" for c in sorted(category_files))
        )

        # Write descriptions.json stub
        desc_path = controlled_dir / "descriptions.json"
        if not desc_path.is_file():
            desc_path.write_text("{}")

        # Update project
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if project:
                project.stage_6_complete = True
                project.current_stage = 7
                project.patron_count = rows_out
                db.commit()

        log.complete(
            f"Patron apply complete — {rows_in:,} rows in, {rows_out:,} out, "
            f"{rows_excluded} excluded, {rules_applied:,} rules applied"
        )
        return {
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_excluded": rows_excluded,
            "rules_applied": rules_applied,
            "output_path": str(output_path),
            "category_files": category_files,
        }

    except Exception as exc:
        log.error(f"Patron apply failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# CONCAT HELPER
# ══════════════════════════════════════════════════════════════════════════


def _concat_patron_files(project_id: str) -> None:
    """Concatenate all per-file parsed CSVs into a single parsed.csv.

    Reads all patrons/parsed_*.csv files, computes the column union,
    and writes a merged parsed.csv. Updates project-level patron_count.
    """
    from cerulean.models import Project

    project_dir = Path(settings.data_root) / project_id / "patrons"
    per_file_csvs = sorted(project_dir.glob("parsed_*.csv"))
    parsed_csv = project_dir / "parsed.csv"

    if not per_file_csvs:
        # No files — remove parsed.csv if it exists
        if parsed_csv.is_file():
            os.remove(str(parsed_csv))
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if project:
                project.patron_count = 0
                db.commit()
        return

    # Collect column union across all files (preserving order)
    all_fieldnames: list[str] = []
    seen: set[str] = set()
    all_rows: list[dict] = []

    for csv_path in per_file_csvs:
        with open(str(csv_path), "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for col in (reader.fieldnames or []):
                if col not in seen:
                    all_fieldnames.append(col)
                    seen.add(col)
            for row in reader:
                all_rows.append(row)

    # Write merged parsed.csv
    with open(str(parsed_csv), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow({col: row.get(col, "") for col in all_fieldnames})

    # Update project-level row count
    with Session(_engine) as db:
        project = db.get(Project, project_id)
        if project:
            project.patron_count = len(all_rows)
            db.commit()


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════


def _parse_csv_tsv(path: Path, parse_settings: dict, ext: str) -> tuple[list[dict], str]:
    """Parse CSV/TSV/fixed-width text file."""
    try:
        import chardet
    except ImportError:
        chardet = None

    # Detect encoding
    encoding = parse_settings.get("encoding")
    if not encoding:
        if chardet:
            with open(str(path), "rb") as f:
                raw = f.read(min(100_000, path.stat().st_size))
            detected = chardet.detect(raw)
            encoding = detected.get("encoding", "utf-8") or "utf-8"
        else:
            encoding = "utf-8"

    # Detect delimiter
    delimiter = parse_settings.get("delimiter")
    if not delimiter:
        with open(str(path), "r", encoding=encoding, errors="replace") as f:
            sample = f.read(8192)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = "\t" if ext == ".tsv" else ","

    skip_rows = parse_settings.get("skip_rows", 0)
    header_row = parse_settings.get("header_row", 0)  # 0-indexed after skipping

    detected_format = "tsv" if delimiter == "\t" else "csv"

    rows = []
    with open(str(path), "r", encoding=encoding, errors="replace", newline="") as f:
        # Skip rows
        for _ in range(skip_rows):
            next(f, None)
        # Skip to header row
        for _ in range(header_row):
            next(f, None)

        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            # Clean None keys and strip values
            cleaned = {(k or "").strip(): (v or "").strip() for k, v in row.items() if k}
            if any(cleaned.values()):
                rows.append(cleaned)

    return rows, detected_format


def _parse_excel(path: Path, parse_settings: dict, ext: str) -> tuple[list[dict], str]:
    """Parse Excel file (.xlsx or .xls)."""
    sheet_name = parse_settings.get("sheet_name", 0)
    header_row = parse_settings.get("header_row", 0)

    if ext == ".xlsx":
        import openpyxl
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        if isinstance(sheet_name, str):
            ws = wb[sheet_name]
        else:
            ws = wb.worksheets[sheet_name]

        all_rows = list(ws.iter_rows(values_only=True))
        wb.close()

        if not all_rows or header_row >= len(all_rows):
            return [], "xlsx"

        headers = [str(h or "").strip() for h in all_rows[header_row]]
        rows = []
        for data_row in all_rows[header_row + 1:]:
            row_dict = {}
            for i, val in enumerate(data_row):
                if i < len(headers) and headers[i]:
                    row_dict[headers[i]] = str(val or "").strip()
            if any(row_dict.values()):
                rows.append(row_dict)
        return rows, "xlsx"

    else:
        import xlrd
        wb = xlrd.open_workbook(str(path))
        if isinstance(sheet_name, str):
            ws = wb.sheet_by_name(sheet_name)
        else:
            ws = wb.sheet_by_index(sheet_name)

        if ws.nrows == 0 or header_row >= ws.nrows:
            return [], "xls"

        headers = [str(ws.cell_value(header_row, c) or "").strip() for c in range(ws.ncols)]
        rows = []
        for r in range(header_row + 1, ws.nrows):
            row_dict = {}
            for c in range(ws.ncols):
                if c < len(headers) and headers[c]:
                    row_dict[headers[c]] = str(ws.cell_value(r, c) or "").strip()
            if any(row_dict.values()):
                rows.append(row_dict)
        return rows, "xls"


def _parse_xml(path: Path, parse_settings: dict) -> tuple[list[dict], str]:
    """Parse XML file — flatten repeating elements to tabular rows."""
    import xml.etree.ElementTree as ET

    element_path = parse_settings.get("element_path", "")
    tree = ET.parse(str(path))
    root = tree.getroot()

    # Find repeating elements
    if element_path:
        elements = root.findall(element_path)
    else:
        # Auto-detect: find the most-repeated child element
        child_counts: Counter = Counter()
        for child in root:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            child_counts[tag] += 1
        if not child_counts:
            return [], "xml"
        most_common_tag = child_counts.most_common(1)[0][0]
        elements = [c for c in root if (c.tag.split("}")[-1] if "}" in c.tag else c.tag) == most_common_tag]

    rows = []
    for elem in elements:
        row = {}
        for child in elem:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            row[tag] = (child.text or "").strip()
        if row:
            rows.append(row)

    return rows, "xml"


def _parse_patron_marc(path: Path, parse_settings: dict) -> tuple[list[dict], str]:
    """Parse Patron MARC file — extract fields into tabular rows."""
    import pymarc

    # Default Voyager patron MARC tag map
    tag_map = parse_settings.get("tag_map", {
        "001": "patron_id",
        "100$a": "surname",
        "100$b": "firstname",
        "270$a": "address",
        "270$b": "city",
        "270$c": "state",
        "270$e": "zipcode",
        "270$k": "email",
        "270$m": "phone",
    })

    rows = []
    with open(str(path), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue
            row = {}
            for spec, col_name in tag_map.items():
                if "$" in spec:
                    tag, sub = spec.split("$", 1)
                    for field in record.get_fields(tag):
                        val = field.get(sub)
                        if val:
                            row[col_name] = val.strip()
                            break
                else:
                    field = record.get(spec)
                    if field:
                        if hasattr(field, "data"):
                            row[col_name] = field.data.strip()
                        else:
                            row[col_name] = str(field).strip()
            if row:
                rows.append(row)

    return rows, "patron_marc"


def _apply_transform(value: str, transform_type: str, config: dict) -> str:
    """Apply inline transform to a patron field value."""
    if not value or transform_type == "copy":
        return value

    if transform_type == "const":
        return config.get("value", "")

    if transform_type == "case_upper":
        return value.upper()
    if transform_type == "case_lower":
        return value.lower()
    if transform_type == "case_title":
        return value.title()

    if transform_type == "date_mdy_to_iso":
        return _convert_date(value, "%m/%d/%Y", "%Y-%m-%d")
    if transform_type == "date_dmy_to_iso":
        return _convert_date(value, "%d/%m/%Y", "%Y-%m-%d")
    if transform_type == "date_ymd_to_iso":
        # Already YYYY-MM-DD, just normalise
        return _convert_date(value, "%Y-%m-%d", "%Y-%m-%d")
    if transform_type == "date_marc_to_iso":
        return _convert_date(value, "%Y%m%d", "%Y-%m-%d")

    if transform_type == "split":
        delimiter = config.get("delimiter", " ")
        index = config.get("index", 0)
        parts = value.split(delimiter)
        # Strip whitespace from parts
        parts = [p.strip() for p in parts if p.strip()]
        if not parts:
            return value
        try:
            return parts[int(index)]
        except (IndexError, ValueError):
            return value

    if transform_type == "regex":
        import re
        pattern = config.get("pattern", "")
        replacement = config.get("replacement", "")
        if pattern:
            try:
                return re.sub(pattern, replacement, value)
            except re.error:
                return value

    return value


def _convert_date(value: str, in_fmt: str, out_fmt: str) -> str:
    """Convert date string between formats, returning original on failure."""
    try:
        return datetime.strptime(value.strip(), in_fmt).strftime(out_fmt)
    except (ValueError, AttributeError):
        return value


def _evaluate_patron_split(row: dict, column_maps: list[dict], rule: dict) -> str | None:
    """Evaluate split conditions for a patron value rule."""
    conditions = rule.get("split_conditions")
    if not conditions:
        return None

    for cond in conditions:
        if cond.get("default"):
            return cond.get("target_value")

        field_check = cond.get("field_check")
        if field_check:
            check_col = field_check.get("column", "")
            check_val = field_check.get("value", "")
            if row.get(check_col, "").strip() == check_val:
                return cond.get("target_value")

    return None


def _parse_ai_suggestions(raw: str) -> list[dict]:
    """Parse Claude's JSON response into a list of suggestion dicts."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []
