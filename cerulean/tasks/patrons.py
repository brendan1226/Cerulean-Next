"""
cerulean/tasks/patrons.py
─────────────────────────────────────────────────────────────────────────────
Stage 9 Celery tasks: Patron Data Transformation.

    patron_parse_task    — parse uploaded patron file into normalised CSV
    patron_scan_task     — scan parsed data for controlled list column values
    patron_apply_task    — apply mappings + rules, write patrons_transformed.csv

Patron data is PII and is never sent to an AI/LLM. No Claude API calls
are made from this module.

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
from cerulean.tasks.helpers import check_paused as _check_paused

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


# ── Header-matching fast path ─────────────────────────────────────────

def match_koha_patron_header(source_column: str) -> str | None:
    """Return the canonical Koha borrower header if ``source_column`` is a
    case-insensitive exact match for one. ``None`` otherwise.

    Pure string function — safe to unit test without DB or task machinery.
    The match is strictly on the column name as it appears in the CSV,
    with whitespace stripped. Anything fuzzier belongs in the AI Suggest
    flow, not here — the point of this helper is fast, deterministic
    mapping only for files that are already Koha-shaped.
    """
    if not source_column:
        return None
    key = source_column.strip().lower()
    if not key:
        return None
    for canonical in KOHA_BORROWER_HEADERS:
        if canonical.lower() == key:
            return canonical
    return None


# ── Controlled-value SQL generator ────────────────────────────────────

# Minimal-viable Koha schema for each controlled header. Users can edit
# the generated SQL before loading; we keep required columns small so
# the output works against vanilla Koha installs (24.x verified).
#
# category_type 'A' (Adult) and enrolmentperiod 99 are sensible defaults
# an operator can tighten in the SQL file before `mysql < ...`.
#
# title / lost go to authorised_values under the category codes Koha
# uses for patron titles and lost-card status respectively.

_CONTROLLED_SQL_SPEC: dict[str, dict] = {
    "categorycode": {
        "table": "categories",
        "comment": "Borrower categories — review category_type before loading",
        "columns": ["categorycode", "description", "category_type", "enrolmentperiod"],
        "row": lambda code: (
            _sql_str(code[:10]),
            _sql_str(code[:100]),
            _sql_str("A"),
            "99",
        ),
    },
    "branchcode": {
        "table": "branches",
        "comment": "Home libraries — branchname defaults to the code (edit before loading)",
        "columns": ["branchcode", "branchname"],
        "row": lambda code: (
            _sql_str(code[:10]),
            _sql_str(code[:100]),
        ),
    },
    "title": {
        "table": "authorised_values",
        "comment": "Patron titles (BOR_TITLES) — salutation list",
        "columns": ["category", "authorised_value", "lib"],
        "row": lambda value: (
            _sql_str("BOR_TITLES"),
            _sql_str(value[:80]),
            _sql_str(value[:200]),
        ),
    },
    "lost": {
        "table": "authorised_values",
        "comment": "Lost-card status values (LOST)",
        "columns": ["category", "authorised_value", "lib"],
        "row": lambda value: (
            _sql_str("LOST"),
            _sql_str(value[:80]),
            _sql_str(value[:200]),
        ),
    },
}


def _sql_str(s: str) -> str:
    """MySQL/MariaDB string literal with minimal escaping. Handles
    single quotes and backslashes — anything that would break the
    statement or open an injection door if the operator pipes the
    output straight into ``mysql`` on the Koha host."""
    if s is None:
        return "''"
    escaped = str(s).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def generate_patron_controlled_sql(
    scan_results_by_header: dict[str, list[tuple[str, int]]],
    *,
    project_code: str | None = None,
) -> str:
    """Render INSERT IGNORE statements for one or more controlled patron
    headers based on scanned distinct source values.

    ``scan_results_by_header`` maps each Koha header to a list of
    ``(source_value, record_count)`` tuples. Only headers present in
    ``_CONTROLLED_SQL_SPEC`` produce output — others are skipped with
    a note in the file header. Empty / whitespace-only values are
    skipped to keep the SQL tight.

    Pure function — unit-testable without a DB or project.
    """
    from datetime import datetime

    lines: list[str] = []
    lines.append("-- Cerulean Next — patron controlled-value SQL export")
    if project_code:
        lines.append(f"-- project: {project_code}")
    lines.append(f"-- generated: {datetime.utcnow().isoformat(timespec='seconds')}Z")
    lines.append("--")
    lines.append("-- Review each block before loading. INSERT IGNORE skips rows whose")
    lines.append("-- primary key already exists in Koha — nothing is ever overwritten.")
    lines.append("-- Load with:  mysql -u koha_admin -p koha_database < this_file.sql")
    lines.append("")

    total_statements = 0
    for header, spec in _CONTROLLED_SQL_SPEC.items():
        rows = scan_results_by_header.get(header, [])
        clean_rows = [(v, c) for (v, c) in rows if v and str(v).strip()]
        if not clean_rows:
            continue

        lines.append(f"-- ── {header} ── {spec['comment']}")
        lines.append(
            f"-- {len(clean_rows)} distinct value(s), "
            f"{sum(c for _, c in clean_rows):,} patron record(s) total"
        )
        col_list = ", ".join(spec["columns"])
        for value, count in clean_rows:
            cells = spec["row"](value.strip())
            lines.append(
                f"INSERT IGNORE INTO {spec['table']} ({col_list}) "
                f"VALUES ({', '.join(cells)});"
                f"  -- {count:,} patron(s)"
            )
            total_statements += 1
        lines.append("")

    if total_statements == 0:
        lines.append("-- No controlled values found. Run the Value Reconciliation scan")
        lines.append("-- on Step 8 first, then re-download.")
        lines.append("")

    return "\n".join(lines)


def _auto_create_koha_header_maps(db: Session, project_id: str, columns: list[str]) -> int:
    """Create approved PatronColumnMap rows for every source column whose
    name matches a Koha borrower header (case-insensitive). Skips
    columns that already have a map so re-uploads are idempotent.

    Returns the number of rows inserted.
    """
    from cerulean.models import PatronColumnMap

    existing_sources = {
        s for (s,) in db.execute(
            select(PatronColumnMap.source_column).where(
                PatronColumnMap.project_id == project_id,
            )
        ).all()
    }

    created = 0
    for col in columns:
        if col in existing_sources:
            continue
        canonical = match_koha_patron_header(col)
        if canonical is None:
            continue
        db.add(PatronColumnMap(
            project_id=project_id,
            source_column=col,
            target_header=canonical,
            transform_type="copy",
            is_controlled_list=canonical in PATRON_CONTROLLED_HEADERS,
            approved=True,
            source_label="auto",
        ))
        existing_sources.add(col)
        created += 1
    if created:
        db.commit()
    return created


# ══════════════════════════════════════════════════════════════════════════
# PATRON PARSE TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.patrons.patron_parse_task",
    max_retries=0,
    queue="patrons",
)
def patron_parse_task(
    self,
    project_id: str,
    patron_file_id: str,
    auto_map_headers: bool = False,
) -> dict:
    """Parse uploaded patron file and write normalised parsed.csv.

    Supports CSV, TSV, Excel (.xlsx/.xls), fixed-width, XML, and Patron MARC.
    Auto-detects format from extension and content sniffing.

    Args:
        project_id: UUID of the project.
        patron_file_id: UUID of the PatronFile row.
        auto_map_headers: When True, after parsing, every source column
            whose name is a case-insensitive exact match for a standard
            Koha borrower header gets an approved ``PatronColumnMap`` row
            auto-created with ``source_label="auto"``. Controlled-list
            headers (categorycode / branchcode / title / lost) also get
            ``is_controlled_list=True`` so the Scan button unblocks
            without the user visiting the Column Mapping tab.

    Returns:
        dict with row_count, columns, format.
    """
    from cerulean.models import PatronFile

    log = AuditLogger(project_id=project_id, stage=9, tag="[patron-parse]")
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

        # Optional fast-path: when the uploader said the file already uses
        # Koha headers, auto-create + auto-approve column maps for every
        # exact-match column. Skip quietly if the map already exists so
        # re-uploads don't duplicate rows.
        if auto_map_headers:
            try:
                with Session(_engine) as db:
                    created = _auto_create_koha_header_maps(db, project_id, columns)
                if created:
                    log.info(
                        f"Auto-approved {created} column map(s) matching Koha patron headers "
                        f"(auto_map_headers=True)"
                    )
            except Exception as exc:
                log.warn(f"Auto-map step failed (manual mapping still available): {exc}")

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

    log = AuditLogger(project_id=project_id, stage=9, tag="[patron-scan]")
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
                    _check_paused(project_id, self)
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

    log = AuditLogger(project_id=project_id, stage=9, tag="[patron-apply]")
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

        # Use full Koha borrower headers — mapped columns get values, rest stay empty
        output_headers = list(KOHA_BORROWER_HEADERS)
        # Also include any mapped headers not in the standard list (future-proofing)
        for cm in column_maps:
            hdr = cm["target_header"]
            if hdr and hdr not in output_headers:
                output_headers.append(hdr)

        rows_in = 0
        rows_out = 0
        rows_excluded = 0
        rules_applied = 0

        # Track final values for controlled list columns
        final_values: dict[tuple[str, str], int] = defaultdict(int)

        with open(str(parsed_csv), "r", encoding="utf-8", errors="replace") as in_f:
            reader = csv.DictReader(in_f)

            with open(str(output_path), "w", newline="", encoding="utf-8") as out_f:
                writer = csv.DictWriter(out_f, fieldnames=output_headers, restval="", extrasaction="ignore")
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
                        _check_paused(project_id, self)
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
                project.stage_9_complete = True
                project.current_stage = 10
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
    """Parse CSV/TSV/fixed-width text file.

    Auto-detects the header row by scanning for the first line with enough
    delimited columns (>= 3), skipping preamble lines like report titles,
    dates, and blank rows.
    """
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
    header_row = parse_settings.get("header_row")  # None = auto-detect
    min_columns = parse_settings.get("min_columns", 3)

    detected_format = "tsv" if delimiter == "\t" else "csv"

    # Read all lines so we can auto-detect the header
    with open(str(path), "r", encoding=encoding, errors="replace", newline="") as f:
        all_lines = f.readlines()

    # Apply skip_rows
    all_lines = all_lines[skip_rows:]

    if header_row is not None:
        # Explicit header row (0-indexed after skip_rows)
        data_start = header_row
    else:
        # Auto-detect: find the first line with >= min_columns delimited fields
        data_start = 0
        for i, line in enumerate(all_lines):
            fields = line.strip().split(delimiter)
            non_empty = [f.strip() for f in fields if f.strip()]
            if len(non_empty) >= min_columns:
                data_start = i
                break

    # Parse from detected header row
    lines_from_header = all_lines[data_start:]
    if not lines_from_header:
        return [], detected_format

    import io
    reader = csv.DictReader(io.StringIO("".join(lines_from_header)), delimiter=delimiter)

    rows = []
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

    if transform_type == "name_split":
        return _name_split(value, config)

    if transform_type == "gender_normalize":
        # FEMALE/MALE/F/M/female/male → F/M (single char, uppercase)
        v = value.strip().upper()
        if v.startswith("F"):
            return "F"
        if v.startswith("M"):
            return "M"
        return ""

    if transform_type == "first_char":
        return value.strip()[0].upper() if value.strip() else ""

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


def _name_split(value: str, config: dict) -> str:
    """Split a name field into surname/firstname components.

    Handles common name formats:
        "Last, First"           → surname="Last", firstname="First"
        "Last, First Middle"    → surname="Last", firstname="First Middle"
        "Last,First"            → surname="Last", firstname="First"
        "First Last"            → surname="Last", firstname="First"
        "First Middle Last"     → surname="Last", firstname="First Middle"
        "Last"                  → surname="Last", firstname=""
        "Last, First (Suffix)"  → surname="Last", firstname="First (Suffix)"
        "prefix Last, First"    → surname="Last", firstname="First" (if prefix detected)

    Config:
        part: "surname" | "firstname" | "middlename"  — which part to return
        format: "auto" | "last_first" | "first_last"   — hint if auto-detect fails
    """
    part = config.get("part", "surname")
    fmt = config.get("format", "auto")

    if not value or not value.strip():
        return ""

    value = value.strip()

    # Determine format
    if fmt == "auto":
        fmt = _detect_name_format(value)

    if fmt == "last_first":
        # "Last, First Middle" or "Last,First"
        if "," in value:
            pieces = value.split(",", 1)
            surname = pieces[0].strip()
            rest = pieces[1].strip() if len(pieces) > 1 else ""
        else:
            # Single word, treat as surname
            surname = value
            rest = ""

        if part == "surname":
            return surname
        elif part == "firstname":
            # First name is the first word of rest
            words = rest.split() if rest else []
            return words[0] if words else ""
        elif part == "middlename":
            words = rest.split() if rest else []
            return " ".join(words[1:]) if len(words) > 1 else ""

    elif fmt == "first_last":
        # "First Last" or "First Middle Last"
        words = value.split()
        if len(words) == 1:
            # Single word — treat as surname
            if part == "surname":
                return words[0]
            return ""
        elif len(words) == 2:
            if part == "surname":
                return words[-1]
            elif part == "firstname":
                return words[0]
            elif part == "middlename":
                return ""
        else:
            # 3+ words: last word is surname, first is firstname, middle is the rest
            if part == "surname":
                return words[-1]
            elif part == "firstname":
                return words[0]
            elif part == "middlename":
                return " ".join(words[1:-1])

    # Fallback: return whole value for surname, empty for others
    if part == "surname":
        return value
    return ""


def _detect_name_format(value: str) -> str:
    """Auto-detect whether a name is 'Last, First' or 'First Last' format.

    Heuristics:
        - Contains comma → "last_first"
        - Otherwise → "first_last"
    """
    if "," in value:
        return "last_first"
    return "first_last"


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

