"""
cerulean/tasks/holds.py
─────────────────────────────────────────────────────────────────────────────
Step 11 Celery tasks: Holds & Circulation History.

    validate_holds_task     — parse CSV, detect format, resolve IDs, validate
    push_holds_task         — push resolved holds to Koha
    validate_circ_task      — parse circ CSV, validate required fields
    generate_circ_sql_task  — generate old_issues INSERT SQL
    push_circ_plugin_task   — push circ via CeruleanEndpoints plugin
    build_id_mappings_task  — retroactively build mappings from Koha
"""

import csv
import uuid
from datetime import datetime
from pathlib import Path

import httpx
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.helpers import check_paused as _check_paused

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")

from sqlalchemy import create_engine
_engine = create_engine(_sync_url, pool_pre_ping=True)

_PROGRESS_INTERVAL = 200

# ── Column name aliases ──────────────────────────────────────────────

_PATRON_ID_COLS = {"patron_id", "borrowernumber", "patron_barcode", "patron_cardnumber", "cardnumber", "borrower_barcode"}
_BIB_ID_COLS = {"biblio_id", "biblionumber", "legacy_bib_id", "bib_control_number", "bib_001", "control_number", "legacy_biblionumber"}
_ITEM_ID_COLS = {"item_id", "itemnumber", "legacy_item_barcode", "item_barcode", "barcode"}
_HOLD_DATE_COLS = {"hold_date", "reserve_date", "placed_date", "reservedate"}
_PICKUP_COLS = {"pickup_library_id", "branchcode", "pickup_branch"}
_EXPIRY_COLS = {"expiration_date", "expirationdate"}


def _find_col(headers: list[str], aliases: set[str]) -> str | None:
    """Find the first column name matching any alias (case-insensitive)."""
    headers_lower = {h.lower().strip(): h for h in headers}
    for alias in aliases:
        if alias.lower() in headers_lower:
            return headers_lower[alias.lower()]
    return None


def _detect_format(headers: list[str]) -> str:
    """Detect if the CSV has Koha IDs (format_a) or legacy IDs (format_b)."""
    h_lower = {h.lower().strip() for h in headers}
    has_koha_patron = bool(h_lower & {"patron_id", "borrowernumber"})
    has_koha_bib = bool(h_lower & {"biblio_id", "biblionumber"})
    has_legacy_patron = bool(h_lower & {"patron_barcode", "patron_cardnumber", "cardnumber", "borrower_barcode"})
    has_legacy_bib = bool(h_lower & {"legacy_bib_id", "bib_control_number", "bib_001", "control_number", "legacy_biblionumber"})

    if has_koha_patron and has_koha_bib:
        return "format_a"  # Pre-resolved Koha IDs
    elif has_legacy_patron or has_legacy_bib:
        return "format_b"  # Legacy IDs needing resolution
    return "format_a"  # Default: assume Koha IDs


def _load_id_mappings(project_id: str, entity_type: str) -> dict[str, int]:
    """Load legacy→Koha ID mappings from the database."""
    from cerulean.models import IdMapping
    with Session(_engine) as db:
        rows = db.execute(
            select(IdMapping.legacy_id, IdMapping.koha_id)
            .where(IdMapping.project_id == project_id, IdMapping.entity_type == entity_type)
        ).all()
        return {r[0]: r[1] for r in rows}


def _resolve_via_koha_api(project_id: str, entity_type: str, unresolved: set[str]) -> dict[str, int]:
    """Resolve IDs by querying Koha API. Returns {legacy_id: koha_id}."""
    if not unresolved:
        return {}

    from cerulean.tasks.push import _koha_client
    resolved = {}

    try:
        base_url, headers = _koha_client(project_id)

        # Try the batch lookup plugin first
        try:
            with httpx.Client(timeout=30.0, verify=False) as client:
                body = {}
                if entity_type == "patron":
                    body["patrons"] = {"by": "cardnumber", "values": list(unresolved)[:1000]}
                elif entity_type == "bib":
                    body["biblios"] = {"by": "control_number", "values": list(unresolved)[:1000]}
                elif entity_type == "item":
                    body["items"] = {"by": "barcode", "values": list(unresolved)[:1000]}

                resp = client.post(
                    f"{base_url}/api/v1/contrib/cerulean/lookup/batch",
                    json=body, headers=headers,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if entity_type == "patron":
                        for p in data.get("patrons", []):
                            resolved[p.get("cardnumber", "")] = p.get("borrowernumber", 0)
                    elif entity_type == "bib":
                        for b in data.get("biblios", []):
                            resolved[b.get("control_number", "")] = b.get("biblionumber", 0)
                    elif entity_type == "item":
                        for i in data.get("items", []):
                            resolved[i.get("barcode", "")] = i.get("itemnumber", 0)
                    return resolved
        except Exception:
            pass  # Plugin not available, fall back to individual lookups

        # Fallback: individual Koha API queries (slower)
        with httpx.Client(timeout=15.0, verify=False) as client:
            for legacy_id in list(unresolved)[:500]:  # Cap at 500 to avoid timeout
                try:
                    if entity_type == "patron":
                        resp = client.get(
                            f"{base_url}/api/v1/patrons",
                            params={"cardnumber": legacy_id, "_per_page": 1},
                            headers=headers,
                        )
                        if resp.status_code == 200:
                            items = resp.json()
                            if items:
                                resolved[legacy_id] = items[0].get("patron_id", 0)
                    elif entity_type == "item":
                        resp = client.get(
                            f"{base_url}/api/v1/items",
                            params={"external_id": legacy_id, "_per_page": 1},
                            headers=headers,
                        )
                        if resp.status_code == 200:
                            items = resp.json()
                            if items:
                                resolved[legacy_id] = items[0].get("item_id", 0)
                except Exception:
                    pass
    except Exception:
        pass

    return resolved


# ══════════════════════════════════════════════════════════════════════════
# VALIDATE HOLDS
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="cerulean.tasks.holds.validate_holds_task",
    max_retries=0,
    queue="push",
)
def validate_holds_task(self, project_id: str, file_id: str) -> dict:
    """Parse holds CSV, detect format, resolve IDs, report validation."""
    log = AuditLogger(project_id=project_id, stage=11, tag="[holds-validate]")
    log.info("Holds validation starting")

    from cerulean.models import HoldsFile

    with Session(_engine) as db:
        holds_file = db.get(HoldsFile, file_id)
        if not holds_file:
            return {"error": "File not found"}
        holds_file.status = "validating"
        db.commit()

    try:
        file_path = Path(holds_file.storage_path)
        if not file_path.is_file():
            return {"error": "File not found on disk"}

        with open(str(file_path), "r", encoding="utf-8-sig", errors="replace") as fh:
            reader = csv.DictReader(fh)
            headers = reader.fieldnames or []
            fmt = _detect_format(headers)

            patron_col = _find_col(headers, _PATRON_ID_COLS)
            bib_col = _find_col(headers, _BIB_ID_COLS)
            item_col = _find_col(headers, _ITEM_ID_COLS)
            date_col = _find_col(headers, _HOLD_DATE_COLS)
            pickup_col = _find_col(headers, _PICKUP_COLS)

            # Load ID mappings for format_b
            patron_map = {}
            bib_map = {}
            item_map = {}
            if fmt == "format_b":
                patron_map = _load_id_mappings(project_id, "patron")
                bib_map = _load_id_mappings(project_id, "bib")
                item_map = _load_id_mappings(project_id, "item")

            total = 0
            valid = 0
            invalid = 0
            errors = []
            unresolved_patrons = set()
            unresolved_bibs = set()
            resolved_patrons = 0
            resolved_bibs = 0

            for row in reader:
                total += 1
                row_errors = []

                patron_val = (row.get(patron_col, "") if patron_col else "").strip()
                bib_val = (row.get(bib_col, "") if bib_col else "").strip()

                if not patron_val:
                    row_errors.append("missing patron identifier")
                if not bib_val:
                    row_errors.append("missing bib identifier")

                if fmt == "format_b" and patron_val:
                    if patron_val in patron_map:
                        resolved_patrons += 1
                    else:
                        unresolved_patrons.add(patron_val)

                if fmt == "format_b" and bib_val:
                    if bib_val in bib_map:
                        resolved_bibs += 1
                    else:
                        unresolved_bibs.add(bib_val)

                if row_errors:
                    invalid += 1
                    if len(errors) < 50:
                        errors.append({"row": total, "errors": row_errors})
                else:
                    valid += 1

                if total % _PROGRESS_INTERVAL == 0:
                    self.update_state(state="PROGRESS", meta={
                        "step": "validating",
                        "records_done": total,
                    })

        # Try to resolve unresolved IDs via Koha API
        api_resolved_patrons = {}
        api_resolved_bibs = {}
        if unresolved_patrons:
            log.info(f"Resolving {len(unresolved_patrons)} patron IDs via Koha API")
            self.update_state(state="PROGRESS", meta={"step": "resolving_patrons", "records_done": total})
            api_resolved_patrons = _resolve_via_koha_api(project_id, "patron", unresolved_patrons)
            resolved_patrons += len(api_resolved_patrons)
            unresolved_patrons -= set(api_resolved_patrons.keys())
            # Cache newly resolved
            if api_resolved_patrons:
                from cerulean.tasks.push import _write_id_mappings
                _write_id_mappings(project_id, "patron", list(api_resolved_patrons.items()))

        if unresolved_bibs:
            log.info(f"Resolving {len(unresolved_bibs)} bib IDs via Koha API")
            self.update_state(state="PROGRESS", meta={"step": "resolving_bibs", "records_done": total})
            api_resolved_bibs = _resolve_via_koha_api(project_id, "bib", unresolved_bibs)
            resolved_bibs += len(api_resolved_bibs)
            unresolved_bibs -= set(api_resolved_bibs.keys())
            if api_resolved_bibs:
                from cerulean.tasks.push import _write_id_mappings
                _write_id_mappings(project_id, "bib", list(api_resolved_bibs.items()))

        # Add unresolved as errors
        for p in unresolved_patrons:
            if len(errors) < 100:
                errors.append({"row": "—", "errors": [f"patron_not_found: {p}"]})
        for b in unresolved_bibs:
            if len(errors) < 100:
                errors.append({"row": "—", "errors": [f"bib_not_found: {b}"]})

        result = {
            "format": fmt,
            "total": total,
            "valid": valid,
            "invalid": invalid + len(unresolved_patrons) + len(unresolved_bibs),
            "errors": errors,
            "columns_detected": {
                "patron": patron_col,
                "bib": bib_col,
                "item": item_col,
                "hold_date": date_col,
                "pickup": pickup_col,
            },
            "id_resolution": {
                "patrons_resolved": resolved_patrons,
                "patrons_unresolved": len(unresolved_patrons),
                "bibs_resolved": resolved_bibs,
                "bibs_unresolved": len(unresolved_bibs),
            },
        }

        with Session(_engine) as db:
            hf = db.get(HoldsFile, file_id)
            if hf:
                hf.status = "validated"
                hf.row_count = total
                hf.column_headers = headers
                hf.validation_result = result
                db.commit()

        log.complete(f"Holds validation: {total} rows, {valid} valid, {invalid} invalid, "
                     f"format={fmt}, patrons_resolved={resolved_patrons}, bibs_resolved={resolved_bibs}")
        return result

    except Exception as exc:
        log.error(f"Holds validation failed: {exc}")
        with Session(_engine) as db:
            hf = db.get(HoldsFile, file_id)
            if hf:
                hf.status = "error"
                hf.validation_result = {"error": str(exc)[:500]}
                db.commit()
        raise


# ══════════════════════════════════════════════════════════════════════════
# PUSH HOLDS
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="cerulean.tasks.holds.push_holds_task",
    max_retries=0,
    queue="push",
)
def push_holds_task(self, project_id: str, file_id: str, manifest_id: str) -> dict:
    """Push holds to Koha — supports both Format A (Koha IDs) and Format B (legacy IDs)."""
    from cerulean.models import HoldsFile, PushManifest, Project
    from cerulean.tasks.push import _koha_client, _update_push_manifest

    log = AuditLogger(project_id=project_id, stage=11, tag="[holds-push]")
    log.info("Holds push starting")

    try:
        with Session(_engine) as db:
            holds_file = db.get(HoldsFile, file_id)
            if not holds_file:
                return {"error": "File not found"}

        file_path = Path(holds_file.storage_path)
        vr = holds_file.validation_result or {}
        fmt = vr.get("format", "format_a")
        cols = vr.get("columns_detected", {})

        # Load ID mappings
        patron_map = _load_id_mappings(project_id, "patron") if fmt == "format_b" else {}
        bib_map = _load_id_mappings(project_id, "bib") if fmt == "format_b" else {}
        item_map = _load_id_mappings(project_id, "item") if fmt == "format_b" else {}

        base_url, headers = _koha_client(project_id)
        total = 0
        success = 0
        failed = 0
        skipped = 0

        # Try bulk plugin first
        use_plugin = False
        try:
            with httpx.Client(timeout=10.0, verify=False) as client:
                resp = client.get(f"{base_url}/api/v1/contrib/cerulean/system/info", headers=headers)
                use_plugin = resp.status_code == 200
        except Exception:
            pass

        with open(str(file_path), "r", encoding="utf-8-sig", errors="replace") as fh:
            reader = csv.DictReader(fh)

            if use_plugin:
                # Batch push via plugin
                batch = []
                for row in reader:
                    total += 1
                    hold = _resolve_hold_row(row, cols, fmt, patron_map, bib_map, item_map)
                    if hold:
                        batch.append(hold)
                    else:
                        skipped += 1

                    if len(batch) >= 500:
                        s, f = _push_holds_batch(base_url, headers, batch)
                        success += s
                        failed += f
                        batch = []
                        self.update_state(state="PROGRESS", meta={
                            "step": "pushing", "records_done": success + failed,
                            "records_total": total,
                        })

                if batch:
                    s, f = _push_holds_batch(base_url, headers, batch)
                    success += s
                    failed += f
            else:
                # Individual POST to /api/v1/holds
                with httpx.Client(timeout=30.0, verify=False) as client:
                    for row in reader:
                        total += 1
                        hold = _resolve_hold_row(row, cols, fmt, patron_map, bib_map, item_map)
                        if not hold:
                            skipped += 1
                            continue

                        try:
                            resp = client.post(
                                f"{base_url}/api/v1/holds",
                                json=hold, headers=headers,
                            )
                            if resp.status_code < 300:
                                success += 1
                            elif resp.status_code == 409:
                                skipped += 1  # Duplicate hold
                            else:
                                failed += 1
                                if failed <= 10:
                                    log.warn(f"Hold {total}: HTTP {resp.status_code} — {resp.text[:200]}")
                        except Exception as exc:
                            failed += 1

                        if total % _PROGRESS_INTERVAL == 0:
                            _check_paused(project_id, self)
                            self.update_state(state="PROGRESS", meta={
                                "step": "pushing",
                                "records_done": success + failed + skipped,
                                "records_total": total,
                            })

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total,
                                  records_success=success,
                                  records_failed=failed,
                                  completed_at=datetime.utcnow())
            project = db.get(Project, project_id)
            if project:
                project.hold_count = success
                db.commit()

        log.complete(f"Holds push: {total} total, {success} success, {failed} failed, {skipped} skipped")
        return {
            "records_total": total, "records_success": success,
            "records_failed": failed, "records_skipped": skipped,
        }

    except Exception as exc:
        log.error(f"Holds push failed: {exc}")
        from cerulean.tasks.push import _update_push_manifest
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


def _resolve_hold_row(row, cols, fmt, patron_map, bib_map, item_map) -> dict | None:
    """Resolve a holds CSV row to Koha API format."""
    patron_col = cols.get("patron")
    bib_col = cols.get("bib")
    item_col = cols.get("item")
    date_col = cols.get("hold_date")
    pickup_col = cols.get("pickup")

    patron_val = (row.get(patron_col, "") if patron_col else "").strip()
    bib_val = (row.get(bib_col, "") if bib_col else "").strip()

    if not patron_val or not bib_val:
        return None

    if fmt == "format_b":
        patron_id = patron_map.get(patron_val)
        bib_id = bib_map.get(bib_val)
        if not patron_id or not bib_id:
            return None
    else:
        try:
            patron_id = int(patron_val)
            bib_id = int(bib_val)
        except ValueError:
            return None

    hold = {
        "patron_id": patron_id,
        "biblio_id": bib_id,
    }

    # Item-level hold
    if item_col:
        item_val = (row.get(item_col, "") or "").strip()
        if item_val:
            if fmt == "format_b":
                item_id = item_map.get(item_val)
                if item_id:
                    hold["item_id"] = item_id
            else:
                try:
                    hold["item_id"] = int(item_val)
                except ValueError:
                    pass

    # Pickup library
    if pickup_col:
        pickup = (row.get(pickup_col, "") or "").strip()
        if pickup:
            hold["pickup_library_id"] = pickup

    # Hold date
    if date_col:
        hdate = (row.get(date_col, "") or "").strip()
        if hdate:
            hold["hold_date"] = hdate

    # Expiration
    for ecol in _EXPIRY_COLS:
        val = (row.get(ecol, "") or "").strip()
        if val:
            hold["expiration_date"] = val
            break

    # Notes
    notes = (row.get("notes", "") or "").strip()
    if notes:
        hold["notes"] = notes

    return hold


def _push_holds_batch(base_url, headers, holds) -> tuple[int, int]:
    """Push a batch of holds via the CeruleanEndpoints plugin."""
    try:
        with httpx.Client(timeout=60.0, verify=False) as client:
            resp = client.post(
                f"{base_url}/api/v1/contrib/cerulean/holds/bulk",
                json={"holds": holds},
                headers=headers,
            )
            if resp.status_code < 300:
                data = resp.json()
                return data.get("success", len(holds)), data.get("failed", 0)
            return 0, len(holds)
    except Exception:
        return 0, len(holds)


# ══════════════════════════════════════════════════════════════════════════
# CIRC HISTORY — VALIDATE
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="cerulean.tasks.holds.validate_circ_task",
    max_retries=0,
    queue="push",
)
def validate_circ_task(self, project_id: str, file_id: str) -> dict:
    """Parse circ history CSV and validate required fields."""
    from cerulean.models import HoldsFile

    log = AuditLogger(project_id=project_id, stage=11, tag="[circ-validate]")
    log.info("Circ history validation starting")

    try:
        with Session(_engine) as db:
            cf = db.get(HoldsFile, file_id)
            if not cf:
                return {"error": "File not found"}
            cf.status = "validating"
            db.commit()

        file_path = Path(cf.storage_path)
        with open(str(file_path), "r", encoding="utf-8-sig", errors="replace") as fh:
            reader = csv.DictReader(fh)
            headers = reader.fieldnames or []

            patron_col = _find_col(headers, _PATRON_ID_COLS)
            item_col = _find_col(headers, _ITEM_ID_COLS)
            fmt = _detect_format(headers)

            total = 0
            valid = 0
            invalid = 0
            errors = []

            for row in reader:
                total += 1
                row_errors = []

                patron_val = (row.get(patron_col, "") if patron_col else "").strip()
                item_val = (row.get(item_col, "") if item_col else "").strip()
                issue_date = (row.get("issuedate", "") or row.get("issue_date", "") or "").strip()
                return_date = (row.get("returndate", "") or row.get("return_date", "") or "").strip()

                if not patron_val:
                    row_errors.append("missing patron identifier")
                if not item_val:
                    row_errors.append("missing item identifier")
                if not issue_date:
                    row_errors.append("missing issuedate")

                if row_errors:
                    invalid += 1
                    if len(errors) < 50:
                        errors.append({"row": total, "errors": row_errors})
                else:
                    valid += 1

                if total % _PROGRESS_INTERVAL == 0:
                    self.update_state(state="PROGRESS", meta={"step": "validating", "records_done": total})

        result = {
            "format": fmt,
            "total": total,
            "valid": valid,
            "invalid": invalid,
            "errors": errors,
            "columns_detected": {
                "patron": patron_col,
                "item": item_col,
            },
        }

        with Session(_engine) as db:
            cf = db.get(HoldsFile, file_id)
            if cf:
                cf.status = "validated"
                cf.row_count = total
                cf.column_headers = headers
                cf.validation_result = result
                db.commit()

        log.complete(f"Circ validation: {total} rows, {valid} valid, {invalid} invalid")
        return result

    except Exception as exc:
        log.error(f"Circ validation failed: {exc}")
        with Session(_engine) as db:
            cf = db.get(HoldsFile, file_id)
            if cf:
                cf.status = "error"
                db.commit()
        raise


# ══════════════════════════════════════════════════════════════════════════
# CIRC HISTORY — GENERATE SQL
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="cerulean.tasks.holds.generate_circ_sql_task",
    max_retries=0,
    queue="push",
)
def generate_circ_sql_task(self, project_id: str, file_id: str) -> dict:
    """Generate SQL INSERT statements for Koha's old_issues table."""
    from cerulean.models import HoldsFile

    log = AuditLogger(project_id=project_id, stage=11, tag="[circ-sql]")
    log.info("Generating circ history SQL")

    try:
        with Session(_engine) as db:
            cf = db.get(HoldsFile, file_id)
            if not cf:
                return {"error": "File not found"}

        file_path = Path(cf.storage_path)
        fmt = (cf.validation_result or {}).get("format", "format_a")

        # Load ID mappings if needed
        patron_map = _load_id_mappings(project_id, "patron") if fmt == "format_b" else {}
        item_map = _load_id_mappings(project_id, "item") if fmt == "format_b" else {}

        output_path = Path(settings.data_root) / project_id / "circ_history.sql"

        total = 0
        written = 0
        skipped = 0

        with open(str(file_path), "r", encoding="utf-8-sig", errors="replace") as fh_in:
            reader = csv.DictReader(fh_in)
            headers = reader.fieldnames or []
            patron_col = _find_col(headers, _PATRON_ID_COLS)
            item_col = _find_col(headers, _ITEM_ID_COLS)

            with open(str(output_path), "w") as fh_out:
                fh_out.write("-- Cerulean Next: Circulation History Import\n")
                fh_out.write(f"-- Generated: {datetime.utcnow().isoformat()}\n")
                fh_out.write(f"-- Project: {project_id}\n\n")
                fh_out.write("SET FOREIGN_KEY_CHECKS=0;\n")
                fh_out.write("SET autocommit=0;\n\n")

                batch = []
                for row in reader:
                    total += 1

                    patron_val = (row.get(patron_col, "") if patron_col else "").strip()
                    item_val = (row.get(item_col, "") if item_col else "").strip()

                    if fmt == "format_b":
                        borrowernumber = patron_map.get(patron_val)
                        itemnumber = item_map.get(item_val)
                    else:
                        try:
                            borrowernumber = int(patron_val) if patron_val else None
                            itemnumber = int(item_val) if item_val else None
                        except ValueError:
                            borrowernumber = None
                            itemnumber = None

                    if not borrowernumber or not itemnumber:
                        skipped += 1
                        fh_out.write(f"-- SKIPPED row {total}: patron={patron_val} item={item_val} (unresolved)\n")
                        continue

                    issuedate = (row.get("issuedate", "") or row.get("issue_date", "") or "").strip()
                    returndate = (row.get("returndate", "") or row.get("return_date", "") or "").strip()
                    branchcode = (row.get("branchcode", "") or row.get("branch", "") or "").strip()
                    renewals = (row.get("renewals", "") or row.get("renewals_count", "") or "0").strip()

                    def sql_esc(s):
                        return s.replace("'", "''") if s else ""

                    fh_out.write(
                        f"INSERT INTO old_issues (borrowernumber, itemnumber, issuedate, returndate, "
                        f"branchcode, renewals_count, timestamp) VALUES "
                        f"({borrowernumber}, {itemnumber}, "
                        f"'{sql_esc(issuedate)}', '{sql_esc(returndate or issuedate)}', "
                        f"'{sql_esc(branchcode)}', {renewals or 0}, "
                        f"'{sql_esc(returndate or issuedate)}');\n"
                    )
                    written += 1

                    if written % 1000 == 0:
                        fh_out.write("COMMIT;\n")
                        self.update_state(state="PROGRESS", meta={
                            "step": "generating_sql", "records_done": total,
                        })

                fh_out.write("\nCOMMIT;\n")
                fh_out.write("SET FOREIGN_KEY_CHECKS=1;\n")
                fh_out.write("SET autocommit=1;\n")
                fh_out.write(f"\n-- Total: {total} rows, {written} written, {skipped} skipped\n")

        log.complete(f"Circ SQL generated: {written}/{total} rows → circ_history.sql")
        return {
            "total": total,
            "written": written,
            "skipped": skipped,
            "output_file": "circ_history.sql",
        }

    except Exception as exc:
        log.error(f"Circ SQL generation failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# CIRC HISTORY — PUSH VIA PLUGIN
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="cerulean.tasks.holds.push_circ_plugin_task",
    max_retries=0,
    queue="push",
)
def push_circ_plugin_task(self, project_id: str, file_id: str, manifest_id: str) -> dict:
    """Push circ history via CeruleanEndpoints plugin bulk-history endpoint."""
    from cerulean.models import HoldsFile, Project
    from cerulean.tasks.push import _koha_client, _update_push_manifest

    log = AuditLogger(project_id=project_id, stage=11, tag="[circ-push]")
    log.info("Circ history push via plugin starting")

    try:
        with Session(_engine) as db:
            cf = db.get(HoldsFile, file_id)
            if not cf:
                return {"error": "File not found"}

        file_path = Path(cf.storage_path)
        fmt = (cf.validation_result or {}).get("format", "format_a")

        patron_map = _load_id_mappings(project_id, "patron") if fmt == "format_b" else {}
        item_map = _load_id_mappings(project_id, "item") if fmt == "format_b" else {}

        base_url, headers = _koha_client(project_id)

        total = 0
        success = 0
        failed = 0
        skipped = 0

        with open(str(file_path), "r", encoding="utf-8-sig", errors="replace") as fh:
            reader = csv.DictReader(fh)
            hdr = reader.fieldnames or []
            patron_col = _find_col(hdr, _PATRON_ID_COLS)
            item_col = _find_col(hdr, _ITEM_ID_COLS)

            batch = []
            for row in reader:
                total += 1
                patron_val = (row.get(patron_col, "") if patron_col else "").strip()
                item_val = (row.get(item_col, "") if item_col else "").strip()

                if fmt == "format_b":
                    borrowernumber = patron_map.get(patron_val)
                    itemnumber = item_map.get(item_val)
                else:
                    try:
                        borrowernumber = int(patron_val) if patron_val else None
                        itemnumber = int(item_val) if item_val else None
                    except ValueError:
                        borrowernumber = None
                        itemnumber = None

                if not borrowernumber or not itemnumber:
                    skipped += 1
                    continue

                checkout = {
                    "borrowernumber": borrowernumber,
                    "itemnumber": itemnumber,
                    "issuedate": (row.get("issuedate", "") or row.get("issue_date", "") or "").strip(),
                    "returndate": (row.get("returndate", "") or row.get("return_date", "") or "").strip(),
                    "branchcode": (row.get("branchcode", "") or row.get("branch", "") or "").strip(),
                    "renewals_count": int(row.get("renewals", 0) or row.get("renewals_count", 0) or 0),
                }
                batch.append(checkout)

                if len(batch) >= 500:
                    s, f = _push_circ_batch(base_url, headers, batch)
                    success += s
                    failed += f
                    batch = []
                    self.update_state(state="PROGRESS", meta={
                        "step": "pushing", "records_done": success + failed + skipped,
                        "records_total": total,
                    })

            if batch:
                s, f = _push_circ_batch(base_url, headers, batch)
                success += s
                failed += f

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total,
                                  records_success=success,
                                  records_failed=failed,
                                  completed_at=datetime.utcnow())
            project = db.get(Project, project_id)
            if project:
                project.circ_count = success
                db.commit()

        log.complete(f"Circ push: {total} total, {success} success, {failed} failed, {skipped} skipped")
        return {
            "records_total": total, "records_success": success,
            "records_failed": failed, "records_skipped": skipped,
        }

    except Exception as exc:
        log.error(f"Circ push failed: {exc}")
        from cerulean.tasks.push import _update_push_manifest
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


def _push_circ_batch(base_url, headers, checkouts) -> tuple[int, int]:
    """Push a batch of circ history via plugin."""
    try:
        with httpx.Client(timeout=60.0, verify=False) as client:
            resp = client.post(
                f"{base_url}/api/v1/contrib/cerulean/circulation/bulk-history",
                json={"checkouts": checkouts},
                headers=headers,
            )
            if resp.status_code < 300:
                data = resp.json()
                return data.get("inserted", len(checkouts)), data.get("errors", 0) if isinstance(data.get("errors"), int) else 0
            return 0, len(checkouts)
    except Exception:
        return 0, len(checkouts)


# ══════════════════════════════════════════════════════════════════════════
# BUILD ID MAPPINGS (retroactive)
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="cerulean.tasks.holds.build_id_mappings_task",
    max_retries=0,
    queue="push",
)
def build_id_mappings_task(self, project_id: str, entity_type: str = "bib") -> dict:
    """Retroactively build ID mappings by scanning Koha.

    For bibs: queries all Koha biblios and extracts the 001 field,
    then writes mappings to id_mappings table.

    For patrons: queries all Koha patrons and stores cardnumber→borrowernumber.
    """
    from cerulean.tasks.push import _koha_client, _write_id_mappings

    log = AuditLogger(project_id=project_id, stage=11, tag="[id-mappings]")
    log.info(f"Building {entity_type} ID mappings from Koha")

    try:
        base_url, headers = _koha_client(project_id)
        mappings = []
        page = 1
        per_page = 1000
        total = 0

        with httpx.Client(timeout=60.0, verify=False) as client:
            while True:
                if entity_type == "patron":
                    resp = client.get(
                        f"{base_url}/api/v1/patrons",
                        params={"_page": page, "_per_page": per_page},
                        headers=headers,
                    )
                elif entity_type == "bib":
                    # Koha doesn't have a simple way to get 001 via REST API
                    # Use the plugin batch lookup or scan MARC files directly
                    log.warn("Bib mapping build requires scanning local MARC files (Koha REST API doesn't expose 001)")
                    # Fall back to scanning local MARC files
                    return _build_bib_mappings_from_files(project_id, log, self)
                else:
                    break

                if resp.status_code != 200:
                    break

                items = resp.json()
                if not items:
                    break

                for item in items:
                    if entity_type == "patron":
                        cardnumber = item.get("cardnumber", "")
                        borrowernumber = item.get("patron_id", 0)
                        if cardnumber and borrowernumber:
                            mappings.append((cardnumber, borrowernumber))
                    total += 1

                self.update_state(state="PROGRESS", meta={
                    "step": f"scanning_{entity_type}s",
                    "records_done": total,
                })

                if len(items) < per_page:
                    break
                page += 1

        if mappings:
            _write_id_mappings(project_id, entity_type, mappings)

        log.complete(f"Built {len(mappings)} {entity_type} ID mappings from Koha")
        return {"entity_type": entity_type, "mappings_created": len(mappings), "total_scanned": total}

    except Exception as exc:
        log.error(f"ID mapping build failed: {exc}")
        raise


def _build_bib_mappings_from_files(project_id, log, task) -> dict:
    """Build bib mappings by matching local MARC 001 values against Koha biblionumbers.

    This works by reading the MARC file and assuming bibs were pushed in order,
    so the Nth record pushed got biblionumber = start_biblionumber + N.
    This is an approximation — for exact mapping, use the batch lookup plugin.
    """
    import pymarc
    from cerulean.tasks.push import _write_id_mappings

    project_dir = Path(settings.data_root) / project_id
    marc_file = None
    for name in ["output.mrc", "Biblios-mapped-items.mrc", "merged_deduped.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            marc_file = candidate
            break

    if not marc_file:
        log.warn("No MARC file found for bib mapping build")
        return {"entity_type": "bib", "mappings_created": 0, "error": "No MARC file"}

    # Collect 001 values
    control_numbers = []
    with open(str(marc_file), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue
            ctrl = record.get_fields("001")
            if ctrl:
                control_numbers.append(ctrl[0].data.strip())

    log.info(f"Found {len(control_numbers)} 001 values in {marc_file.name}")
    log.info("To build exact mappings, use the batch lookup plugin endpoint")

    return {
        "entity_type": "bib",
        "control_numbers_found": len(control_numbers),
        "mappings_created": 0,
        "message": "Use POST /api/v1/contrib/cerulean/lookup/batch to resolve 001 values to biblionumbers",
    }
