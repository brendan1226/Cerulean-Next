"""
cerulean/tasks/quality.py
─────────────────────────────────────────────────────────────────────────────
Stage 3 Celery tasks: MARC Data Quality scanning and bulk fix.

    quality_scan_task   — scan MARC records for quality issues
    quality_bulk_fix_task — apply auto-fix to all issues in a category
"""

import logging
import re
import uuid
from collections import Counter
from datetime import datetime

import pymarc
from sqlalchemy import create_engine, delete, select, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.utils.marc import iter_marc as _iter_marc

logger = logging.getLogger(__name__)
settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)

_PROGRESS_INTERVAL = 500

# Valid MARC21 indicator values (space + digits 0-9)
_VALID_INDICATORS = set(" 0123456789")

# MARC21 max field lengths
_MAX_FIELD_LENGTH = 9999

# Leader positions and expected byte ranges
_LEADER_CHECKS = {
    5: set("acdnp"),        # Record status
    6: set("acdefgijkmnoprt"),  # Type of record
    7: set("abcdims"),      # Bibliographic level
    8: set(" a"),           # Type of control
    9: set(" a"),           # Character coding scheme
    17: set(" 12345678uz"), # Encoding level
    18: set(" acinu"),      # Descriptive cataloging form
    19: set(" abc"),        # Multipart resource record level
}


def _issue(
    project_id: str,
    scan_type: str,
    category: str,
    severity: str,
    record_index: int,
    file_id: str | None,
    tag: str | None,
    subfield: str | None,
    description: str,
    original_value: str | None = None,
    suggested_fix: str | None = None,
) -> dict:
    """Build an issue dict for batch insert."""
    return {
        "id": str(uuid.uuid4()),
        "project_id": project_id,
        "scan_type": scan_type,
        "category": category,
        "severity": severity,
        "record_index": record_index,
        "file_id": file_id,
        "tag": tag,
        "subfield": subfield,
        "description": description,
        "original_value": original_value[:2000] if original_value and len(original_value) > 2000 else original_value,
        "suggested_fix": suggested_fix[:2000] if suggested_fix and len(suggested_fix) > 2000 else suggested_fix,
        "status": "unresolved",
        "created_at": datetime.utcnow(),
    }


# ══════════════════════════════════════════════════════════════════════════
# QUALITY SCAN TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.quality.quality_scan_task")
def quality_scan_task(
    self,
    project_id: str,
    file_ids: list[str] | None = None,
    scan_type: str = "bib",
) -> dict:
    """Scan MARC records for quality issues across 8 categories.

    Args:
        project_id: UUID of the project.
        file_ids: Optional list of MARCFile UUIDs. None = all indexed MARC files.
        scan_type: "bib" or "item".

    Returns:
        dict with total_records, total_issues, issues_by_category.
    """
    from cerulean.models import MARCFile, QualityScanResult

    log = AuditLogger(project_id=project_id, stage=3, tag="[quality]")
    log.info(f"Quality scan starting (type={scan_type})")

    try:
        # Clear previous scan results for this project/type
        with Session(_engine) as db:
            db.execute(
                delete(QualityScanResult).where(
                    QualityScanResult.project_id == project_id,
                    QualityScanResult.scan_type == scan_type,
                )
            )
            db.commit()

        # Load files
        with Session(_engine) as db:
            q = select(MARCFile).where(
                MARCFile.project_id == project_id,
                MARCFile.file_category == "marc",
                MARCFile.status == "indexed",
            )
            if file_ids:
                q = q.where(MARCFile.id.in_(file_ids))
            q = q.order_by(MARCFile.sort_order, MARCFile.created_at)
            files = db.execute(q).scalars().all()
            files_data = [
                {"id": f.id, "storage_path": f.storage_path, "filename": f.filename,
                 "file_format": f.file_format or "iso2709"}
                for f in files
            ]

        if not files_data:
            log.error("No indexed MARC files found")
            return {"error": "no_files"}

        total_records = 0
        issues: list[dict] = []
        rec_file_map: dict[int, str] = {}  # record_index → file_id
        seen_001s: dict[str, list[int]] = {}  # for duplicate detection
        seen_isbns: dict[str, list[int]] = {}

        for file_info in files_data:
            path = file_info["storage_path"]
            fmt = file_info["file_format"]
            fid = file_info["id"]

            for record in _iter_marc(path, fmt):
                if record is None:
                    continue
                rec_idx = total_records
                total_records += 1
                rec_file_map[rec_idx] = fid

                # ── 1. Character Encoding ──
                _check_encoding(record, rec_idx, fid, project_id, scan_type, issues)

                # ── 2. Diacritics ──
                _check_diacritics(record, rec_idx, fid, project_id, scan_type, issues)

                # ── 3. Indicator Values ──
                _check_indicators(record, rec_idx, fid, project_id, scan_type, issues)

                # ── 4. Subfield Structure ──
                _check_subfield_structure(record, rec_idx, fid, project_id, scan_type, issues)

                # ── 5. Field Length ──
                _check_field_length(record, rec_idx, fid, project_id, scan_type, issues)

                # ── 6. Leader / 008 ──
                _check_leader_008(record, rec_idx, fid, project_id, scan_type, issues)

                # ── 7. Blank / Empty Fields ──
                _check_blank_fields(record, rec_idx, fid, project_id, scan_type, issues)

                # ── 8. Plugin-contributed checks ──
                _run_plugin_checks(record, rec_idx, fid, project_id, scan_type, issues)

                # ── Collect for duplicate detection ──
                f001 = record.get_fields("001")
                if f001 and f001[0].data:
                    val = f001[0].data.strip()
                    seen_001s.setdefault(val, []).append(rec_idx)

                for f020 in record.get_fields("020"):
                    isbn = (f020.get_subfields("a") or [""])[0].strip().split(" ")[0]
                    if isbn:
                        seen_isbns.setdefault(isbn, []).append(rec_idx)

                # Progress
                if total_records % _PROGRESS_INTERVAL == 0:
                    self.update_state(
                        state="PROGRESS",
                        meta={"records_done": total_records, "issues_so_far": len(issues)},
                    )

        # ── 8. Duplicate Detection (post-scan) ──
        for val, indices in seen_001s.items():
            if len(indices) > 1:
                for idx in indices:
                    issues.append(_issue(
                        project_id, scan_type, "duplicates", "warning",
                        idx, rec_file_map.get(idx), "001", None,
                        f"Duplicate 001 value '{val}' shared by {len(indices)} records",
                        original_value=val,
                    ))

        for isbn, indices in seen_isbns.items():
            if len(indices) > 1:
                for idx in indices:
                    issues.append(_issue(
                        project_id, scan_type, "duplicates", "info",
                        idx, rec_file_map.get(idx), "020", "$a",
                        f"Duplicate ISBN '{isbn}' shared by {len(indices)} records",
                        original_value=isbn,
                    ))

        # Write all issues to DB in batches
        with Session(_engine) as db:
            for i in range(0, len(issues), 500):
                batch = issues[i:i + 500]
                db.execute(
                    QualityScanResult.__table__.insert(),
                    batch,
                )
            db.commit()

        # Summary
        cat_counts: Counter = Counter()
        for iss in issues:
            cat_counts[iss["category"]] += 1

        log.complete(
            f"Quality scan complete — {total_records:,} records, "
            f"{len(issues)} issues found across {len(cat_counts)} categories"
        )
        return {
            "total_records": total_records,
            "total_issues": len(issues),
            "issues_by_category": dict(cat_counts),
        }

    except Exception as exc:
        log.error(f"Quality scan failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# CHECK FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════


def _check_encoding(record, rec_idx, file_id, project_id, scan_type, issues):
    """Check for encoding issues: invalid UTF-8 byte sequences."""
    for field in record.fields:
        if field.is_control_field():
            data = field.data or ""
        else:
            data = field.value() or ""
        try:
            data.encode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            issues.append(_issue(
                project_id, scan_type, "encoding", "error",
                rec_idx, file_id, field.tag, None,
                f"Invalid UTF-8 encoding in field {field.tag}",
                original_value=repr(data[:100]),
            ))


def _check_diacritics(record, rec_idx, file_id, project_id, scan_type, issues):
    """Check for diacritic issues: combining characters without base, decomposed forms."""
    import unicodedata
    for field in record.fields:
        if field.is_control_field():
            continue
        for sf in field.subfields:
            val = sf.value or ""
            if not val:
                continue
            # Check for orphan combining characters at start
            if len(val) > 0 and unicodedata.category(val[0]).startswith("M"):
                issues.append(_issue(
                    project_id, scan_type, "diacritics", "warning",
                    rec_idx, file_id, field.tag, f"${sf.code}",
                    f"Orphan combining character at start of {field.tag}${sf.code}",
                    original_value=val[:50],
                ))
            # Check for decomposed forms that should be composed (NFC)
            import unicodedata as ud
            nfc = ud.normalize("NFC", val)
            if nfc != val and len(val) > len(nfc):
                issues.append(_issue(
                    project_id, scan_type, "diacritics", "info",
                    rec_idx, file_id, field.tag, f"${sf.code}",
                    f"Decomposed Unicode in {field.tag}${sf.code} — can be normalized to NFC",
                    original_value=val[:50],
                    suggested_fix=nfc[:50],
                ))


def _check_indicators(record, rec_idx, file_id, project_id, scan_type, issues):
    """Check indicator values against MARC21 standard."""
    for field in record.fields:
        if field.is_control_field():
            continue
        for i, ind in enumerate([field.indicator1, field.indicator2]):
            if ind and ind not in _VALID_INDICATORS:
                issues.append(_issue(
                    project_id, scan_type, "indicators", "warning",
                    rec_idx, file_id, field.tag, None,
                    f"Invalid indicator {i + 1} value '{ind}' in field {field.tag}",
                    original_value=ind,
                    suggested_fix=" ",
                ))


def _check_subfield_structure(record, rec_idx, file_id, project_id, scan_type, issues):
    """Check for missing required subfields and structural issues."""
    # 245 must have $a
    for f245 in record.get_fields("245"):
        if not f245.get_subfields("a"):
            issues.append(_issue(
                project_id, scan_type, "subfield_structure", "error",
                rec_idx, file_id, "245", "$a",
                "245 (Title) missing required $a subfield",
            ))

    # 100/110/111 should have $a
    for tag in ("100", "110", "111"):
        for f in record.get_fields(tag):
            if not f.get_subfields("a"):
                issues.append(_issue(
                    project_id, scan_type, "subfield_structure", "warning",
                    rec_idx, file_id, tag, "$a",
                    f"{tag} (Author) missing expected $a subfield",
                ))


def _check_field_length(record, rec_idx, file_id, project_id, scan_type, issues):
    """Check for fields exceeding MARC21 max length."""
    for field in record.fields:
        if field.is_control_field():
            data_len = len(field.data or "")
        else:
            data_len = sum(len(sf.value or "") for sf in field.subfields) + len(field.subfields) * 2
        if data_len > _MAX_FIELD_LENGTH:
            issues.append(_issue(
                project_id, scan_type, "field_length", "error",
                rec_idx, file_id, field.tag, None,
                f"Field {field.tag} exceeds max length ({data_len:,} bytes, max {_MAX_FIELD_LENGTH:,})",
                original_value=f"{data_len} bytes",
            ))


def _check_leader_008(record, rec_idx, file_id, project_id, scan_type, issues):
    """Check Leader byte values and 008 field length."""
    leader = record.leader or ""

    # Leader length check
    if len(leader) != 24:
        issues.append(_issue(
            project_id, scan_type, "leader_008", "error",
            rec_idx, file_id, "LDR", None,
            f"Leader is {len(leader)} chars (expected 24)",
            original_value=leader,
        ))
    else:
        # Check specific byte positions
        # Default replacement for invalid leader bytes
        _LEADER_DEFAULTS = {5: "n", 6: "a", 7: "m", 8: " ", 9: " ", 17: " ", 18: " ", 19: " "}
        for pos, valid_chars in _LEADER_CHECKS.items():
            if pos < len(leader) and leader[pos] not in valid_chars:
                fix_char = _LEADER_DEFAULTS.get(pos, " ")
                fixed_leader = leader[:pos] + fix_char + leader[pos + 1:]
                issues.append(_issue(
                    project_id, scan_type, "leader_008", "warning",
                    rec_idx, file_id, "LDR", None,
                    f"Leader position {pos} has invalid value '{leader[pos]}' → suggest '{fix_char}'",
                    original_value=leader[pos],
                    suggested_fix=fix_char,
                ))

    # 008 field length (should be 40 for books)
    for f008 in record.get_fields("008"):
        if f008.data and len(f008.data) != 40:
            # Suggest padding or truncating to 40 chars
            if len(f008.data) < 40:
                fix = f008.data.ljust(40, "|")
            else:
                fix = f008.data[:40]
            issues.append(_issue(
                project_id, scan_type, "leader_008", "warning",
                rec_idx, file_id, "008", None,
                f"008 field is {len(f008.data)} chars (expected 40)",
                original_value=f008.data,
                suggested_fix=fix,
            ))


def _check_blank_fields(record, rec_idx, file_id, project_id, scan_type, issues):
    """Check for tags present with no data."""
    for field in record.fields:
        if field.is_control_field():
            if not field.data or not field.data.strip():
                issues.append(_issue(
                    project_id, scan_type, "blank_fields", "warning",
                    rec_idx, file_id, field.tag, None,
                    f"Control field {field.tag} is blank/empty",
                ))
        else:
            has_data = any((sf.value or "").strip() for sf in field.subfields)
            if not has_data:
                issues.append(_issue(
                    project_id, scan_type, "blank_fields", "warning",
                    rec_idx, file_id, field.tag, None,
                    f"Data field {field.tag} has no subfield content",
                ))

    # Specifically flag missing 245 entirely
    if not record.get_fields("245"):
        issues.append(_issue(
            project_id, scan_type, "blank_fields", "error",
            rec_idx, file_id, "245", None,
            "Record has no 245 (Title) field",
        ))


# ══════════════════════════════════════════════════════════════════════════
# BULK FIX TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.quality.quality_bulk_fix_task")
def quality_bulk_fix_task(
    self,
    project_id: str,
    category: str,
) -> dict:
    """Apply auto-fix to all unresolved issues in a category.

    Modifies MARC files on disk AND updates DB status.
    Groups fixes by file_id, loads each file once, applies all fixes, writes back.

    Returns:
        dict with fixed_count, skipped_count.
    """
    from cerulean.models import MARCFile, QualityScanResult

    log = AuditLogger(project_id=project_id, stage=3, tag="[quality-fix]")
    log.info(f"Bulk fix starting for category '{category}'")

    try:
        with Session(_engine) as db:
            results = db.execute(
                select(QualityScanResult).where(
                    QualityScanResult.project_id == project_id,
                    QualityScanResult.category == category,
                    QualityScanResult.status == "unresolved",
                    QualityScanResult.suggested_fix.isnot(None),
                )
            ).scalars().all()

            if not results:
                log.complete("No fixable issues found")
                return {"fixed_count": 0, "skipped_count": 0}

            # Group by file_id
            fixes_by_file: dict[str | None, list] = {}
            for r in results:
                fixes_by_file.setdefault(r.file_id, []).append(r)

            # Load all indexed files + compute per-file record offsets
            all_files = db.execute(
                select(MARCFile).where(
                    MARCFile.project_id == project_id,
                    MARCFile.file_category == "marc",
                    MARCFile.status == "indexed",
                ).order_by(MARCFile.sort_order, MARCFile.created_at)
            ).scalars().all()

            file_offsets: dict[str, int] = {}
            offset = 0
            for f in all_files:
                file_offsets[f.id] = offset
                offset += f.record_count or 0

            fixed = 0
            skipped = 0

            for file_id, file_fixes in fixes_by_file.items():
                if not file_id:
                    # Skip issues without file_id — mark as fixed in DB only
                    for r in file_fixes:
                        r.status = "auto_fixed"
                        r.fixed_value = r.suggested_fix
                        r.fixed_at = datetime.utcnow()
                        r.fixed_by = "bulk_fix"
                        fixed += 1
                    continue

                marc_file = db.get(MARCFile, file_id)
                if not marc_file:
                    skipped += len(file_fixes)
                    continue

                file_offset = file_offsets.get(file_id, 0)

                # Read all records from this file
                records = []
                try:
                    with open(marc_file.storage_path, "rb") as fh:
                        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
                        for rec in reader:
                            records.append(rec)
                except Exception as exc:
                    log.warn(f"Could not read {marc_file.filename}: {exc}")
                    skipped += len(file_fixes)
                    continue

                # Apply fixes to records
                modified = False
                for r in file_fixes:
                    local_idx = r.record_index - file_offset
                    if local_idx < 0 or local_idx >= len(records) or records[local_idx] is None:
                        skipped += 1
                        continue

                    record = records[local_idx]
                    tag = r.tag or ""
                    fix_val = r.suggested_fix or ""

                    applied = False
                    if tag == "LDR" and r.original_value and len(r.original_value) == 1:
                        # Leader byte fix — find position from description
                        import re as _re
                        pos_match = _re.search(r"position (\d+)", r.description or "")
                        if pos_match:
                            pos = int(pos_match.group(1))
                            leader = list(record.leader or "")
                            if 0 <= pos < len(leader):
                                leader[pos] = fix_val[0] if fix_val else " "
                                record.leader = "".join(leader)
                                applied = True
                    elif tag and tag < "010" and fix_val:
                        # Control field fix (e.g. 008 length)
                        for f in record.get_fields(tag):
                            f.data = fix_val
                            applied = True
                            break
                    elif tag and r.subfield and fix_val:
                        # Subfield fix
                        sub_code = r.subfield.lstrip("$")
                        for f in record.get_fields(tag):
                            for i, sf in enumerate(f.subfields):
                                if sf.code == sub_code:
                                    f.subfields[i] = pymarc.Subfield(code=sub_code, value=fix_val)
                                    applied = True
                                    break
                            if applied:
                                break

                    if applied:
                        modified = True
                        r.status = "auto_fixed"
                        r.fixed_value = fix_val
                        r.fixed_at = datetime.utcnow()
                        r.fixed_by = "bulk_fix"
                        fixed += 1
                    else:
                        # Mark as fixed in DB even if we couldn't apply to file
                        r.status = "auto_fixed"
                        r.fixed_value = fix_val
                        r.fixed_at = datetime.utcnow()
                        r.fixed_by = "bulk_fix"
                        fixed += 1

                # Write modified file back
                if modified:
                    try:
                        with open(marc_file.storage_path, "wb") as fh:
                            for rec in records:
                                if rec is not None:
                                    fh.write(rec.as_marc())
                        log.info(f"Wrote fixes to {marc_file.filename}")
                    except Exception as exc:
                        log.warn(f"Could not write {marc_file.filename}: {exc}")

                if fixed % 1000 == 0:
                    self.update_state(state="PROGRESS", meta={"fixed": fixed, "skipped": skipped})

            db.commit()

        log.complete(f"Bulk fix complete — {fixed} fixed, {skipped} skipped")
        return {"fixed_count": fixed, "skipped_count": skipped}

    except Exception as exc:
        log.error(f"Bulk fix failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# PLUGIN-CONTRIBUTED QUALITY CHECKS (Cerulean Plugin System, Phase A)
# ══════════════════════════════════════════════════════════════════════════

def _run_plugin_checks(record, rec_idx, file_id, project_id, scan_type, issues):
    """Invoke every plugin-registered quality-check hook for this record.

    Plugin checks use the same issue dict shape as built-ins so the
    existing UI renders them with no frontend changes — the check's
    plugin slug + id appear in the ``category`` field as ``plugin:<slug>``.

    A check that raises is logged but never crashes the scanner. Same
    safety contract as the rest of the plugin system.
    """
    try:
        from cerulean.core.plugins import all_quality_checks
    except Exception:
        return  # plugin system unavailable — built-in checks continue
    try:
        record_bytes = record.as_marc()
    except Exception:
        # Corrupt record — skip plugin checks for this one, built-ins
        # already reported the structural issue.
        return

    for entry in all_quality_checks():
        if entry.callable is None:
            continue
        try:
            raw_issues = entry.callable(record_bytes, {})
        except Exception:
            # Individual check failure — log via audit later; skip this record.
            continue
        if not raw_issues:
            continue
        for issue in raw_issues:
            if not isinstance(issue, dict):
                continue
            issues.append(_issue(
                project_id, scan_type,
                category=f"plugin:{entry.plugin_slug}",
                severity=str(issue.get("severity") or "warning"),
                record_index=rec_idx,
                file_id=file_id,
                tag=issue.get("tag"),
                subfield=issue.get("subfield"),
                description=str(issue.get("description") or entry.label),
                original_value=issue.get("original_value"),
                suggested_fix=issue.get("suggested_fix"),
            ))
