"""
cerulean/tasks/transform.py
─────────────────────────────────────────────────────────────────────────────
Stage 3 Celery tasks:

    transform_pipeline_task  — apply approved FieldMaps to indexed MARC files
    merge_pipeline_task      — merge transformed files + optional items CSV join

All tasks write AuditEvent rows via AuditLogger.
"""

import csv
import json
import logging
import os
import re
import time

logger = logging.getLogger(__name__)
from collections import Counter
from datetime import datetime
from pathlib import Path

import pymarc
from pymarc import Subfield
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.helpers import check_paused as _check_paused
from cerulean.utils.marc import iter_marc as _iter_marc, get_001 as _get_001, write_marc as _write_marc

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)

_PROGRESS_INTERVAL = 500

_VALID_TRANSFORM_TYPES = {"copy", "regex", "lookup", "const", "fn", "preset"}


def _validate_map(m: dict) -> tuple[bool, str]:
    """Validate a single field map dict. Returns (is_valid, reason)."""
    src = m.get("source_tag") or ""
    tgt = m.get("target_tag") or ""
    ttype = m.get("transform_type") or ""
    tfn = m.get("transform_fn") or ""
    pkey = m.get("preset_key") or ""

    if not src or not src.strip().isdigit() or len(src.strip()) != 3:
        return False, f"Invalid source tag '{src}'"
    if not tgt or not tgt.strip().isdigit() or len(tgt.strip()) != 3:
        return False, f"Invalid target tag '{tgt}'"
    if ttype not in _VALID_TRANSFORM_TYPES:
        return False, f"Unknown transform type '{ttype}'"
    if ttype in ("regex", "fn") and not tfn.strip():
        return False, f"Transform type '{ttype}' requires an expression"
    if ttype == "const" and not tfn.strip():
        return False, f"Constant transform requires a value"
    if ttype == "lookup":
        if not tfn.strip():
            return False, "Lookup transform requires a JSON table"
        try:
            import json as _json
            _json.loads(tfn)
        except (ValueError, TypeError):
            return False, "Lookup transform_fn is not valid JSON"
    if ttype == "preset" and not pkey.strip():
        return False, "Preset transform requires a preset_key"
    return True, ""


# ══════════════════════════════════════════════════════════════════════════
# TRANSFORM PIPELINE TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.transform.transform_pipeline_task")
def transform_pipeline_task(
    self,
    project_id: str,
    manifest_id: str,
    file_ids: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """Apply all approved FieldMaps to indexed MARC files, write transformed output.

    Args:
        project_id: UUID string of the project.
        manifest_id: UUID of the TransformManifest row created by the API.
        file_ids: Optional list of MARCFile UUIDs to process (None = all indexed).
        dry_run: If True, reads and validates but does not write output files.

    Returns:
        dict with files_processed, total_records, records_skipped.
    """
    from cerulean.models import FieldMap, MARCFile, Project, TransformManifest

    log = AuditLogger(project_id=project_id, stage=3, tag="[transform]")
    log.info("Transform pipeline starting")

    try:
        # 1. Load approved field maps
        with Session(_engine) as db:
            maps_q = (
                select(FieldMap)
                .where(FieldMap.project_id == project_id, FieldMap.approved == True)  # noqa: E712
                .order_by(FieldMap.sort_order, FieldMap.created_at)
            )
            field_maps = db.execute(maps_q).scalars().all()

            if not field_maps:
                log.error("No approved field maps found")
                _update_manifest_error(db, manifest_id, "No approved field maps")
                return {"error": "no_approved_maps"}

            all_maps_data = [
                {
                    "source_tag": m.source_tag,
                    "source_sub": m.source_sub,
                    "target_tag": m.target_tag,
                    "target_sub": m.target_sub,
                    "transform_type": m.transform_type,
                    "transform_fn": m.transform_fn,
                    "preset_key": m.preset_key,
                    "delete_source": m.delete_source,
                }
                for m in field_maps
            ]

            # Validate maps — skip invalid ones and log warnings
            maps_data = []
            skipped_maps = []
            for md in all_maps_data:
                valid, reason = _validate_map(md)
                if valid:
                    maps_data.append(md)
                else:
                    label = f"{md.get('source_tag','?')}{md.get('source_sub','')}->{md.get('target_tag','?')}{md.get('target_sub','')}"
                    log.warn(f"Skipping invalid map {label}: {reason}")
                    skipped_maps.append({"map": label, "reason": reason})

            if not maps_data:
                log.error("No valid approved field maps after validation")
                _update_manifest_error(db, manifest_id, "No valid approved field maps")
                return {"error": "no_valid_maps", "skipped_maps": skipped_maps}

            # 2. Load MARC files
            files_q = select(MARCFile).where(
                MARCFile.project_id == project_id,
                MARCFile.status == "indexed",
            )
            if file_ids:
                files_q = files_q.where(MARCFile.id.in_(file_ids))
            files_q = files_q.order_by(MARCFile.sort_order, MARCFile.created_at)
            marc_files = db.execute(files_q).scalars().all()

            if not marc_files:
                log.error("No indexed MARC files found")
                _update_manifest_error(db, manifest_id, "No indexed MARC files")
                return {"error": "no_indexed_files"}

            files_data = [
                {"id": f.id, "storage_path": f.storage_path, "filename": f.filename,
                 "file_format": f.file_format or "iso2709"}
                for f in marc_files
            ]

        if skipped_maps:
            log.warn(f"Skipped {len(skipped_maps)} invalid map(s) during validation")
        log.info(f"Processing {len(files_data)} file(s) with {len(maps_data)} valid map(s)")

        # Log each map being applied so user can audit
        for md in maps_data:
            src = f"{md['source_tag']}{md['source_sub'] or ''}"
            tgt = f"{md['target_tag']}{md['target_sub'] or ''}"
            log.info(f"  Map: {src} → {tgt} ({md['transform_type']})")

        # 3. Output directory — clean up old transformed files first
        project_dir = Path(settings.data_root) / project_id / "transformed"
        if not dry_run:
            project_dir.mkdir(parents=True, exist_ok=True)
            for old_file in project_dir.glob("*_transformed.mrc"):
                old_file.unlink()
                log.info(f"Removed stale file: {old_file.name}")

        # 4. Transform each file
        total_records = 0
        records_skipped = 0
        files_processed = 0
        aggregate_stats: dict[str, dict[str, int]] = {}

        for file_info in files_data:
            file_records, file_skipped, file_stats = _transform_file(
                self, log, file_info, maps_data, project_dir, dry_run,
                total_records, files_processed, len(files_data), project_id,
            )
            total_records += file_records
            records_skipped += file_skipped
            files_processed += 1
            # Merge per-file stats into aggregate
            for label, counts in file_stats.items():
                agg = aggregate_stats.setdefault(label, {"applied": 0, "skipped": 0, "errors": 0})
                agg["applied"] += counts["applied"]
                agg["skipped"] += counts["skipped"]
                agg["errors"] += counts["errors"]

        # 5. Update manifest + mark stage 3 complete
        with Session(_engine) as db:
            db.execute(
                update(TransformManifest)
                .where(TransformManifest.id == manifest_id)
                .values(
                    status="complete",
                    files_processed=files_processed,
                    total_records=total_records,
                    records_skipped=records_skipped,
                    file_ids=[f["id"] for f in files_data],
                    completed_at=datetime.utcnow(),
                )
            )
            project = db.get(Project, project_id)
            if project and not project.stage_3_complete:
                project.stage_3_complete = True
                project.current_stage = max(project.current_stage or 0, 4)
                project.bib_count_ingested = total_records
            db.commit()

        # Build summary of maps with issues for quick visibility
        map_stats_list = [
            {"map": label, **counts}
            for label, counts in aggregate_stats.items()
        ]

        log.complete(
            f"Transform complete — {files_processed} file(s), "
            f"{total_records:,} records ({records_skipped} skipped), "
            f"{len(maps_data)} maps applied"
        )
        return {
            "files_processed": files_processed,
            "total_records": total_records,
            "records_skipped": records_skipped,
            "dry_run": dry_run,
            "map_stats": map_stats_list,
        }

    except Exception as exc:
        log.error(f"Transform pipeline failed: {exc}")
        with Session(_engine) as db:
            _update_manifest_error(db, manifest_id, str(exc))
        raise


# ══════════════════════════════════════════════════════════════════════════
# MERGE PIPELINE TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.transform.merge_pipeline_task")
def merge_pipeline_task(
    self,
    project_id: str,
    manifest_id: str,
    file_ids: list[str] | None = None,
    items_csv_path: str | None = None,
    items_csv_match_tag: str = "001",
    items_csv_key_column: str = "biblionumber",
) -> dict:
    """Merge transformed MARC files into one output. Optionally join items CSV as 952 fields.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the TransformManifest row.
        file_ids: Ordered list of MARCFile UUIDs (None = all by sort_order).
        items_csv_path: Optional path to items CSV for 952 join.
        items_csv_match_tag: MARC tag to match against CSV key (default "001").
        items_csv_key_column: CSV column name for bib key (default "biblionumber").

    Returns:
        dict with total_records, items_joined, duplicate_001s, output_path.
    """
    from cerulean.models import MARCFile, Project, TransformManifest

    log = AuditLogger(project_id=project_id, stage=3, tag="[merge]")
    log.info("Merge pipeline starting")

    try:
        # 1. Load file metadata
        with Session(_engine) as db:
            files_q = select(MARCFile).where(MARCFile.project_id == project_id)
            if file_ids:
                files_q = files_q.where(MARCFile.id.in_(file_ids))
            else:
                files_q = files_q.where(MARCFile.status == "indexed")
            files_q = files_q.order_by(MARCFile.sort_order, MARCFile.created_at)
            marc_files = db.execute(files_q).scalars().all()

            if not marc_files:
                log.error("No files found for merge")
                _update_manifest_error(db, manifest_id, "No files to merge")
                return {"error": "no_files"}

            files_data = [
                {"id": f.id, "storage_path": f.storage_path, "filename": f.filename,
                 "file_format": f.file_format or "iso2709"}
                for f in marc_files
            ]

        # Re-sort by caller's file_ids order if specified
        if file_ids:
            id_order = {fid: i for i, fid in enumerate(file_ids)}
            files_data.sort(key=lambda f: id_order.get(f["id"], 0))

        # 2. Load items CSV if provided
        items_lookup: dict[str, list[dict]] | None = None
        if items_csv_path and os.path.isfile(items_csv_path):
            items_lookup = _load_items_csv(items_csv_path, items_csv_key_column)
            log.info(f"Loaded items CSV: {len(items_lookup)} unique bib keys")

        # 3. Resolve file paths — prefer transformed/ versions
        project_dir = Path(settings.data_root) / project_id
        transformed_dir = project_dir / "transformed"
        merge_sources: list[tuple[str, str]] = []
        for f in files_data:
            stem = Path(f["filename"]).stem
            transformed_path = transformed_dir / f"{stem}_transformed.mrc"
            if transformed_path.is_file():
                merge_sources.append((str(transformed_path), "iso2709"))
            else:
                merge_sources.append((f["storage_path"], f["file_format"]))

        # 4. Merge all records into one file
        output_path = project_dir / "merged.mrc"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        total_records = 0
        items_joined = 0
        seen_001: Counter = Counter()

        with open(str(output_path), "wb") as out_fh:
            for file_idx, (path, fmt) in enumerate(merge_sources):
                log.info(f"Merging file {file_idx + 1}/{len(merge_sources)}: {Path(path).name}")

                for record in _iter_marc(path, fmt):
                    total_records += 1

                    # Track 001 values for dupe detection
                    record_001 = _get_001(record)
                    if record_001:
                        seen_001[record_001] += 1

                    # Items CSV join
                    if items_lookup and record_001:
                        match_values = _extract_values(record, items_csv_match_tag, None)
                        for mv in match_values:
                            for item_row in items_lookup.get(mv, []):
                                _add_952_from_csv(record, item_row)
                                items_joined += 1

                    _sort_record(record)
                    out_fh.write(record.as_marc())

                    if total_records % _PROGRESS_INTERVAL == 0:
                        self.update_state(
                            state="PROGRESS",
                            meta={
                                "files_done": file_idx,
                                "files_total": len(merge_sources),
                                "records_done": total_records,
                                "items_joined": items_joined,
                            },
                        )

        # 5. Detect duplicate 001 values
        duplicate_001s = [
            {"value": k, "count": v}
            for k, v in seen_001.items()
            if v > 1
        ]
        if duplicate_001s:
            log.warn(
                f"Detected {len(duplicate_001s)} duplicate 001 values — "
                f"review in Stage 4 (Dedup)"
            )

        # 6. Update project & manifest
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if project:
                project.stage_3_complete = True
                project.current_stage = 4
                project.bib_count_ingested = total_records

            db.execute(
                update(TransformManifest)
                .where(TransformManifest.id == manifest_id)
                .values(
                    status="complete",
                    output_path=str(output_path),
                    files_processed=len(merge_sources),
                    total_records=total_records,
                    items_joined=items_joined,
                    duplicate_001s=duplicate_001s,
                    items_csv_path=items_csv_path,
                    file_ids=[f["id"] for f in files_data],
                    completed_at=datetime.utcnow(),
                )
            )
            db.commit()

        log.complete(
            f"Merge complete — {total_records:,} records, "
            f"{items_joined} items joined, {len(duplicate_001s)} duplicate 001s"
        )
        return {
            "total_records": total_records,
            "items_joined": items_joined,
            "duplicate_001s": duplicate_001s,
            "output_path": str(output_path),
        }

    except Exception as exc:
        log.error(f"Merge pipeline failed: {exc}")
        with Session(_engine) as db:
            _update_manifest_error(db, manifest_id, str(exc))
        raise


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════


def _transform_file(
    self,
    log: AuditLogger,
    file_info: dict,
    maps_data: list[dict],
    output_dir: Path,
    dry_run: bool,
    global_record_offset: int,
    files_done: int,
    files_total: int,
    project_id: str = "",
) -> tuple[int, int, dict]:
    """Transform a single MARC file by applying all field maps.

    Returns (total_records, records_skipped, stats_dict).
    """
    storage_path = file_info["storage_path"]
    filename = file_info["filename"]
    fmt = file_info["file_format"]
    log.info(f"Transforming {filename}")

    records_in_file = 0
    skipped = 0
    stats: dict[str, dict[str, int]] = {}  # per-map applied/skipped/errors

    # Stream records to disk to avoid OOM on large files
    stem = Path(filename).stem
    out_path = output_dir / f"{stem}_transformed.mrc"
    out_fh = None
    if not dry_run:
        out_fh = open(str(out_path), "wb")

    # What source tags do maps expect?
    expected_tags = sorted(set(m["source_tag"] for m in maps_data))

    try:
        for record in _iter_marc(storage_path, fmt):
            if record is None:
                skipped += 1
                continue
            _check_paused(project_id, self)
            records_in_file += 1

            # First-record diagnostic: log which source tags actually exist
            if records_in_file == 1:
                actual_tags = sorted(set(f.tag for f in record.fields))
                missing = sorted(set(expected_tags) - set(actual_tags))
                log.info(f"Record 1 tags present: {', '.join(actual_tags)}")
                if missing:
                    log.warn(f"Maps expect these tags but record 1 lacks them: {', '.join(missing)}")
            try:
                _apply_maps_to_record(record, maps_data, stats)
                _sort_record(record)
                if out_fh:
                    out_fh.write(record.as_marc())
            except Exception as exc:
                record_001 = _get_001(record)
                log.warn(
                    f"Skipped record {records_in_file} (001={record_001}): {exc}",
                    record_001=record_001,
                )
                skipped += 1

            total_so_far = global_record_offset + records_in_file
            if records_in_file % _PROGRESS_INTERVAL == 0:
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "files_done": files_done,
                        "files_total": files_total,
                        "current_file": filename,
                        "records_done": total_so_far,
                    },
                )
    finally:
        if out_fh:
            out_fh.close()

    if not dry_run and records_in_file > 0:
        log.info(f"Wrote {records_in_file:,} records to {out_path.name}")

    # Log per-map stats to audit log so user can see in Project Log
    if stats:
        for map_label, counts in stats.items():
            a, s, e = counts["applied"], counts["skipped"], counts["errors"]
            level = "warn" if e > 0 else "info"
            msg = f"Map {map_label}: applied={a}, skipped(no source)={s}, errors={e}"
            getattr(log, level)(msg)

    return records_in_file, skipped, stats


def _apply_maps_to_record(
    record: pymarc.Record,
    maps_data: list[dict],
    stats: dict | None = None,
) -> pymarc.Record:
    """Apply all field maps to a MARC record (mutates in place).

    Maps that share the same (source_tag, target_tag) are grouped and applied
    field-instance-by-instance so repeating fields stay correlated.
    For example, if three 999 fields each map subfields to 952, the result
    is three 952 fields — one per source 999 instance.

    Source field deletions (delete_source) are deferred until ALL maps have
    finished reading, so later maps can still access shared source tags.

    If *stats* dict is provided, it tracks per-map hit/skip/error counts.
    """
    from collections import defaultdict

    deferred_deletes: set[str] = set()

    # ── Step 1: Group maps by (source_tag, target_tag) ──────────────
    correlated: dict[tuple[str, str], list[dict]] = defaultdict(list)
    const_maps: list[dict] = []

    for m in maps_data:
        if m["transform_type"] == "const":
            const_maps.append(m)
        else:
            correlated[(m["source_tag"], m["target_tag"])].append(m)

    # ── Step 2: Process correlated groups ────────────────────────────
    for (src_tag, tgt_tag), group in correlated.items():
        source_fields = record.get_fields(src_tag)

        if not source_fields:
            for m in group:
                label = _map_label(m)
                if stats is not None:
                    stats.setdefault(label, {"applied": 0, "skipped": 0, "errors": 0})
                    stats[label]["skipped"] += 1
            continue

        # For each source field instance, build one target field
        for src_field in source_fields:
            new_subs: list[Subfield] = []
            labels_hit: list[str] = []

            for m in group:
                label = _map_label(m)
                try:
                    src_sub = m["source_sub"]
                    tgt_sub = m["target_sub"]
                    sub_code = src_sub.lstrip("$") if src_sub else None
                    tgt_sub_code = tgt_sub.lstrip("$") if tgt_sub else "a"

                    # Extract from THIS specific field instance
                    if src_field.is_control_field():
                        vals = [src_field.data] if src_field.data else []
                    elif sub_code:
                        vals = [v for v in src_field.get_subfields(sub_code) if v]
                    else:
                        val = src_field.value()
                        vals = [val] if val else []

                    if not vals:
                        if stats is not None:
                            stats.setdefault(label, {"applied": 0, "skipped": 0, "errors": 0})
                            stats[label]["skipped"] += 1
                        continue

                    # Transform values
                    if m["transform_type"] == "preset":
                        from cerulean.core.transform_presets import apply_preset
                        transformed = [
                            r for r in (apply_preset(m.get("preset_key", ""), v) for v in vals)
                            if r is not None
                        ]
                    else:
                        transformed = [
                            r for r in (_apply_transform(v, m["transform_type"], m["transform_fn"]) for v in vals)
                            if r is not None
                        ]

                    for tv in transformed:
                        new_subs.append(Subfield(code=tgt_sub_code, value=tv))

                    if transformed:
                        labels_hit.append(label)

                except Exception as exc:
                    logger.warning("Map %s failed on field instance: %s", label, exc)
                    if stats is not None:
                        stats.setdefault(label, {"applied": 0, "skipped": 0, "errors": 0})
                        stats[label]["errors"] += 1

            # Create the target field from collected subfields
            if new_subs:
                if tgt_tag < "010":
                    # Control field — overwrite or create
                    existing = record.get_fields(tgt_tag)
                    if existing:
                        existing[0].data = new_subs[0].value
                    else:
                        record.add_field(pymarc.Field(tag=tgt_tag, data=new_subs[0].value))
                else:
                    record.add_field(pymarc.Field(
                        tag=tgt_tag, indicators=[" ", " "], subfields=new_subs,
                    ))

            for label in labels_hit:
                if stats is not None:
                    stats.setdefault(label, {"applied": 0, "skipped": 0, "errors": 0})
                    stats[label]["applied"] += 1

        # Collect deferred deletes
        for m in group:
            if m.get("delete_source") and m["source_tag"] != m["target_tag"]:
                deferred_deletes.add(m["source_tag"])

    # ── Step 3: Process const maps (no source field) ─────────────────
    for m in const_maps:
        label = _map_label(m)
        try:
            _set_value(record, m["target_tag"], m["target_sub"], m["transform_fn"] or "")
            if stats is not None:
                stats.setdefault(label, {"applied": 0, "skipped": 0, "errors": 0})
                stats[label]["applied"] += 1
        except Exception as exc:
            logger.warning("Const map %s failed: %s", label, exc)
            if stats is not None:
                stats.setdefault(label, {"applied": 0, "skipped": 0, "errors": 0})
                stats[label]["errors"] += 1

    # ── Step 4: Deferred source field deletions ──────────────────────
    for tag in deferred_deletes:
        for field in record.get_fields(tag):
            record.remove_field(field)

    return record


def _map_label(m: dict) -> str:
    return f"{m.get('source_tag','?')}{m.get('source_sub','')}->{m.get('target_tag','?')}{m.get('target_sub','')}"


def _extract_values(record: pymarc.Record, tag: str, sub: str | None) -> list[str]:
    """Extract value(s) from a MARC record for the given tag/subfield."""
    values: list[str] = []
    sub_code = sub.lstrip("$") if sub else None

    for field in record.get_fields(tag):
        if field.is_control_field():
            if field.data:
                values.append(field.data)
        elif sub_code:
            for val in field.get_subfields(sub_code):
                if val:
                    values.append(val)
        else:
            # No subfield specified — use field.value() which joins all subfields
            val = field.value()
            if val:
                values.append(val)

    return values


def _apply_transform(value: str, transform_type: str, transform_fn: str | None) -> str | None:
    """Apply a single transform to a value. Returns transformed value or None."""
    if transform_type == "copy":
        return value
    elif transform_type == "regex":
        return _apply_regex(value, transform_fn)
    elif transform_type == "lookup":
        return _apply_lookup(value, transform_fn)
    elif transform_type == "const":
        return transform_fn or ""
    elif transform_type == "fn":
        return _apply_fn(value, transform_fn)
    elif transform_type == "preset":
        # Handled in _apply_maps_to_record, but fallback here
        from cerulean.core.transform_presets import apply_preset
        return apply_preset(transform_fn or "", value)
    else:
        return value


def _apply_regex(value: str, pattern: str | None) -> str:
    """Apply s/pattern/replacement/[flags] substitution."""
    if not pattern or not pattern.startswith("s") or len(pattern) < 4:
        return value

    delim = pattern[1]
    parts = pattern[2:].split(delim)
    if len(parts) < 2:
        return value

    regex_pat = parts[0]
    replacement = parts[1]
    flags_str = parts[2] if len(parts) > 2 else ""

    flags = 0
    if "i" in flags_str:
        flags |= re.IGNORECASE
    count = 0 if "g" in flags_str else 1

    try:
        return re.sub(regex_pat, replacement, value, count=count, flags=flags)
    except re.error:
        return value


def _apply_lookup(value: str, lookup_json: str | None) -> str:
    """Apply a JSON lookup table. Case-insensitive fallback."""
    if not lookup_json:
        return value
    try:
        table = json.loads(lookup_json) if isinstance(lookup_json, str) else lookup_json
        if value in table:
            return table[value]
        lower_table = {k.lower(): v for k, v in table.items()}
        if value.lower() in lower_table:
            return lower_table[value.lower()]
        return value
    except (json.JSONDecodeError, TypeError):
        return value


# Sandboxed builtins for fn transforms — no file I/O, no imports
_SAFE_BUILTINS = {
    "len": len, "str": str, "int": int, "float": float, "bool": bool,
    "list": list, "dict": dict, "tuple": tuple, "set": set,
    "min": min, "max": max, "abs": abs, "round": round,
    "sorted": sorted, "reversed": reversed, "enumerate": enumerate,
    "zip": zip, "map": map, "filter": filter, "isinstance": isinstance,
    "range": range, "True": True, "False": False, "None": None,
}


def _apply_fn(value: str, expression: str | None) -> str:
    """Apply a sandboxed Python expression. `value` is the input variable."""
    if not expression:
        return value
    try:
        result = eval(expression, {"__builtins__": _SAFE_BUILTINS}, {"value": value})
        return str(result) if result is not None else value
    except Exception:
        logger.debug("Expression eval failed: %r on value %r", expression, value, exc_info=True)
        return value


def _subfield_sort_key(code: str) -> tuple[int, str]:
    """Sort key for MARC subfield codes: 0-9 first, then a-z."""
    if not code:
        return (2, "")
    if code.isdigit():
        return (0, code)
    return (1, code.lower())


def _sort_record(record: pymarc.Record) -> None:
    """Sort a MARC record's fields by tag and subfields within each field by 0-9, a-z."""
    record.fields.sort(key=lambda f: f.tag)
    for field in record.fields:
        if not field.is_control_field() and field.subfields:
            field.subfields.sort(key=lambda sf: _subfield_sort_key(sf.code))


def _set_value(record: pymarc.Record, tag: str, sub: str | None, value: str) -> None:
    """Write a value to a target tag/subfield on a MARC record."""
    sub_code = sub.lstrip("$") if sub else None

    if tag < "010":
        # Control field
        existing = record.get_fields(tag)
        if existing:
            existing[0].data = value
        else:
            record.add_field(pymarc.Field(tag=tag, data=value))
    elif sub_code:
        existing = record.get_fields(tag)
        if existing:
            existing[0].add_subfield(sub_code, value)
        else:
            record.add_field(pymarc.Field(
                tag=tag, indicators=[" ", " "],
                subfields=[Subfield(code=sub_code, value=value)],
            ))
    else:
        record.add_field(pymarc.Field(
            tag=tag, indicators=[" ", " "],
            subfields=[Subfield(code="a", value=value)],
        ))



# _iter_marc, _write_marc, _get_001 imported from cerulean.utils.marc


def _update_manifest_error(db: Session, manifest_id: str, error_msg: str) -> None:
    """Mark a TransformManifest as failed."""
    from cerulean.models import TransformManifest
    db.execute(
        update(TransformManifest)
        .where(TransformManifest.id == manifest_id)
        .values(status="error", error_message=error_msg, completed_at=datetime.utcnow())
    )
    db.commit()


# ── Items CSV ─────────────────────────────────────────────────────────

def _load_items_csv(csv_path: str, key_column: str) -> dict[str, list[dict]]:
    """Load an items CSV into a lookup dict keyed by bib identifier."""
    lookup: dict[str, list[dict]] = {}
    with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = row.get(key_column, "").strip()
            if key:
                lookup.setdefault(key, []).append(row)
    return lookup


# Column name → 952 subfield code mapping
_ITEMS_CSV_TO_952 = {
    "homebranch": "a",
    "holdingbranch": "b",
    "location": "c",
    "callnumber": "o",
    "barcode": "p",
    "itype": "y",
    "itemtype": "y",
    "ccode": "8",
    "notforloan": "7",
    "damaged": "4",
    "itemlost": "1",
    "withdrawn": "0",
    "copynumber": "t",
    "enumchron": "h",
    "itemnotes": "z",
}


def _add_952_from_csv(record: pymarc.Record, item_row: dict) -> None:
    """Add a 952 item field to a MARC record from a CSV row."""
    subfields: list[Subfield] = []
    for col_name, value in item_row.items():
        if not value or not value.strip():
            continue
        sub_code = _ITEMS_CSV_TO_952.get(col_name.lower().strip())
        if sub_code:
            subfields.append(Subfield(code=sub_code, value=value.strip()))

    if subfields:
        record.add_field(pymarc.Field(
            tag="952", indicators=[" ", " "], subfields=subfields,
        ))
