"""
cerulean/tasks/versioning.py
─────────────────────────────────────────────────────────────────────────────
Celery tasks for creating versioned snapshots and computing diffs between
migration versions.

    create_version_snapshot_task  — copy output file to a versioned snapshot,
                                    record metadata in MigrationVersion row
    compute_version_diff_task    — compare two version snapshots field-by-field
"""

import csv
import json
import os
import shutil
import uuid
from pathlib import Path

import pymarc
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.utils.marc import iter_marc as _iter_marc
from cerulean.models import MigrationVersion, FieldMap

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)


# ── helpers ────────────────────────────────────────────────────────────────

def _field_map_to_dict(fm: FieldMap) -> dict:
    """Serialise a FieldMap row to a plain dict for the mapping snapshot."""
    return {
        "id": fm.id,
        "source_tag": fm.source_tag,
        "source_sub": fm.source_sub,
        "target_tag": fm.target_tag,
        "target_sub": fm.target_sub,
        "transform_type": fm.transform_type,
        "transform_fn": fm.transform_fn,
        "preset_key": fm.preset_key,
        "delete_source": fm.delete_source,
        "approved": fm.approved,
        "sort_order": fm.sort_order,
    }


def _count_marc_records(path: str) -> int:
    """Count records in a binary MARC file."""
    count = 0
    for _ in _iter_marc(path):
        count += 1
    return count


def _count_csv_rows(path: str) -> int:
    """Count data rows in a CSV file (excludes header)."""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # skip header
        return sum(1 for _ in reader)


def _get_001(record: pymarc.Record) -> str | None:
    """Extract 001 control number from a MARC record."""
    fields = record.get_fields("001")
    return fields[0].data.strip() if fields else None


def _record_fields_dict(record: pymarc.Record) -> dict[str, list[str]]:
    """Build {tag: [string_values]} for every field in a record."""
    result: dict[str, list[str]] = {}
    for field in record.fields:
        tag = field.tag
        if tag not in result:
            result[tag] = []
        if field.is_control_field():
            result[tag].append(field.data)
        else:
            result[tag].append(str(field))
    return result


# ══════════════════════════════════════════════════════════════════════════
# CREATE VERSION SNAPSHOT
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="cerulean.tasks.versioning.create_version_snapshot_task",
)
def create_version_snapshot_task(
    self,
    project_id: str,
    data_type: str,
    source_path: str | None = None,
    label: str | None = None,
) -> dict:
    """
    Copy the current output file to a versioned snapshot and record metadata.

    Args:
        project_id: UUID string of the project.
        data_type: "bib" or "patron".
        source_path: Optional explicit path to the source file. When *None*,
                     the default location is derived from data_type.

    Returns:
        dict with version_number, snapshot_path, record_count.
    """
    log = AuditLogger(project_id=project_id, stage=None, tag="[version]")
    log.info(f"Creating {data_type} version snapshot")

    project_dir = Path(settings.data_root) / project_id
    versions_dir = project_dir / "versions"
    versions_dir.mkdir(parents=True, exist_ok=True)

    # ── determine source file ──────────────────────────────────────────
    if source_path is not None:
        src = Path(source_path)
    elif data_type == "bib":
        # prefer merged.mrc, fall back to output.mrc
        merged = project_dir / "merged.mrc"
        output = project_dir / "output.mrc"
        src = merged if merged.exists() else output
    elif data_type == "patron":
        src = project_dir / "patrons" / "patrons_transformed.csv"
    else:
        raise ValueError(f"Unsupported data_type: {data_type!r}")

    if not src.exists():
        raise FileNotFoundError(f"Source file not found: {src}")

    # ── determine next version number ──────────────────────────────────
    with Session(_engine) as session:
        max_ver = session.execute(
            select(func.max(MigrationVersion.version_number)).where(
                MigrationVersion.project_id == project_id,
                MigrationVersion.data_type == data_type,
            )
        ).scalar()
        version_number = (max_ver or 0) + 1

    # ── copy file to versioned snapshot ────────────────────────────────
    ext = src.suffix  # .mrc or .csv
    snapshot_name = f"{data_type}_v{version_number}{ext}"
    snapshot_path = versions_dir / snapshot_name
    shutil.copy2(str(src), str(snapshot_path))

    # ── count records ──────────────────────────────────────────────────
    if data_type == "bib":
        record_count = _count_marc_records(str(snapshot_path))
    else:
        record_count = _count_csv_rows(str(snapshot_path))

    # ── snapshot current approved field maps ───────────────────────────
    with Session(_engine) as session:
        maps = session.execute(
            select(FieldMap).where(
                FieldMap.project_id == project_id,
                FieldMap.approved.is_(True),
            )
        ).scalars().all()
        mapping_snapshot = [_field_map_to_dict(m) for m in maps]

    # ── create MigrationVersion row ────────────────────────────────────
    with Session(_engine) as session:
        mv = MigrationVersion(
            id=str(uuid.uuid4()),
            project_id=project_id,
            data_type=data_type,
            version_number=version_number,
            label=label,
            snapshot_path=str(snapshot_path),
            record_count=record_count,
            mapping_snapshot=mapping_snapshot,
        )
        session.add(mv)
        session.commit()

    log.complete(
        f"{data_type} v{version_number} snapshot created — "
        f"{record_count} records at {snapshot_path}"
    )

    return {
        "version_number": version_number,
        "snapshot_path": str(snapshot_path),
        "record_count": record_count,
    }


# ══════════════════════════════════════════════════════════════════════════
# COMPUTE VERSION DIFF
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="cerulean.tasks.versioning.compute_version_diff_task",
)
def compute_version_diff_task(
    self,
    project_id: str,
    data_type: str,
    version_a_id: str,
    version_b_id: str,
) -> dict:
    """
    Compare two version snapshots and return a summary plus per-record changes.

    Args:
        project_id: UUID string of the project.
        data_type: "bib" or "patron".
        version_a_id: MigrationVersion.id for the *older* version.
        version_b_id: MigrationVersion.id for the *newer* version.

    Returns:
        dict with ``summary`` and ``changed_records`` keys.
    """
    log = AuditLogger(project_id=project_id, stage=None, tag="[version-diff]")

    # ── load version rows ──────────────────────────────────────────────
    with Session(_engine) as session:
        ver_a = session.get(MigrationVersion, version_a_id)
        ver_b = session.get(MigrationVersion, version_b_id)
        if ver_a is None:
            raise ValueError(f"Version A not found: {version_a_id}")
        if ver_b is None:
            raise ValueError(f"Version B not found: {version_b_id}")
        path_a = ver_a.snapshot_path
        path_b = ver_b.snapshot_path
        label = f"v{ver_a.version_number} → v{ver_b.version_number}"

    log.info(f"Computing {data_type} diff: {label}")

    if data_type == "bib":
        result = _diff_bib(path_a, path_b)
    elif data_type == "patron":
        result = _diff_patron(path_a, path_b)
    else:
        raise ValueError(f"Unsupported data_type: {data_type!r}")

    summary = result["summary"]
    log.complete(
        f"{data_type} diff {label}: "
        f"+{summary['added']} -{summary['removed']} "
        f"~{summary['changed']} ={summary['unchanged']}"
    )

    return result


# ── bib diff ───────────────────────────────────────────────────────────────

def _diff_bib(path_a: str, path_b: str) -> dict:
    """Compare two MARC snapshot files by 001 value."""

    # Build index: 001 -> {tag: [values]}
    index_a: dict[str, dict[str, list[str]]] = {}
    for rec in _iter_marc(path_a):
        rec_id = _get_001(rec)
        if rec_id is not None:
            index_a[rec_id] = _record_fields_dict(rec)

    index_b: dict[str, dict[str, list[str]]] = {}
    for rec in _iter_marc(path_b):
        rec_id = _get_001(rec)
        if rec_id is not None:
            index_b[rec_id] = _record_fields_dict(rec)

    keys_a = set(index_a.keys())
    keys_b = set(index_b.keys())

    added_ids = keys_b - keys_a
    removed_ids = keys_a - keys_b
    common_ids = keys_a & keys_b

    records_changed = 0
    records_unchanged = 0
    changed_records: list[dict] = []

    for rec_id in sorted(common_ids):
        fields_a = index_a[rec_id]
        fields_b = index_b[rec_id]
        all_tags = sorted(set(fields_a.keys()) | set(fields_b.keys()))

        changes: list[dict] = []
        for tag in all_tags:
            vals_a = fields_a.get(tag, [])
            vals_b = fields_b.get(tag, [])
            if vals_a != vals_b:
                changes.append({
                    "tag": tag,
                    "old_value": "; ".join(vals_a) if vals_a else None,
                    "new_value": "; ".join(vals_b) if vals_b else None,
                })

        if changes:
            records_changed += 1
            if len(changed_records) < 100:
                changed_records.append({
                    "record_id": rec_id,
                    "changes": changes,
                })
        else:
            records_unchanged += 1

    return {
        "summary": {
            "added": len(added_ids),
            "removed": len(removed_ids),
            "changed": records_changed,
            "unchanged": records_unchanged,
        },
        "changed_records": changed_records,
    }


# ── patron diff ────────────────────────────────────────────────────────────

def _diff_patron(path_a: str, path_b: str) -> dict:
    """Compare two patron CSV snapshots by cardnumber column."""

    def _load_csv(path: str) -> dict[str, dict[str, str]]:
        index: dict[str, dict[str, str]] = {}
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for row in csv.DictReader(fh):
                key = row.get("cardnumber", "")
                if key:
                    index[key] = dict(row)
        return index

    index_a = _load_csv(path_a)
    index_b = _load_csv(path_b)

    keys_a = set(index_a.keys())
    keys_b = set(index_b.keys())

    added_keys = keys_b - keys_a
    removed_keys = keys_a - keys_b
    common_keys = keys_a & keys_b

    rows_changed = 0
    rows_unchanged = 0
    changed_records: list[dict] = []

    for cardnumber in sorted(common_keys):
        row_a = index_a[cardnumber]
        row_b = index_b[cardnumber]
        all_fields = sorted(set(row_a.keys()) | set(row_b.keys()))

        changes: list[dict] = []
        for field in all_fields:
            val_a = row_a.get(field, "")
            val_b = row_b.get(field, "")
            if val_a != val_b:
                changes.append({
                    "field": field,
                    "old_value": val_a or None,
                    "new_value": val_b or None,
                })

        if changes:
            rows_changed += 1
            if len(changed_records) < 100:
                changed_records.append({
                    "cardnumber": cardnumber,
                    "changes": changes,
                })
        else:
            rows_unchanged += 1

    return {
        "summary": {
            "added": len(added_keys),
            "removed": len(removed_keys),
            "changed": rows_changed,
            "unchanged": rows_unchanged,
        },
        "changed_records": changed_records,
    }
