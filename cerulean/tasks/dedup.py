"""
cerulean/tasks/dedup.py
─────────────────────────────────────────────────────────────────────────────
Stage 4 Celery tasks: Dedup & Validate.

    dedup_scan_task   — scan merged.mrc for duplicates using a DedupRule
    dedup_apply_task  — apply dedup resolution, write merged_deduped.mrc

All tasks write AuditEvent rows via AuditLogger.
"""

import csv
import os
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pymarc
from pymarc import Subfield
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)

_PROGRESS_INTERVAL = 500

# ── Preset field configs ─────────────────────────────────────────────────

_PRESET_FIELDS = {
    "001": [{"tag": "001", "sub": None, "match_type": "exact"}],
    "isbn": [{"tag": "020", "sub": "a", "match_type": "normalised"}],
    "title_author": [
        {"tag": "245", "sub": "a", "match_type": "normalised"},
        {"tag": "100", "sub": "a", "match_type": "normalised"},
    ],
    "oclc": [{"tag": "035", "sub": "a", "match_type": "exact"}],  # filter (OCoLC) prefix
}


# ══════════════════════════════════════════════════════════════════════════
# DEDUP SCAN TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.dedup.dedup_scan_task",
    max_retries=0,
    queue="dedup",
)
def dedup_scan_task(self, project_id: str, rule_id: str) -> dict:
    """Scan merged.mrc for duplicate records using a DedupRule's field config.

    Args:
        project_id: UUID string of the project.
        rule_id: UUID of the DedupRule to scan with.

    Returns:
        dict with clusters_found and records_affected.
    """
    from cerulean.models import DedupCluster, DedupRule

    log = AuditLogger(project_id=project_id, stage=4, tag="[dedup-scan]")
    log.info(f"Dedup scan starting for rule {rule_id}")

    try:
        with Session(_engine) as db:
            rule = db.get(DedupRule, rule_id)
            if not rule:
                log.error(f"DedupRule {rule_id} not found")
                return {"error": "rule_not_found"}

            # Resolve fields config from preset or custom
            fields_config = rule.fields
            if not fields_config and rule.preset_key and rule.preset_key in _PRESET_FIELDS:
                fields_config = _PRESET_FIELDS[rule.preset_key]

            if not fields_config:
                log.error("No fields config for rule")
                return {"error": "no_fields_config"}

            # Delete existing clusters for this rule (re-scan clears old results)
            db.execute(
                select(DedupCluster).where(DedupCluster.rule_id == rule_id)
            )
            existing = db.query(DedupCluster).filter(DedupCluster.rule_id == rule_id).all()
            for c in existing:
                db.delete(c)
            db.commit()

        # Read merged.mrc record by record, extract match keys
        merged_path = Path(settings.data_root) / project_id / "merged.mrc"
        if not merged_path.is_file():
            log.error(f"merged.mrc not found at {merged_path}")
            return {"error": "merged_mrc_not_found"}

        # Group records by match key
        key_groups: dict[str, list[dict]] = defaultdict(list)
        record_index = 0

        for record in _iter_marc(str(merged_path)):
            match_key = _extract_match_key(record, fields_config)

            if match_key:
                key_groups[match_key].append({
                    "record_index": record_index,
                    "marc_001": _get_001(record),
                    "field_count": len(record.fields),
                    "item_count": len(record.get_fields("952")),
                })

            record_index += 1
            if record_index % _PROGRESS_INTERVAL == 0:
                self.update_state(
                    state="PROGRESS",
                    meta={"records_scanned": record_index},
                )

        # Filter to groups with 2+ records (actual duplicates)
        clusters = {k: v for k, v in key_groups.items() if len(v) >= 2}

        # Write DedupCluster rows
        records_affected = 0
        with Session(_engine) as db:
            for match_key, records in clusters.items():
                cluster = DedupCluster(
                    rule_id=rule_id,
                    records=records,
                    primary_index=0,
                    match_key=match_key,
                    resolved=False,
                )
                db.add(cluster)
                records_affected += len(records)

            # Update rule scan stats
            db.execute(
                update(DedupRule)
                .where(DedupRule.id == rule_id)
                .values(
                    last_scan_clusters=len(clusters),
                    last_scan_affected=records_affected,
                    last_scan_at=datetime.utcnow(),
                )
            )
            db.commit()

        log.complete(
            f"Dedup scan complete — {record_index:,} records scanned, "
            f"{len(clusters)} clusters, {records_affected} records affected"
        )
        return {
            "clusters_found": len(clusters),
            "records_affected": records_affected,
            "records_scanned": record_index,
        }

    except Exception as exc:
        log.error(f"Dedup scan failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# DEDUP APPLY TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.dedup.dedup_apply_task",
    max_retries=0,
    queue="dedup",
)
def dedup_apply_task(self, project_id: str, rule_id: str) -> dict:
    """Apply dedup resolution: remove/merge duplicates and write merged_deduped.mrc.

    Args:
        project_id: UUID string of the project.
        rule_id: UUID of the DedupRule to apply.

    Returns:
        dict with records_in, records_out, records_removed.
    """
    from cerulean.models import DedupCluster, DedupRule, Project

    log = AuditLogger(project_id=project_id, stage=4, tag="[dedup-apply]")
    log.info(f"Dedup apply starting for rule {rule_id}")

    try:
        with Session(_engine) as db:
            rule = db.get(DedupRule, rule_id)
            if not rule:
                log.error(f"DedupRule {rule_id} not found")
                return {"error": "rule_not_found"}

            strategy = rule.on_duplicate or "keep_first"

            clusters = db.query(DedupCluster).filter(
                DedupCluster.rule_id == rule_id
            ).all()
            clusters_data = [
                {
                    "id": c.id,
                    "records": c.records,
                    "primary_index": c.primary_index,
                    "match_key": c.match_key,
                }
                for c in clusters
            ]

        # Read all records from merged.mrc into a list (need random access)
        merged_path = Path(settings.data_root) / project_id / "merged.mrc"
        if not merged_path.is_file():
            log.error(f"merged.mrc not found at {merged_path}")
            return {"error": "merged_mrc_not_found"}

        all_records: list[pymarc.Record] = []
        for record in _iter_marc(str(merged_path)):
            all_records.append(record)

        log.info(f"Loaded {len(all_records):,} records, {len(clusters_data)} clusters to resolve")

        # Build set of record indices to discard and merge operations
        discard_indices: set[int] = set()
        merge_ops: list[tuple[int, list[int]]] = []  # (keep_idx, [donor_indices])
        exceptions_rows: list[dict] = []

        for cluster in clusters_data:
            records_in_cluster = cluster["records"]
            if len(records_in_cluster) < 2:
                continue

            indices = [r["record_index"] for r in records_in_cluster]
            primary_idx = cluster["primary_index"]

            if strategy == "keep_first":
                keep_idx = indices[primary_idx] if primary_idx < len(indices) else indices[0]
                for idx in indices:
                    if idx != keep_idx:
                        discard_indices.add(idx)

            elif strategy == "keep_most_fields":
                best_idx = max(indices, key=lambda i: len(all_records[i].fields) if i < len(all_records) else 0)
                for idx in indices:
                    if idx != best_idx:
                        discard_indices.add(idx)

            elif strategy == "keep_most_items":
                best_idx = max(indices, key=lambda i: len(all_records[i].get_fields("952")) if i < len(all_records) else 0)
                for idx in indices:
                    if idx != best_idx:
                        discard_indices.add(idx)

            elif strategy == "merge_holdings":
                keep_idx = indices[primary_idx] if primary_idx < len(indices) else indices[0]
                donor_indices = [idx for idx in indices if idx != keep_idx]
                merge_ops.append((keep_idx, donor_indices))
                for idx in donor_indices:
                    discard_indices.add(idx)

            elif strategy == "write_exceptions":
                # Keep all records, write exceptions report
                for r in records_in_cluster:
                    exceptions_rows.append({
                        "match_key": cluster["match_key"],
                        "record_index": r["record_index"],
                        "marc_001": r.get("marc_001", ""),
                        "field_count": r.get("field_count", 0),
                        "item_count": r.get("item_count", 0),
                    })

        # Apply merge_holdings: copy 952 fields from donors onto keeper
        for keep_idx, donor_indices in merge_ops:
            if keep_idx >= len(all_records):
                continue
            keeper = all_records[keep_idx]
            for donor_idx in donor_indices:
                if donor_idx >= len(all_records):
                    continue
                for field_952 in all_records[donor_idx].get_fields("952"):
                    keeper.add_field(field_952)

        # Write exceptions CSV if strategy is write_exceptions
        project_dir = Path(settings.data_root) / project_id
        if exceptions_rows:
            exceptions_path = project_dir / "dedup_exceptions.csv"
            with open(str(exceptions_path), "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=["match_key", "record_index", "marc_001", "field_count", "item_count"])
                writer.writeheader()
                writer.writerows(exceptions_rows)
            log.info(f"Wrote {len(exceptions_rows)} exception rows to dedup_exceptions.csv")

        # Write merged_deduped.mrc
        output_path = project_dir / "merged_deduped.mrc"
        records_out = 0
        with open(str(output_path), "wb") as out_fh:
            for idx, record in enumerate(all_records):
                if idx in discard_indices:
                    continue
                out_fh.write(record.as_marc())
                records_out += 1
                if records_out % _PROGRESS_INTERVAL == 0:
                    self.update_state(
                        state="PROGRESS",
                        meta={"records_written": records_out, "records_total": len(all_records)},
                    )

        records_removed = len(all_records) - records_out

        # Update project + mark clusters resolved
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if project:
                project.stage_4_complete = True
                project.current_stage = 5
                project.bib_count_post_dedup = records_out

            # Mark all clusters as resolved
            db.execute(
                update(DedupCluster)
                .where(DedupCluster.rule_id == rule_id)
                .values(resolved=True)
            )
            db.commit()

        log.complete(
            f"Dedup apply complete — {len(all_records):,} in, {records_out:,} out, "
            f"{records_removed:,} removed"
        )
        return {
            "records_in": len(all_records),
            "records_out": records_out,
            "records_removed": records_removed,
            "output_path": str(output_path),
        }

    except Exception as exc:
        log.error(f"Dedup apply failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════


def _extract_match_key(record: pymarc.Record, fields_config: list[dict]) -> str | None:
    """Build a composite match key from a record using the fields config.

    Joins normalised field values with "||". Returns None if any required
    field is missing.
    """
    parts: list[str] = []

    for fc in fields_config:
        tag = fc["tag"]
        sub = fc.get("sub")
        match_type = fc.get("match_type", "exact")

        values = _extract_values(record, tag, sub)
        if not values:
            return None  # all fields required for a match key

        value = values[0]

        # Special handling for OCLC prefix
        if tag == "035" and sub == "a":
            value = _filter_oclc(value)
            if not value:
                return None

        if match_type == "normalised":
            if tag == "020":
                value = _normalize_isbn(value)
            else:
                value = _normalize_text(value)

        if not value:
            return None

        parts.append(value)

    return "||".join(parts) if parts else None


def _normalize_text(value: str) -> str:
    """Lowercase, strip diacritics, collapse whitespace, remove punctuation."""
    # Strip diacritics
    nfkd = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase
    text = ascii_text.lower()
    # Remove punctuation (keep alphanumeric and spaces)
    text = re.sub(r"[^\w\s]", "", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_isbn(value: str) -> str:
    """Strip hyphens/spaces, remove trailing qualifiers like (pbk.)."""
    # Remove qualifiers in parentheses
    value = re.sub(r"\s*\(.*?\)\s*$", "", value)
    # Remove hyphens and spaces
    value = re.sub(r"[\s\-]", "", value)
    return value.strip()


def _filter_oclc(value: str) -> str | None:
    """Extract OCLC number from (OCoLC) prefix in 035$a."""
    match = re.match(r"\(OCoLC\)\s*(\d+)", value)
    return match.group(1) if match else None


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
            val = field.value()
            if val:
                values.append(val)

    return values


def _get_001(record: pymarc.Record) -> str | None:
    """Extract 001 control number from a record."""
    fields = record.get_fields("001")
    return fields[0].data if fields else None


def _iter_marc(path: str):
    """Yield pymarc Record objects from an ISO2709 MARC file."""
    with open(path, "rb") as fh:
        reader = pymarc.MARCReader(
            fh, to_unicode=True, force_utf8=True, utf8_handling="replace",
        )
        yield from reader
