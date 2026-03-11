"""
cerulean/tasks/reconcile.py
─────────────────────────────────────────────────────────────────────────────
Stage 5 Celery tasks: Items.

    reconciliation_scan_task   — scan MARC 952 fields, extract subfield values
    reconciliation_apply_task  — apply reconciliation rules, write output file

All tasks write AuditEvent rows via AuditLogger.
"""

import csv
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pymarc
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.utils.marc import iter_marc as _iter_marc

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)

_PROGRESS_INTERVAL = 500

# ── Vocab category → 952 subfield mapping ──────────────────────────────

VOCAB_SUBFIELD_MAP: dict[str, str] = {
    "itype":         "y",
    "loc":           "c",
    "ccode":         "8",
    "not_loan":      "7",
    "withdrawn":     "1",
    "damaged":       "4",
    "homebranch":    "a",
    "holdingbranch": "b",
}

# Reverse: subfield code → list of vocab categories (for scanning)
_SUBFIELD_VOCAB_MAP: dict[str, list[str]] = defaultdict(list)
for _cat, _sf in VOCAB_SUBFIELD_MAP.items():
    _SUBFIELD_VOCAB_MAP[_sf].append(_cat)


# ══════════════════════════════════════════════════════════════════════════
# RECONCILIATION SCAN TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.reconcile.reconciliation_scan_task",
    max_retries=0,
    queue="reconcile",
)
def reconciliation_scan_task(self, project_id: str) -> dict:
    """Scan MARC 952 fields and extract subfield values per vocab category.

    Deletes existing scan results for this project before scanning.
    Reads the reconcile_source_file from the project record.

    Args:
        project_id: UUID of the project.

    Returns:
        dict with records_scanned and categories breakdown.
    """
    from cerulean.models import Project, ReconciliationScanResult

    log = AuditLogger(project_id=project_id, stage=5, tag="[items-scan]")
    log.info("Items scan starting")

    try:
        # Load project to get source file path
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if not project:
                log.error("Project not found")
                return {"error": "project_not_found"}
            source_file = project.reconcile_source_file

        if not source_file or not os.path.isfile(source_file):
            log.error(f"Source file not found: {source_file}")
            return {"error": "source_file_not_found"}

        # Delete existing scan results
        with Session(_engine) as db:
            db.execute(
                delete(ReconciliationScanResult)
                .where(ReconciliationScanResult.project_id == project_id)
            )
            db.commit()

        # Scan: accumulate {(category, value): count}
        value_counts: dict[tuple[str, str], int] = defaultdict(int)
        records_scanned = 0

        for record in _iter_marc(source_file):
            if record is None:
                continue
            records_scanned += 1
            for field_952 in record.get_fields("952"):
                for sf in field_952.subfields:
                    categories = _SUBFIELD_VOCAB_MAP.get(sf.code)
                    if categories and sf.value:
                        val = sf.value.strip()
                        if val:
                            for cat in categories:
                                value_counts[(cat, val)] += 1

            if records_scanned % _PROGRESS_INTERVAL == 0:
                self.update_state(state="PROGRESS", meta={
                    "records_scanned": records_scanned,
                })

        # Bulk-insert scan results
        now = datetime.utcnow()
        with Session(_engine) as db:
            for (category, value), count in value_counts.items():
                row = ReconciliationScanResult(
                    project_id=project_id,
                    vocab_category=category,
                    source_value=value,
                    record_count=count,
                    scanned_at=now,
                )
                db.add(row)
            db.commit()

        # Build category summary
        categories: dict[str, int] = defaultdict(int)
        for (cat, _val), _count in value_counts.items():
            categories[cat] += 1

        log.complete(
            f"Items scan complete — {records_scanned:,} records, "
            f"{len(value_counts)} distinct values across {len(categories)} categories"
        )
        return {
            "records_scanned": records_scanned,
            "distinct_values": len(value_counts),
            "categories": dict(categories),
        }

    except Exception as exc:
        log.error(f"Items scan failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# RECONCILIATION APPLY TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.reconcile.reconciliation_apply_task",
    max_retries=0,
    queue="reconcile",
)
def reconciliation_apply_task(self, project_id: str) -> dict:
    """Apply reconciliation rules to MARC 952 fields and write output file.

    Reads active rules ordered by sort_order, builds lookup table,
    iterates source file record-by-record applying transformations.

    Args:
        project_id: UUID of the project.

    Returns:
        dict with stats: records_in, records_out, rules_applied, etc.
    """
    from cerulean.models import Project, ReconciliationRule

    log = AuditLogger(project_id=project_id, stage=5, tag="[items-apply]")
    log.info("Items apply starting")

    try:
        # Load project and rules
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if not project:
                log.error("Project not found")
                return {"error": "project_not_found"}
            source_file = project.reconcile_source_file

            rules = db.execute(
                select(ReconciliationRule)
                .where(
                    ReconciliationRule.project_id == project_id,
                    ReconciliationRule.active == True,  # noqa: E712
                )
                .order_by(ReconciliationRule.sort_order)
            ).scalars().all()

            # Snapshot rules into dicts (detach from session)
            rules_data = []
            for r in rules:
                rules_data.append({
                    "id": r.id,
                    "vocab_category": r.vocab_category,
                    "marc_subfield": r.marc_subfield,
                    "operation": r.operation,
                    "source_values": set(r.source_values) if r.source_values else set(),
                    "target_value": r.target_value,
                    "split_conditions": r.split_conditions,
                    "delete_mode": r.delete_mode or "subfield",
                })

        if not source_file or not os.path.isfile(source_file):
            log.error(f"Source file not found: {source_file}")
            return {"error": "source_file_not_found"}

        # Build lookup: {(subfield_code, value): rule_data}
        rule_lookup: dict[tuple[str, str], dict] = {}
        for rule in rules_data:
            sf_code = rule["marc_subfield"]
            for sv in rule["source_values"]:
                rule_lookup[(sf_code, sv)] = rule

        log.info(f"Loaded {len(rules_data)} active rules, {len(rule_lookup)} value mappings")

        # Process records
        project_dir = Path(settings.data_root) / project_id
        output_path = project_dir / "Biblios-mapped-items.mrc"

        records_in = 0
        records_out = 0
        rules_applied = 0
        fields_deleted = 0

        # Collect post-reconciliation values per vocab category
        final_values: dict[tuple[str, str], int] = defaultdict(int)

        with open(str(output_path), "wb") as out_fh:
            for record in _iter_marc(source_file):
                if record is None:
                    continue
                records_in += 1
                modified = _apply_rules_to_record(record, rule_lookup)
                rules_applied += modified["rules_applied"]
                fields_deleted += modified["fields_deleted"]

                # Extract final 952 subfield values from the modified record
                for field_952 in record.get_fields("952"):
                    for sf in field_952.subfields:
                        categories = _SUBFIELD_VOCAB_MAP.get(sf.code)
                        if categories and sf.value:
                            val = sf.value.strip()
                            if val:
                                for cat in categories:
                                    final_values[(cat, val)] += 1

                out_fh.write(record.as_marc())
                records_out += 1

                if records_in % _PROGRESS_INTERVAL == 0:
                    self.update_state(state="PROGRESS", meta={
                        "records_processed": records_in,
                        "rules_applied": rules_applied,
                    })

        # Write per-category CSV files with final values
        items_dir = project_dir / "items"
        items_dir.mkdir(parents=True, exist_ok=True)
        # Clean old CSVs
        for old in items_dir.glob("*.csv"):
            old.unlink()

        category_files = {}
        cats_with_values: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for (cat, val), count in final_values.items():
            cats_with_values[cat].append((val, count))

        for cat, entries in cats_with_values.items():
            entries.sort(key=lambda x: (-x[1], x[0]))
            csv_path = items_dir / f"items_{cat}.csv"
            with open(str(csv_path), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["value", "record_count"])
                for val, count in entries:
                    writer.writerow([val, count])
            category_files[cat] = f"items/{csv_path.name}"

        log.info(
            f"Wrote {len(category_files)} category files: "
            + ", ".join(f"{c} ({len(cats_with_values[c])} values)" for c in sorted(category_files))
        )

        # Update project
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if project:
                project.stage_5_complete = True
                project.current_stage = 6
                db.commit()

        log.complete(
            f"Items apply complete — {records_in:,} records processed, "
            f"{rules_applied:,} rules applied, {fields_deleted} fields deleted"
        )
        return {
            "records_in": records_in,
            "records_out": records_out,
            "rules_applied": rules_applied,
            "fields_deleted": fields_deleted,
            "output_path": str(output_path),
            "category_files": category_files,
        }

    except Exception as exc:
        log.error(f"Items apply failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════


def _apply_rules_to_record(
    record: pymarc.Record,
    rule_lookup: dict[tuple[str, str], dict],
) -> dict:
    """Apply reconciliation rules to all 952 fields in a record (in-place).

    Returns stats dict with rules_applied and fields_deleted counts.
    """
    stats = {"rules_applied": 0, "fields_deleted": 0}
    fields_to_remove: list[pymarc.Field] = []

    for field_952 in record.get_fields("952"):
        subs_to_remove: list[int] = []
        remove_entire_field = False

        # Build new subfields list with modifications
        new_subfields = []
        for i, sf in enumerate(field_952.subfields):
            rule = rule_lookup.get((sf.code, sf.value.strip() if sf.value else ""))
            if not rule:
                new_subfields.append(sf)
                continue

            op = rule["operation"]

            if op in ("rename", "merge"):
                # Replace value with target
                new_subfields.append(pymarc.Subfield(code=sf.code, value=rule["target_value"] or ""))
                stats["rules_applied"] += 1

            elif op == "split":
                # Evaluate split conditions
                new_value = _evaluate_split(record, field_952, rule)
                if new_value is not None:
                    new_subfields.append(pymarc.Subfield(code=sf.code, value=new_value))
                else:
                    new_subfields.append(sf)
                stats["rules_applied"] += 1

            elif op == "delete":
                if rule["delete_mode"] == "field":
                    remove_entire_field = True
                    stats["fields_deleted"] += 1
                    stats["rules_applied"] += 1
                    break
                else:
                    # Remove just this subfield (don't append it)
                    stats["rules_applied"] += 1

            else:
                new_subfields.append(sf)

        if remove_entire_field:
            fields_to_remove.append(field_952)
        else:
            # Rebuild the field subfields
            field_952.subfields = new_subfields

    for f in fields_to_remove:
        record.remove_field(f)

    return stats


def _evaluate_split(
    record: pymarc.Record,
    field_952: pymarc.Field,
    rule: dict,
) -> str | None:
    """Evaluate split conditions and return the target value for the first match.

    Split conditions are a list of objects:
    - {field_check: {tag, sub, value}, target_value} — match if another field/sub has value
    - {same_field_check: {sub, value}, target_value} — match if same 952 has sub with value
    - {regex_check: {sub, pattern}, target_value} — match if sub value matches regex
    - {default: true, target_value} — fallback
    """
    conditions = rule.get("split_conditions")
    if not conditions:
        return None

    for cond in conditions:
        # Default condition
        if cond.get("default"):
            return cond.get("target_value")

        # Same-field subfield check
        same_check = cond.get("same_field_check")
        if same_check:
            check_sub = same_check.get("sub", "")
            check_val = same_check.get("value", "")
            for sf in field_952.subfields:
                if sf.code == check_sub and sf.value and sf.value.strip() == check_val:
                    return cond.get("target_value")
            continue

        # Other MARC field check
        field_check = cond.get("field_check")
        if field_check:
            check_tag = field_check.get("tag", "")
            check_sub = field_check.get("sub", "")
            check_val = field_check.get("value", "")
            for f in record.get_fields(check_tag):
                for sf in f.subfields:
                    if sf.code == check_sub and sf.value and sf.value.strip() == check_val:
                        return cond.get("target_value")
            continue

        # Regex check on a subfield within the same 952
        regex_check = cond.get("regex_check")
        if regex_check:
            check_sub = regex_check.get("sub", "")
            pattern = regex_check.get("pattern", "")
            for sf in field_952.subfields:
                if sf.code == check_sub and sf.value and re.search(pattern, sf.value):
                    return cond.get("target_value")
            continue

    return None
