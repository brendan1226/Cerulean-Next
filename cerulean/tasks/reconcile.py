"""
cerulean/tasks/reconcile.py
─────────────────────────────────────────────────────────────────────────────
Stage 8 Celery tasks: Items.

    reconciliation_scan_task            — scan MARC 952 fields, extract subfield values
    reconciliation_apply_task           — apply reconciliation rules, write output file
    ai_reconciliation_suggest_task      — Phase 5: Claude-powered code matching

All tasks write AuditEvent rows via AuditLogger.
"""

import csv
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx
import pymarc
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.core.preferences import pref_enabled_sync
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.helpers import check_paused as _check_paused
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

    log = AuditLogger(project_id=project_id, stage=8, tag="[items-scan]")
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
                _check_paused(project_id, self)
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

    log = AuditLogger(project_id=project_id, stage=8, tag="[items-apply]")
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
                    _check_paused(project_id, self)
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
                project.stage_8_complete = True
                project.current_stage = 9
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


# ══════════════════════════════════════════════════════════════════════════
# PHASE 5 — AI CODE RECONCILIATION
# ══════════════════════════════════════════════════════════════════════════
# cerulean_ai_spec.md §6. Claude suggests matches between source codes
# (from the scan) and Koha authorized values (from the Koha REST API).
# Suggestions land as INACTIVE ReconciliationRule rows with
# ai_suggested=True; approving a row flips active=True and the existing
# reconciliation_apply_task consumes it with no changes.


_AI_RECON_SYSTEM_PROMPT = """You are an expert library-systems migration specialist.
You are helping a ByWater Solutions migration engineer match legacy ILS codes
(branch, shelving location, item type, collection, status) to Koha authorized
values.

You will be given, for a single vocabulary category:
  * SOURCE CODES — distinct values extracted from the legacy MARC 952 subfield,
    each with a record count showing how common it is.
  * KOHA VALUES — the library's configured authorized values for this category,
    each with its human-readable description.

For every source code, suggest the single best-matching Koha value.

RULES:
1. Output ONLY a JSON array — no preamble, no markdown fences, no commentary.
2. Each array item must have these keys exactly:
     {
       "source_value":   <string>,
       "koha_value":     <string or null when no match>,
       "confidence":     <float 0.0–1.0>,
       "reasoning":      <1–2 sentence plain-English justification>
     }
3. Confidence calibration:
     0.90–1.00  exact case-insensitive match on code OR on description
     0.70–0.89  strong semantic match (abbreviation, obvious synonym, name match)
     0.50–0.69  plausible match but ambiguous — the engineer should verify
     0.01–0.49  weak match — prefer returning null koha_value unless clearly best
     0.00       no reasonable match; return koha_value=null
4. Cite specific evidence in reasoning: "code 'JUV' matches Koha value
   'JUVENILE' which is the Children's Collection". Do not invent Koha values
   that aren't in the supplied list.
5. Never fabricate codes. If nothing matches, return koha_value=null with a
   short explanation so the engineer knows what to do.
6. A source code maps to exactly one Koha value (or null) — never an array.
"""


# Human-readable labels for each vocab category, used in the prompt so the
# model understands what kind of codes it's looking at.
_VOCAB_LABELS: dict[str, str] = {
    "itype":         "item type",
    "loc":           "shelving location",
    "ccode":         "collection code",
    "not_loan":      "not-for-loan status",
    "withdrawn":     "withdrawn status",
    "damaged":       "damaged status",
    "homebranch":    "home branch (library)",
    "holdingbranch": "holding branch (library)",
}


# Max source codes per category sent in one Claude call. 200 distinct
# codes in a single category is already unusual; beyond that we truncate
# the tail and let the engineer run a fresh suggest after confirming the
# common ones.
_AI_RECON_MAX_SOURCE_PER_CAT = 200


@celery_app.task(
    bind=True,
    name="cerulean.tasks.reconcile.ai_reconciliation_suggest_task",
    max_retries=0,
    queue="reconcile",
)
def ai_reconciliation_suggest_task(
    self,
    project_id: str,
    user_id: str | None = None,
) -> dict:
    """Phase 5 — AI Code Reconciliation.

    For every vocab category that has scan rows, fetch the Koha authorized
    values list and ask Claude to suggest a match for each source code.
    Matched suggestions are persisted as inactive ReconciliationRule rows
    (operation='rename', active=False, ai_suggested=True) that the engineer
    approves from the Rules tab.

    Re-running the task is safe:
      * Active (approved) rules are never touched.
      * Existing ai_suggested+inactive rules are updated in place.
      * New suggestions are added as fresh rows.

    Args:
        project_id: Project UUID.
        user_id: Optional calling user UUID — gates on the per-user
            ``ai.code_reconciliation`` feature flag. Tasks triggered
            without a user_id default to the registry default (off).
    """
    from cerulean.models import (
        Project,
        ReconciliationRule,
        ReconciliationScanResult,
    )

    log = AuditLogger(project_id=project_id, stage=8, tag="[ai-reconcile]")

    try:
        with Session(_engine) as db:
            if not pref_enabled_sync(db, user_id, "ai.code_reconciliation"):
                log.info("ai.code_reconciliation disabled for caller — skipping")
                return {"skipped": True, "reason": "feature_disabled"}

            project = db.get(Project, project_id)
            if not project:
                log.error("Project not found")
                return {"error": "project_not_found"}

            if not project.koha_url or not project.koha_token_enc:
                log.error("Koha URL and token must be configured")
                return {"error": "koha_not_configured"}

            base_url = project.koha_url.rstrip("/")
            from cerulean.tasks.push import _decrypt_token
            koha_token = _decrypt_token(project.koha_token_enc)

            # Load scan rows, grouped by vocab_category
            scan_rows = db.execute(
                select(ReconciliationScanResult)
                .where(ReconciliationScanResult.project_id == project_id)
                .order_by(
                    ReconciliationScanResult.vocab_category,
                    ReconciliationScanResult.record_count.desc(),
                )
            ).scalars().all()

        if not scan_rows:
            log.error("No scan results — run a scan before asking for AI suggestions")
            return {"error": "no_scan_results"}

        by_category: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for r in scan_rows:
            by_category[r.vocab_category].append((r.source_value, r.record_count))

        log.info(
            f"AI reconciliation starting — {len(scan_rows)} scan rows across "
            f"{len(by_category)} categor(ies)"
        )

        total_suggestions = 0
        total_matched = 0
        total_unmatched = 0
        per_category: dict[str, dict] = {}
        categories_done = 0

        for cat, source_rows in by_category.items():
            categories_done += 1
            self.update_state(state="PROGRESS", meta={
                "categories_total": len(by_category),
                "categories_done": categories_done,
                "current_category": cat,
            })
            _check_paused(project_id, self)

            # Fetch Koha authorized values for this category
            koha_values = _fetch_koha_values_for_vocab(cat, base_url, koha_token, log)
            if koha_values is None:
                per_category[cat] = {"error": "koha_fetch_failed"}
                continue
            if not koha_values:
                log.warn(f"[{cat}] Koha returned no authorized values — skipping")
                per_category[cat] = {"error": "no_koha_values"}
                continue

            # Build + send prompt
            truncated = source_rows[:_AI_RECON_MAX_SOURCE_PER_CAT]
            prompt = _build_recon_user_prompt(cat, truncated, koha_values)

            try:
                raw = _call_claude_for_recon(prompt)
            except Exception as exc:
                log.error(f"[{cat}] Claude call failed: {exc}")
                per_category[cat] = {"error": f"claude_failed: {exc}"}
                continue

            suggestions = _parse_recon_suggestions(raw)
            log.info(f"[{cat}] Claude returned {len(suggestions)} suggestions")

            cat_matched, cat_unmatched, cat_written, cat_updated = _persist_recon_suggestions(
                project_id=project_id,
                vocab_category=cat,
                suggestions=suggestions,
                log=log,
            )
            total_suggestions += len(suggestions)
            total_matched += cat_matched
            total_unmatched += cat_unmatched
            per_category[cat] = {
                "source_codes_sent":    len(truncated),
                "koha_values_available": len(koha_values),
                "matched":   cat_matched,
                "unmatched": cat_unmatched,
                "written":   cat_written,
                "updated":   cat_updated,
                "truncated": len(source_rows) > _AI_RECON_MAX_SOURCE_PER_CAT,
            }

        log.complete(
            f"AI reconciliation complete — {total_matched} matches, "
            f"{total_unmatched} unmatched across {len(by_category)} categor(ies). "
            f"Engineer review required on the Rules tab."
        )
        return {
            "total_suggestions":   total_suggestions,
            "total_matched":       total_matched,
            "total_unmatched":     total_unmatched,
            "categories_analyzed": len(by_category),
            "per_category":        per_category,
        }

    except Exception as exc:
        log.error(f"AI reconciliation failed: {exc}")
        raise


# ── Phase 5 helpers ────────────────────────────────────────────────────


def _koha_endpoint_for_vocab(vocab: str, base_url: str) -> str | None:
    """Map vocab category to Koha REST API endpoint URL.

    Kept in sync with the router-side helper of the same name — duplicated
    here so the task stays self-contained and doesn't import a router.
    """
    if vocab == "itype":
        return f"{base_url}/api/v1/item_types"
    elif vocab in ("homebranch", "holdingbranch"):
        return f"{base_url}/api/v1/libraries"
    elif vocab in ("loc", "ccode", "not_loan", "withdrawn", "damaged"):
        av_category = vocab.upper()
        return f"{base_url}/api/v1/authorised_value_categories/{av_category}/authorised_values"
    return None


def _fetch_koha_values_for_vocab(
    vocab: str,
    base_url: str,
    token: str,
    log: AuditLogger,
) -> list[dict] | None:
    """Fetch Koha authorized values for a vocab category (sync, for tasks)."""
    endpoint = _koha_endpoint_for_vocab(vocab, base_url)
    if not endpoint:
        log.warn(f"[{vocab}] no Koha endpoint mapping — skipping")
        return None
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        resp = httpx.get(endpoint, headers=headers, timeout=30.0, verify=False)
    except httpx.HTTPError as exc:
        log.error(f"[{vocab}] Koha fetch failed: {exc}")
        return None
    if resp.status_code >= 400:
        log.error(f"[{vocab}] Koha returned {resp.status_code}: {resp.text[:200]}")
        return None
    try:
        data = resp.json()
    except json.JSONDecodeError:
        log.error(f"[{vocab}] Koha response was not JSON")
        return None
    return data if isinstance(data, list) else []


def _summarize_koha_value(vocab: str, v: dict) -> tuple[str, str]:
    """Extract (code, description) from one Koha authorized value record.

    Koha's REST API uses different key names depending on the endpoint:
      * /item_types           → {itemtype, description}
      * /libraries            → {branchcode, branchname}
      * /authorised_values    → {value or authorised_value, description or lib}
    """
    if vocab == "itype":
        return (
            str(v.get("itemtype") or v.get("item_type") or ""),
            str(v.get("description") or ""),
        )
    if vocab in ("homebranch", "holdingbranch"):
        return (
            str(v.get("branchcode") or v.get("library_id") or ""),
            str(v.get("branchname") or v.get("name") or ""),
        )
    # Authorized-value endpoints: "value" / "authorised_value" + "description" / "lib"
    code = v.get("value") or v.get("authorised_value") or v.get("authorised_value_id") or ""
    desc = v.get("description") or v.get("lib") or ""
    return (str(code), str(desc))


def _build_recon_user_prompt(
    vocab_category: str,
    source_rows: list[tuple[str, int]],
    koha_values: list[dict],
) -> str:
    """Build the user message for the Claude reconciliation call."""
    label = _VOCAB_LABELS.get(vocab_category, vocab_category)

    source_lines = []
    for value, count in source_rows:
        source_lines.append(f"  - {value!r} ({count:,} record{'s' if count != 1 else ''})")

    koha_lines = []
    for v in koha_values:
        code, desc = _summarize_koha_value(vocab_category, v)
        if not code:
            continue
        if desc:
            koha_lines.append(f"  - {code!r} — {desc}")
        else:
            koha_lines.append(f"  - {code!r}")

    return (
        f"VOCABULARY CATEGORY: {vocab_category} ({label})\n\n"
        f"SOURCE CODES (legacy ILS, from MARC 952 scan):\n"
        + ("\n".join(source_lines) if source_lines else "  (none)")
        + "\n\n"
        + f"KOHA AUTHORIZED VALUES (target):\n"
        + ("\n".join(koha_lines) if koha_lines else "  (none)")
        + "\n\n"
        + "Match every SOURCE CODE to the best KOHA VALUE. Return ONLY a JSON array."
    )


def _call_claude_for_recon(user_message: str) -> str:
    """Call Claude with the reconciliation system prompt. Returns raw text."""
    import anthropic
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    response = client.messages.create(
        model=settings.anthropic_model,
        max_tokens=4096,
        system=_AI_RECON_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    return response.content[0].text.strip()


def _parse_recon_suggestions(raw: str) -> list[dict]:
    """Parse Claude's JSON response into a list of {source_value, koha_value,
    confidence, reasoning} dicts. Swallows parse errors — returns []."""
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        if len(parts) >= 2:
            raw = parts[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    clean: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        src = item.get("source_value")
        if not isinstance(src, str) or not src:
            continue
        koha = item.get("koha_value")
        if koha is not None and not isinstance(koha, str):
            continue
        conf = item.get("confidence")
        try:
            conf_f = float(conf) if conf is not None else 0.0
        except (TypeError, ValueError):
            conf_f = 0.0
        if conf_f < 0.0:
            conf_f = 0.0
        if conf_f > 1.0:
            conf_f = 1.0
        reasoning = item.get("reasoning")
        if reasoning is not None and not isinstance(reasoning, str):
            reasoning = str(reasoning)
        clean.append({
            "source_value": src,
            "koha_value": (koha or None) or None,
            "confidence": conf_f,
            "reasoning": reasoning,
        })
    return clean


def _persist_recon_suggestions(
    project_id: str,
    vocab_category: str,
    suggestions: list[dict],
    log: AuditLogger,
) -> tuple[int, int, int, int]:
    """Upsert suggestions into reconciliation_rules.

    Idempotency contract:
      * Active (approved) rules are never touched — an active rule for
        source_value X wins.
      * Existing ai_suggested + inactive rules for the same
        (vocab_category, source_value) get their target/confidence/reasoning
        refreshed rather than duplicated.
      * Suggestions with koha_value=None are recorded as ``ai_reasoning``
        commentary only — no rule is created, because the apply pipeline
        can't do anything useful with a null target.

    Returns ``(matched, unmatched, written, updated)``.
    """
    from cerulean.models import ReconciliationRule

    sf_code = VOCAB_SUBFIELD_MAP.get(vocab_category)
    if not sf_code:
        log.warn(f"[{vocab_category}] unknown vocab — skipping persistence")
        return (0, 0, 0, 0)

    matched = 0
    unmatched = 0
    written = 0
    updated = 0

    with Session(_engine) as db:
        existing = db.execute(
            select(ReconciliationRule).where(
                ReconciliationRule.project_id == project_id,
                ReconciliationRule.vocab_category == vocab_category,
            )
        ).scalars().all()

        # Map source_value → existing rule. Active rules take precedence
        # so we never overwrite an engineer's approved work. Inactive
        # ai_suggested rules are the ones we refresh.
        active_sources: set[str] = set()
        refreshable: dict[str, ReconciliationRule] = {}
        for r in existing:
            for sv in (r.source_values or []):
                if r.active:
                    active_sources.add(sv)
                elif r.ai_suggested:
                    refreshable.setdefault(sv, r)

        for s in suggestions:
            src = s["source_value"]
            target = s["koha_value"]
            conf = float(s["confidence"])
            reasoning = s.get("reasoning")

            if target is None or not target.strip():
                unmatched += 1
                # We don't create a rule without a target, but we still
                # want the engineer to see the AI commentary. If there's
                # no inactive ai_suggested rule yet, seed one that's
                # obviously unusable (empty target) so it renders as an
                # "unresolved — AI could not match" row.
                existing_row = refreshable.get(src)
                if existing_row is not None:
                    existing_row.target_value = None
                    existing_row.ai_confidence = conf
                    existing_row.ai_reasoning = reasoning
                    updated += 1
                else:
                    if src in active_sources:
                        continue
                    row = ReconciliationRule(
                        project_id=project_id,
                        vocab_category=vocab_category,
                        marc_tag="952",
                        marc_subfield=sf_code,
                        operation="rename",
                        source_values=[src],
                        target_value=None,
                        sort_order=0,
                        active=False,
                        ai_suggested=True,
                        ai_confidence=conf,
                        ai_reasoning=reasoning,
                    )
                    db.add(row)
                    written += 1
                continue

            matched += 1

            # Skip when engineer has already approved a rule for this source
            if src in active_sources:
                continue

            existing_row = refreshable.get(src)
            if existing_row is not None:
                existing_row.operation = "rename"
                existing_row.marc_subfield = sf_code
                existing_row.target_value = target
                existing_row.ai_confidence = conf
                existing_row.ai_reasoning = reasoning
                updated += 1
            else:
                row = ReconciliationRule(
                    project_id=project_id,
                    vocab_category=vocab_category,
                    marc_tag="952",
                    marc_subfield=sf_code,
                    operation="rename",
                    source_values=[src],
                    target_value=target,
                    sort_order=0,
                    active=False,
                    ai_suggested=True,
                    ai_confidence=conf,
                    ai_reasoning=reasoning,
                )
                db.add(row)
                written += 1

        db.commit()

    return (matched, unmatched, written, updated)
