"""
cerulean/tasks/analyze.py
─────────────────────────────────────────────────────────────────────────────
Stage 2 Celery tasks:

    ai_field_map_task    — call Claude API with tag frequency + sample records,
                           write AI-suggested FieldMap rows (approved=False)
    save_template_task   — serialise approved maps to a MapTemplate row
    load_template_task   — apply a template to a project (replace or merge mode)
"""

import json
import uuid
from collections import Counter

import pymarc
from celery import shared_task
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)

# ── Claude system prompt ───────────────────────────────────────────────
_SYSTEM_PROMPT = """You are an expert MARC21 cataloguing and library systems migration specialist.
You will be given a tag frequency report from a legacy ILS export and a sample of raw MARC records.
Your task is to suggest field mappings from the source MARC structure to Koha's expected MARC structure.

KOHA FIELD REFERENCE (most important):
- 952 = Local holdings / items (subfields: $a=homebranch, $b=holdingbranch, $c=shelving location,
        $o=callnumber, $p=barcode, $y=itemtype, $8=ccode)
- 942 = Koha item type at bib level ($c=itemtype)
- 099 = Local free-text fields sometimes used for call numbers

SOURCE FIELDS TO MAP TO KOHA 952:
- Location/branch codes (often 852$a, 852$b, 949$l, 945$l)
- Call numbers (often 092$a, 099$a, 852$h+$i, 949$a)
- Barcodes (often 876$p, 949$i, 945$i, item-level fields)
- Item types (often 998$b, 949$t, 007 leader)

AVAILABLE TRANSFORM PRESETS:
You may suggest transform_type="preset" with one of these preset_key values:
- Date: date_mdy_to_dmy, date_dmy_to_iso, date_ymd_to_iso, date_marc_to_iso
- Case: case_upper, case_lower, case_title, case_sentence
- Text: text_trim, text_normalize_spaces, text_strip_punctuation
- Clean: clean_isbn (remove hyphens/qualifiers), clean_lccn, clean_html
- Extract: extract_year, extract_isbn13
Use presets when appropriate (e.g., clean_isbn for 020$a, date_marc_to_iso for date fields in 008).

RULES:
1. Only suggest mappings you are confident about. A confident wrong mapping is worse than no mapping.
2. For each suggestion provide: source_tag, source_sub (or null), target_tag, target_sub (or null),
   transform_type (copy|regex|lookup|const|fn|preset), transform_fn (expression or null),
   preset_key (preset key string or null), delete_source (boolean),
   confidence (0.0–1.0), reasoning (1–2 sentences).
3. Set delete_source=true when the source tag is ILS-specific (e.g. 945, 949, 999, 907) and
   won't be needed in Koha after mapping. Keep delete_source=false for standard fields.
4. Do not suggest mappings for standard bibliographic fields (1xx, 2xx, 3xx, 4xx, 5xx, 6xx, 7xx, 8xx)
   that already carry across unchanged — only flag these if they need transformation.
5. Focus on local/item fields and fields that need renaming or transformation for Koha.
6. No PII is present in the sample. Do not mention patron data.

Return ONLY a JSON array of suggestion objects. No preamble, no markdown fences."""


# ══════════════════════════════════════════════════════════════════════════
# AI FIELD MAP TASK
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, name="cerulean.tasks.analyze.ai_field_map_task")
def ai_field_map_task(self, project_id: str, file_ids: list[str]) -> dict:
    """
    Call Claude API for field map suggestions.

    Sends: combined tag frequency report + up to AI_MAX_SAMPLE_RECORDS records.
    Writes: FieldMap rows with approved=False, ai_suggested=True.
    Does NOT set approved=True on any row.

    Args:
        project_id: Project UUID.
        file_ids: List of MARCFile UUIDs to analyse (combined frequency).
    """
    from cerulean.models import FieldMap, MARCFile

    log = AuditLogger(project_id=project_id, stage=2, tag="[ai-map]")
    log.info(f"Starting AI field map analysis across {len(file_ids)} file(s)")

    try:
        # 1. Build combined tag frequency
        with Session(_engine) as db:
            files = db.execute(
                select(MARCFile).where(MARCFile.id.in_(file_ids))
            ).scalars().all()

        if not files:
            log.error("No files found for AI analysis")
            return {"error": "no_files"}

        combined_freq: Counter = Counter()
        sample_records = []
        storage_paths = []

        for f in files:
            if f.tag_frequency:
                combined_freq.update(f.tag_frequency)
            storage_paths.append(f.storage_path)

        # 2. Collect sample records (no PII — bibliographic only)
        for path in storage_paths:
            if len(sample_records) >= settings.ai_max_sample_records:
                break
            sample_records.extend(_read_bib_sample(
                path,
                max_records=settings.ai_max_sample_records - len(sample_records),
            ))

        log.info(f"Sending {len(combined_freq)} tags, {len(sample_records)} sample records to Claude")

        # 3. Build prompt
        freq_report = json.dumps(
            dict(sorted(combined_freq.items(), key=lambda x: x[1], reverse=True)[:100]),
            indent=2,
        )
        sample_text = _records_to_text(sample_records[:settings.ai_max_sample_records])

        user_message = (
            f"TAG FREQUENCY REPORT (top 100 tags, count of occurrences):\n{freq_report}\n\n"
            f"SAMPLE MARC RECORDS ({len(sample_records)} records):\n{sample_text}"
        )

        # 4. Call Claude API
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        response = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=4096,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        raw_text = response.content[0].text.strip()

        # 5. Parse suggestions
        suggestions = _parse_suggestions(raw_text)
        log.info(f"Claude returned {len(suggestions)} suggestions")

        # 6. Write FieldMap rows — approved=False, ai_suggested=True
        #    Skip if an approved map already exists for same source;
        #    replace if an unapproved map exists for same source+target.
        written = 0
        skipped = 0
        replaced = 0
        with Session(_engine) as db:
            existing = db.execute(
                select(FieldMap).where(FieldMap.project_id == project_id)
            ).scalars().all()
            approved_keys = {
                (m.source_tag, m.source_sub)
                for m in existing if m.approved
            }
            unapproved_by_key = {
                (m.source_tag, m.source_sub, m.target_tag, m.target_sub): m
                for m in existing if not m.approved
            }

            for s in suggestions:
                if not _is_valid_suggestion(s):
                    continue

                src_key = (s.get("source_tag", ""), s.get("source_sub"))
                full_key = (
                    s.get("source_tag", ""), s.get("source_sub"),
                    s.get("target_tag", ""), s.get("target_sub"),
                )

                # Never override an approved map
                if src_key in approved_keys:
                    skipped += 1
                    continue

                # Replace existing unapproved map with same source+target
                existing_map = unapproved_by_key.get(full_key)
                if existing_map:
                    existing_map.transform_type = s.get("transform_type", "copy")
                    existing_map.transform_fn = s.get("transform_fn")
                    existing_map.preset_key = s.get("preset_key")
                    existing_map.delete_source = bool(s.get("delete_source", False))
                    existing_map.ai_confidence = float(s.get("confidence", 0.5))
                    existing_map.ai_reasoning = s.get("reasoning")
                    replaced += 1
                    continue

                row = FieldMap(
                    id=str(uuid.uuid4()),
                    project_id=project_id,
                    source_tag=s.get("source_tag", ""),
                    source_sub=s.get("source_sub"),
                    target_tag=s.get("target_tag", ""),
                    target_sub=s.get("target_sub"),
                    transform_type=s.get("transform_type", "copy"),
                    transform_fn=s.get("transform_fn"),
                    preset_key=s.get("preset_key"),
                    delete_source=bool(s.get("delete_source", False)),
                    ai_suggested=True,
                    ai_confidence=float(s.get("confidence", 0.5)),
                    ai_reasoning=s.get("reasoning"),
                    source_label="ai",
                    approved=False,  # NEVER auto-approve
                    notes=None,
                )
                db.add(row)
                written += 1
            db.commit()

        log.complete(
            f"AI analysis complete — {written} new, {replaced} updated, "
            f"{skipped} skipped (approved). Engineer review required."
        )
        return {"suggestions_written": written, "suggestions_replaced": replaced, "suggestions_skipped": skipped}

    except Exception as exc:
        log.error(f"AI field map task failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# SAVE TEMPLATE TASK
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, name="cerulean.tasks.analyze.save_template_task")
def save_template_task(
    self,
    project_id: str,
    template_name: str,
    version: str,
    scope: str,
    description: str | None,
    include_pending: bool,
    created_by: str | None,
) -> dict:
    """
    Serialise approved (and optionally pending) maps to a MapTemplate row.

    Args:
        project_id: Source project UUID.
        template_name: Human-readable template name.
        version: Version string e.g. "1.0".
        scope: "project" | "global".
        description: Optional description.
        include_pending: Whether to include unapproved AI-suggested maps.
        created_by: Engineer email/name.
    """
    from cerulean.models import FieldMap, MapTemplate, Project

    log = AuditLogger(project_id=project_id, stage=2, tag="[templates]")
    log.info(f"Saving template: {template_name} (scope={scope})")

    try:
        with Session(_engine) as db:
            project = db.get(Project, project_id)

            q = select(FieldMap).where(
                FieldMap.project_id == project_id,
                FieldMap.approved == True,  # noqa: E712
            )
            maps = db.execute(q).scalars().all()

            if include_pending:
                pending = db.execute(
                    select(FieldMap).where(
                        FieldMap.project_id == project_id,
                        FieldMap.approved == False,  # noqa: E712
                        FieldMap.ai_suggested == True,  # noqa: E712
                    )
                ).scalars().all()
                maps = list(maps) + list(pending)

            serialised = [
                {
                    "source_tag": m.source_tag,
                    "source_sub": m.source_sub,
                    "target_tag": m.target_tag,
                    "target_sub": m.target_sub,
                    "transform_type": m.transform_type,
                    "transform_fn": m.transform_fn,
                    "notes": m.notes,
                }
                for m in maps
            ]

            template = MapTemplate(
                id=str(uuid.uuid4()),
                name=template_name,
                version=version,
                description=description,
                scope=scope,
                project_id=project_id if scope == "project" else None,
                source_ils=project.source_ils if project else None,
                ai_generated=False,
                reviewed=True,
                maps=serialised,
                created_by=created_by,
            )
            db.add(template)
            db.commit()

            template_id = template.id

        log.complete(f"Template saved: {template_name} v{version} ({len(serialised)} maps, scope={scope})")
        return {"template_id": template_id, "map_count": len(serialised)}

    except Exception as exc:
        log.error(f"Save template failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# LOAD TEMPLATE TASK
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, name="cerulean.tasks.analyze.load_template_task")
def load_template_task(self, project_id: str, template_id: str, mode: str) -> dict:
    """
    Apply a template to a project's field maps.

    mode="replace": Delete all existing maps, then insert template maps.
    mode="merge":   Skip source_tag+source_sub where approved=True already exists.
                    Overwrite unapproved entries; log conflicts as warnings.

    Rule: merge mode NEVER touches approved maps.
    """
    from cerulean.models import FieldMap, MapTemplate

    log = AuditLogger(project_id=project_id, stage=2, tag="[templates]")

    try:
        with Session(_engine) as db:
            template = db.get(MapTemplate, template_id)
            if not template or not template.maps:
                log.error(f"Template {template_id} not found or empty")
                return {"error": "template_not_found"}

            log.info(f"Loading template '{template.name}' v{template.version} (mode={mode})")

            if mode == "replace":
                # Delete all existing maps for this project
                existing = db.execute(
                    select(FieldMap).where(FieldMap.project_id == project_id)
                ).scalars().all()
                for m in existing:
                    db.delete(m)
                db.flush()
                maps_added = _insert_template_maps(db, project_id, template)
                maps_skipped = 0
                maps_conflicted = 0

            else:  # merge
                # Build index of existing approved maps by (source_tag, source_sub)
                existing_approved = {
                    (m.source_tag, m.source_sub): m
                    for m in db.execute(
                        select(FieldMap).where(
                            FieldMap.project_id == project_id,
                            FieldMap.approved == True,  # noqa: E712
                        )
                    ).scalars().all()
                }
                maps_added = 0
                maps_skipped = 0
                maps_conflicted = 0

                for entry in template.maps:
                    key = (entry.get("source_tag"), entry.get("source_sub"))
                    if key in existing_approved:
                        # Rule: never touch approved maps in merge mode
                        log.warn(
                            f"Merge skipped {key[0]}{'$'+key[1] if key[1] else ''} — "
                            f"approved map exists"
                        )
                        maps_skipped += 1
                    else:
                        # Remove any existing unapproved entry for this key first
                        unapproved = db.execute(
                            select(FieldMap).where(
                                FieldMap.project_id == project_id,
                                FieldMap.source_tag == key[0],
                                FieldMap.source_sub == key[1],
                                FieldMap.approved == False,  # noqa: E712
                            )
                        ).scalar_one_or_none()
                        if unapproved:
                            db.delete(unapproved)
                            maps_conflicted += 1

                        _insert_one_map(db, project_id, entry, template.id)
                        maps_added += 1

            # Increment use_count
            template.use_count = (template.use_count or 0) + 1
            db.commit()

        log.complete(
            f"Template loaded: {maps_added} added, {maps_skipped} skipped (approved), "
            f"{maps_conflicted} replaced (unapproved)"
        )
        return {
            "maps_added": maps_added,
            "maps_skipped": maps_skipped,
            "maps_conflicted": maps_conflicted,
            "mode": mode,
        }

    except Exception as exc:
        log.error(f"Load template failed: {exc}")
        raise


# ── Helpers ────────────────────────────────────────────────────────────

def _insert_template_maps(db: Session, project_id: str, template) -> int:
    count = 0
    for entry in template.maps:
        _insert_one_map(db, project_id, entry, template.id)
        count += 1
    return count


def _insert_one_map(db: Session, project_id: str, entry: dict, template_id: str) -> None:
    from cerulean.models import FieldMap
    db.add(FieldMap(
        id=str(uuid.uuid4()),
        project_id=project_id,
        source_tag=entry.get("source_tag", ""),
        source_sub=entry.get("source_sub"),
        target_tag=entry.get("target_tag", ""),
        target_sub=entry.get("target_sub"),
        transform_type=entry.get("transform_type", "copy"),
        transform_fn=entry.get("transform_fn"),
        notes=entry.get("notes"),
        ai_suggested=False,
        source_label=f"template:{template_id}",
        approved=False,
    ))


def _read_bib_sample(storage_path: str, max_records: int) -> list:
    """Read bibliographic-only sample records (skip records with 852/949/945 patron links)."""
    sample = []
    try:
        with open(storage_path, "rb") as fh:
            reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
            for record in reader:
                # Skip records that are purely item-level (no title)
                if not record.get_fields("245"):
                    continue
                sample.append(record)
                if len(sample) >= max_records:
                    break
    except Exception:
        pass
    return sample


def _records_to_text(records: list) -> str:
    """Convert pymarc records to a compact text representation for the Claude prompt."""
    lines = []
    for i, record in enumerate(records):
        lines.append(f"--- Record {i + 1} ---")
        for field in record.fields:
            if field.is_control_field():
                lines.append(f"  {field.tag}: {field.data}")
            else:
                subs = " ".join(f"${sf.code} {sf.value}" for sf in field.subfields)
                lines.append(f"  {field.tag} {field.indicator1}{field.indicator2} {subs}")
    return "\n".join(lines)


def _parse_suggestions(raw: str) -> list[dict]:
    """Parse Claude's JSON response into a list of suggestion dicts."""
    raw = raw.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def _is_valid_suggestion(s: dict) -> bool:
    """Basic validation — require source_tag and target_tag."""
    return bool(s.get("source_tag") and s.get("target_tag"))
