"""
cerulean/tasks/items.py
─────────────────────────────────────────────────────────────────────────────
Celery task: AI-suggest item CSV column → Koha 952 subfield mappings.

    items_ai_map_task  — call Claude to suggest mappings + bib match key
"""

import csv
import json
import uuid
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)


# ── 952 subfield reference ────────────────────────────────────────────

_952_SUBFIELD_REFERENCE = """
Koha 952 item record subfield codes:
  $a = homebranch (branchcode of owning library, REQUIRED)
  $b = holdingbranch (branchcode of current holding library, REQUIRED)
  $c = location (shelving location code, e.g. GEN, REF, CHILD)
  $d = date_accessioned (YYYY-MM-DD)
  $e = booksellerid (acquisition source)
  $g = purchase_price (decimal)
  $h = enumchron (serial enumeration/chronology, e.g. "v.1 no.2 2024")
  $l = issues (number of times checked out)
  $o = callnumber (full call number string)
  $p = barcode (unique item barcode, REQUIRED)
  $t = copynumber (copy number)
  $v = replacementpricedate (YYYY-MM-DD)
  $y = itype (Koha item type code, REQUIRED)
  $z = itemnotes_nonpublic (internal staff notes)
  $0 = withdrawn (0=not withdrawn, 1=withdrawn)
  $1 = itemlost (0=not lost, 1=lost, 2=long overdue, etc.)
  $2 = classification_source (e.g. "ddc", "lcc")
  $4 = damaged (0=not damaged, 1=damaged)
  $7 = notforloan (0=for loan, -1=ordered, 1=not for loan, 2=staff collection)
  $8 = ccode (collection code, e.g. FICTION, NONFIC, DVD)
"""

_ITEMS_AI_SYSTEM_PROMPT = f"""You are a library data migration specialist mapping CSV columns from a legacy ILS item/holdings export to Koha 952 MARC subfields.

{_952_SUBFIELD_REFERENCE}

TASK:
1. For each CSV column, determine the best 952 subfield mapping (or null if no mapping).
2. Identify which column contains the bibliographic record key (the value used to link items to bib records — often named "CATKEY", "biblionumber", "BIB_ID", "RECORD_NUMBER", etc.).
3. Suggest a matching MARC tag (usually "001" or "035") that holds the corresponding bib key in the MARC records.

Return ONLY a JSON object with this structure:
{{
  "bib_key_column": "<CSV column name that contains the bib record key>",
  "bib_match_tag": "<MARC tag to match, usually 001>",
  "mappings": [
    {{
      "source_column": "<CSV column name>",
      "target_subfield": "<952 subfield code or null>",
      "confidence": <0.0-1.0>,
      "reasoning": "<brief explanation>",
      "transform_type": "copy",
      "transform_config": null
    }}
  ]
}}

Guidelines:
- Use null for target_subfield if the column has no 952 equivalent (e.g. internal IDs, timestamps).
- The bib_key_column should NOT be mapped to a 952 subfield — it's the join key.
- confidence should be high (0.9+) for clear matches like barcode, callnumber, item type.
- Use transform_type "copy" for direct mappings. Suggest other types only when clearly needed.

Return ONLY valid JSON. No markdown fences, no preamble."""


@celery_app.task(
    bind=True,
    name="cerulean.tasks.items.items_ai_map_task",
    max_retries=0,
    queue="ingest",
)
def items_ai_map_task(self, project_id: str) -> dict:
    """Call Claude API for item CSV column → 952 subfield mapping suggestions.

    Reads items CSV file(s), sends column headers + sample rows to Claude,
    writes ItemColumnMap rows with approved=False.

    Args:
        project_id: UUID of the project.

    Returns:
        dict with suggestions_written, suggestions_replaced, suggestions_skipped.
    """
    from cerulean.models import ItemColumnMap, MARCFile, Project

    log = AuditLogger(project_id=project_id, stage=2, tag="[items-ai]")
    log.info("Items AI column mapping starting")

    try:
        # Find items CSV file
        with Session(_engine) as db:
            items_file = db.execute(
                select(MARCFile).where(
                    MARCFile.project_id == project_id,
                    MARCFile.file_category.in_(["items", "items_csv"]),
                ).limit(1)
            ).scalar_one_or_none()

            if not items_file:
                log.error("No items CSV file found")
                return {"error": "no_items_csv"}

            storage_path = items_file.storage_path

        # Read headers + sample rows
        csv.field_size_limit(10 * 1024 * 1024)
        headers: list[str] = []
        sample_rows: list[dict] = []
        stored_headers = items_file.column_headers or []
        has_generated = stored_headers and any(h.startswith("col_") for h in stored_headers)
        with open(storage_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f, fieldnames=stored_headers) if has_generated else csv.DictReader(f)
            headers = list(reader.fieldnames or [])
            for i, row in enumerate(reader):
                if i >= 20:
                    break
                sample_rows.append(row)

        if not headers:
            log.error("No headers found in items CSV")
            return {"error": "no_headers"}

        log.info(f"Sending {len(headers)} columns, {len(sample_rows)} sample rows to Claude")

        # Build prompt
        header_list = "\n".join(f"  - {h}" for h in headers)
        sample_text = json.dumps(sample_rows[:10], indent=2, default=str)

        user_message = (
            f"SOURCE CSV COLUMN HEADERS:\n{header_list}\n\n"
            f"SAMPLE ROWS ({len(sample_rows)} rows, first 10 shown):\n{sample_text}"
        )

        # Call Claude API
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        response = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=4096,
            system=_ITEMS_AI_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        raw_text = response.content[0].text.strip()
        parsed = _parse_ai_response(raw_text)
        if not parsed:
            log.error("Failed to parse Claude response")
            return {"error": "parse_failed"}

        mappings = parsed.get("mappings", [])
        bib_key_column = parsed.get("bib_key_column")
        bib_match_tag = parsed.get("bib_match_tag", "001")

        log.info(f"Claude returned {len(mappings)} mappings, bib_key_column={bib_key_column}")

        # Write ItemColumnMap rows + update project match config
        written = 0
        replaced = 0
        skipped = 0

        with Session(_engine) as db:
            existing = db.execute(
                select(ItemColumnMap).where(ItemColumnMap.project_id == project_id)
            ).scalars().all()

            approved_keys = {m.source_column for m in existing if m.approved}
            unapproved_by_key = {
                m.source_column: m for m in existing if not m.approved
            }

            for idx, s in enumerate(mappings):
                src_col = s.get("source_column", "").strip()
                if not src_col:
                    continue

                # Never override an approved map
                if src_col in approved_keys:
                    skipped += 1
                    continue

                target_sub = s.get("target_subfield")

                # Replace existing unapproved map
                existing_map = unapproved_by_key.get(src_col)
                if existing_map:
                    existing_map.target_subfield = target_sub
                    existing_map.transform_type = s.get("transform_type", "copy")
                    existing_map.transform_config = s.get("transform_config")
                    existing_map.ignored = target_sub is None
                    existing_map.ai_confidence = float(s.get("confidence", 0.5))
                    existing_map.ai_reasoning = s.get("reasoning")
                    existing_map.ai_suggested = True
                    existing_map.source_label = "ai"
                    replaced += 1
                    continue

                row = ItemColumnMap(
                    id=str(uuid.uuid4()),
                    project_id=project_id,
                    source_column=src_col,
                    target_subfield=target_sub,
                    ignored=target_sub is None,
                    transform_type=s.get("transform_type", "copy"),
                    transform_config=s.get("transform_config"),
                    approved=False,
                    ai_suggested=True,
                    ai_confidence=float(s.get("confidence", 0.5)),
                    ai_reasoning=s.get("reasoning"),
                    source_label="ai",
                    sort_order=idx,
                )
                db.add(row)
                written += 1

            # Update project match config from AI suggestion
            if bib_key_column:
                project = db.get(Project, project_id)
                if project:
                    project.items_csv_key_column = bib_key_column
                    project.items_csv_match_tag = bib_match_tag

            db.commit()

        log.complete(
            f"Items AI mapping complete — {written} new, {replaced} updated, "
            f"{skipped} skipped (approved). Engineer review required."
        )
        return {
            "suggestions_written": written,
            "suggestions_replaced": replaced,
            "suggestions_skipped": skipped,
            "bib_key_column": bib_key_column,
            "bib_match_tag": bib_match_tag,
        }

    except Exception as exc:
        log.error(f"Items AI map task failed: {exc}")
        raise


def _parse_ai_response(raw: str) -> dict | None:
    """Parse Claude's JSON response into a dict with mappings + bib_key_column."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None
