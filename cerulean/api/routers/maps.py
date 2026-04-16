"""
cerulean/api/routers/maps.py
─────────────────────────────────────────────────────────────────────────────
GET    /projects/{id}/maps                      — list maps (filter by status)
POST   /projects/{id}/maps                      — create map manually
PATCH  /projects/{id}/maps/{mid}                — edit map (AI-created → manual)
DELETE /projects/{id}/maps/{mid}                — delete map
POST   /projects/{id}/maps/ai-suggest           — Claude field-map suggestion run
POST   /projects/{id}/maps/{mid}/approve        — approve single map
POST   /projects/{id}/maps/approve-all          — batch approve by confidence
POST   /projects/{id}/maps/ai-transform/generate — describe a transform in plain
                                                   English, get back a sandboxed
                                                   Python expression + preview
POST   /projects/{id}/maps/ai-transform/preview  — re-run preview for an
                                                   edited expression
"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field as PField
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.database import get_db
from cerulean.models import FieldMap, MARCFile, Project
from cerulean.schemas.maps import (
    AIMapSuggestResponse,
    AISuggestRequest,
    ApproveAllRequest,
    ApproveAllResponse,
    FieldMapCreate,
    FieldMapOut,
    FieldMapUpdate,
)
from cerulean.tasks.analyze import ai_field_map_task

router = APIRouter(prefix="/projects", tags=["maps"])


@router.get("/{project_id}/maps", response_model=list[FieldMapOut])
async def list_maps(
    project_id: str,
    status: str | None = Query(None, description="Filter: approved | pending | rejected"),
    db: AsyncSession = Depends(get_db),
):
    """
    List all field maps for a project.
    status=approved  → approved=True
    status=pending   → approved=False, ai_suggested=True
    status=manual    → approved=False, ai_suggested=False
    """
    await require_project(project_id, db)

    q = select(FieldMap).where(FieldMap.project_id == project_id)

    if status == "approved":
        q = q.where(FieldMap.approved == True)  # noqa: E712
    elif status == "pending":
        q = q.where(FieldMap.approved == False, FieldMap.ai_suggested == True)  # noqa: E712
    elif status == "manual":
        q = q.where(FieldMap.approved == False, FieldMap.ai_suggested == False)  # noqa: E712

    q = q.order_by(FieldMap.sort_order, FieldMap.created_at)
    result = await db.execute(q)
    return result.scalars().all()


@router.post("/{project_id}/maps", response_model=FieldMapOut, status_code=201)
async def create_map(
    project_id: str,
    body: FieldMapCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a field map manually. source_label is always 'manual'."""
    await require_project(project_id, db)

    field_map = FieldMap(
        id=str(uuid.uuid4()),
        project_id=project_id,
        source_tag=body.source_tag,
        source_sub=body.source_sub,
        target_tag=body.target_tag,
        target_sub=body.target_sub,
        transform_type=body.transform_type,
        transform_fn=body.transform_fn,
        preset_key=body.preset_key,
        delete_source=body.delete_source,
        notes=body.notes,
        approved=body.approved,
        ai_prompt=body.ai_prompt,
        ai_suggested=False,
        source_label="manual",
    )
    db.add(field_map)

    await audit_log(db, project_id, stage=2, level="info", tag="[maps]",
                    message=f"Map created manually: {body.source_tag} → {body.target_tag}")

    await db.flush()
    await db.refresh(field_map)
    return field_map


@router.patch("/{project_id}/maps/{map_id}", response_model=FieldMapOut)
async def update_map(
    project_id: str,
    map_id: str,
    body: FieldMapUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Edit a field map. If the map was AI-suggested, source_label is set to 'manual'
    (preserving ai_suggested=True for audit, but marking it as human-reviewed).
    """
    field_map = await _get_map(project_id, map_id, db)

    was_ai = field_map.ai_suggested

    for attr in ("source_tag", "source_sub", "target_tag", "target_sub",
                 "transform_type", "transform_fn", "preset_key", "delete_source",
                 "notes", "approved", "ai_prompt"):
        val = getattr(body, attr)
        if val is not None:
            setattr(field_map, attr, val)

    # Key rule from coding standards: editing an AI map sets source_label to manual
    if was_ai:
        field_map.source_label = "manual"

    field_map.updated_at = datetime.utcnow()

    action = "approved" if body.approved else "edited"
    label = " (was AI-suggested)" if was_ai else ""
    await audit_log(db, project_id, stage=2, level="info", tag="[maps]",
                    message=f"Map {action}: {field_map.source_tag} → {field_map.target_tag}{label}")

    await db.flush()
    await db.refresh(field_map)
    return field_map


@router.delete("/{project_id}/maps/{map_id}", status_code=204)
async def delete_map(
    project_id: str,
    map_id: str,
    db: AsyncSession = Depends(get_db),
):
    field_map = await _get_map(project_id, map_id, db)
    src = f"{field_map.source_tag} → {field_map.target_tag}"
    await db.execute(delete(FieldMap).where(FieldMap.id == map_id))
    await audit_log(db, project_id, stage=2, level="info", tag="[maps]",
                    message=f"Map deleted: {src}")


@router.post("/{project_id}/maps/ai-suggest", response_model=AIMapSuggestResponse)
async def ai_suggest_maps(
    project_id: str,
    request: Request,
    body: AISuggestRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger Claude API analysis for field map suggestions.
    Creates FieldMap rows with approved=False, ai_suggested=True.
    Does NOT approve any map — engineer must review.
    Optionally accepts file_ids to analyze only selected files.

    The calling user's id is forwarded to the task so it can check the
    ``ai.value_aware_mapping`` feature flag — when enabled, the task
    includes aggregated subfield value samples in the Claude prompt for
    value-informed reasoning.
    """
    await require_project(project_id, db)

    # Get files with tag frequency data
    from cerulean.models import MARCFile
    q = select(MARCFile).where(
        MARCFile.project_id == project_id, MARCFile.tag_frequency != None  # noqa: E711
    )
    if body and body.file_ids:
        q = q.where(MARCFile.id.in_(body.file_ids))
    result = await db.execute(q)
    files = result.scalars().all()

    if not files:
        raise HTTPException(409, detail={
            "error": "NO_TAG_DATA",
            "message": "No files with tag frequency data found. Complete Stage 1 ingest first.",
        })

    await audit_log(db, project_id, stage=2, level="info", tag="[ai-map]",
                    message=f"AI mapping analysis requested across {len(files)} file(s)")

    user_id = getattr(request.state, "user_id", None)

    task = ai_field_map_task.apply_async(
        args=[project_id, [f.id for f in files]],
        kwargs={"user_id": user_id},
        queue="analyze",
    )
    from cerulean.api.routers.tasks import register_task
    register_task(project_id, task.id, "ai_field_map")

    return AIMapSuggestResponse(
        task_id=task.id,
        message="AI analysis started. Suggestions will appear in the AI Suggestions tab.",
    )


@router.get("/{project_id}/maps/validate")
async def validate_maps(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Validate all approved maps before running a transform.

    Returns a list of issues (empty = all good).
    """
    from cerulean.tasks.transform import _validate_map

    await require_project(project_id, db)

    result = await db.execute(
        select(FieldMap)
        .where(FieldMap.project_id == project_id, FieldMap.approved == True)  # noqa: E712
        .order_by(FieldMap.sort_order, FieldMap.created_at)
    )
    maps = result.scalars().all()

    if not maps:
        return {"valid": False, "approved_count": 0, "issues": [
            {"map": "—", "source": "—", "target": "—", "reason": "No approved maps found"}
        ]}

    issues = []
    for m in maps:
        md = {
            "source_tag": m.source_tag, "source_sub": m.source_sub,
            "target_tag": m.target_tag, "target_sub": m.target_sub,
            "transform_type": m.transform_type, "transform_fn": m.transform_fn,
            "preset_key": m.preset_key,
        }
        ok, reason = _validate_map(md)
        if not ok:
            issues.append({
                "map": m.id,
                "source": f"{m.source_tag}{m.source_sub or ''}",
                "target": f"{m.target_tag}{m.target_sub or ''}",
                "reason": reason,
            })

    return {
        "valid": len(issues) == 0,
        "approved_count": len(maps),
        "valid_count": len(maps) - len(issues),
        "issues": issues,
    }


@router.post("/{project_id}/maps/{map_id}/approve", response_model=FieldMapOut)
async def approve_map(
    project_id: str,
    map_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Approve a single field map."""
    field_map = await _get_map(project_id, map_id, db)
    field_map.approved = True
    field_map.updated_at = datetime.utcnow()

    await audit_log(db, project_id, stage=2, level="info", tag="[maps]",
                    message=f"Map approved: {field_map.source_tag} → {field_map.target_tag}")

    await db.flush()
    await db.refresh(field_map)
    return field_map


@router.post("/{project_id}/maps/approve-all", response_model=ApproveAllResponse)
async def approve_all_maps(
    project_id: str,
    body: ApproveAllRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Batch-approve all unapproved maps with ai_confidence >= min_confidence.
    Also approves all manually-created unapproved maps (confidence is null).
    """
    await require_project(project_id, db)

    result = await db.execute(
        select(FieldMap).where(
            FieldMap.project_id == project_id,
            FieldMap.approved == False,  # noqa: E712
        )
    )
    maps = result.scalars().all()

    approved_count = 0
    for m in maps:
        # Approve if manual (no AI confidence) or AI confidence meets threshold
        if m.ai_confidence is None or m.ai_confidence >= body.min_confidence:
            m.approved = True
            m.updated_at = datetime.utcnow()
            approved_count += 1

    await audit_log(db, project_id, stage=2, level="info", tag="[maps]",
                    message=f"Bulk approved {approved_count} maps (threshold: {body.min_confidence:.0%})")

    return ApproveAllResponse(approved_count=approved_count)


# ══════════════════════════════════════════════════════════════════════════
# AI TRANSFORM RULE GENERATION (cerulean_ai_spec.md §5)
# ══════════════════════════════════════════════════════════════════════════
# Two synchronous endpoints (no Celery task — user is actively waiting):
#
#   POST /maps/ai-transform/generate  describe → expression + preview
#   POST /maps/ai-transform/preview   expression → preview
#
# Both run in the request/response cycle because:
#  1. The generation call is short (< 2 s typical — small prompt, single turn)
#  2. The user is staring at a modal waiting to see results
#  3. No work worth saving across a page reload


class AITransformGenerateRequest(BaseModel):
    description: str = PField(..., min_length=1, max_length=500)
    source_tag: str
    source_sub: str | None = None


class AITransformPreviewRequest(BaseModel):
    expression: str
    source_tag: str
    source_sub: str | None = None


class TransformPreviewRow(BaseModel):
    before: str
    after: str
    error: str | None = None


class AITransformGenerateResponse(BaseModel):
    expression: str
    reasoning: str
    preview: list[TransformPreviewRow]


class AITransformPreviewResponse(BaseModel):
    preview: list[TransformPreviewRow]


_TRANSFORM_SYSTEM_PROMPT = """You are a MARC data migration specialist who writes single-line
Python 3 expressions that transform a single string value. The expression is evaluated in
a sandboxed environment where these names are available:

  value        the incoming string from the MARC subfield (also aliased as `v`)
  re           the Python `re` regex module
  datetime     the `datetime.datetime` class
  date         the `datetime.date` class
  timedelta    the `datetime.timedelta` class

Builtin names available: len, str, int, float, bool, list, dict, tuple, set, min, max, abs,
round, sorted, reversed, enumerate, zip, map, filter, isinstance, range, True, False, None.

NOT AVAILABLE: import, open, exec, compile, any file/network/OS call — those are blocked.

REQUIREMENTS
1. Produce ONE expression (not a statement, not multiple lines). The expression must
   evaluate to a string OR to None (None means "leave the value unchanged").
2. The expression must be safe against empty strings and unexpected input shapes — prefer
   `value.strip()` and guarded regex over assumptions about formatting.
3. Do not reach outside the sandbox (no I/O, no imports).
4. If the user's request cannot be accomplished safely with a single expression, explain
   that in the reasoning and emit `value` as the expression so nothing changes.

RETURN ONLY A JSON OBJECT matching this shape — no markdown, no prose:

{
  "expression": "<one-line Python expression>",
  "reasoning": "<one sentence explaining what the expression does>"
}

EXAMPLES

User: Strip the trailing period and comma
{
  "expression": "re.sub(r'[.,]+$', '', value.strip())",
  "reasoning": "Strips any run of trailing periods or commas from the value after trimming whitespace."
}

User: Convert MM/DD/YYYY dates to ISO YYYY-MM-DD
{
  "expression": "datetime.strptime(value.strip(), '%m/%d/%Y').strftime('%Y-%m-%d') if value.strip() else value",
  "reasoning": "Parses a MM/DD/YYYY date and reformats as ISO 8601; leaves empty values untouched."
}

User: Remove an author name appended after a slash at the end of the title
{
  "expression": "re.sub(r'\\\\s*/.*$', '', value).rstrip()",
  "reasoning": "Removes everything from the last slash onward (including leading whitespace) then trims trailing space."
}
"""


@router.post("/{project_id}/maps/ai-transform/generate", response_model=AITransformGenerateResponse)
async def ai_transform_generate(
    project_id: str,
    request: Request,
    body: AITransformGenerateRequest = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Plain-English description → sandboxed Python expression + preview.

    Gated by ``ai.transform_rule_gen``. The response always includes a
    before/after preview on up to 10 sample values drawn from the project's
    MARC files — per spec the preview is mandatory before approve.
    """
    from cerulean.api.routers.preferences import _resolve_user
    from cerulean.core.preferences import get_user_pref

    await require_project(project_id, db, request)

    user = await _resolve_user(request, db)
    if not await get_user_pref(db, user.id, "ai.transform_rule_gen"):
        raise HTTPException(403, detail={
            "error": "PREFERENCE_DISABLED",
            "feature_key": "ai.transform_rule_gen",
            "message": "Enable 'Transform Rule Generation' in My Preferences to use this.",
        })

    # Pull up to 10 sample values so both Claude and the preview table see
    # the same data. Pulling early lets us include a few examples in the
    # user-prompt context too.
    samples = await _collect_sample_values(project_id, body.source_tag, body.source_sub, db, limit=10)
    if not samples:
        raise HTTPException(409, detail={
            "error": "NO_SAMPLES",
            "message": f"No values found for {body.source_tag}{'$' + body.source_sub if body.source_sub else ''}. Ingest a file first.",
        })

    from cerulean.core.config import get_settings
    settings = get_settings()
    import anthropic
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    user_message = (
        f"SOURCE FIELD: {body.source_tag}"
        f"{'$' + body.source_sub if body.source_sub else ''}\n"
        f"SAMPLE VALUES (up to 10):\n"
        + "\n".join(f"  - {s!r}" for s in samples) + "\n\n"
        f"USER REQUEST: {body.description}"
    )

    try:
        response = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=512,
            system=_TRANSFORM_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
    except Exception as exc:
        raise HTTPException(502, detail={"error": "AI_UPSTREAM", "message": str(exc)})

    raw = response.content[0].text.strip()
    parsed = _parse_transform_response(raw)
    if parsed is None:
        raise HTTPException(502, detail={
            "error": "AI_PARSE",
            "message": "The AI response was not valid JSON. Try rephrasing your description.",
            "raw": raw[:400],
        })

    expression = parsed["expression"]
    reasoning = parsed.get("reasoning") or ""

    preview = _run_preview(expression, samples)

    await audit_log(
        db, project_id, stage=2, level="info", tag="[ai-transform]",
        message=f"AI transform rule generated for {body.source_tag}{'$' + body.source_sub if body.source_sub else ''}: "
                f"{body.description[:80]}",
    )

    return AITransformGenerateResponse(
        expression=expression,
        reasoning=reasoning,
        preview=[TransformPreviewRow(**row) for row in preview],
    )


@router.post("/{project_id}/maps/ai-transform/preview", response_model=AITransformPreviewResponse)
async def ai_transform_preview(
    project_id: str,
    request: Request,
    body: AITransformPreviewRequest = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Run an expression against sample values and return before/after + any errors.

    Gated by ``ai.transform_rule_gen``: the preview endpoint is part of the
    AI transform flow, so hiding it when the feature is off keeps the flag
    surface consistent.
    """
    from cerulean.api.routers.preferences import _resolve_user
    from cerulean.core.preferences import get_user_pref

    await require_project(project_id, db, request)

    user = await _resolve_user(request, db)
    if not await get_user_pref(db, user.id, "ai.transform_rule_gen"):
        raise HTTPException(403, detail={
            "error": "PREFERENCE_DISABLED",
            "feature_key": "ai.transform_rule_gen",
            "message": "Enable 'Transform Rule Generation' in My Preferences to use this.",
        })

    samples = await _collect_sample_values(project_id, body.source_tag, body.source_sub, db, limit=10)
    preview = _run_preview(body.expression, samples)
    return AITransformPreviewResponse(
        preview=[TransformPreviewRow(**row) for row in preview],
    )


# ── Helpers ────────────────────────────────────────────────────────────


async def _get_map(project_id: str, map_id: str, db: AsyncSession) -> FieldMap:
    field_map = await db.get(FieldMap, map_id)
    if not field_map or field_map.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Map not found."})
    return field_map


async def _collect_sample_values(
    project_id: str,
    source_tag: str,
    source_sub: str | None,
    db: AsyncSession,
    limit: int = 10,
) -> list[str]:
    """Return up to `limit` distinct non-empty values for a source tag/sub.

    Scans the project's MARC files (raw uploads) until the limit is reached
    or all files are exhausted. Cheap for small samples — aborts as soon as
    it has enough values.
    """
    result = await db.execute(
        select(MARCFile.storage_path).where(
            MARCFile.project_id == project_id,
            MARCFile.file_category == "marc",
            MARCFile.status == "indexed",
        )
    )
    paths = [row[0] for row in result.all() if row[0]]
    if not paths:
        return []

    import pymarc
    seen: list[str] = []
    dedupe: set[str] = set()
    for path in paths:
        if len(seen) >= limit:
            break
        try:
            with open(path, "rb") as fh:
                reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
                for record in reader:
                    if record is None:
                        continue
                    for field in record.get_fields(source_tag):
                        if field.is_control_field():
                            val = (field.data or "").strip()
                            if val and val not in dedupe:
                                dedupe.add(val)
                                seen.append(val)
                        elif source_sub:
                            for sf in field.subfields:
                                if sf.code == source_sub and sf.value:
                                    val = sf.value.strip()
                                    if val and val not in dedupe:
                                        dedupe.add(val)
                                        seen.append(val)
                        else:
                            # No subfield specified — take the first subfield value
                            for sf in field.subfields:
                                if sf.value:
                                    val = sf.value.strip()
                                    if val and val not in dedupe:
                                        dedupe.add(val)
                                        seen.append(val)
                                    break
                    if len(seen) >= limit:
                        break
        except Exception:
            continue
    return seen[:limit]


def _parse_transform_response(raw: str) -> dict | None:
    """Extract the JSON object from Claude's response."""
    import json
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end < start:
        return None
    try:
        obj = json.loads(raw[start : end + 1])
        if not isinstance(obj, dict) or "expression" not in obj:
            return None
        return obj
    except Exception:
        return None


def _run_preview(expression: str, samples: list[str]) -> list[dict]:
    """Evaluate a fn expression against each sample and return before/after
    rows with per-row error messages. Uses the production sandbox so what
    the preview shows is exactly what the transform pipeline will produce."""
    from cerulean.tasks.transform import apply_fn_safe
    rows: list[dict] = []
    for before in samples:
        after, err = apply_fn_safe(before, expression)
        rows.append({"before": before, "after": after, "error": err})
    return rows
