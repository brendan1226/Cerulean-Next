"""
cerulean/api/routers/maps.py
─────────────────────────────────────────────────────────────────────────────
GET    /projects/{id}/maps              — list maps (filter by status)
POST   /projects/{id}/maps              — create map manually
PATCH  /projects/{id}/maps/{mid}        — edit map (sets source_label=manual if AI-created)
DELETE /projects/{id}/maps/{mid}        — delete map
POST   /projects/{id}/maps/ai-suggest   — trigger Claude API analysis
POST   /projects/{id}/maps/{mid}/approve — approve single map
POST   /projects/{id}/maps/approve-all  — batch approve by confidence threshold
"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.database import get_db
from cerulean.models import FieldMap, Project
from cerulean.schemas.maps import (
    AIMapSuggestResponse,
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
                 "notes", "approved"):
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
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger Claude API analysis for field map suggestions.
    Creates FieldMap rows with approved=False, ai_suggested=True.
    Does NOT approve any map — engineer must review.
    """
    await require_project(project_id, db)

    # Get files with tag frequency data
    from cerulean.models import MARCFile
    result = await db.execute(
        select(MARCFile)
        .where(MARCFile.project_id == project_id, MARCFile.tag_frequency != None)  # noqa: E711
    )
    files = result.scalars().all()

    if not files:
        raise HTTPException(409, detail={
            "error": "NO_TAG_DATA",
            "message": "No files with tag frequency data found. Complete Stage 1 ingest first.",
        })

    await audit_log(db, project_id, stage=2, level="info", tag="[ai-map]",
                    message=f"AI mapping analysis requested across {len(files)} file(s)")

    task = ai_field_map_task.apply_async(
        args=[project_id, [f.id for f in files]],
        queue="analyze",
    )

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


# ── Helpers ────────────────────────────────────────────────────────────


async def _get_map(project_id: str, map_id: str, db: AsyncSession) -> FieldMap:
    field_map = await db.get(FieldMap, map_id)
    if not field_map or field_map.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Map not found."})
    return field_map
