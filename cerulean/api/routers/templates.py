"""
cerulean/api/routers/templates.py
─────────────────────────────────────────────────────────────────────────────
GET    /templates                          — list templates (filter by scope/ils)
POST   /templates                          — save current maps as template
GET    /templates/{tid}                    — template detail
GET    /templates/{tid}/csv                — download template as CSV
PATCH  /templates/{tid}                    — edit name/version/scope/description
DELETE /templates/{tid}                    — delete template
POST   /templates/{tid}/promote            — promote project→global
POST   /templates/import-csv              — create template from uploaded CSV
POST   /projects/{id}/maps/load-template   — load template into project maps
"""

import csv
import io
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.database import get_db
from cerulean.models import AuditEvent, MapTemplate, Project
from cerulean.schemas.maps import (
    MapTemplateCreate,
    MapTemplateOut,
    MapTemplateUpdate,
    TemplateLoadRequest,
    TemplateLoadResult,
)
from cerulean.tasks.analyze import load_template_task, save_template_task

router = APIRouter(tags=["templates"])


@router.get("/templates", response_model=list[MapTemplateOut])
async def list_templates(
    scope: str | None = Query(None, description="project | global"),
    source_ils: str | None = Query(None),
    ai_generated: bool | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    q = select(MapTemplate)
    if scope:
        q = q.where(MapTemplate.scope == scope)
    if source_ils:
        q = q.where(MapTemplate.source_ils.ilike(f"%{source_ils}%"))
    if ai_generated is not None:
        q = q.where(MapTemplate.ai_generated == ai_generated)
    q = q.order_by(MapTemplate.use_count.desc(), MapTemplate.created_at.desc())
    result = await db.execute(q)
    return result.scalars().all()


@router.post("/templates", response_model=MapTemplateOut, status_code=202)
async def save_template(
    body: MapTemplateCreate,
    project_id: str = Query(..., description="Project to save maps from"),
    db: AsyncSession = Depends(get_db),
):
    """Save current project maps as a named template. Dispatches Celery task."""
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    # Dispatch to Celery — returns immediately
    save_template_task.apply_async(
        args=[
            project_id,
            body.name,
            body.version,
            body.scope,
            body.description,
            body.include_pending,
            None,  # created_by — wire up from auth when auth is added
        ],
        queue="analyze",
    )

    # Return placeholder — actual row written async
    return MapTemplateOut(
        id="pending",
        name=body.name,
        version=body.version,
        description=body.description,
        scope=body.scope,
        project_id=project_id if body.scope == "project" else None,
        source_ils=body.source_ils,
        ai_generated=False,
        reviewed=True,
        use_count=0,
        maps=None,
        created_by=None,
        created_at=__import__("datetime").datetime.utcnow(),
        updated_at=__import__("datetime").datetime.utcnow(),
    )


@router.get("/templates/{template_id}", response_model=MapTemplateOut)
async def get_template(template_id: str, db: AsyncSession = Depends(get_db)):
    template = await _require_template(template_id, db)
    return template


@router.patch("/templates/{template_id}", response_model=MapTemplateOut)
async def update_template(
    template_id: str,
    body: MapTemplateUpdate,
    db: AsyncSession = Depends(get_db),
):
    template = await _require_template(template_id, db)
    if body.name is not None:
        template.name = body.name
    if body.version is not None:
        template.version = body.version
    if body.description is not None:
        template.description = body.description
    if body.scope is not None:
        template.scope = body.scope
    await db.flush()
    await db.refresh(template)
    return template


@router.delete("/templates/{template_id}", status_code=204)
async def delete_template(template_id: str, db: AsyncSession = Depends(get_db)):
    template = await _require_template(template_id, db)
    if template.scope == "global":
        raise HTTPException(403, detail={
            "error": "FORBIDDEN",
            "message": "Global templates can only be deleted by an admin.",
        })
    await db.delete(template)


@router.post("/templates/{template_id}/promote", response_model=MapTemplateOut)
async def promote_template(template_id: str, db: AsyncSession = Depends(get_db)):
    """Promote a project-scoped template to global scope."""
    template = await _require_template(template_id, db)
    if template.scope == "global":
        raise HTTPException(409, detail={"error": "ALREADY_GLOBAL", "message": "Template is already global."})
    template.scope = "global"
    template.project_id = None
    await db.flush()
    await db.refresh(template)
    return template


@router.post("/projects/{project_id}/maps/load-template", response_model=TemplateLoadResult)
async def load_template(
    project_id: str,
    body: TemplateLoadRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Load a template into project maps.
    mode=replace: overwrites all maps.
    mode=merge: adds maps, skips source_tag+source_sub where approved=True exists.
    """
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    template = await db.get(MapTemplate, body.template_id)
    if not template:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Template not found."})

    # Add audit event
    db.add(AuditEvent(
        id=str(uuid.uuid4()),
        project_id=project_id,
        stage=2,
        level="info",
        tag="[templates]",
        message=f"Loading template '{template.name}' v{template.version} (mode={body.mode})",
    ))
    await db.flush()

    # Dispatch Celery task — runs synchronously here via .get() for immediate response
    task = load_template_task.apply_async(
        args=[project_id, body.template_id, body.mode],
        queue="analyze",
    )
    result = task.get(timeout=30)  # wait up to 30s — template loads are fast

    return TemplateLoadResult(**result)


# ── CSV Export ────────────────────────────────────────────────────────

_CSV_HEADERS = [
    "source_tag", "source_sub", "target_tag", "target_sub",
    "transform_type", "transform_fn", "preset_key", "delete_source", "notes",
]


@router.get("/templates/{template_id}/csv")
async def export_template_csv(template_id: str, db: AsyncSession = Depends(get_db)):
    """Download a template as CSV for editing in Google Sheets / Excel."""
    template = await _require_template(template_id, db)
    maps = template.maps or []

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_HEADERS, extrasaction="ignore")
    writer.writeheader()
    for m in maps:
        row = {k: (m.get(k, "") or "") for k in _CSV_HEADERS}
        if row.get("delete_source") in (True, "True", "true"):
            row["delete_source"] = "true"
        elif row.get("delete_source") in (False, "False", "false", ""):
            row["delete_source"] = "false"
        writer.writerow(row)

    filename = f"{template.name.replace(' ', '_')}_v{template.version}.csv"
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── CSV Import ────────────────────────────────────────────────────────

@router.post("/templates/import-csv", response_model=MapTemplateOut, status_code=201)
async def import_template_csv(
    file: UploadFile,
    name: str = Query(..., description="Template name"),
    request: Request = None,
    version: str = Query("1.0"),
    scope: str = Query("project"),
    description: str = Query(None),
    source_ils: str = Query(None),
    project_id: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Create a template from an uploaded CSV file (Google Sheets export, Excel CSV, etc.)."""
    content = await file.read()
    # Handle BOM from Google Sheets/Excel
    text = content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(text))
    maps = []
    seen = set()

    for row in reader:
        # Normalize keys (strip whitespace, lowercase)
        row = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}

        source_tag = row.get("source_tag", "").strip()
        target_tag = row.get("target_tag", "").strip()
        if not source_tag or not target_tag:
            continue

        source_sub = row.get("source_sub") or None
        target_sub = row.get("target_sub") or None

        # Deduplicate
        key = (source_tag, source_sub or "", target_tag, target_sub or "")
        if key in seen:
            continue
        seen.add(key)

        entry = {
            "source_tag": source_tag,
            "source_sub": source_sub if source_sub else None,
            "target_tag": target_tag,
            "target_sub": target_sub if target_sub else None,
            "transform_type": row.get("transform_type", "copy") or "copy",
            "transform_fn": row.get("transform_fn") or None,
            "preset_key": row.get("preset_key") or None,
            "delete_source": row.get("delete_source", "").lower() in ("true", "1", "yes"),
            "notes": row.get("notes") or None,
        }
        maps.append(entry)

    if not maps:
        raise HTTPException(400, detail="No valid mapping rows found in CSV. Ensure columns: source_tag, target_tag")

    # Get uploader info
    user_email = None
    user_id = getattr(request.state, "user_id", None) if request else None
    if user_id:
        from cerulean.models import User
        user = await db.get(User, user_id)
        if user:
            user_email = user.email

    template = MapTemplate(
        id=str(uuid.uuid4()),
        name=name,
        version=version,
        description=description,
        scope=scope,
        project_id=project_id if scope == "project" else None,
        source_ils=source_ils,
        ai_generated=False,
        reviewed=True,
        use_count=0,
        maps=maps,
        created_by=user_email,
    )
    db.add(template)
    await db.flush()
    await db.refresh(template)

    return template


async def _require_template(template_id: str, db: AsyncSession) -> MapTemplate:
    t = await db.get(MapTemplate, template_id)
    if not t:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Template not found."})
    return t
