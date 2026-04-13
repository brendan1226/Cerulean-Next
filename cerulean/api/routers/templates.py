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


# ── Google Sheets URL Import ──────────────────────────────────────────

@router.post("/templates/import-google-sheet", response_model=MapTemplateOut, status_code=201)
async def import_from_google_sheet(
    url: str = Query(..., description="Google Sheets URL"),
    name: str = Query(...),
    request: Request = None,
    version: str = Query("1.0"),
    scope: str = Query("project"),
    description: str = Query(None),
    source_ils: str = Query(None),
    project_id: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Import a template directly from a Google Sheets URL.

    The sheet must be shared as 'Anyone with the link can view'.
    Fetches the sheet as CSV via Google's export URL.
    """
    import httpx
    import re

    # Extract sheet ID from various Google Sheets URL formats
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', url)
    if not match:
        raise HTTPException(400, detail="Invalid Google Sheets URL. Expected: https://docs.google.com/spreadsheets/d/SHEET_ID/...")

    sheet_id = match.group(1)
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(export_url)
            if resp.status_code != 200:
                raise HTTPException(400, detail=f"Could not fetch sheet (HTTP {resp.status_code}). Is it shared as 'Anyone with the link'?")
            text = resp.text
    except httpx.RequestError as exc:
        raise HTTPException(400, detail=f"Failed to fetch Google Sheet: {str(exc)[:200]}")

    # Parse CSV — same logic as import-csv
    reader = csv.DictReader(io.StringIO(text))
    maps = []
    seen = set()

    for row in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}
        source_tag = row.get("source_tag", "").strip()
        target_tag = row.get("target_tag", "").strip()
        if not source_tag or not target_tag:
            continue
        source_sub = row.get("source_sub") or None
        target_sub = row.get("target_sub") or None
        key = (source_tag, source_sub or "", target_tag, target_sub or "")
        if key in seen:
            continue
        seen.add(key)
        maps.append({
            "source_tag": source_tag,
            "source_sub": source_sub if source_sub else None,
            "target_tag": target_tag,
            "target_sub": target_sub if target_sub else None,
            "transform_type": row.get("transform_type", "copy") or "copy",
            "transform_fn": row.get("transform_fn") or None,
            "preset_key": row.get("preset_key") or None,
            "delete_source": row.get("delete_source", "").lower() in ("true", "1", "yes"),
            "notes": row.get("notes") or None,
        })

    if not maps:
        raise HTTPException(400, detail="No valid mapping rows found in the Google Sheet. Ensure columns: source_tag, target_tag")

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
        description=description or f"Imported from Google Sheets",
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


# ── Sample Template Seed ─────────────────────────────────────────────

@router.post("/templates/seed-sample", response_model=MapTemplateOut, status_code=201)
async def seed_sample_template(db: AsyncSession = Depends(get_db)):
    """Create a sample Symphony→Koha template for testing and demonstration."""
    sample_maps = [
        {"source_tag": "001", "target_tag": "001", "transform_type": "copy", "notes": "Control number"},
        {"source_tag": "005", "target_tag": "005", "transform_type": "copy", "notes": "Date/time of last transaction"},
        {"source_tag": "008", "target_tag": "008", "transform_type": "copy", "notes": "Fixed-length data elements"},
        {"source_tag": "010", "source_sub": "$a", "target_tag": "010", "target_sub": "$a", "transform_type": "copy", "notes": "LCCN"},
        {"source_tag": "020", "source_sub": "$a", "target_tag": "020", "target_sub": "$a", "transform_type": "preset", "preset_key": "clean_isbn", "notes": "ISBN — cleaned"},
        {"source_tag": "035", "source_sub": "$a", "target_tag": "035", "target_sub": "$a", "transform_type": "copy", "notes": "System control number (OCLC)"},
        {"source_tag": "050", "source_sub": "$a", "target_tag": "050", "target_sub": "$a", "transform_type": "copy", "notes": "LC call number"},
        {"source_tag": "082", "source_sub": "$a", "target_tag": "082", "target_sub": "$a", "transform_type": "copy", "notes": "Dewey classification"},
        {"source_tag": "100", "source_sub": "$a", "target_tag": "100", "target_sub": "$a", "transform_type": "copy", "notes": "Main entry — personal name"},
        {"source_tag": "245", "source_sub": "$a", "target_tag": "245", "target_sub": "$a", "transform_type": "copy", "notes": "Title proper"},
        {"source_tag": "245", "source_sub": "$b", "target_tag": "245", "target_sub": "$b", "transform_type": "copy", "notes": "Subtitle"},
        {"source_tag": "245", "source_sub": "$c", "target_tag": "245", "target_sub": "$c", "transform_type": "copy", "notes": "Statement of responsibility"},
        {"source_tag": "246", "source_sub": "$a", "target_tag": "246", "target_sub": "$a", "transform_type": "copy", "notes": "Varying form of title"},
        {"source_tag": "250", "source_sub": "$a", "target_tag": "250", "target_sub": "$a", "transform_type": "copy", "notes": "Edition statement"},
        {"source_tag": "260", "source_sub": "$a", "target_tag": "264", "target_sub": "$a", "transform_type": "copy", "notes": "Place of publication → 264"},
        {"source_tag": "260", "source_sub": "$b", "target_tag": "264", "target_sub": "$b", "transform_type": "copy", "notes": "Publisher → 264"},
        {"source_tag": "260", "source_sub": "$c", "target_tag": "264", "target_sub": "$c", "transform_type": "preset", "preset_key": "extract_year", "notes": "Date of publication → year only"},
        {"source_tag": "300", "source_sub": "$a", "target_tag": "300", "target_sub": "$a", "transform_type": "copy", "notes": "Physical description — extent"},
        {"source_tag": "490", "source_sub": "$a", "target_tag": "490", "target_sub": "$a", "transform_type": "copy", "notes": "Series statement"},
        {"source_tag": "500", "source_sub": "$a", "target_tag": "500", "target_sub": "$a", "transform_type": "copy", "notes": "General note"},
        {"source_tag": "520", "source_sub": "$a", "target_tag": "520", "target_sub": "$a", "transform_type": "copy", "notes": "Summary"},
        {"source_tag": "600", "source_sub": "$a", "target_tag": "600", "target_sub": "$a", "transform_type": "copy", "notes": "Subject — personal name"},
        {"source_tag": "650", "source_sub": "$a", "target_tag": "650", "target_sub": "$a", "transform_type": "copy", "notes": "Subject — topical term"},
        {"source_tag": "651", "source_sub": "$a", "target_tag": "651", "target_sub": "$a", "transform_type": "copy", "notes": "Subject — geographic name"},
        {"source_tag": "700", "source_sub": "$a", "target_tag": "700", "target_sub": "$a", "transform_type": "copy", "notes": "Added entry — personal name"},
        {"source_tag": "852", "source_sub": "$a", "target_tag": "952", "target_sub": "$a", "transform_type": "copy", "delete_source": True, "notes": "Location → homebranch (MOVE)"},
        {"source_tag": "852", "source_sub": "$b", "target_tag": "952", "target_sub": "$b", "transform_type": "copy", "delete_source": True, "notes": "Sublocation → holdingbranch (MOVE)"},
        {"source_tag": "852", "source_sub": "$h", "target_tag": "952", "target_sub": "$o", "transform_type": "copy", "delete_source": True, "notes": "Call number → 952$o (MOVE)"},
        {"source_tag": "852", "source_sub": "$p", "target_tag": "952", "target_sub": "$p", "transform_type": "copy", "delete_source": True, "notes": "Barcode → 952$p (MOVE)"},
        {"source_tag": "852", "source_sub": "$t", "target_tag": "952", "target_sub": "$t", "transform_type": "copy", "notes": "Copy number"},
        {"source_tag": "999", "source_sub": "$a", "target_tag": "942", "target_sub": "$c", "transform_type": "lookup", "transform_fn": "{\"BOOK\":\"BK\",\"DVD\":\"DVD\",\"CD\":\"CD\",\"JBOOK\":\"JBK\",\"JDVD\":\"JDVD\",\"REF\":\"REF\",\"MAG\":\"CR\"}", "delete_source": True, "notes": "Item type lookup (Symphony → Koha)"},
        {"source_tag": "999", "source_sub": "$l", "target_tag": "952", "target_sub": "$c", "transform_type": "lookup", "transform_fn": "{\"ADULT\":\"ADULT\",\"CHILD\":\"CHILD\",\"YA\":\"YA\",\"REF\":\"REF\",\"MEDIA\":\"AV\"}", "notes": "Location code lookup"},
    ]

    template = MapTemplate(
        id=str(uuid.uuid4()),
        name="Symphony → Koha (Sample)",
        version="1.0",
        description="Sample template demonstrating a typical SirsiDynix Symphony to Koha migration mapping. Includes bibliographic fields, item field moves (852→952), item type lookups (999→942), and location code mappings.",
        scope="global",
        project_id=None,
        source_ils="SirsiDynix Symphony",
        ai_generated=False,
        reviewed=True,
        use_count=0,
        maps=sample_maps,
        created_by="system",
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
