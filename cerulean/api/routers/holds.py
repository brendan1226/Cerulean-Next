"""
cerulean/api/routers/holds.py
─────────────────────────────────────────────────────────────────────────────
Step 11 — Holds & Circulation History.

POST   /projects/{id}/holds/upload           — upload holds or circ CSV
GET    /projects/{id}/holds/files             — list uploaded files
POST   /projects/{id}/holds/validate/{fid}    — validate CSV + resolve IDs
POST   /projects/{id}/holds/push/{fid}        — push holds to Koha
POST   /projects/{id}/circ/validate/{fid}     — validate circ CSV
POST   /projects/{id}/circ/generate-sql/{fid} — generate SQL for old_issues
POST   /projects/{id}/circ/push/{fid}         — push via plugin
GET    /projects/{id}/circ/download-sql       — download generated SQL
GET    /projects/{id}/holds/id-mappings       — view ID mapping counts
POST   /projects/{id}/holds/id-mappings/build — build mappings from Koha
"""

import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import HoldsFile, IdMapping, PushManifest

router = APIRouter(prefix="/projects", tags=["holds"])
settings = get_settings()


# ── Upload ───────────────────────────────────────────────────────────

@router.post("/{project_id}/holds/upload", status_code=201)
async def upload_holds_file(
    project_id: str,
    file: UploadFile,
    category: str = "holds",  # "holds" | "circ_history"
    db: AsyncSession = Depends(get_db),
):
    """Upload a holds or circ history CSV file."""
    await require_project(project_id, db)

    if category not in ("holds", "circ_history"):
        raise HTTPException(400, detail="category must be 'holds' or 'circ_history'")

    project_dir = Path(settings.data_root) / project_id / "holds"
    project_dir.mkdir(parents=True, exist_ok=True)

    file_id = str(uuid.uuid4())
    dest = project_dir / f"{file_id}.csv"
    content = await file.read()
    dest.write_bytes(content)

    holds_file = HoldsFile(
        id=file_id,
        project_id=project_id,
        filename=file.filename or "upload.csv",
        file_category=category,
        storage_path=str(dest),
    )
    db.add(holds_file)
    await db.flush()

    # Register task for Job Watcher
    from cerulean.api.routers.tasks import register_task

    if category == "holds":
        from cerulean.tasks.holds import validate_holds_task
        task = validate_holds_task.apply_async(args=[project_id, file_id], queue="push")
        register_task(project_id, task.id, "holds_validate")
    else:
        from cerulean.tasks.holds import validate_circ_task
        task = validate_circ_task.apply_async(args=[project_id, file_id], queue="push")
        register_task(project_id, task.id, "circ_validate")

    return {
        "file_id": file_id,
        "filename": file.filename,
        "category": category,
        "task_id": task.id,
        "message": f"{category} file uploaded. Validation started.",
    }


# ── List Files ───────────────────────────────────────────────────────

@router.get("/{project_id}/holds/files")
async def list_holds_files(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    await require_project(project_id, db)
    result = await db.execute(
        select(HoldsFile)
        .where(HoldsFile.project_id == project_id)
        .order_by(HoldsFile.uploaded_at.desc())
    )
    files = result.scalars().all()
    return [
        {
            "id": f.id,
            "filename": f.filename,
            "category": f.file_category,
            "row_count": f.row_count,
            "status": f.status,
            "validation_result": f.validation_result,
            "column_headers": f.column_headers,
            "uploaded_at": f.uploaded_at.isoformat() if f.uploaded_at else None,
        }
        for f in files
    ]


# ── Validate ─────────────────────────────────────────────────────────

@router.post("/{project_id}/holds/validate/{file_id}")
async def validate_holds(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Re-run validation on a holds file."""
    await require_project(project_id, db)
    hf = await db.get(HoldsFile, file_id)
    if not hf or hf.project_id != project_id:
        raise HTTPException(404, detail="File not found.")

    from cerulean.tasks.holds import validate_holds_task
    from cerulean.api.routers.tasks import register_task
    task = validate_holds_task.apply_async(args=[project_id, file_id], queue="push")
    register_task(project_id, task.id, "holds_validate")

    return {"task_id": task.id, "message": "Validation started."}


# ── Push Holds ───────────────────────────────────────────────────────

@router.post("/{project_id}/holds/push/{file_id}", status_code=202)
async def push_holds(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Push validated holds to Koha."""
    project = await require_project(project_id, db)
    hf = await db.get(HoldsFile, file_id)
    if not hf or hf.project_id != project_id:
        raise HTTPException(404, detail="File not found.")
    if hf.status != "validated":
        raise HTTPException(409, detail="File must be validated first.")
    if not project.koha_url:
        raise HTTPException(409, detail="Configure Koha URL in project settings first.")

    manifest = PushManifest(
        project_id=project_id,
        task_type="holds_push",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    from cerulean.tasks.holds import push_holds_task
    from cerulean.api.routers.tasks import register_task
    task = push_holds_task.apply_async(args=[project_id, file_id, manifest.id], queue="push")
    manifest.celery_task_id = task.id
    await db.flush()
    register_task(project_id, task.id, "holds_push")

    await audit_log(db, project_id, stage=11, level="info", tag="[holds]",
                    message=f"Holds push dispatched ({hf.row_count or '?'} rows)")

    return {"task_id": task.id, "manifest_id": manifest.id, "message": "Holds push started."}


# ── Circ Validate ────────────────────────────────────────────────────

@router.post("/{project_id}/circ/validate/{file_id}")
async def validate_circ(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    await require_project(project_id, db)
    hf = await db.get(HoldsFile, file_id)
    if not hf or hf.project_id != project_id:
        raise HTTPException(404, detail="File not found.")

    from cerulean.tasks.holds import validate_circ_task
    from cerulean.api.routers.tasks import register_task
    task = validate_circ_task.apply_async(args=[project_id, file_id], queue="push")
    register_task(project_id, task.id, "circ_validate")

    return {"task_id": task.id, "message": "Circ validation started."}


# ── Circ Generate SQL ────────────────────────────────────────────────

@router.post("/{project_id}/circ/generate-sql/{file_id}")
async def generate_circ_sql(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    await require_project(project_id, db)
    hf = await db.get(HoldsFile, file_id)
    if not hf or hf.project_id != project_id:
        raise HTTPException(404, detail="File not found.")

    from cerulean.tasks.holds import generate_circ_sql_task
    from cerulean.api.routers.tasks import register_task
    task = generate_circ_sql_task.apply_async(args=[project_id, file_id], queue="push")
    register_task(project_id, task.id, "circ_sql_generate")

    return {"task_id": task.id, "message": "SQL generation started."}


# ── Circ Push via Plugin ─────────────────────────────────────────────

@router.post("/{project_id}/circ/push/{file_id}", status_code=202)
async def push_circ(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    project = await require_project(project_id, db)
    hf = await db.get(HoldsFile, file_id)
    if not hf or hf.project_id != project_id:
        raise HTTPException(404, detail="File not found.")
    if not project.koha_url:
        raise HTTPException(409, detail="Configure Koha URL first.")

    manifest = PushManifest(
        project_id=project_id,
        task_type="circ_push",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    from cerulean.tasks.holds import push_circ_plugin_task
    from cerulean.api.routers.tasks import register_task
    task = push_circ_plugin_task.apply_async(args=[project_id, file_id, manifest.id], queue="push")
    manifest.celery_task_id = task.id
    await db.flush()
    register_task(project_id, task.id, "circ_push")

    return {"task_id": task.id, "manifest_id": manifest.id, "message": "Circ push started."}


# ── Circ Download SQL ────────────────────────────────────────────────

@router.get("/{project_id}/circ/download-sql")
async def download_circ_sql(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    await require_project(project_id, db)
    sql_path = Path(settings.data_root) / project_id / "circ_history.sql"
    if not sql_path.is_file():
        raise HTTPException(404, detail="SQL file not generated yet.")
    return FileResponse(
        path=str(sql_path),
        filename="circ_history.sql",
        media_type="text/sql",
    )


# ── ID Mappings ──────────────────────────────────────────────────────

@router.get("/{project_id}/holds/id-mappings")
async def get_id_mapping_stats(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get ID mapping counts by entity type."""
    await require_project(project_id, db)

    result = await db.execute(
        select(IdMapping.entity_type, func.count(IdMapping.id))
        .where(IdMapping.project_id == project_id)
        .group_by(IdMapping.entity_type)
    )
    counts = {row[0]: row[1] for row in result.all()}

    return {
        "bib_mappings": counts.get("bib", 0),
        "patron_mappings": counts.get("patron", 0),
        "item_mappings": counts.get("item", 0),
        "total": sum(counts.values()),
    }


@router.post("/{project_id}/holds/id-mappings/build")
async def build_id_mappings(
    project_id: str,
    entity_type: str = "patron",
    db: AsyncSession = Depends(get_db),
):
    """Retroactively build ID mappings by scanning Koha."""
    await require_project(project_id, db)

    from cerulean.tasks.holds import build_id_mappings_task
    from cerulean.api.routers.tasks import register_task
    task = build_id_mappings_task.apply_async(args=[project_id, entity_type], queue="push")
    register_task(project_id, task.id, f"build_{entity_type}_mappings")

    return {"task_id": task.id, "message": f"Building {entity_type} ID mappings from Koha."}


# ── Delete File ──────────────────────────────────────────────────────

@router.delete("/{project_id}/holds/files/{file_id}", status_code=204)
async def delete_holds_file(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    await require_project(project_id, db)
    hf = await db.get(HoldsFile, file_id)
    if not hf or hf.project_id != project_id:
        raise HTTPException(404, detail="File not found.")
    path = Path(hf.storage_path)
    if path.is_file():
        path.unlink()
    await db.delete(hf)
    await db.flush()
