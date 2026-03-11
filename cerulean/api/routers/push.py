"""
cerulean/api/routers/push.py
─────────────────────────────────────────────────────────────────────────────
Stage 7 — Push to Koha API endpoints.

POST  /projects/{id}/push/preflight   — dispatch preflight check
POST  /projects/{id}/push/start       — dispatch selected push tasks
GET   /projects/{id}/push/status      — poll Celery task status
GET   /projects/{id}/push/manifests   — list push manifests
GET   /projects/{id}/push/log         — alias for manifests (by started_at desc)
GET   /projects/{id}/push/files       — list push-ready MARC files
GET   /projects/{id}/push/files/download — download a push-ready file
GET   /projects/{id}/push/files/preview  — preview records from a push-ready file
"""

import os
from datetime import datetime
from pathlib import Path

import pymarc
from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import PushManifest
from cerulean.schemas.push import (
    PreflightRequest,
    PreflightResponse,
    PushManifestOut,
    PushStartRequest,
    PushStartResponse,
    PushStatusResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.push import (
    es_reindex_task,
    push_bulkmarc_task,
    push_circ_task,
    push_holds_task,
    push_patrons_task,
    push_preflight_task,
)
from cerulean.utils.marc import record_to_dict

settings = get_settings()
router = APIRouter(prefix="/projects", tags=["push"])


# ── Endpoints ────────────────────────────────────────────────────────────


@router.post("/{project_id}/push/preflight", response_model=PreflightResponse, status_code=202)
async def preflight(
    project_id: str,
    body: PreflightRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch preflight check. Precondition: koha_url must be set."""
    project = await require_project(project_id, db)

    if not project.koha_url:
        raise HTTPException(409, detail={
            "error": "NO_KOHA_URL",
            "message": "Set koha_url on the project before running preflight.",
        })

    manifest = PushManifest(
        project_id=project_id,
        task_type="preflight",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await audit_log(db, project_id, stage=7, level="info", tag="[preflight]",
                    message="Preflight check dispatched")
    await db.flush()
    await db.refresh(manifest)

    task = push_preflight_task.apply_async(
        args=[project_id, manifest.id],
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    return PreflightResponse(task_id=task.id, message="Preflight check started.")


@router.post("/{project_id}/push/start", response_model=PushStartResponse, status_code=202)
async def start_push(
    project_id: str,
    body: PushStartRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch selected push tasks. Precondition: MARC output files exist."""
    project = await require_project(project_id, db)

    # Check that transformed or merged files exist
    project_dir = Path(settings.data_root) / project_id
    has_output = (
        (project_dir / "Biblios-mapped-items.mrc").is_file()
        or (project_dir / "merged_deduped.mrc").is_file()
        or (project_dir / "merged.mrc").is_file()
        or (
            (project_dir / "transformed").is_dir()
            and any((project_dir / "transformed").glob("*_transformed.mrc"))
        )
    )
    if not has_output and body.push_bibs:
        raise HTTPException(409, detail={
            "error": "NO_OUTPUT_FILES",
            "message": "No transformed or merged MARC files found. Complete Stage 3 first.",
        })

    task_ids: dict[str, str] = {}

    # Map of task_type → (task_func, needs_dry_run)
    tasks_to_dispatch: list[tuple[str, object, bool]] = []
    if body.push_bibs:
        tasks_to_dispatch.append(("bulkmarc", push_bulkmarc_task, True))
    if body.push_patrons:
        tasks_to_dispatch.append(("patrons", push_patrons_task, True))
    if body.push_holds:
        tasks_to_dispatch.append(("holds", push_holds_task, True))
    if body.push_circ:
        tasks_to_dispatch.append(("circ", push_circ_task, True))
    if body.reindex:
        tasks_to_dispatch.append(("reindex", es_reindex_task, False))

    if not tasks_to_dispatch:
        raise HTTPException(400, detail={
            "error": "NO_TASKS_SELECTED",
            "message": "Select at least one push task to start.",
        })

    for task_type, task_func, needs_dry_run in tasks_to_dispatch:
        manifest = PushManifest(
            project_id=project_id,
            task_type=task_type,
            status="running",
            dry_run=body.dry_run if needs_dry_run else False,
            started_at=datetime.utcnow(),
        )
        db.add(manifest)
        await db.flush()
        await db.refresh(manifest)

        kwargs = {"project_id": project_id, "manifest_id": manifest.id}
        if needs_dry_run:
            kwargs["dry_run"] = body.dry_run

        task = task_func.apply_async(
            args=[project_id, manifest.id],
            kwargs={"dry_run": body.dry_run} if needs_dry_run else {},
            queue="push",
        )
        manifest.celery_task_id = task.id
        task_ids[task_type] = task.id

    await audit_log(db, project_id, stage=7, level="info", tag="[push]",
                    message=f"Push started: {', '.join(task_ids.keys())} (dry_run={body.dry_run})")
    await db.flush()

    return PushStartResponse(
        task_ids=task_ids,
        message=f"Push tasks dispatched: {', '.join(task_ids.keys())}.",
    )


@router.get("/{project_id}/push/status", response_model=PushStatusResponse)
async def push_status(
    project_id: str,
    task_id: str | None = Query(None, description="Celery task_id to poll"),
    db: AsyncSession = Depends(get_db),
):
    """Poll Celery task status."""
    await require_project(project_id, db)

    if not task_id:
        # Find most recent manifest
        result = await db.execute(
            select(PushManifest)
            .where(PushManifest.project_id == project_id)
            .order_by(PushManifest.started_at.desc())
            .limit(1)
        )
        manifest = result.scalar_one_or_none()
        if not manifest or not manifest.celery_task_id:
            return PushStatusResponse(task_id=None, state="IDLE")
        task_id = manifest.celery_task_id

    async_result = AsyncResult(task_id, app=celery_app)
    state = async_result.state
    progress = None
    result_data = None
    error = None

    if state == "PROGRESS":
        progress = async_result.info
    elif state == "SUCCESS":
        result_data = async_result.result
    elif state == "FAILURE":
        error = str(async_result.info)

    return PushStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


@router.get("/{project_id}/push/manifests", response_model=list[PushManifestOut])
async def list_manifests(
    project_id: str,
    task_type: str | None = Query(None, description="Filter: preflight|bulkmarc|patrons|holds|circ|reindex"),
    db: AsyncSession = Depends(get_db),
):
    """List all push manifests for a project."""
    await require_project(project_id, db)

    q = select(PushManifest).where(PushManifest.project_id == project_id)
    if task_type:
        q = q.where(PushManifest.task_type == task_type)
    q = q.order_by(PushManifest.started_at.desc())

    result = await db.execute(q)
    return result.scalars().all()


@router.get("/{project_id}/push/log", response_model=list[PushManifestOut])
async def push_log(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Alias — list manifests ordered by started_at desc."""
    await require_project(project_id, db)

    result = await db.execute(
        select(PushManifest)
        .where(PushManifest.project_id == project_id)
        .order_by(PushManifest.started_at.desc())
    )
    return result.scalars().all()


# ── File review endpoints ────────────────────────────────────────────────


def _list_push_files(project_id: str) -> list[dict]:
    """Return push-ready files: best MARC file + controlled value CSVs."""
    project_dir = Path(settings.data_root) / project_id
    files: list[dict] = []

    # Find the single best MARC file (highest priority wins)
    source_labels = {
        "Biblios-mapped-items.mrc": "Items",
        "merged_deduped.mrc": "Deduped",
        "merged.mrc": "Merged",
    }
    best_marc = None
    for name in ["Biblios-mapped-items.mrc", "merged_deduped.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            best_marc = candidate
            break

    if not best_marc:
        # Fall back to transformed files
        transformed_dir = project_dir / "transformed"
        if transformed_dir.is_dir():
            transformed = sorted(transformed_dir.glob("*_transformed.mrc"))
            if transformed:
                best_marc = transformed[0]

    if best_marc:
        stat = best_marc.stat()
        files.append({
            "filename": best_marc.name,
            "path": str(best_marc),
            "size": stat.st_size,
            "modified": datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z",
            "source": source_labels.get(best_marc.name, "Transformed"),
            "type": "marc",
        })

    # Include controlled value CSVs from items stage
    items_dir = project_dir / "items"
    if items_dir.is_dir():
        for csv_path in sorted(items_dir.glob("items_*.csv")):
            stat = csv_path.stat()
            cat = csv_path.stem.replace("items_", "")
            files.append({
                "filename": csv_path.name,
                "path": str(csv_path),
                "size": stat.st_size,
                "modified": datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z",
                "source": cat,
                "type": "controlled_values",
            })

    return files


@router.get("/{project_id}/push/files")
async def list_push_files(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List MARC files available for push, with sizes and record counts."""
    await require_project(project_id, db)
    files = _list_push_files(project_id)

    # Quick record count per file
    for f in files:
        try:
            count = 0
            with open(f["path"], "rb") as fh:
                reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True,
                                            utf8_handling="replace")
                for rec in reader:
                    if rec is not None:
                        count += 1
            f["record_count"] = count
        except Exception:
            f["record_count"] = None

    # Don't expose internal paths to the frontend
    for f in files:
        del f["path"]

    return {"files": files}


@router.get("/{project_id}/push/files/download")
async def download_push_file(
    project_id: str,
    filename: str = Query(..., description="Filename to download"),
    db: AsyncSession = Depends(get_db),
):
    """Download a push-ready MARC file."""
    await require_project(project_id, db)

    # Resolve safely — only allow known filenames
    files = _list_push_files(project_id)
    match = next((f for f in files if f["filename"] == filename), None)
    if not match or not os.path.isfile(match["path"]):
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": f"File '{filename}' not found."})

    return FileResponse(
        match["path"],
        media_type="application/marc",
        filename=filename,
    )


@router.get("/{project_id}/push/files/preview")
async def preview_push_file(
    project_id: str,
    filename: str = Query(..., description="Filename to preview"),
    record_index: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Preview a record from a push-ready MARC file."""
    await require_project(project_id, db)

    files = _list_push_files(project_id)
    match = next((f for f in files if f["filename"] == filename), None)
    if not match or not os.path.isfile(match["path"]):
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": f"File '{filename}' not found."})

    total = 0
    record_data = None
    try:
        with open(match["path"], "rb") as fh:
            reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True,
                                        utf8_handling="replace")
            valid_idx = 0
            for rec in reader:
                if rec is None:
                    continue
                if valid_idx == record_index:
                    record_data = record_to_dict(rec, record_index)
                valid_idx += 1
    except Exception as exc:
        raise HTTPException(500, detail={"error": "READ_ERROR", "message": str(exc)})

    total = valid_idx
    if not record_data:
        raise HTTPException(404, detail={
            "error": "RECORD_NOT_FOUND",
            "message": f"No record at index {record_index} (file has {total} records).",
        })

    return {
        "filename": filename,
        "record_index": record_index,
        "total_records": total,
        "record": record_data,
    }
