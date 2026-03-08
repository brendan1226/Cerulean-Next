"""
cerulean/api/routers/push.py
─────────────────────────────────────────────────────────────────────────────
Stage 5 — Push to Koha API endpoints.

POST  /projects/{id}/push/preflight   — dispatch preflight check
POST  /projects/{id}/push/start       — dispatch selected push tasks
GET   /projects/{id}/push/status      — poll Celery task status
GET   /projects/{id}/push/manifests   — list push manifests
GET   /projects/{id}/push/log         — alias for manifests (by started_at desc)
"""

import uuid
from datetime import datetime
from pathlib import Path

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import AuditEvent, Project, PushManifest
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

router = APIRouter(prefix="/projects", tags=["push"])
settings = get_settings()


# ── Endpoints ────────────────────────────────────────────────────────────


@router.post("/{project_id}/push/preflight", response_model=PreflightResponse, status_code=202)
async def preflight(
    project_id: str,
    body: PreflightRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch preflight check. Precondition: koha_url must be set."""
    project = await _require_project(project_id, db)

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
    await _log(db, project_id, stage=5, level="info", tag="[preflight]",
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
    """Dispatch selected push tasks. Precondition: merged file exists."""
    project = await _require_project(project_id, db)

    # Check that merged file exists
    project_dir = Path(settings.data_root) / project_id
    has_merged = (project_dir / "merged_deduped.mrc").is_file() or (project_dir / "merged.mrc").is_file()
    if not has_merged and body.push_bibs:
        raise HTTPException(409, detail={
            "error": "NO_MERGED_FILE",
            "message": "No merged MARC file found. Complete merge (Stage 3) first.",
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

    await _log(db, project_id, stage=5, level="info", tag="[push]",
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
    await _require_project(project_id, db)

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
    await _require_project(project_id, db)

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
    await _require_project(project_id, db)

    result = await db.execute(
        select(PushManifest)
        .where(PushManifest.project_id == project_id)
        .order_by(PushManifest.started_at.desc())
    )
    return result.scalars().all()


# ── Helpers ──────────────────────────────────────────────────────────────


async def _require_project(project_id: str, db: AsyncSession) -> Project:
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})
    return project


async def _log(db: AsyncSession, project_id: str, stage: int, level: str, tag: str, message: str) -> None:
    event = AuditEvent(
        id=str(uuid.uuid4()),
        project_id=project_id,
        stage=stage,
        level=level,
        tag=tag,
        message=message,
    )
    db.add(event)
