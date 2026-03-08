"""
cerulean/api/routers/transform.py
─────────────────────────────────────────────────────────────────────────────
POST /projects/{id}/transform/start      — dispatch transform_pipeline_task
POST /projects/{id}/transform/merge      — dispatch merge_pipeline_task
GET  /projects/{id}/transform/status     — poll Celery task status
GET  /projects/{id}/transform/manifest   — list transform/merge manifests
"""

import uuid
from datetime import datetime

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.database import get_db
from cerulean.models import AuditEvent, FieldMap, MARCFile, Project, TransformManifest
from cerulean.schemas.transform import (
    MergeStartRequest,
    MergeStartResponse,
    TransformManifestOut,
    TransformStartRequest,
    TransformStartResponse,
    TransformStatusResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.transform import merge_pipeline_task, transform_pipeline_task

router = APIRouter(prefix="/projects", tags=["transform"])


# ── Endpoints ─────────────────────────────────────────────────────────


@router.post("/{project_id}/transform/start", response_model=TransformStartResponse, status_code=202)
async def start_transform(
    project_id: str,
    body: TransformStartRequest,
    db: AsyncSession = Depends(get_db),
):
    """Validate preconditions and dispatch transform_pipeline_task."""
    await _require_project(project_id, db)

    # Must have at least one approved map
    result = await db.execute(
        select(FieldMap)
        .where(FieldMap.project_id == project_id, FieldMap.approved == True)  # noqa: E712
        .limit(1)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(409, detail={
            "error": "NO_APPROVED_MAPS",
            "message": "No approved field maps. Complete Stage 2 first.",
        })

    # Must have indexed files
    files_q = select(MARCFile).where(
        MARCFile.project_id == project_id, MARCFile.status == "indexed",
    )
    if body.file_ids:
        files_q = files_q.where(MARCFile.id.in_(body.file_ids))
    result = await db.execute(files_q.limit(1))
    if not result.scalar_one_or_none():
        raise HTTPException(409, detail={
            "error": "NO_INDEXED_FILES",
            "message": "No indexed MARC files found.",
        })

    # Create manifest
    manifest_id = str(uuid.uuid4())
    manifest = TransformManifest(
        id=manifest_id,
        project_id=project_id,
        task_type="transform",
        status="running",
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await _log(db, project_id, stage=3, level="info", tag="[transform]",
               message="Transform pipeline dispatched")
    await db.flush()

    # Dispatch
    task = transform_pipeline_task.apply_async(
        args=[project_id, manifest_id],
        kwargs={"file_ids": body.file_ids, "dry_run": body.dry_run},
        queue="transform",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    return TransformStartResponse(
        task_id=task.id, manifest_id=manifest_id,
        message="Transform pipeline started.",
    )


@router.post("/{project_id}/transform/merge", response_model=MergeStartResponse, status_code=202)
async def start_merge(
    project_id: str,
    body: MergeStartRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch merge_pipeline_task."""
    await _require_project(project_id, db)

    manifest_id = str(uuid.uuid4())
    manifest = TransformManifest(
        id=manifest_id,
        project_id=project_id,
        task_type="merge",
        status="running",
        items_csv_path=body.items_csv_path,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await _log(db, project_id, stage=3, level="info", tag="[merge]",
               message="Merge pipeline dispatched")
    await db.flush()

    task = merge_pipeline_task.apply_async(
        args=[project_id, manifest_id],
        kwargs={
            "file_ids": body.file_ids,
            "items_csv_path": body.items_csv_path,
            "items_csv_match_tag": body.items_csv_match_tag,
            "items_csv_key_column": body.items_csv_key_column,
        },
        queue="transform",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    return MergeStartResponse(
        task_id=task.id, manifest_id=manifest_id,
        message="Merge pipeline started.",
    )


@router.get("/{project_id}/transform/status", response_model=TransformStatusResponse)
async def transform_status(
    project_id: str,
    task_id: str | None = Query(None, description="Celery task_id to poll"),
    db: AsyncSession = Depends(get_db),
):
    """Poll Celery task status. Falls back to most recent manifest if no task_id given."""
    await _require_project(project_id, db)

    if not task_id:
        result = await db.execute(
            select(TransformManifest)
            .where(TransformManifest.project_id == project_id)
            .order_by(TransformManifest.started_at.desc())
            .limit(1)
        )
        manifest = result.scalar_one_or_none()
        if not manifest or not manifest.celery_task_id:
            return TransformStatusResponse(
                task_id=None, state="IDLE", progress=None, result=None, error=None,
            )
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

    return TransformStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


@router.get("/{project_id}/transform/manifest", response_model=list[TransformManifestOut])
async def list_manifests(
    project_id: str,
    task_type: str | None = Query(None, description="Filter: transform | merge"),
    db: AsyncSession = Depends(get_db),
):
    """List all transform/merge manifests for a project."""
    await _require_project(project_id, db)

    q = select(TransformManifest).where(TransformManifest.project_id == project_id)
    if task_type:
        q = q.where(TransformManifest.task_type == task_type)
    q = q.order_by(TransformManifest.started_at.desc())

    result = await db.execute(q)
    return result.scalars().all()


# ── Helpers ────────────────────────────────────────────────────────────


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
