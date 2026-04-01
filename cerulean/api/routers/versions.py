"""
cerulean/api/routers/versions.py
─────────────────────────────────────────────────────────────────────────────
Versioning API — create, list, inspect, diff migration version snapshots.

POST   /projects/{id}/versions/create       — dispatch snapshot creation
GET    /projects/{id}/versions              — list versions
GET    /projects/{id}/versions/{vid}        — version detail
PATCH  /projects/{id}/versions/{vid}        — update label
DELETE /projects/{id}/versions/{vid}        — delete version + snapshot file
POST   /projects/{id}/versions/diff         — dispatch diff computation
GET    /projects/{id}/versions/diff/status  — poll diff task
"""

import os

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.database import get_db
from cerulean.models import MigrationVersion
from cerulean.schemas.versions import (
    CreateVersionRequest,
    CreateVersionResponse,
    MigrationVersionOut,
    UpdateVersionRequest,
    VersionDiffRequest,
    VersionDiffResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.versioning import (
    compute_version_diff_task,
    create_version_snapshot_task,
)

router = APIRouter(prefix="/projects", tags=["versions"])


# ── Create ───────────────────────────────────────────────────────────


@router.post(
    "/{project_id}/versions/create",
    response_model=CreateVersionResponse,
    status_code=202,
)
async def create_version(
    project_id: str,
    body: CreateVersionRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch a version snapshot creation task."""
    await require_project(project_id, db)

    await audit_log(
        db, project_id, stage=4, level="info", tag="[versions]",
        message=f"Version snapshot dispatched (data_type={body.data_type})",
    )
    await db.flush()

    task = create_version_snapshot_task.apply_async(
        args=[project_id],
        kwargs={"data_type": body.data_type, "label": body.label},
        queue="analyze",
    )

    return CreateVersionResponse(task_id=task.id, message="Version snapshot creation started.")


# ── List / Detail ────────────────────────────────────────────────────


@router.get("/{project_id}/versions", response_model=list[MigrationVersionOut])
async def list_versions(
    project_id: str,
    data_type: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """List versions for a project, ordered by version_number descending."""
    await require_project(project_id, db)

    q = select(MigrationVersion).where(MigrationVersion.project_id == project_id)
    if data_type:
        q = q.where(MigrationVersion.data_type == data_type)
    q = q.order_by(MigrationVersion.version_number.desc())

    rows = (await db.execute(q)).scalars().all()
    return rows


@router.get("/{project_id}/versions/{version_id}", response_model=MigrationVersionOut)
async def get_version(
    project_id: str,
    version_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get a single version's detail."""
    await require_project(project_id, db)
    version = await db.get(MigrationVersion, version_id)
    if not version or version.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Version not found."})
    return version


# ── Update ───────────────────────────────────────────────────────────


@router.patch("/{project_id}/versions/{version_id}", response_model=MigrationVersionOut)
async def update_version(
    project_id: str,
    version_id: str,
    body: UpdateVersionRequest,
    db: AsyncSession = Depends(get_db),
):
    """Update a version's label."""
    await require_project(project_id, db)
    version = await db.get(MigrationVersion, version_id)
    if not version or version.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Version not found."})

    if body.label is not None:
        version.label = body.label

    await db.flush()
    await db.refresh(version)

    await audit_log(
        db, project_id, stage=4, level="info", tag="[versions]",
        message=f"Version {version.version_number} label updated to '{body.label}'",
    )

    return version


# ── Delete ───────────────────────────────────────────────────────────


@router.delete("/{project_id}/versions/{version_id}", status_code=204)
async def delete_version(
    project_id: str,
    version_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a version row and its snapshot file from disk."""
    await require_project(project_id, db)
    version = await db.get(MigrationVersion, version_id)
    if not version or version.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Version not found."})

    snapshot_path = version.snapshot_path

    await db.delete(version)
    await db.flush()

    # Best-effort removal of the snapshot file
    if snapshot_path and os.path.exists(snapshot_path):
        os.remove(snapshot_path)

    await audit_log(
        db, project_id, stage=4, level="info", tag="[versions]",
        message=f"Version {version_id} deleted (snapshot: {snapshot_path})",
    )

    return None


# ── Diff ─────────────────────────────────────────────────────────────


@router.post(
    "/{project_id}/versions/diff",
    response_model=VersionDiffResponse,
    status_code=202,
)
async def start_version_diff(
    project_id: str,
    body: VersionDiffRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch a diff computation between two versions."""
    await require_project(project_id, db)

    await audit_log(
        db, project_id, stage=4, level="info", tag="[versions]",
        message=f"Version diff dispatched: {body.version_a} vs {body.version_b} ({body.data_type})",
    )
    await db.flush()

    task = compute_version_diff_task.apply_async(
        args=[project_id, body.data_type, body.version_a, body.version_b],
        queue="analyze",
    )

    return VersionDiffResponse(task_id=task.id, message="Version diff computation started.")


@router.get("/{project_id}/versions/diff/status")
async def version_diff_status(
    project_id: str,
    task_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Poll the status of a version diff task."""
    await require_project(project_id, db)
    ar = AsyncResult(task_id, app=celery_app)
    state = ar.state
    progress = ar.info if state == "PROGRESS" else None
    result = ar.result if state == "SUCCESS" else None
    error = str(ar.info) if state == "FAILURE" else None
    return {
        "task_id": task_id,
        "state": state,
        "progress": progress,
        "result": result,
        "error": error,
    }
