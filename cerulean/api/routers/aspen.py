"""
cerulean/api/routers/aspen.py
─────────────────────────────────────────────────────────────────────────────
Stage 12 — Aspen Discovery Integration.

POST  /projects/{id}/aspen/turbo-migration  — start Turbo Migration
POST  /projects/{id}/aspen/turbo-reindex    — start Turbo Reindex
GET   /projects/{id}/aspen/counts           — get Koha + Aspen record counts
GET   /projects/{id}/aspen/status           — poll a background process
PATCH /projects/{id}/aspen/config           — set Aspen URL
"""

from datetime import datetime

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.database import get_db
from cerulean.models import PushManifest
from cerulean.tasks.aspen import (
    aspen_turbo_migration_task,
    aspen_turbo_reindex_task,
)

router = APIRouter(prefix="/projects", tags=["aspen"])


# ── Schemas ──────────────────────────────────────────────────────────────


class AspenConfigUpdate(BaseModel):
    aspen_url: str


class TurboMigrationRequest(BaseModel):
    worker_threads: int = 4
    batch_size: int = 500
    profile_name: str = "ils"


class TurboReindexRequest(BaseModel):
    worker_threads: int = 8
    clear_index: bool = False


class TurboResponse(BaseModel):
    task_id: str
    manifest_id: str
    message: str


# ── Config ───────────────────────────────────────────────────────────────


@router.patch("/{project_id}/aspen/config")
async def update_aspen_config(
    project_id: str,
    body: AspenConfigUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Set the Aspen Discovery URL for this project."""
    project = await require_project(project_id, db)
    project.aspen_url = body.aspen_url.rstrip("/")
    await db.flush()
    await audit_log(db, project_id, stage=12, level="info", tag="[config]",
                    message=f"Aspen URL set to {body.aspen_url}")
    return {"aspen_url": project.aspen_url}


# ── Counts / Preflight ───────────────────────────────────────────────────


@router.get("/{project_id}/aspen/counts")
async def aspen_counts(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Fetch record counts from both Koha and Aspen via Aspen's API."""
    project = await require_project(project_id, db)
    if not project.aspen_url:
        raise HTTPException(409, detail={"error": "NO_ASPEN_URL",
                                         "message": "Set aspen_url first."})
    url = f"{project.aspen_url}/API/SystemAPI"
    async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
        try:
            r = await client.get(url, params={"method": "getTurboCounts"})
        except httpx.HTTPError as exc:
            raise HTTPException(502, detail={"error": "ASPEN_UNREACHABLE",
                                             "message": str(exc)[:200]})
        if r.status_code >= 400:
            raise HTTPException(r.status_code, detail={"error": "ASPEN_ERROR",
                                                       "message": r.text[:500]})
        data = r.json()
        return data.get("result", data)


# ── Status Poll ──────────────────────────────────────────────────────────


@router.get("/{project_id}/aspen/status")
async def aspen_status(
    project_id: str,
    process_id: int = Query(..., description="Aspen backgroundProcessId"),
    db: AsyncSession = Depends(get_db),
):
    """Poll an Aspen background process for status + progress notes."""
    project = await require_project(project_id, db)
    if not project.aspen_url:
        raise HTTPException(409, detail={"error": "NO_ASPEN_URL"})
    url = f"{project.aspen_url}/API/SystemAPI"
    async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
        r = await client.get(url, params={
            "method": "getTurboStatus",
            "backgroundProcessId": process_id,
        })
        if r.status_code >= 400:
            raise HTTPException(r.status_code, detail=r.text[:500])
        return r.json().get("result", r.json())


# ── Turbo Migration ─────────────────────────────────────────────────────


@router.post("/{project_id}/aspen/turbo-migration", response_model=TurboResponse, status_code=202)
async def start_turbo_migration(
    project_id: str,
    body: TurboMigrationRequest,
    db: AsyncSession = Depends(get_db),
):
    """Start Aspen Turbo Migration — bulk extract Koha → Aspen DB."""
    project = await require_project(project_id, db)
    if not project.aspen_url:
        raise HTTPException(409, detail={"error": "NO_ASPEN_URL",
                                         "message": "Set aspen_url first."})

    manifest = PushManifest(
        project_id=project_id,
        task_type="aspen_migration",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    task = aspen_turbo_migration_task.apply_async(
        args=[project_id, manifest.id],
        kwargs={
            "worker_threads": body.worker_threads,
            "batch_size": body.batch_size,
            "profile_name": body.profile_name,
        },
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    await audit_log(db, project_id, stage=12, level="info", tag="[aspen]",
                    message=f"Turbo Migration dispatched (threads={body.worker_threads})")

    return TurboResponse(
        task_id=task.id,
        manifest_id=manifest.id,
        message="Aspen Turbo Migration dispatched.",
    )


# ── Turbo Reindex ───────────────────────────────────────────────────────


@router.post("/{project_id}/aspen/turbo-reindex", response_model=TurboResponse, status_code=202)
async def start_turbo_reindex(
    project_id: str,
    body: TurboReindexRequest,
    db: AsyncSession = Depends(get_db),
):
    """Start Aspen Turbo Reindex — parallel Solr indexing."""
    project = await require_project(project_id, db)
    if not project.aspen_url:
        raise HTTPException(409, detail={"error": "NO_ASPEN_URL",
                                         "message": "Set aspen_url first."})

    manifest = PushManifest(
        project_id=project_id,
        task_type="aspen_reindex",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    task = aspen_turbo_reindex_task.apply_async(
        args=[project_id, manifest.id],
        kwargs={
            "worker_threads": body.worker_threads,
            "clear_index": body.clear_index,
        },
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    await audit_log(db, project_id, stage=12, level="info", tag="[aspen]",
                    message=f"Turbo Reindex dispatched (threads={body.worker_threads})")

    return TurboResponse(
        task_id=task.id,
        manifest_id=manifest.id,
        message="Aspen Turbo Reindex dispatched.",
    )
