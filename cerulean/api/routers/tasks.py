"""
cerulean/api/routers/tasks.py
─────────────────────────────────────────────────────────────────────────────
Generic task control endpoints (pause / resume / cancel / list active).

GET   /projects/{id}/tasks/active            — list running tasks
POST  /projects/{id}/tasks/{task_id}/pause   — pause a running task
POST  /projects/{id}/tasks/{task_id}/resume  — resume a paused task
POST  /projects/{id}/tasks/{task_id}/cancel  — cancel / revoke a task
"""

from datetime import datetime

import redis
from celery.result import AsyncResult
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import PushManifest, TransformManifest
from cerulean.tasks.celery_app import celery_app

settings = get_settings()
_redis = redis.from_url(settings.redis_url)

router = APIRouter(prefix="/projects", tags=["tasks"])


@router.get("/{project_id}/tasks/active")
async def list_active_tasks(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return all running/started tasks for a project.

    Queries both TransformManifest and PushManifest where status='running',
    enriches each with live Celery AsyncResult state + progress metadata.
    """
    await require_project(project_id, db)

    tasks: list[dict] = []

    # Transform / merge manifests
    tm_rows = (
        await db.execute(
            select(TransformManifest).where(
                TransformManifest.project_id == project_id,
                TransformManifest.status == "running",
            )
        )
    ).scalars().all()

    for m in tm_rows:
        entry = _manifest_to_task_info(m.celery_task_id, m.task_type, m.started_at)
        tasks.append(entry)

    # Push manifests
    pm_rows = (
        await db.execute(
            select(PushManifest).where(
                PushManifest.project_id == project_id,
                PushManifest.status == "running",
            )
        )
    ).scalars().all()

    for m in pm_rows:
        entry = _manifest_to_task_info(m.celery_task_id, m.task_type, m.started_at)
        tasks.append(entry)

    return tasks


@router.post("/{project_id}/tasks/{celery_task_id}/pause")
async def pause_task(
    project_id: str,
    celery_task_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Set project-level pause flag in Redis."""
    await require_project(project_id, db)
    _redis.set(f"cerulean:pause:{project_id}", "1")
    return {"status": "paused"}


@router.post("/{project_id}/tasks/{celery_task_id}/resume")
async def resume_task(
    project_id: str,
    celery_task_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Clear project-level pause flag in Redis."""
    await require_project(project_id, db)
    _redis.delete(f"cerulean:pause:{project_id}")
    return {"status": "resumed"}


@router.post("/{project_id}/tasks/{celery_task_id}/cancel")
async def cancel_task(
    project_id: str,
    celery_task_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Revoke a Celery task, clear pause flag, mark manifest as cancelled."""
    await require_project(project_id, db)

    # Revoke the Celery task
    celery_app.control.revoke(celery_task_id, terminate=True, signal="SIGTERM")

    # Clear any pause flag
    _redis.delete(f"cerulean:pause:{project_id}")

    # Update TransformManifest if it matches
    tm = (
        await db.execute(
            select(TransformManifest).where(
                TransformManifest.celery_task_id == celery_task_id,
            )
        )
    ).scalars().first()
    if tm:
        tm.status = "cancelled"
        tm.completed_at = datetime.utcnow()

    # Update PushManifest if it matches
    pm = (
        await db.execute(
            select(PushManifest).where(
                PushManifest.celery_task_id == celery_task_id,
            )
        )
    ).scalars().first()
    if pm:
        pm.status = "cancelled"
        pm.completed_at = datetime.utcnow()

    await db.commit()
    return {"status": "cancelled"}


def _manifest_to_task_info(
    celery_task_id: str | None,
    task_type: str,
    started_at: datetime | None,
) -> dict:
    """Build a task info dict, enriching with live Celery state."""
    info: dict = {
        "task_id": celery_task_id,
        "task_type": task_type,
        "state": "PENDING",
        "progress": {},
        "started_at": started_at.isoformat() if started_at else None,
    }

    if celery_task_id:
        result = AsyncResult(celery_task_id, app=celery_app)
        info["state"] = result.state or "PENDING"
        if result.state in ("PROGRESS", "PAUSED") and isinstance(result.info, dict):
            info["progress"] = result.info

    return info
