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
    """Set per-task pause flag in Redis."""
    await require_project(project_id, db)
    _redis.set(f"cerulean:pause:task:{celery_task_id}", "1")
    return {"status": "paused"}


@router.post("/{project_id}/tasks/{celery_task_id}/resume")
async def resume_task(
    project_id: str,
    celery_task_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Clear per-task pause flag in Redis."""
    await require_project(project_id, db)
    _redis.delete(f"cerulean:pause:task:{celery_task_id}")
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
    _redis.delete(f"cerulean:pause:task:{celery_task_id}")

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


@router.get("/{project_id}/tasks/all")
async def list_all_tasks(
    project_id: str,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    """Return all tasks (running + recent completed) for the Job Watcher.

    Merges TransformManifest and PushManifest, sorted by started_at desc.
    Enriches running tasks with live Celery progress metadata.
    """
    await require_project(project_id, db)
    from sqlalchemy import or_, desc

    tasks: list[dict] = []

    # Transform manifests
    tm_rows = (
        await db.execute(
            select(TransformManifest)
            .where(TransformManifest.project_id == project_id)
            .order_by(desc(TransformManifest.started_at))
            .limit(limit)
        )
    ).scalars().all()

    for m in tm_rows:
        entry = _manifest_to_task_detail(m.celery_task_id, m.task_type, m)
        tasks.append(entry)

    # Push manifests
    pm_rows = (
        await db.execute(
            select(PushManifest)
            .where(PushManifest.project_id == project_id)
            .order_by(desc(PushManifest.started_at))
            .limit(limit)
        )
    ).scalars().all()

    for m in pm_rows:
        entry = _manifest_to_task_detail(m.celery_task_id, m.task_type, m)
        tasks.append(entry)

    # Sort by started_at descending
    tasks.sort(key=lambda t: t.get("started_at") or "", reverse=True)

    return tasks[:limit]


def _manifest_to_task_detail(celery_task_id, task_type, manifest) -> dict:
    """Build a detailed task info dict with metrics."""
    started = manifest.started_at
    completed = manifest.completed_at if hasattr(manifest, "completed_at") else None
    duration = None
    if started and completed:
        duration = int((completed - started).total_seconds())

    records_total = getattr(manifest, "records_total", None) or 0
    records_success = getattr(manifest, "records_success", None) or 0
    records_failed = getattr(manifest, "records_failed", None) or 0
    status = getattr(manifest, "status", "unknown")
    error_message = getattr(manifest, "error_message", None)
    result_data = getattr(manifest, "result_data", None)

    # Extract records from result_data if the manifest columns are empty
    # (TransformManifest stores totals in result_data JSONB, not in columns)
    if result_data and isinstance(result_data, dict):
        if not records_total:
            records_total = result_data.get("total_records", 0) or result_data.get("records_total", 0) or 0
        if not records_success:
            records_success = result_data.get("records_success", 0) or result_data.get("num_added", 0) or 0
        if not records_failed:
            records_failed = result_data.get("records_failed", 0) or result_data.get("num_errors", 0) or 0

    # For transform manifests, check total_records column directly
    if hasattr(manifest, "total_records") and manifest.total_records and not records_total:
        records_total = manifest.total_records

    # Check if task is paused
    is_paused = False
    if celery_task_id:
        try:
            is_paused = bool(_redis.get(f"cerulean:pause:task:{celery_task_id}"))
        except Exception:
            pass

    # Calculate rate
    rate = 0
    if duration and duration > 0 and records_total > 0:
        rate = round(records_total / duration, 1)

    info = {
        "task_id": celery_task_id,
        "task_type": task_type,
        "status": status,
        "started_at": started.isoformat() if started else None,
        "completed_at": completed.isoformat() if completed else None,
        "duration_seconds": duration,
        "records_total": records_total,
        "records_success": records_success,
        "records_failed": records_failed,
        "records_per_second": rate,
        "error_message": error_message[:200] if error_message else None,
        "progress": {},
        "eta_seconds": None,
        "is_paused": is_paused,
        "can_cancel": status == "running",
        "can_pause": status == "running" and not is_paused,
        "can_resume": is_paused,
    }

    # Enrich running tasks with live Celery state
    if status == "running" and celery_task_id:
        result = AsyncResult(celery_task_id, app=celery_app)
        info["celery_state"] = result.state or "PENDING"
        if result.state in ("PROGRESS", "PAUSED") and isinstance(result.info, dict):
            prog = result.info
            info["progress"] = prog
            done = prog.get("records_done", 0)
            total = prog.get("records_total", 0)
            info["records_total"] = total or info["records_total"]

            # Calculate live rate and ETA
            if started and done > 0:
                elapsed = (datetime.utcnow() - started).total_seconds()
                if elapsed > 0:
                    live_rate = round(done / elapsed, 1)
                    info["records_per_second"] = live_rate
                    if total > done and live_rate > 0:
                        info["eta_seconds"] = int((total - done) / live_rate)

    return info


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
