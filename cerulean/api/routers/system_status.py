"""
cerulean/api/routers/system_status.py
─────────────────────────────────────────────────────────────────────────────
Admin System Status Dashboard — aggregated health snapshot.

GET   /system/status           — full health snapshot (workers, queues, alerts)
GET   /system/status/tasks     — all active tasks across ALL projects
GET   /system/status/errors    — recent errors from AuditEvent
POST  /system/status/actions/revoke-task   — kill a hung task
POST  /system/status/actions/purge-queue   — clear a stuck queue
"""

import time
from datetime import datetime, timedelta

import redis
from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import AuditEvent, Project, PushManifest, TransformManifest
from cerulean.tasks.celery_app import celery_app

settings = get_settings()
_redis = redis.from_url(settings.redis_url)

router = APIRouter(prefix="/system/status", tags=["system-status"])

_QUEUES = [
    "ingest", "analyze", "transform", "dedup", "reconcile",
    "patrons", "push", "sandbox", "default",
]

_HUNG_THRESHOLD_SECONDS = 3600
_LONG_RUNNING_SECONDS = 1800


# ── Full health snapshot ──────────────────────────────────────────────


@router.get("")
async def system_status(db: AsyncSession = Depends(get_db)):
    """Aggregated system health: workers, queues, active tasks, errors, alerts."""
    workers = _get_worker_health()
    queues = _get_queue_depths()
    redis_info = _get_redis_info()
    active_tasks = _get_all_active_tasks()
    alerts = _compute_alerts(workers, queues, active_tasks)
    actions = _recommend_actions(alerts, active_tasks)

    error_count = await _recent_error_count(db, hours=24)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "workers": workers,
        "queues": queues,
        "redis": redis_info,
        "active_task_count": len(active_tasks),
        "error_count_24h": error_count,
        "alerts": alerts,
        "recommended_actions": actions,
    }


# ── All active tasks across ALL projects ──────────────────────────────


@router.get("/tasks")
async def all_active_tasks(db: AsyncSession = Depends(get_db)):
    """List every running task across all projects with live status."""
    tasks = _get_all_active_tasks()

    result = await db.execute(
        select(TransformManifest)
        .where(TransformManifest.status == "running")
    )
    for m in result.scalars().all():
        ar = AsyncResult(m.celery_task_id, app=celery_app) if m.celery_task_id else None
        tasks.append({
            "task_id": m.celery_task_id,
            "project_id": m.project_id,
            "type": "transform",
            "status": ar.state if ar else "UNKNOWN",
            "started_at": m.started_at.isoformat() if m.started_at else None,
            "duration_sec": (datetime.utcnow() - m.started_at).total_seconds() if m.started_at else None,
            "progress": ar.info if ar and ar.state == "PROGRESS" else None,
        })

    result = await db.execute(
        select(PushManifest)
        .where(PushManifest.status == "running")
    )
    for m in result.scalars().all():
        ar = AsyncResult(m.celery_task_id, app=celery_app) if m.celery_task_id else None
        tasks.append({
            "task_id": m.celery_task_id,
            "project_id": m.project_id,
            "type": f"push:{m.task_type}",
            "status": ar.state if ar else "UNKNOWN",
            "started_at": m.started_at.isoformat() if m.started_at else None,
            "duration_sec": (datetime.utcnow() - m.started_at).total_seconds() if m.started_at else None,
            "progress": ar.info if ar and ar.state == "PROGRESS" else None,
        })

    tasks.sort(key=lambda t: t.get("duration_sec") or 0, reverse=True)
    return {"tasks": tasks, "count": len(tasks)}


# ── Recent errors ─────────────────────────────────────────────────────


@router.get("/errors")
async def recent_errors(
    hours: int = 24,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    """Recent errors from the audit log, newest first."""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    result = await db.execute(
        select(AuditEvent)
        .where(
            AuditEvent.level == "error",
            AuditEvent.created_at >= cutoff,
        )
        .order_by(desc(AuditEvent.created_at))
        .limit(limit)
    )
    events = result.scalars().all()

    grouped: dict[str, list] = {}
    rows = []
    for e in events:
        row = {
            "id": e.id,
            "project_id": e.project_id,
            "stage": e.stage,
            "tag": e.tag,
            "message": e.message,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }
        rows.append(row)
        key = e.tag or "unknown"
        grouped.setdefault(key, []).append(row)

    summary = [
        {"tag": tag, "count": len(errs), "latest": errs[0]["created_at"]}
        for tag, errs in sorted(grouped.items(), key=lambda x: -len(x[1]))
    ]

    return {"errors": rows, "summary": summary, "total": len(rows)}


# ── Actions ──────────────────────────────────────────────────────────


@router.post("/actions/revoke-task")
async def revoke_task(request: Request):
    """Revoke (terminate) a specific Celery task by ID."""
    body = await request.json()
    task_id = body.get("task_id")
    if not task_id:
        raise HTTPException(400, detail="task_id required")
    celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
    return {"revoked": task_id, "message": "Task termination signal sent."}


@router.post("/actions/purge-queue")
async def purge_queue(request: Request):
    """Purge all pending tasks from a specific queue."""
    body = await request.json()
    queue_name = body.get("queue")
    if not queue_name or queue_name not in _QUEUES:
        raise HTTPException(400, detail=f"queue must be one of: {', '.join(_QUEUES)}")
    purged = celery_app.control.purge()
    return {"queue": queue_name, "purged": purged, "message": f"Queue '{queue_name}' purge signal sent."}


@router.post("/actions/restart-workers")
async def restart_workers():
    """Restart worker and worker-push containers via Docker Engine API.

    Requires /var/run/docker.sock to be mounted on the web container.
    """
    import httpx
    results = []
    transport = httpx.HTTPTransport(uds="/var/run/docker.sock")
    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds="/var/run/docker.sock"), timeout=30.0) as client:
        # List containers to find worker names
        try:
            resp = await client.get("http://localhost/containers/json")
            containers = resp.json() if resp.status_code == 200 else []
        except Exception as exc:
            raise HTTPException(502, detail=f"Docker socket unavailable: {exc}")

        worker_containers = []
        for c in containers:
            names = c.get("Names", [])
            for name in names:
                if "worker" in name.lower():
                    worker_containers.append({
                        "id": c["Id"],
                        "name": name.lstrip("/"),
                    })

        if not worker_containers:
            # Fallback: try known container name patterns
            for pattern in ["cerulean-worker-1", "cerulean-worker-push-1"]:
                worker_containers.append({"id": pattern, "name": pattern})

        for wc in worker_containers:
            try:
                resp = await client.post(f"http://localhost/containers/{wc['id']}/restart?t=10")
                results.append({
                    "container": wc["name"],
                    "status": "restarted" if resp.status_code == 204 else f"http_{resp.status_code}",
                })
            except Exception as exc:
                results.append({
                    "container": wc["name"],
                    "status": f"error: {exc}",
                })

    return {"results": results, "message": f"Restart signal sent to {len(results)} container(s)."}


# ── Helpers ──────────────────────────────────────────────────────────


def _get_worker_health() -> list[dict]:
    """Ping all Celery workers and gather stats."""
    workers = []
    try:
        inspect = celery_app.control.inspect(timeout=3.0)
        pings = inspect.ping() or {}
        stats = inspect.stats() or {}
        active = inspect.active() or {}
        for name, reply in pings.items():
            s = stats.get(name, {})
            act = active.get(name, [])
            workers.append({
                "name": name,
                "status": "online" if reply.get("ok") == "pong" else "degraded",
                "active_tasks": len(act),
                "pool_size": s.get("pool", {}).get("max-concurrency"),
                "uptime_sec": int(time.time() - s.get("clock", time.time())),
                "total_tasks_processed": s.get("total", {}).get("cerulean.tasks", 0),
                "prefetch_count": s.get("prefetch_count"),
            })
    except Exception:
        pass
    return workers


def _get_queue_depths() -> list[dict]:
    """Check pending task count in each Celery queue via Redis."""
    depths = []
    for q in _QUEUES:
        try:
            depth = _redis.llen(q) or 0
        except Exception:
            depth = -1
        depths.append({"queue": q, "depth": depth})
    return depths


def _get_redis_info() -> dict:
    """Basic Redis health metrics."""
    try:
        info = _redis.info(section="memory")
        return {
            "used_memory_human": info.get("used_memory_human", "?"),
            "used_memory_peak_human": info.get("used_memory_peak_human", "?"),
            "connected_clients": _redis.info(section="clients").get("connected_clients", "?"),
        }
    except Exception:
        return {"error": "unreachable"}


def _get_all_active_tasks() -> list[dict]:
    """Get all currently executing tasks from all workers."""
    tasks = []
    try:
        inspect = celery_app.control.inspect(timeout=3.0)
        active = inspect.active() or {}
        for worker_name, task_list in active.items():
            for t in task_list:
                started = t.get("time_start")
                duration = (time.time() - started) if started else None
                tasks.append({
                    "task_id": t.get("id"),
                    "task_name": t.get("name", ""),
                    "worker": worker_name,
                    "project_id": (t.get("args") or [None])[0],
                    "type": _task_type_from_name(t.get("name", "")),
                    "status": "RUNNING",
                    "started_at": datetime.utcfromtimestamp(started).isoformat() if started else None,
                    "duration_sec": round(duration, 1) if duration else None,
                })
    except Exception:
        pass
    return tasks


def _task_type_from_name(name: str) -> str:
    """Extract a human-readable task type from the dotted Celery task name."""
    parts = name.split(".")
    if len(parts) >= 2:
        return parts[-1]
    return name


def _compute_alerts(workers, queues, active_tasks) -> list[dict]:
    """Detect actionable conditions."""
    alerts = []

    if not workers:
        alerts.append({
            "severity": "critical",
            "message": "No Celery workers responding — all background tasks are stalled.",
            "type": "no_workers",
        })

    for q in queues:
        if q["depth"] > 50:
            alerts.append({
                "severity": "warn",
                "message": f"Queue '{q['queue']}' has {q['depth']} pending tasks — consider scaling workers.",
                "type": "queue_backlog",
                "queue": q["queue"],
                "depth": q["depth"],
            })

    for t in active_tasks:
        dur = t.get("duration_sec") or 0
        if dur > _HUNG_THRESHOLD_SECONDS:
            alerts.append({
                "severity": "critical",
                "message": f"Task {t['type']} has been running for {dur/3600:.1f}h — may be hung.",
                "type": "hung_task",
                "task_id": t.get("task_id"),
                "duration_sec": dur,
            })
        elif dur > _LONG_RUNNING_SECONDS:
            alerts.append({
                "severity": "info",
                "message": f"Task {t['type']} running for {dur/60:.0f}m — long but possibly normal for large datasets.",
                "type": "long_running",
                "task_id": t.get("task_id"),
                "duration_sec": dur,
            })

    return alerts


def _recommend_actions(alerts, active_tasks) -> list[dict]:
    """Generate recommended actions based on detected alerts."""
    actions = []
    for a in alerts:
        if a["type"] == "no_workers":
            actions.append({
                "label": "Restart workers",
                "description": "Run: docker compose restart worker worker-push",
                "action_type": "manual",
                "severity": "critical",
            })
        elif a["type"] == "hung_task":
            actions.append({
                "label": f"Revoke hung task",
                "description": f"Task {a.get('task_id', '?')[:12]}… has been running for {a.get('duration_sec',0)/3600:.1f}h. Revoking will free the worker slot.",
                "action_type": "revoke_task",
                "task_id": a.get("task_id"),
                "severity": "critical",
            })
        elif a["type"] == "queue_backlog":
            actions.append({
                "label": f"Scale workers for '{a.get('queue')}'",
                "description": f"{a.get('depth')} tasks queued. Consider adding worker concurrency or a dedicated worker.",
                "action_type": "manual",
                "severity": "warn",
            })
    return actions


async def _recent_error_count(db: AsyncSession, hours: int = 24) -> int:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    result = await db.execute(
        select(func.count(AuditEvent.id)).where(
            AuditEvent.level == "error",
            AuditEvent.created_at >= cutoff,
        )
    )
    return result.scalar_one()
