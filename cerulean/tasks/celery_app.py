"""
cerulean/tasks/celery_app.py
─────────────────────────────────────────────────────────────────────────────
Celery application instance. All task modules are imported here so Celery
discovers them on worker startup.

Queues:
    ingest      — file upload, ILS detection, tag frequency
    analyze     — AI mapping, template operations
    transform   — merge pipeline, field map application
    dedup       — dedup scan and apply
    reconcile   — item data reconciliation scan and apply
    patrons     — patron data parse, AI map, scan, apply
    push        — Koha push (bulkmarcimport, patrons, holds, circ, ES reindex)
    sandbox     — KTD provisioning (future)
    default     — miscellaneous / low-priority
"""

from celery import Celery

from cerulean.core.config import get_settings

settings = get_settings()

celery_app = Celery(
    "cerulean",
    broker=settings.redis_url,
    backend=settings.celery_result_backend,
    include=[
        "cerulean.tasks.ingest",
        "cerulean.tasks.analyze",
        "cerulean.tasks.transform",
        "cerulean.tasks.items",
        "cerulean.tasks.dedup",
        "cerulean.tasks.reconcile",
        "cerulean.tasks.patrons",
        "cerulean.tasks.push",
        "cerulean.tasks.sandbox",
    ],
)

celery_app.conf.update(
    # Routing: tasks → queues
    task_routes={
        "cerulean.tasks.ingest.*": {"queue": "ingest"},
        "cerulean.tasks.items.*": {"queue": "ingest"},
        "cerulean.tasks.analyze.*": {"queue": "analyze"},
        "cerulean.tasks.transform.*": {"queue": "transform"},
        "cerulean.tasks.dedup.*": {"queue": "dedup"},
        "cerulean.tasks.reconcile.*": {"queue": "reconcile"},
        "cerulean.tasks.patrons.*": {"queue": "patrons"},
        "cerulean.tasks.push.*": {"queue": "push"},
        "cerulean.tasks.sandbox.*": {"queue": "sandbox"},
    },
    # Serialisation
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Reliability
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,   # don't pre-fetch — long tasks can starve queues
    # Progress events (used by /status endpoints)
    task_track_started=True,
    result_expires=86400,           # 24 hours
    # Timezone
    timezone="UTC",
    enable_utc=True,
)
