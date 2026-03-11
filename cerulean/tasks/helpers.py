"""
cerulean/tasks/helpers.py
─────────────────────────────────────────────────────────────────────────────
Shared helpers for Celery tasks.
"""

import time

import redis

from cerulean.core.config import get_settings

settings = get_settings()
_redis = redis.from_url(settings.redis_url)


def check_paused(project_id: str, self_task=None) -> None:
    """Block while this task's pause flag is set in Redis.

    Uses the Celery task ID for per-task granularity, falling back to
    the project ID if no task instance is provided.

    Args:
        project_id: UUID string of the project (fallback key).
        self_task: The bound Celery task instance (optional). When provided,
                   uses its request.id for a per-task pause key and updates
                   the task state to PAUSED.
    """
    if self_task and getattr(self_task, 'request', None) and self_task.request.id:
        key = f"cerulean:pause:task:{self_task.request.id}"
    else:
        key = f"cerulean:pause:task:{project_id}"
    while _redis.exists(key):
        if self_task:
            self_task.update_state(state="PAUSED", meta={"paused": True})
        time.sleep(1)
