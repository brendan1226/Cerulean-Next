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
    """Block while the project's pause flag is set in Redis.

    Args:
        project_id: UUID string of the project.
        self_task: The bound Celery task instance (optional). When provided,
                   updates the task state to PAUSED so the frontend can
                   reflect it.
    """
    key = f"cerulean:pause:{project_id}"
    while _redis.exists(key):
        if self_task:
            self_task.update_state(state="PAUSED", meta={"paused": True})
        time.sleep(1)
