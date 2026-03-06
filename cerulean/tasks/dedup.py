"""cerulean/tasks/dedup.py — Stage 4 stubs (next sprint)."""
from cerulean.tasks.celery_app import celery_app

@celery_app.task(name="cerulean.tasks.dedup.dedup_scan_task")
def dedup_scan_task(project_id: str, rule_id: str) -> dict:
    raise NotImplementedError("Dedup scan — Stage 4, coming next sprint")
