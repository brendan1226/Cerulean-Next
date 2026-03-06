"""cerulean/tasks/push.py — Stage 5 stubs (next sprint)."""
from cerulean.tasks.celery_app import celery_app

@celery_app.task(name="cerulean.tasks.push.push_bulkmarc_task")
def push_bulkmarc_task(project_id: str) -> dict:
    raise NotImplementedError("Push bulkmarc — Stage 5, coming next sprint")

@celery_app.task(name="cerulean.tasks.push.es_reindex_task")
def es_reindex_task(project_id: str) -> dict:
    raise NotImplementedError("ES reindex — Stage 5, coming next sprint")
