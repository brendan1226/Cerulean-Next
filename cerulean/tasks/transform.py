"""cerulean/tasks/transform.py — Stage 3 stubs (next sprint)."""
from cerulean.tasks.celery_app import celery_app

@celery_app.task(name="cerulean.tasks.transform.transform_pipeline_task")
def transform_pipeline_task(project_id: str) -> dict:
    raise NotImplementedError("Transform pipeline — Stage 3, coming next sprint")

@celery_app.task(name="cerulean.tasks.transform.merge_pipeline_task")
def merge_pipeline_task(project_id: str) -> dict:
    raise NotImplementedError("Merge pipeline — Stage 3, coming next sprint")
