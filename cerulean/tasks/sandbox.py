"""
cerulean/tasks/sandbox.py — Stage 6 KTD Sandbox stubs (future sprint).
"""
from cerulean.tasks.celery_app import celery_app


@celery_app.task(name="cerulean.tasks.sandbox.ktd_provision_task")
def ktd_provision_task(project_id: str) -> dict:
    raise NotImplementedError("KTD Sandbox — Stage 6, coming in a future sprint")


@celery_app.task(name="cerulean.tasks.sandbox.ktd_teardown_task")
def ktd_teardown_task(project_id: str) -> dict:
    raise NotImplementedError("KTD Sandbox — Stage 6, coming in a future sprint")
