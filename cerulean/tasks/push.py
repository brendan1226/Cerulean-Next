"""
cerulean/tasks/push.py
─────────────────────────────────────────────────────────────────────────────
Stage 5 Celery tasks: Push to Koha.

Current status: STUB — Stage 5 is not yet implemented.
All stubs are registered with Celery so they can be enqueued, but each
writes an AuditEvent (error level) and raises NotImplementedError so
failures are visible in Flower, the audit log, and the UI.

Exports:
    push_bulkmarc_task, push_patrons_task, push_holds_task,
    push_circ_task, es_reindex_task
"""

from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app


def _stub_error(project_id: str, stage: int, name: str) -> None:
    """Write an AuditEvent and raise NotImplementedError for unimplemented tasks.

    Args:
        project_id: UUID string of the owning project.
        stage: Pipeline stage number (always 5 for push tasks).
        name: Human-readable name of the task for the error message.

    Raises:
        NotImplementedError: Always — task is not yet implemented.
    """
    log = AuditLogger(project_id=project_id, stage=stage, tag="[push]")
    log.error(
        f"{name} called before Stage 5 is implemented. "
        "This task is scheduled for the next sprint."
    )
    raise NotImplementedError(f"{name} — Stage 5, coming next sprint")


@celery_app.task(
    name="cerulean.tasks.push.push_bulkmarc_task",
    bind=True,
    max_retries=0,
    queue="push",
)
def push_bulkmarc_task(self, project_id: str, dry_run: bool = True) -> dict:
    """Run bulkmarcimport.pl for a project's merged MARC file.

    Args:
        project_id: UUID string of the project to push.
        dry_run: If True, validate only — do not write to Koha.

    Returns:
        dict with task status.

    Raises:
        NotImplementedError: Stage 5 is not yet implemented.
    """
    _stub_error(project_id, stage=5, name="push_bulkmarc_task")


@celery_app.task(
    name="cerulean.tasks.push.push_patrons_task",
    bind=True,
    max_retries=0,
    queue="push",
)
def push_patrons_task(self, project_id: str, dry_run: bool = True) -> dict:
    """Import patron CSV into Koha via the Patron Import Tool.

    Args:
        project_id: UUID string of the project to push.
        dry_run: If True, validate only — do not write to Koha.

    Returns:
        dict with task status.

    Raises:
        NotImplementedError: Stage 5 is not yet implemented.
    """
    _stub_error(project_id, stage=5, name="push_patrons_task")


@celery_app.task(
    name="cerulean.tasks.push.push_holds_task",
    bind=True,
    max_retries=0,
    queue="push",
)
def push_holds_task(self, project_id: str, dry_run: bool = True) -> dict:
    """Push holds via Koha REST API /api/v1/holds.

    Args:
        project_id: UUID string of the project to push.
        dry_run: If True, validate only — do not write to Koha.

    Returns:
        dict with task status.

    Raises:
        NotImplementedError: Stage 5 is not yet implemented.
    """
    _stub_error(project_id, stage=5, name="push_holds_task")


@celery_app.task(
    name="cerulean.tasks.push.push_circ_task",
    bind=True,
    max_retries=0,
    queue="push",
)
def push_circ_task(self, project_id: str, dry_run: bool = True) -> dict:
    """Insert circulation history directly into old_issues via SQL.

    Args:
        project_id: UUID string of the project to push.
        dry_run: If True, validate only — do not write to Koha.

    Returns:
        dict with task status.

    Raises:
        NotImplementedError: Stage 5 is not yet implemented.
    """
    _stub_error(project_id, stage=5, name="push_circ_task")


@celery_app.task(
    name="cerulean.tasks.push.es_reindex_task",
    bind=True,
    max_retries=0,
    queue="push",
)
def es_reindex_task(self, project_id: str) -> dict:
    """Run koha-elasticsearch --rebuild -d -b -a after all data is loaded.

    Args:
        project_id: UUID string of the project to reindex.

    Returns:
        dict with task status.

    Raises:
        NotImplementedError: Stage 5 is not yet implemented.
    """
    _stub_error(project_id, stage=5, name="es_reindex_task")
