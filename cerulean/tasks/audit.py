"""
cerulean/tasks/audit.py
─────────────────────────────────────────────────────────────────────────────
AuditLogger — thin helper for writing AuditEvent rows from inside Celery tasks.

Every Celery task MUST:
  1. Call log.info() or log.warn() at each major milestone
  2. Call log.complete() before returning success
  3. Call log.error() before re-raising any exception

Usage:
    log = AuditLogger(project_id="abc-123", stage=5, tag="[push]")
    log.info("Starting bulkmarcimport.pl")
    ...
    log.complete("Import finished")
"""

import uuid
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Sync engine for use inside Celery tasks (which are not async)
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_sync_engine = create_engine(_sync_url, pool_pre_ping=True)


class AuditLogger:
    """
    Writes AuditEvent rows synchronously from within a Celery task.

    Args:
        project_id: UUID string of the project being worked on.
        stage: Pipeline stage number (1–6). None for cross-project events.
        tag: Short label shown in the log UI, e.g. "[push]", "[ai-map]".
    """

    def __init__(self, project_id: str, stage: int | None, tag: str | None = None):
        self.project_id = project_id
        self.stage = stage
        self.tag = tag

    def _write(
        self,
        level: str,
        message: str,
        record_001: str | None = None,
        extra: dict | None = None,
    ) -> None:
        """Insert one AuditEvent row. Commits immediately."""
        from cerulean.models import AuditEvent  # local import avoids circular

        row = AuditEvent(
            id=str(uuid.uuid4()),
            project_id=self.project_id,
            stage=self.stage,
            level=level,
            tag=self.tag,
            message=message,
            record_001=record_001,
            extra=extra,
            created_at=datetime.utcnow(),
        )
        with Session(_sync_engine) as session:
            session.add(row)
            session.commit()

        # Also emit to structlog so Docker logs capture everything
        log_fn = getattr(logger, level if level != "complete" else "info")
        log_fn(message, project_id=self.project_id, stage=self.stage)

    def info(self, message: str, **kwargs) -> None:
        self._write("info", message, **kwargs)

    def warn(self, message: str, **kwargs) -> None:
        self._write("warn", message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        self._write("error", message, **kwargs)

    def complete(self, message: str, **kwargs) -> None:
        self._write("complete", message, **kwargs)
