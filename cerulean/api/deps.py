"""
cerulean/api/deps.py
─────────────────────────────────────────────────────────────────────────────
Shared helpers used by multiple routers.
"""

import uuid

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.models import AuditEvent, Project


async def require_project(project_id: str, db: AsyncSession) -> Project:
    """Fetch a project or raise 404."""
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})
    return project


async def audit_log(
    db: AsyncSession, project_id: str, stage: int, level: str, tag: str, message: str,
) -> None:
    """Write a single audit event row."""
    event = AuditEvent(
        id=str(uuid.uuid4()),
        project_id=project_id,
        stage=stage,
        level=level,
        tag=tag,
        message=message,
    )
    db.add(event)
