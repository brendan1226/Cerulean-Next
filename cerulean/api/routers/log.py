"""
cerulean/api/routers/log.py
─────────────────────────────────────────────────────────────────────────────
GET  /projects/{id}/log         — paginated audit log (filterable)
GET  /projects/{id}/log/stream  — SSE real-time event stream
GET  /projects/{id}/log/export  — full log as CSV or plain text download
"""

import asyncio
import csv
import io
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from cerulean.core.database import get_db
from cerulean.models import AuditEvent
from cerulean.schemas.events import AuditEventOut, AuditLogPage

router = APIRouter(prefix="/projects", tags=["log"])


@router.get("/{project_id}/log", response_model=AuditLogPage)
async def get_log(
    project_id: str,
    stage: int | None = Query(None, ge=1, le=6, description="Filter by pipeline stage"),
    level: str | None = Query(None, description="info | warn | error | complete"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Paginated audit log with optional stage and level filters."""
    q = select(AuditEvent).where(AuditEvent.project_id == project_id)
    count_q = select(func.count()).select_from(AuditEvent).where(AuditEvent.project_id == project_id)

    if stage is not None:
        q = q.where(AuditEvent.stage == stage)
        count_q = count_q.where(AuditEvent.stage == stage)
    if level:
        q = q.where(AuditEvent.level == level)
        count_q = count_q.where(AuditEvent.level == level)

    q = q.order_by(AuditEvent.created_at.desc()).limit(limit).offset(offset)

    total = (await db.execute(count_q)).scalar_one()
    items = (await db.execute(q)).scalars().all()

    return AuditLogPage(total=total, items=items)


@router.get("/{project_id}/log/stream")
async def stream_log(
    project_id: str,
    since_id: str | None = Query(None, description="Start streaming events after this event ID"),
    db: AsyncSession = Depends(get_db),
):
    """
    SSE endpoint — streams new AuditEvent rows in real time.
    Polls the database every 1 second for new rows after since_id.
    Clients reconnect automatically on disconnect (SSE standard behaviour).
    """

    async def event_generator():
        last_id = since_id

        # If no since_id, start from the most recent event
        if not last_id:
            result = await db.execute(
                select(AuditEvent.id)
                .where(AuditEvent.project_id == project_id)
                .order_by(AuditEvent.created_at.desc())
                .limit(1)
            )
            row = result.scalar_one_or_none()
            last_id = row if row else None

        while True:
            q = select(AuditEvent).where(AuditEvent.project_id == project_id)
            if last_id:
                # Get events newer than the last seen (by created_at of last_id row)
                last_event = await db.get(AuditEvent, last_id)
                if last_event:
                    q = q.where(AuditEvent.created_at > last_event.created_at)

            q = q.order_by(AuditEvent.created_at.asc()).limit(50)
            result = await db.execute(q)
            new_events = result.scalars().all()

            for event in new_events:
                last_id = event.id
                yield {
                    "id": event.id,
                    "data": AuditEventOut.model_validate(event).model_dump_json(),
                }

            await asyncio.sleep(1)

    return EventSourceResponse(event_generator())


@router.get("/{project_id}/log/export")
async def export_log(
    project_id: str,
    format: str = Query("csv", description="csv | txt"),
    db: AsyncSession = Depends(get_db),
):
    """Export the full project audit log as CSV or plain text. No pagination limit."""
    result = await db.execute(
        select(AuditEvent)
        .where(AuditEvent.project_id == project_id)
        .order_by(AuditEvent.created_at.asc())
    )
    events = result.scalars().all()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"cerulean_log_{project_id[:8]}_{timestamp}"

    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["created_at", "stage", "level", "tag", "message", "record_001"])
        for e in events:
            writer.writerow([
                e.created_at.isoformat(),
                e.stage or "",
                e.level,
                e.tag or "",
                e.message,
                e.record_001 or "",
            ])
        content = output.getvalue()
        media_type = "text/csv"
        filename += ".csv"
    else:
        lines = []
        for e in events:
            stage = f"[stage {e.stage}]" if e.stage else "[global]"
            lines.append(f"{e.created_at.isoformat()}  {stage}  {e.tag or ''}  [{e.level.upper()}]  {e.message}")
        content = "\n".join(lines)
        media_type = "text/plain"
        filename += ".txt"

    return StreamingResponse(
        iter([content]),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
