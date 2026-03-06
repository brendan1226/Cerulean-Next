"""
cerulean/api/routers/suggestions.py
─────────────────────────────────────────────────────────────────────────────
GET   /suggestions            — list (filter by type/status/project)
POST  /suggestions            — submit
POST  /suggestions/{id}/vote  — toggle upvote
PATCH /suggestions/{id}       — update status (admin)
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.database import get_db
from cerulean.models import Suggestion, SuggestionVote
from cerulean.schemas.events import SuggestionCreate, SuggestionOut, SuggestionUpdate, VoteResponse

router = APIRouter(prefix="/suggestions", tags=["suggestions"])

VALID_TYPES = {"feature", "bug", "workflow", "discussion"}
VALID_STATUSES = {"open", "confirmed", "in_progress", "shipped", "closed"}


@router.get("", response_model=list[SuggestionOut])
async def list_suggestions(
    type: str | None = Query(None),
    status: str | None = Query(None),
    project_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    q = select(Suggestion)
    if type:
        q = q.where(Suggestion.type == type)
    if status:
        q = q.where(Suggestion.status == status)
    if project_id:
        q = q.where(Suggestion.project_id == project_id)
    q = q.order_by(Suggestion.vote_count.desc(), Suggestion.created_at.desc())
    result = await db.execute(q)
    return result.scalars().all()


@router.post("", response_model=SuggestionOut, status_code=201)
async def create_suggestion(body: SuggestionCreate, db: AsyncSession = Depends(get_db)):
    if body.type not in VALID_TYPES:
        raise HTTPException(400, detail={"error": "INVALID_TYPE", "message": f"Type must be one of: {', '.join(VALID_TYPES)}"})

    suggestion = Suggestion(
        id=str(uuid.uuid4()),
        type=body.type,
        title=body.title,
        body=body.body,
        project_id=body.project_id,
        status="open",
        vote_count=0,
    )
    db.add(suggestion)
    await db.flush()
    await db.refresh(suggestion)
    return suggestion


@router.post("/{suggestion_id}/vote", response_model=VoteResponse)
async def vote_suggestion(
    suggestion_id: str,
    user_email: str = Query(..., description="Voter identity (replace with auth token later)"),
    db: AsyncSession = Depends(get_db),
):
    """Toggle upvote. Returns voted=True if vote was added, False if removed."""
    suggestion = await _require_suggestion(suggestion_id, db)

    existing = (await db.execute(
        select(SuggestionVote).where(
            SuggestionVote.suggestion_id == suggestion_id,
            SuggestionVote.user_email == user_email,
        )
    )).scalar_one_or_none()

    if existing:
        await db.execute(delete(SuggestionVote).where(SuggestionVote.id == existing.id))
        suggestion.vote_count = max(0, suggestion.vote_count - 1)
        voted = False
    else:
        db.add(SuggestionVote(
            id=str(uuid.uuid4()),
            suggestion_id=suggestion_id,
            user_email=user_email,
        ))
        suggestion.vote_count += 1
        voted = True

    await db.flush()
    return VoteResponse(voted=voted, vote_count=suggestion.vote_count)


@router.patch("/{suggestion_id}", response_model=SuggestionOut)
async def update_suggestion_status(
    suggestion_id: str,
    body: SuggestionUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update suggestion status. Admin-only in production (auth gates added later)."""
    if body.status not in VALID_STATUSES:
        raise HTTPException(400, detail={"error": "INVALID_STATUS", "message": f"Status must be one of: {', '.join(VALID_STATUSES)}"})

    suggestion = await _require_suggestion(suggestion_id, db)
    suggestion.status = body.status
    await db.flush()
    await db.refresh(suggestion)
    return suggestion


async def _require_suggestion(suggestion_id: str, db: AsyncSession) -> Suggestion:
    s = await db.get(Suggestion, suggestion_id)
    if not s:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Suggestion not found."})
    return s
