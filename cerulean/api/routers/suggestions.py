"""
cerulean/api/routers/suggestions.py
─────────────────────────────────────────────────────────────────────────────
GET    /suggestions               — list (filter by type/status/project)
POST   /suggestions               — submit
GET    /suggestions/{id}          — get single with comments
PATCH  /suggestions/{id}          — edit (owner) or update status (anyone)
DELETE /suggestions/{id}          — delete (owner only)
POST   /suggestions/{id}/vote     — toggle upvote
POST   /suggestions/{id}/comments — add comment
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from cerulean.core.database import get_db
from cerulean.models import Suggestion, SuggestionComment, SuggestionVote, User
from cerulean.schemas.events import (
    CommentCreate,
    CommentOut,
    SuggestionCreate,
    SuggestionOut,
    SuggestionUpdate,
    VoteResponse,
)

router = APIRouter(prefix="/suggestions", tags=["suggestions"])

VALID_TYPES = {"feature", "bug", "workflow", "discussion"}
VALID_STATUSES = {
    "open", "confirmed", "in_progress", "shipped", "closed",
    "fixed", "future_dev", "wont_fix", "system_action",
}


async def _get_user_info(request: Request, db: AsyncSession) -> tuple[str, str]:
    """Return (email, name) from the authenticated user, or fallback."""
    user_id = getattr(request.state, "user_id", None)
    if user_id:
        user = await db.get(User, user_id)
        if user:
            return user.email, user.name
    return "anonymous", "Anonymous"


# ── List ─────────────────────────────────────────────────────────────────

@router.get("", response_model=list[SuggestionOut])
async def list_suggestions(
    type: str | None = Query(None),
    status: str | None = Query(None),
    project_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    q = select(Suggestion).options(selectinload(Suggestion.comments))
    if type:
        q = q.where(Suggestion.type == type)
    if status:
        q = q.where(Suggestion.status == status)
    if project_id:
        q = q.where(Suggestion.project_id == project_id)
    q = q.order_by(Suggestion.vote_count.desc(), Suggestion.created_at.desc())
    result = await db.execute(q)
    return result.scalars().unique().all()


# ── Get Single ───────────────────────────────────────────────────────────

@router.get("/{suggestion_id}", response_model=SuggestionOut)
async def get_suggestion(
    suggestion_id: str,
    db: AsyncSession = Depends(get_db),
):
    q = select(Suggestion).options(selectinload(Suggestion.comments)).where(Suggestion.id == suggestion_id)
    result = await db.execute(q)
    suggestion = result.scalar_one_or_none()
    if not suggestion:
        raise HTTPException(404, detail="Suggestion not found.")
    return suggestion


# ── Create ───────────────────────────────────────────────────────────────

@router.post("", response_model=SuggestionOut, status_code=201)
async def create_suggestion(
    body: SuggestionCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if body.type not in VALID_TYPES:
        raise HTTPException(400, detail={"error": "INVALID_TYPE", "message": f"Type must be one of: {', '.join(VALID_TYPES)}"})

    email, name = await _get_user_info(request, db)

    suggestion = Suggestion(
        id=str(uuid.uuid4()),
        type=body.type,
        title=body.title,
        body=body.body,
        project_id=body.project_id,
        status="open",
        submitted_by=email,
        vote_count=0,
    )
    db.add(suggestion)
    await db.flush()
    await db.refresh(suggestion)
    return suggestion


# ── Edit / Update Status ─────────────────────────────────────────────────

@router.patch("/{suggestion_id}", response_model=SuggestionOut)
async def update_suggestion(
    suggestion_id: str,
    body: SuggestionUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Edit title/body (owner only) or update status/type (anyone)."""
    suggestion = await _require_suggestion(suggestion_id, db)

    if body.status is not None:
        if body.status not in VALID_STATUSES:
            raise HTTPException(400, detail={"error": "INVALID_STATUS", "message": f"Status must be one of: {', '.join(sorted(VALID_STATUSES))}"})
        suggestion.status = body.status

    if body.title is not None:
        suggestion.title = body.title
    if body.body is not None:
        suggestion.body = body.body
    if body.type is not None:
        if body.type not in VALID_TYPES:
            raise HTTPException(400, detail={"error": "INVALID_TYPE", "message": f"Type must be one of: {', '.join(VALID_TYPES)}"})
        suggestion.type = body.type

    await db.flush()
    await db.refresh(suggestion)
    return suggestion


# ── Delete ───────────────────────────────────────────────────────────────

@router.delete("/{suggestion_id}", status_code=204)
async def delete_suggestion(
    suggestion_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    suggestion = await _require_suggestion(suggestion_id, db)
    email, _ = await _get_user_info(request, db)
    if suggestion.submitted_by and suggestion.submitted_by != email:
        raise HTTPException(403, detail="Only the author can delete this suggestion.")
    await db.delete(suggestion)
    await db.flush()


# ── Vote ─────────────────────────────────────────────────────────────────

@router.post("/{suggestion_id}/vote", response_model=VoteResponse)
async def vote_suggestion(
    suggestion_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Toggle upvote. Returns voted=True if vote was added, False if removed."""
    email, _ = await _get_user_info(request, db)
    suggestion = await _require_suggestion(suggestion_id, db)

    existing = (await db.execute(
        select(SuggestionVote).where(
            SuggestionVote.suggestion_id == suggestion_id,
            SuggestionVote.user_email == email,
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
            user_email=email,
        ))
        suggestion.vote_count += 1
        voted = True

    await db.flush()
    return VoteResponse(voted=voted, vote_count=suggestion.vote_count)


# ── Comments ─────────────────────────────────────────────────────────────

@router.post("/{suggestion_id}/comments", response_model=CommentOut, status_code=201)
async def add_comment(
    suggestion_id: str,
    body: CommentCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    await _require_suggestion(suggestion_id, db)
    email, name = await _get_user_info(request, db)

    comment = SuggestionComment(
        id=str(uuid.uuid4()),
        suggestion_id=suggestion_id,
        author_email=email,
        author_name=name,
        body=body.body,
    )
    db.add(comment)
    await db.flush()
    await db.refresh(comment)
    return comment


@router.get("/{suggestion_id}/comments", response_model=list[CommentOut])
async def list_comments(
    suggestion_id: str,
    db: AsyncSession = Depends(get_db),
):
    await _require_suggestion(suggestion_id, db)
    result = await db.execute(
        select(SuggestionComment)
        .where(SuggestionComment.suggestion_id == suggestion_id)
        .order_by(SuggestionComment.created_at)
    )
    return result.scalars().all()


@router.delete("/{suggestion_id}/comments/{comment_id}", status_code=204)
async def delete_comment(
    suggestion_id: str,
    comment_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    comment = await db.get(SuggestionComment, comment_id)
    if not comment or comment.suggestion_id != suggestion_id:
        raise HTTPException(404, detail="Comment not found.")
    email, _ = await _get_user_info(request, db)
    if comment.author_email != email:
        raise HTTPException(403, detail="Only the author can delete this comment.")
    await db.delete(comment)
    await db.flush()


# ── Helpers ──────────────────────────────────────────────────────────────

async def _require_suggestion(suggestion_id: str, db: AsyncSession) -> Suggestion:
    s = await db.get(Suggestion, suggestion_id)
    if not s:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Suggestion not found."})
    return s
