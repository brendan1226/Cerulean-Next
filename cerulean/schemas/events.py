"""
cerulean/schemas/events.py
─────────────────────────────────────────────────────────────────────────────
Pydantic schemas for AuditEvent, Suggestion, Comment, and related types.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


# ── Audit Event ────────────────────────────────────────────────────────

class AuditEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    stage: int | None
    level: str
    tag: str | None
    message: str
    record_001: str | None
    extra: dict | None
    created_at: datetime


class AuditLogPage(BaseModel):
    total: int
    items: list[AuditEventOut]


# ── Suggestion ─────────────────────────────────────────────────────────

class SuggestionCreate(BaseModel):
    type: str           # "feature" | "bug" | "workflow" | "discussion"
    title: str
    body: str
    project_id: str | None = None


class SuggestionEdit(BaseModel):
    title: str | None = None
    body: str | None = None
    type: str | None = None


class SuggestionUpdate(BaseModel):
    status: str | None = None
    title: str | None = None
    body: str | None = None
    type: str | None = None


class CommentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    suggestion_id: str
    author_email: str
    author_name: str
    body: str
    created_at: datetime


class SuggestionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    type: str
    title: str
    body: str
    project_id: str | None
    status: str
    submitted_by: str | None
    vote_count: int
    created_at: datetime
    updated_at: datetime
    comments: list[CommentOut] = []


class CommentCreate(BaseModel):
    body: str


class VoteResponse(BaseModel):
    voted: bool         # True = vote added, False = vote removed
    vote_count: int
