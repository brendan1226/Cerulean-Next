"""
cerulean/schemas/items.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Item CSV → 952 column mapping API contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


# ── Column Mapping ────────────────────────────────────────────────────────


class ItemColumnMapCreate(BaseModel):
    source_column: str
    target_subfield: str | None = None
    ignored: bool = False
    transform_type: str | None = None
    transform_config: dict | None = None
    sort_order: int = 0


class ItemColumnMapUpdate(BaseModel):
    target_subfield: str | None = None
    ignored: bool | None = None
    transform_type: str | None = None
    transform_config: dict | None = None
    approved: bool | None = None
    sort_order: int | None = None


class ItemColumnMapOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    source_column: str
    target_subfield: str | None
    ignored: bool
    transform_type: str | None
    transform_config: dict | None
    approved: bool
    ai_suggested: bool
    ai_confidence: float | None
    ai_reasoning: str | None
    source_label: str
    sort_order: int
    created_at: datetime
    updated_at: datetime


# ── AI Suggest ────────────────────────────────────────────────────────────


class ItemAISuggestResponse(BaseModel):
    task_id: str
    message: str


class ItemAISuggestStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None


# ── Preview ───────────────────────────────────────────────────────────────


class ItemPreviewResponse(BaseModel):
    rows: list[dict]
    total_rows: int
    columns: list[str]


# ── Match Config ──────────────────────────────────────────────────────────


class ItemMatchConfig(BaseModel):
    items_csv_match_tag: str | None
    items_csv_key_column: str | None


class ItemMatchConfigUpdate(BaseModel):
    items_csv_match_tag: str | None = None
    items_csv_key_column: str | None = None


# ── Rename Column ─────────────────────────────────────────────────────────


class RenameColumnRequest(BaseModel):
    old_name: str
    new_name: str


class RenameColumnResponse(BaseModel):
    old_name: str
    new_name: str
    maps_updated: int
    message: str


# ── Approve ───────────────────────────────────────────────────────────────


class ApproveAllRequest(BaseModel):
    min_confidence: float = 0.85


class ApproveAllResponse(BaseModel):
    approved_count: int
    message: str
