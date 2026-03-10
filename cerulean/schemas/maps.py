"""
cerulean/schemas/maps.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for FieldMap, MapTemplate, and AI suggestion contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


# ── FieldMap ───────────────────────────────────────────────────────────

class FieldMapCreate(BaseModel):
    source_tag: str
    source_sub: str | None = None
    target_tag: str
    target_sub: str | None = None
    transform_type: str = "copy"
    transform_fn: str | None = None
    preset_key: str | None = None
    delete_source: bool = False
    notes: str | None = None
    approved: bool = False


class FieldMapUpdate(BaseModel):
    source_tag: str | None = None
    source_sub: str | None = None
    target_tag: str | None = None
    target_sub: str | None = None
    transform_type: str | None = None
    transform_fn: str | None = None
    preset_key: str | None = None
    delete_source: bool | None = None
    notes: str | None = None
    approved: bool | None = None


class FieldMapOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    source_tag: str
    source_sub: str | None
    target_tag: str
    target_sub: str | None
    transform_type: str
    transform_fn: str | None
    preset_key: str | None
    delete_source: bool
    ai_suggested: bool
    ai_confidence: float | None
    ai_reasoning: str | None
    source_label: str
    approved: bool
    notes: str | None
    sort_order: int
    created_at: datetime
    updated_at: datetime


# ── AI Suggestions ─────────────────────────────────────────────────────

class AISuggestion(BaseModel):
    """One AI-proposed field mapping returned by /maps/ai-suggest."""
    source_tag: str
    source_sub: str | None
    target_tag: str
    target_sub: str | None
    transform_type: str
    transform_fn: str | None
    preset_key: str | None = None
    delete_source: bool = False
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str


class AIMapSuggestResponse(BaseModel):
    task_id: str
    message: str
    suggestion_count: int | None = None


class ApproveAllRequest(BaseModel):
    min_confidence: float = Field(default=0.70, ge=0.0, le=1.0)


class ApproveAllResponse(BaseModel):
    approved_count: int


# ── Map Template ───────────────────────────────────────────────────────

class TemplateMapEntry(BaseModel):
    """One map entry stored inside a MapTemplate.maps JSONB column."""
    source_tag: str
    source_sub: str | None = None
    target_tag: str
    target_sub: str | None = None
    transform_type: str = "copy"
    transform_fn: str | None = None
    notes: str | None = None


class MapTemplateCreate(BaseModel):
    name: str
    version: str = "1.0"
    description: str | None = None
    scope: str = "project"          # "project" | "global"
    source_ils: str | None = None
    include_pending: bool = False   # whether to include unapproved AI maps


class MapTemplateUpdate(BaseModel):
    name: str | None = None
    version: str | None = None
    description: str | None = None
    scope: str | None = None


class MapTemplateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    version: str
    description: str | None
    scope: str
    project_id: str | None
    source_ils: str | None
    ai_generated: bool
    reviewed: bool
    use_count: int
    maps: list | None
    created_by: str | None
    created_at: datetime
    updated_at: datetime


class TemplateLoadRequest(BaseModel):
    template_id: str
    mode: str = "replace"   # "replace" | "merge"


class TemplateLoadResult(BaseModel):
    maps_added: int
    maps_skipped: int       # merge mode: source already exists and approved
    maps_conflicted: int    # merge mode: source exists but not approved — overwritten
    mode: str
