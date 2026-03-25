"""
cerulean/schemas/transform.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Stage 3 — Transform & Merge API contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


# ── Transform ─────────────────────────────────────────────────────────

class TransformStartRequest(BaseModel):
    file_ids: list[str] | None = None   # specific files; None = all indexed
    dry_run: bool = False


class TransformStartResponse(BaseModel):
    task_id: str
    manifest_id: str
    message: str


# ── Merge ─────────────────────────────────────────────────────────────

class MergeStartRequest(BaseModel):
    file_ids: list[str] | None = None
    items_csv_path: str | None = None
    items_csv_match_tag: str = "001"
    items_csv_key_column: str = "biblionumber"


class MergeStartResponse(BaseModel):
    task_id: str
    manifest_id: str
    message: str


# ── Build ─────────────────────────────────────────────────────────────

class BuildStartRequest(BaseModel):
    file_ids: list[str]
    include_items: bool = False
    items_match_tag: str = "001"
    items_key_column: str = "biblionumber"


class BuildStartResponse(BaseModel):
    task_id: str
    manifest_id: str
    message: str


# ── Status ────────────────────────────────────────────────────────────

class TransformStatusResponse(BaseModel):
    task_id: str | None
    state: str              # PENDING | STARTED | PROGRESS | SUCCESS | FAILURE
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None


# ── Manifest output ──────────────────────────────────────────────────

class TransformManifestOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    task_type: str
    status: str
    celery_task_id: str | None

    output_path: str | None
    files_processed: int | None
    total_records: int | None
    records_skipped: int | None
    items_joined: int | None
    duplicate_001s: list | None
    file_ids: list | None
    items_csv_path: str | None

    error_message: str | None

    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
