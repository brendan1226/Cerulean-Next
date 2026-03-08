"""
cerulean/schemas/push.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Stage 5 — Push to Koha API contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


# ── Preflight ────────────────────────────────────────────────────────────

class PreflightRequest(BaseModel):
    koha_url: str | None = None     # override project setting
    koha_token: str | None = None   # override project setting


class PreflightResponse(BaseModel):
    task_id: str
    message: str


# ── Push start ───────────────────────────────────────────────────────────

class PushStartRequest(BaseModel):
    dry_run: bool = True
    push_bibs: bool = True
    push_patrons: bool = False
    push_holds: bool = False
    push_circ: bool = False
    reindex: bool = False


class PushStartResponse(BaseModel):
    task_ids: dict[str, str]    # e.g. {"bulkmarc": "celery-task-id", ...}
    message: str


# ── Status / Manifest ───────────────────────────────────────────────────

class PushStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None


class PushManifestOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    task_type: str
    status: str
    celery_task_id: str | None
    dry_run: bool
    records_total: int | None
    records_success: int | None
    records_failed: int | None
    error_message: str | None
    result_data: dict | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
