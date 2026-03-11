"""
cerulean/schemas/push.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Stage 7 — Push to Koha API contracts.
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

class BibPushOptions(BaseModel):
    method: str = "rest_api"           # "rest_api" | "bulkmarcimport"
    match_field: str | None = None     # e.g. "001", "020a", "999c"
    insert: bool = True                # --insert: insert new records
    update: bool = False               # --update: update matched records
    framework: str | None = None       # --framework CODE
    container: str | None = None       # Docker container name (auto-detect if None)


class PushStartRequest(BaseModel):
    dry_run: bool = True
    push_bibs: bool = True
    push_patrons: bool = False
    push_holds: bool = False
    push_circ: bool = False
    reindex: bool = False
    bib_options: BibPushOptions | None = None   # None = use defaults (REST API)


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


# ── Reference Data (Setup tab) ────────────────────────────────────────

class KohaRefDataResponse(BaseModel):
    """What currently exists in Koha."""
    libraries: list[dict]
    patron_categories: list[dict]
    item_types: list[dict]
    authorised_values: dict[str, list[dict]]   # {"LOC": [...], "CCODE": [...], "LOST": [...]}


class NeededRefValue(BaseModel):
    value: str
    record_count: int


class NeededRefDataResponse(BaseModel):
    """What the project data requires."""
    libraries: list[NeededRefValue]
    patron_categories: list[NeededRefValue]
    item_types: list[NeededRefValue]
    locations: list[NeededRefValue]
    ccodes: list[NeededRefValue]


class LibraryPushItem(BaseModel):
    library_id: str
    name: str


class PushLibrariesRequest(BaseModel):
    libraries: list[LibraryPushItem]


class PushLibraryResult(BaseModel):
    library_id: str
    success: bool
    error: str | None = None


class PushLibrariesResponse(BaseModel):
    total: int
    success_count: int
    failed_count: int
    results: list[PushLibraryResult]
