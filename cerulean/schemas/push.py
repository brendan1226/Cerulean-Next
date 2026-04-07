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
    method: str = "rest_api"           # "rest_api" | "bulkmarcimport" | "bulk_api" | "plugin_fast" | "toolkit"
    match_field: str | None = None     # e.g. "001", "020a", "999c"  (bulkmarcimport)
    insert: bool = True                # --insert: insert new records (bulkmarcimport)
    update: bool = False               # --update: update matched records (bulkmarcimport)
    framework: str | None = None       # Framework code (bulkmarcimport + bulk_api)
    container: str | None = None       # Docker container name (bulkmarcimport only)
    # ── bulk_api options ─────────────────────────────────────────────
    matcher_id: int | None = None          # Koha matching rule ID for dedup
    overlay_action: str | None = None      # "replace" | "create_new" | "ignore"
    nomatch_action: str | None = None      # "create_new" | "ignore"
    item_action: str | None = None         # "always_add" | "add_only_for_matches" | "add_only_for_new" | "ignore"
    parse_items: bool = True               # Parse 952 item data from MARC
    comments: str | None = None            # Import batch comments
    auto_commit: bool = False              # Auto-commit after staging (skip explicit commit step)
    # ── toolkit / plugin options ─────────────────────────────────────
    db_tuning: bool = True                     # Apply DB tuning before import (toolkit)
    worker_count: int | None = None            # Parallel workers (toolkit/plugin_fast, 1-16)


class PushStartRequest(BaseModel):
    dry_run: bool = True
    push_bibs: bool = True
    push_patrons: bool = False
    push_holds: bool = False
    push_circ: bool = False
    reindex: bool = False
    reindex_engine: str | None = None           # "elasticsearch" | "zebra" (auto-detect if None)
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


# ── Patron Categories Push ───────────────────────────────────────────

class PatronCategoryPushItem(BaseModel):
    category_id: str
    name: str
    enrolmentperiod: int = 99          # months — default generous


class PushPatronCategoriesRequest(BaseModel):
    categories: list[PatronCategoryPushItem]


class PushPatronCategoryResult(BaseModel):
    category_id: str
    success: bool
    error: str | None = None


class PushPatronCategoriesResponse(BaseModel):
    total: int
    success_count: int
    failed_count: int
    results: list[PushPatronCategoryResult]


# ── Item Types Push ─────────────────────────────────────────────────

class ItemTypePushItem(BaseModel):
    item_type_id: str
    description: str


class PushItemTypesRequest(BaseModel):
    item_types: list[ItemTypePushItem]


class PushItemTypeResult(BaseModel):
    item_type_id: str
    success: bool
    error: str | None = None


class PushItemTypesResponse(BaseModel):
    total: int
    success_count: int
    failed_count: int
    results: list[PushItemTypeResult]


# ── TurboIndex (plugin) reindex ─────────────────────────────────────

# ── Authorised Values Push ───────────────────────────────────────

class AuthorisedValuePushItem(BaseModel):
    value: str
    description: str


class PushAuthorisedValuesRequest(BaseModel):
    category: str      # LOC, CCODE, NOT_LOAN, LOST, DAMAGED, WITHDRAWN
    values: list[AuthorisedValuePushItem]


class PushAuthorisedValueResult(BaseModel):
    value: str
    success: bool
    error: str | None = None


class PushAuthorisedValuesResponse(BaseModel):
    category: str
    total: int
    success_count: int
    failed_count: int
    results: list[PushAuthorisedValueResult]


# ── TurboIndex ───────────────────────────────────────────────────

class TurboIndexRequest(BaseModel):
    reset: bool = True
    processes: int = 4
    commit: int = 5000
    force_merge: bool = True


class TurboIndexResponse(BaseModel):
    task_id: str
    manifest_id: str
    message: str
