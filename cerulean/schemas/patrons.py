"""
cerulean/schemas/patrons.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Stage 6 — Patron Data Transformation API contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


# ── Patron File ───────────────────────────────────────────────────────────


class PatronFileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    filename: str
    file_format: str | None
    detected_format: str | None
    parse_settings: dict | None
    row_count: int | None
    column_headers: list | None
    status: str
    uploaded_at: datetime


class PatronFileUploadResponse(BaseModel):
    file_id: str
    filename: str
    task_id: str
    message: str


class AvailablePatronFile(BaseModel):
    """A Stage 1 uploaded file that may contain patron data."""
    file_id: str
    filename: str
    file_format: str | None
    file_size_bytes: int | None
    record_count: int | None
    status: str
    is_patron_likely: bool


class SelectFileRequest(BaseModel):
    """Request body for selecting an existing Stage 1 file as patron data."""
    marc_file_id: str
    # Same semantics as the upload endpoint: auto-approve column maps for
    # columns whose names already match Koha borrower headers.
    auto_map_headers: bool = False


class PatronPreviewResponse(BaseModel):
    rows: list[dict]
    total_rows: int
    columns: list[str]


# ── Column Mapping ────────────────────────────────────────────────────────


class PatronColumnMapCreate(BaseModel):
    source_column: str
    target_header: str | None = None
    ignored: bool = False
    transform_type: str | None = None
    transform_config: dict | None = None
    is_controlled_list: bool = False
    sort_order: int = 0


class PatronColumnMapUpdate(BaseModel):
    target_header: str | None = None
    ignored: bool | None = None
    transform_type: str | None = None
    transform_config: dict | None = None
    is_controlled_list: bool | None = None
    approved: bool | None = None
    sort_order: int | None = None


class PatronColumnMapOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    source_column: str
    target_header: str | None
    ignored: bool
    transform_type: str | None
    transform_config: dict | None
    is_controlled_list: bool
    approved: bool
    ai_suggested: bool
    ai_confidence: float | None
    ai_reasoning: str | None
    source_label: str
    sort_order: int
    created_at: datetime
    updated_at: datetime


class PatronAISuggestResponse(BaseModel):
    task_id: str
    message: str


class ApproveAllRequest(BaseModel):
    min_confidence: float = 0.85


class ApproveAllResponse(BaseModel):
    approved_count: int
    message: str


# ── Value Rules ───────────────────────────────────────────────────────────


class PatronRuleCreate(BaseModel):
    koha_header: str
    operation: str                          # rename | merge | split | delete
    source_values: list[str]
    target_value: str | None = None
    split_conditions: list[dict] | None = None
    delete_mode: str | None = None          # exclude_row | blank_field
    sort_order: int = 0
    active: bool = True


class PatronRuleUpdate(BaseModel):
    koha_header: str | None = None
    operation: str | None = None
    source_values: list[str] | None = None
    target_value: str | None = None
    split_conditions: list[dict] | None = None
    delete_mode: str | None = None
    sort_order: int | None = None
    active: bool | None = None


class PatronRuleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    koha_header: str
    operation: str
    source_values: list
    target_value: str | None
    split_conditions: list | None
    delete_mode: str | None
    sort_order: int
    active: bool
    created_at: datetime
    updated_at: datetime


# ── Scan ──────────────────────────────────────────────────────────────────


class PatronScanResponse(BaseModel):
    task_id: str
    message: str


class PatronScanStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None


class PatronScanValueOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    koha_header: str
    source_value: str
    record_count: int
    scanned_at: datetime


# ── Apply ─────────────────────────────────────────────────────────────────


class PatronApplyResponse(BaseModel):
    task_id: str
    message: str


class PatronApplyStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None


# ── Validate ──────────────────────────────────────────────────────────────


class PatronValidationIssue(BaseModel):
    koha_header: str
    source_value: str
    record_count: int
    issue: str               # "unmatched" | "no_target" | etc.


class PatronValidateResponse(BaseModel):
    total_values: int
    matched_values: int
    unmatched_values: int
    issues: list[PatronValidationIssue]


# ── Report ────────────────────────────────────────────────────────────────


class PatronReportOut(BaseModel):
    task_id: str | None
    state: str
    result: dict | None = None
    error: str | None = None
