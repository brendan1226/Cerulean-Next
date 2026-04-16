"""
cerulean/schemas/projects.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Project and MARCFile API contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


# ── Project ────────────────────────────────────────────────────────────

class ProjectCreate(BaseModel):
    code: str
    library_name: str
    koha_url: str | None = None
    koha_token: str | None = None   # plaintext — encrypted before storage
    visibility: str = "private"     # "private" | "shared"


class ProjectUpdate(BaseModel):
    library_name: str | None = None
    koha_url: str | None = None
    koha_token: str | None = None
    koha_auth_type: str | None = None
    source_ils: str | None = None
    archived: bool | None = None
    visibility: str | None = None   # "private" | "shared"


class ProjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    code: str
    library_name: str
    source_ils: str | None
    ils_confidence: float | None
    koha_url: str | None
    koha_auth_type: str
    koha_version: str | None
    search_engine: str | None
    current_stage: int
    stage_1_complete: bool
    stage_2_complete: bool
    stage_3_complete: bool
    stage_4_complete: bool
    stage_5_complete: bool
    stage_6_complete: bool
    stage_7_complete: bool
    stage_8_complete: bool
    stage_9_complete: bool
    stage_10_complete: bool
    stage_11_complete: bool
    stage_12_complete: bool
    stage_13_complete: bool
    aspen_url: str | None
    evergreen_db_host: str | None
    item_structure: str | None
    items_csv_match_tag: str | None
    items_csv_key_column: str | None
    archived: bool
    owner_id: str | None = None
    visibility: str = "private"
    bib_count_ingested: int | None
    bib_count_post_dedup: int | None
    bib_count_pushed: int | None
    patron_count: int | None
    hold_count: int | None
    created_at: datetime
    updated_at: datetime


# ── MARCFile ───────────────────────────────────────────────────────────

class MARCFileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    filename: str
    file_format: str | None
    file_category: str
    file_size_bytes: int | None
    record_count: int | None
    ils_signal: str | None
    tag_frequency: dict | None
    column_headers: list | None
    status: str
    error_message: str | None
    sort_order: int
    created_at: datetime
    updated_at: datetime
    # AI Data Health Report (cerulean_ai_spec.md §3). Status fields only —
    # the full report body is fetched on demand via /health-report.
    health_report_status: str | None = None
    health_report_generated_at: datetime | None = None


class MARCFileUploadResponse(BaseModel):
    file_id: str
    filename: str
    task_id: str
    message: str


class TagFrequencyOut(BaseModel):
    file_id: str
    record_count: int
    tags: dict[str, int]   # {"245": 42118, "852": 41900, ...}
    subfield_frequency: dict[str, dict[str, int]] | None = None
