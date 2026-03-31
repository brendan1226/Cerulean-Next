"""
cerulean/schemas/quality.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Stage 3 — MARC Data Quality.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class QualityScanRequest(BaseModel):
    file_ids: list[str] | None = None
    scan_type: str = "bib"  # "bib" | "item"


class QualityScanResponse(BaseModel):
    task_id: str
    message: str


class QualityScanStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None


class QualitySummaryCategory(BaseModel):
    category: str
    total: int
    errors: int
    warnings: int
    info: int
    unresolved: int
    auto_fixed: int
    manually_fixed: int
    ignored: int
    has_auto_fix: bool


class QualitySummaryResponse(BaseModel):
    total_issues: int
    total_records_affected: int
    categories: list[QualitySummaryCategory]
    scan_type: str


class QualityIssueOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    scan_type: str
    category: str
    severity: str
    record_index: int
    file_id: str | None
    tag: str | None
    subfield: str | None
    description: str
    original_value: str | None
    suggested_fix: str | None
    status: str
    fixed_value: str | None
    fixed_by: str | None
    fixed_at: datetime | None
    created_at: datetime


class QualityFixRequest(BaseModel):
    fixed_value: str


class QualityBulkFixRequest(BaseModel):
    category: str


class QualityBulkFixResponse(BaseModel):
    task_id: str
    message: str


class QualityApproveResponse(BaseModel):
    approved: bool
    unresolved_errors: int
    message: str


class QualityIssueListResponse(BaseModel):
    issues: list[QualityIssueOut]
    total: int
    offset: int
    limit: int
