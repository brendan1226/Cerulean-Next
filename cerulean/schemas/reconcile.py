"""
cerulean/schemas/reconcile.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Stage 5 — Item Data Reconciliation API contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


# ── Rule CRUD ────────────────────────────────────────────────────────────


class ReconcileRuleCreate(BaseModel):
    vocab_category: str
    operation: str                          # rename | merge | split | delete
    source_values: list[str]
    target_value: str | None = None
    split_conditions: list[dict] | None = None
    delete_mode: str | None = None          # subfield | field
    sort_order: int = 0
    active: bool = True


class ReconcileRuleUpdate(BaseModel):
    vocab_category: str | None = None
    operation: str | None = None
    source_values: list[str] | None = None
    target_value: str | None = None
    split_conditions: list[dict] | None = None
    delete_mode: str | None = None
    sort_order: int | None = None
    active: bool | None = None


class ReconcileRuleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    vocab_category: str
    marc_tag: str
    marc_subfield: str
    operation: str
    source_values: list
    target_value: str | None
    split_conditions: list | None
    delete_mode: str | None
    sort_order: int
    active: bool
    ai_suggested: bool = False
    ai_confidence: float | None = None
    ai_reasoning: str | None = None
    created_at: datetime
    updated_at: datetime


# ── Source confirmation ──────────────────────────────────────────────────


class ConfirmSourceRequest(BaseModel):
    """Optionally override automatic source file resolution."""
    source_file: str | None = None


class ConfirmSourceResponse(BaseModel):
    source_file: str
    record_count: int
    message: str


# ── Scan ─────────────────────────────────────────────────────────────────


class ReconcileScanResponse(BaseModel):
    task_id: str
    message: str


class ReconcileScanStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None


class ScanValueOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    vocab_category: str
    source_value: str
    record_count: int
    scanned_at: datetime


# ── Apply ────────────────────────────────────────────────────────────────


class ReconcileApplyResponse(BaseModel):
    task_id: str
    message: str


class ReconcileApplyStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None


# ── Validate ─────────────────────────────────────────────────────────────


class ValidationIssue(BaseModel):
    vocab_category: str
    source_value: str
    record_count: int
    issue: str               # "unmatched" | "no_target" | etc.


class ValidateResponse(BaseModel):
    total_values: int
    matched_values: int
    unmatched_values: int
    issues: list[ValidationIssue]


# ── Report ───────────────────────────────────────────────────────────────


class ReconcileReportOut(BaseModel):
    task_id: str | None
    state: str
    result: dict | None = None
    error: str | None = None


# ── Phase 5: AI Code Reconciliation ──────────────────────────────────────


class ReconcileAiSuggestResponse(BaseModel):
    task_id: str
    message: str


class ReconcileAiSuggestStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None
