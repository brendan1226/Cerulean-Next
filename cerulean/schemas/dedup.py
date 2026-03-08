"""
cerulean/schemas/dedup.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Stage 4 — Dedup & Validate API contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


# ── Rule CRUD ────────────────────────────────────────────────────────────

class DedupRuleCreate(BaseModel):
    name: str
    preset_key: str | None = None
    fields: list[dict] | None = None        # [{tag, sub, match_type}]
    on_duplicate: str = "keep_first"
    active: bool = False


class DedupRuleUpdate(BaseModel):
    name: str | None = None
    preset_key: str | None = None
    fields: list[dict] | None = None
    on_duplicate: str | None = None
    active: bool | None = None


class DedupRuleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    name: str
    is_preset: bool
    preset_key: str | None
    fields: list[dict] | None
    on_duplicate: str
    active: bool
    last_scan_clusters: int | None
    last_scan_affected: int | None
    last_scan_at: datetime | None
    created_at: datetime


# ── Cluster ──────────────────────────────────────────────────────────────

class DedupClusterOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    rule_id: str
    records: list[dict]
    primary_index: int
    match_key: str | None
    resolved: bool
    created_at: datetime


class ClusterPage(BaseModel):
    total: int
    items: list[DedupClusterOut]


class ClusterResolveRequest(BaseModel):
    primary_index: int


# ── Scan / Apply ─────────────────────────────────────────────────────────

class DedupScanRequest(BaseModel):
    rule_id: str


class DedupScanResponse(BaseModel):
    task_id: str
    rule_id: str
    message: str


class DedupApplyRequest(BaseModel):
    rule_id: str | None = None   # None = use active rule


class DedupApplyResponse(BaseModel):
    task_id: str
    message: str


class DedupStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None
