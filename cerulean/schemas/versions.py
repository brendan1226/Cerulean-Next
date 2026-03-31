"""
cerulean/schemas/versions.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for the Versioning API.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class CreateVersionRequest(BaseModel):
    data_type: str = "bib"
    label: str | None = None


class CreateVersionResponse(BaseModel):
    task_id: str
    message: str


class MigrationVersionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    data_type: str
    version_number: int
    label: str | None
    snapshot_path: str
    record_count: int
    run_duration_seconds: int | None
    load_success: int | None
    load_failed: int | None
    created_by: str | None
    created_at: datetime
    mapping_snapshot: dict | None = None
    quality_log: dict | None = None


class UpdateVersionRequest(BaseModel):
    label: str | None = None


class VersionDiffRequest(BaseModel):
    version_a: str
    version_b: str
    data_type: str = "bib"


class VersionDiffResponse(BaseModel):
    task_id: str
    message: str
