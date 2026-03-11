"""
cerulean/schemas/sandbox.py
─────────────────────────────────────────────────────────────────────────────
Pydantic v2 schemas for Stage 8 — KTD Sandbox API contracts.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class SandboxProvisionRequest(BaseModel):
    koha_version: str | None = None


class SandboxProvisionResponse(BaseModel):
    task_id: str
    instance_id: str
    message: str


class SandboxInstanceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    container_id: str | None
    container_name: str | None
    status: str
    koha_url: str | None
    exposed_port: int | None
    celery_task_id: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime


class SandboxTeardownResponse(BaseModel):
    task_id: str
    message: str


class SandboxStatusResponse(BaseModel):
    task_id: str | None
    state: str
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None
