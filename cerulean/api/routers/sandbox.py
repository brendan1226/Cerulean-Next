"""
cerulean/api/routers/sandbox.py
─────────────────────────────────────────────────────────────────────────────
Stage 6 — KTD Sandbox API endpoints.

POST  /projects/{id}/sandbox/provision   — provision KTD sandbox
GET   /projects/{id}/sandbox/status      — current sandbox status
POST  /projects/{id}/sandbox/teardown    — teardown KTD sandbox
GET   /projects/{id}/sandbox/instances   — list all instances
"""

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.database import get_db
from cerulean.models import SandboxInstance
from cerulean.schemas.sandbox import (
    SandboxInstanceOut,
    SandboxProvisionRequest,
    SandboxProvisionResponse,
    SandboxStatusResponse,
    SandboxTeardownResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.sandbox import ktd_provision_task, ktd_teardown_task

router = APIRouter(prefix="/projects", tags=["sandbox"])


# ── Endpoints ────────────────────────────────────────────────────────────


@router.post("/{project_id}/sandbox/provision", response_model=SandboxProvisionResponse, status_code=202)
async def provision_sandbox(
    project_id: str,
    body: SandboxProvisionRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Provision a KTD sandbox. Precondition: no active sandbox."""
    await require_project(project_id, db)

    # Check no active sandbox
    result = await db.execute(
        select(SandboxInstance)
        .where(
            SandboxInstance.project_id == project_id,
            SandboxInstance.status.in_(["provisioning", "running"]),
        )
        .limit(1)
    )
    if result.scalar_one_or_none():
        raise HTTPException(409, detail={
            "error": "SANDBOX_ACTIVE",
            "message": "An active sandbox already exists. Teardown first.",
        })

    koha_version = body.koha_version if body else None

    instance = SandboxInstance(
        project_id=project_id,
        status="provisioning",
    )
    db.add(instance)
    await audit_log(db, project_id, stage=6, level="info", tag="[sandbox]",
                    message="KTD sandbox provision dispatched")
    await db.flush()
    await db.refresh(instance)

    task = ktd_provision_task.apply_async(
        args=[project_id, instance.id],
        kwargs={"koha_version": koha_version},
        queue="sandbox",
    )
    instance.celery_task_id = task.id
    await db.flush()

    return SandboxProvisionResponse(
        task_id=task.id, instance_id=instance.id,
        message="KTD sandbox provisioning started.",
    )


@router.get("/{project_id}/sandbox/status", response_model=SandboxStatusResponse)
async def sandbox_status(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Current sandbox status."""
    await require_project(project_id, db)

    # Find most recent active instance
    result = await db.execute(
        select(SandboxInstance)
        .where(SandboxInstance.project_id == project_id)
        .order_by(SandboxInstance.created_at.desc())
        .limit(1)
    )
    instance = result.scalar_one_or_none()
    if not instance:
        return SandboxStatusResponse(task_id=None, state="none")

    if not instance.celery_task_id:
        return SandboxStatusResponse(task_id=None, state=instance.status)

    async_result = AsyncResult(instance.celery_task_id, app=celery_app)
    state = async_result.state
    progress = None
    result_data = None
    error = None

    if state == "PROGRESS":
        progress = async_result.info
    elif state == "SUCCESS":
        result_data = async_result.result
    elif state == "FAILURE":
        error = str(async_result.info)

    return SandboxStatusResponse(
        task_id=instance.celery_task_id, state=state,
        progress=progress, result=result_data, error=error,
    )


@router.post("/{project_id}/sandbox/teardown", response_model=SandboxTeardownResponse, status_code=202)
async def teardown_sandbox(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Teardown active KTD sandbox. Precondition: active sandbox exists."""
    await require_project(project_id, db)

    # Find active instance
    result = await db.execute(
        select(SandboxInstance)
        .where(
            SandboxInstance.project_id == project_id,
            SandboxInstance.status.in_(["provisioning", "running"]),
        )
        .limit(1)
    )
    instance = result.scalar_one_or_none()
    if not instance:
        raise HTTPException(409, detail={
            "error": "NO_ACTIVE_SANDBOX",
            "message": "No active sandbox to teardown.",
        })

    await audit_log(db, project_id, stage=6, level="info", tag="[sandbox]",
                    message="KTD sandbox teardown dispatched")
    await db.flush()

    task = ktd_teardown_task.apply_async(
        args=[project_id, instance.id],
        queue="sandbox",
    )

    return SandboxTeardownResponse(
        task_id=task.id,
        message="KTD sandbox teardown started.",
    )


@router.get("/{project_id}/sandbox/instances", response_model=list[SandboxInstanceOut])
async def list_instances(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all sandbox instances for a project."""
    await require_project(project_id, db)

    result = await db.execute(
        select(SandboxInstance)
        .where(SandboxInstance.project_id == project_id)
        .order_by(SandboxInstance.created_at.desc())
    )
    return result.scalars().all()
