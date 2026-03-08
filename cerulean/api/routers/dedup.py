"""
cerulean/api/routers/dedup.py
─────────────────────────────────────────────────────────────────────────────
Stage 4 — Dedup & Validate API endpoints.

GET    /projects/{id}/dedup/rules           — list all rules
POST   /projects/{id}/dedup/rules           — create rule
PATCH  /projects/{id}/dedup/rules/{rid}     — update rule
DELETE /projects/{id}/dedup/rules/{rid}     — delete rule + cascaded clusters
POST   /projects/{id}/dedup/presets         — create all 4 preset rules
POST   /projects/{id}/dedup/scan            — dispatch scan task
POST   /projects/{id}/dedup/apply           — dispatch apply task
GET    /projects/{id}/dedup/clusters        — paginated cluster list
PATCH  /projects/{id}/dedup/clusters/{cid}  — resolve cluster
GET    /projects/{id}/dedup/status          — poll Celery task status
"""

import uuid
from datetime import datetime

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.database import get_db
from cerulean.models import AuditEvent, DedupCluster, DedupRule, Project
from cerulean.schemas.dedup import (
    ClusterPage,
    ClusterResolveRequest,
    DedupApplyRequest,
    DedupApplyResponse,
    DedupClusterOut,
    DedupRuleCreate,
    DedupRuleOut,
    DedupRuleUpdate,
    DedupScanRequest,
    DedupScanResponse,
    DedupStatusResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.dedup import dedup_apply_task, dedup_scan_task

router = APIRouter(prefix="/projects", tags=["dedup"])

# Preset field configs (mirrored from tasks/dedup.py for auto-populate)
_PRESET_FIELDS = {
    "001": [{"tag": "001", "sub": None, "match_type": "exact"}],
    "isbn": [{"tag": "020", "sub": "a", "match_type": "normalised"}],
    "title_author": [
        {"tag": "245", "sub": "a", "match_type": "normalised"},
        {"tag": "100", "sub": "a", "match_type": "normalised"},
    ],
    "oclc": [{"tag": "035", "sub": "a", "match_type": "exact"}],
}

_PRESET_NAMES = {
    "001": "Control Number (001)",
    "isbn": "ISBN (020$a)",
    "title_author": "Title + Author (245$a / 100$a)",
    "oclc": "OCLC Number (035$a)",
}


# ── Rules CRUD ───────────────────────────────────────────────────────────


@router.get("/{project_id}/dedup/rules", response_model=list[DedupRuleOut])
async def list_dedup_rules(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    await _require_project(project_id, db)
    result = await db.execute(
        select(DedupRule)
        .where(DedupRule.project_id == project_id)
        .order_by(DedupRule.created_at)
    )
    return result.scalars().all()


@router.post("/{project_id}/dedup/rules", response_model=DedupRuleOut, status_code=201)
async def create_dedup_rule(
    project_id: str,
    body: DedupRuleCreate,
    db: AsyncSession = Depends(get_db),
):
    await _require_project(project_id, db)

    fields = body.fields
    is_preset = False
    if body.preset_key and body.preset_key in _PRESET_FIELDS:
        fields = _PRESET_FIELDS[body.preset_key]
        is_preset = True

    # If setting active, deactivate others
    if body.active:
        await _deactivate_all_rules(project_id, db)

    rule = DedupRule(
        project_id=project_id,
        name=body.name,
        is_preset=is_preset,
        preset_key=body.preset_key,
        fields=fields,
        on_duplicate=body.on_duplicate,
        active=body.active,
    )
    db.add(rule)
    await db.flush()
    await db.refresh(rule)
    return rule


@router.patch("/{project_id}/dedup/rules/{rule_id}", response_model=DedupRuleOut)
async def update_dedup_rule(
    project_id: str,
    rule_id: str,
    body: DedupRuleUpdate,
    db: AsyncSession = Depends(get_db),
):
    await _require_project(project_id, db)
    rule = await db.get(DedupRule, rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})

    # If setting active=True, deactivate others first
    if body.active is True:
        await _deactivate_all_rules(project_id, db)

    update_data = body.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(rule, key, value)

    await db.flush()
    await db.refresh(rule)
    return rule


@router.delete("/{project_id}/dedup/rules/{rule_id}", status_code=204)
async def delete_dedup_rule(
    project_id: str,
    rule_id: str,
    db: AsyncSession = Depends(get_db),
):
    await _require_project(project_id, db)
    rule = await db.get(DedupRule, rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})
    await db.delete(rule)
    await db.flush()


@router.post("/{project_id}/dedup/presets", response_model=list[DedupRuleOut], status_code=201)
async def create_preset_rules(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Create all 4 preset dedup rules if they don't already exist."""
    await _require_project(project_id, db)

    # Check which presets already exist
    result = await db.execute(
        select(DedupRule.preset_key)
        .where(DedupRule.project_id == project_id, DedupRule.is_preset == True)  # noqa: E712
    )
    existing_keys = {r for r in result.scalars().all()}

    created: list[DedupRule] = []
    for key, fields in _PRESET_FIELDS.items():
        if key in existing_keys:
            continue
        rule = DedupRule(
            project_id=project_id,
            name=_PRESET_NAMES[key],
            is_preset=True,
            preset_key=key,
            fields=fields,
            on_duplicate="keep_first",
            active=False,
        )
        db.add(rule)
        created.append(rule)

    await db.flush()
    for r in created:
        await db.refresh(r)
    return created


# ── Scan & Apply ─────────────────────────────────────────────────────────


@router.post("/{project_id}/dedup/scan", response_model=DedupScanResponse, status_code=202)
async def scan_dedup(
    project_id: str,
    body: DedupScanRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch dedup scan task. Precondition: stage_3_complete."""
    project = await _require_project(project_id, db)

    if not project.stage_3_complete:
        raise HTTPException(409, detail={
            "error": "STAGE_3_INCOMPLETE",
            "message": "Stage 3 (merge) must be complete before dedup scan.",
        })

    # Verify rule exists
    rule = await db.get(DedupRule, body.rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})

    await _log(db, project_id, stage=4, level="info", tag="[dedup-scan]",
               message=f"Dedup scan dispatched for rule '{rule.name}'")
    await db.flush()

    task = dedup_scan_task.apply_async(
        args=[project_id, body.rule_id],
        queue="dedup",
    )

    return DedupScanResponse(
        task_id=task.id, rule_id=body.rule_id,
        message=f"Dedup scan started for rule '{rule.name}'.",
    )


@router.post("/{project_id}/dedup/apply", response_model=DedupApplyResponse, status_code=202)
async def apply_dedup(
    project_id: str,
    body: DedupApplyRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch dedup apply task. Precondition: scan results exist."""
    project = await _require_project(project_id, db)

    # Resolve rule_id
    rule_id = body.rule_id
    if not rule_id:
        # Find active rule
        result = await db.execute(
            select(DedupRule)
            .where(DedupRule.project_id == project_id, DedupRule.active == True)  # noqa: E712
            .limit(1)
        )
        rule = result.scalar_one_or_none()
        if not rule:
            raise HTTPException(409, detail={
                "error": "NO_ACTIVE_RULE",
                "message": "No active dedup rule. Set a rule as active or provide rule_id.",
            })
        rule_id = rule.id
    else:
        rule = await db.get(DedupRule, rule_id)
        if not rule or rule.project_id != project_id:
            raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})

    # Verify scan results exist
    if not rule.last_scan_clusters and rule.last_scan_clusters != 0:
        raise HTTPException(409, detail={
            "error": "NO_SCAN_RESULTS",
            "message": "Run a dedup scan first before applying.",
        })

    await _log(db, project_id, stage=4, level="info", tag="[dedup-apply]",
               message=f"Dedup apply dispatched for rule '{rule.name}'")
    await db.flush()

    task = dedup_apply_task.apply_async(
        args=[project_id, rule_id],
        queue="dedup",
    )

    return DedupApplyResponse(
        task_id=task.id,
        message=f"Dedup apply started for rule '{rule.name}'.",
    )


# ── Clusters ─────────────────────────────────────────────────────────────


@router.get("/{project_id}/dedup/clusters", response_model=ClusterPage)
async def list_dedup_clusters(
    project_id: str,
    rule_id: str = Query(..., description="DedupRule ID to list clusters for"),
    resolved: bool | None = Query(None, description="Filter by resolved status"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    await _require_project(project_id, db)

    # Verify rule belongs to project
    rule = await db.get(DedupRule, rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})

    q = select(DedupCluster).where(DedupCluster.rule_id == rule_id)
    count_q = select(func.count()).select_from(DedupCluster).where(DedupCluster.rule_id == rule_id)

    if resolved is not None:
        q = q.where(DedupCluster.resolved == resolved)
        count_q = count_q.where(DedupCluster.resolved == resolved)

    total_result = await db.execute(count_q)
    total = total_result.scalar() or 0

    q = q.order_by(DedupCluster.created_at).offset(skip).limit(limit)
    result = await db.execute(q)
    items = result.scalars().all()

    return ClusterPage(total=total, items=items)


@router.patch("/{project_id}/dedup/clusters/{cluster_id}", response_model=DedupClusterOut)
async def resolve_cluster(
    project_id: str,
    cluster_id: str,
    body: ClusterResolveRequest,
    db: AsyncSession = Depends(get_db),
):
    await _require_project(project_id, db)

    cluster = await db.get(DedupCluster, cluster_id)
    if not cluster:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Cluster not found."})

    # Verify cluster belongs to this project via its rule
    rule = await db.get(DedupRule, cluster.rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Cluster not found."})

    cluster.primary_index = body.primary_index
    cluster.resolved = True
    await db.flush()
    await db.refresh(cluster)
    return cluster


# ── Status ───────────────────────────────────────────────────────────────


@router.get("/{project_id}/dedup/status", response_model=DedupStatusResponse)
async def dedup_status(
    project_id: str,
    task_id: str | None = Query(None, description="Celery task_id to poll"),
    db: AsyncSession = Depends(get_db),
):
    await _require_project(project_id, db)

    if not task_id:
        return DedupStatusResponse(task_id=None, state="IDLE")

    async_result = AsyncResult(task_id, app=celery_app)
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

    return DedupStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


# ── Helpers ──────────────────────────────────────────────────────────────


async def _require_project(project_id: str, db: AsyncSession) -> Project:
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})
    return project


async def _log(db: AsyncSession, project_id: str, stage: int, level: str, tag: str, message: str) -> None:
    event = AuditEvent(
        id=str(uuid.uuid4()),
        project_id=project_id,
        stage=stage,
        level=level,
        tag=tag,
        message=message,
    )
    db.add(event)


async def _deactivate_all_rules(project_id: str, db: AsyncSession) -> None:
    await db.execute(
        update(DedupRule)
        .where(DedupRule.project_id == project_id)
        .values(active=False)
    )
