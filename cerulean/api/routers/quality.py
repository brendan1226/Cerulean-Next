"""
cerulean/api/routers/quality.py
─────────────────────────────────────────────────────────────────────────────
Stage 3 — MARC Data Quality & Remediation endpoints.

POST /projects/{id}/quality/scan              — dispatch quality scan
GET  /projects/{id}/quality/scan/status       — poll scan task
GET  /projects/{id}/quality/summary           — issue counts by category
GET  /projects/{id}/quality/issues            — paginated issue list
GET  /projects/{id}/quality/issues/{iid}      — single issue detail
POST /projects/{id}/quality/fix/{iid}         — manually fix one issue
POST /projects/{id}/quality/bulk-fix          — bulk auto-fix a category
POST /projects/{id}/quality/ignore/{iid}      — ignore one issue
POST /projects/{id}/quality/ignore-category   — ignore all in a category
POST /projects/{id}/quality/approve           — approve quality pass (gate)
"""

from datetime import datetime

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.database import get_db
from cerulean.models import QualityScanResult
from cerulean.schemas.quality import (
    QualityApproveResponse,
    QualityBulkFixRequest,
    QualityBulkFixResponse,
    QualityFixRequest,
    QualityIssueListResponse,
    QualityIssueOut,
    QualityScanRequest,
    QualityScanResponse,
    QualityScanStatusResponse,
    QualitySummaryCategory,
    QualitySummaryResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.quality import quality_bulk_fix_task, quality_scan_task

router = APIRouter(prefix="/projects", tags=["quality"])


# ── Scan ──────────────────────────────────────────────────────────────


@router.post("/{project_id}/quality/scan", response_model=QualityScanResponse, status_code=202)
async def start_quality_scan(
    project_id: str,
    body: QualityScanRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch quality scan task."""
    await require_project(project_id, db)
    scan_type = body.scan_type if body else "bib"
    file_ids = body.file_ids if body else None

    await audit_log(db, project_id, stage=3, level="info", tag="[quality]",
                    message=f"Quality scan dispatched (type={scan_type})")
    await db.flush()

    task = quality_scan_task.apply_async(
        args=[project_id],
        kwargs={"file_ids": file_ids, "scan_type": scan_type},
        queue="analyze",
    )

    return QualityScanResponse(task_id=task.id, message="Quality scan started.")


@router.get("/{project_id}/quality/scan/status", response_model=QualityScanStatusResponse)
async def quality_scan_status(
    project_id: str,
    task_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Poll quality scan task status."""
    await require_project(project_id, db)
    ar = AsyncResult(task_id, app=celery_app)
    state = ar.state
    progress = ar.info if state == "PROGRESS" else None
    result = ar.result if state == "SUCCESS" else None
    error = str(ar.info) if state == "FAILURE" else None
    return QualityScanStatusResponse(
        task_id=task_id, state=state, progress=progress, result=result, error=error,
    )


# ── Summary ──────────────────────────────────────────────────────────


@router.get("/{project_id}/quality/summary", response_model=QualitySummaryResponse)
async def quality_summary(
    project_id: str,
    scan_type: str = Query("bib"),
    db: AsyncSession = Depends(get_db),
):
    """Aggregate issue counts by category."""
    await require_project(project_id, db)

    # Get counts grouped by category, severity, status
    rows = (await db.execute(
        select(
            QualityScanResult.category,
            QualityScanResult.severity,
            QualityScanResult.status,
            func.count().label("cnt"),
        )
        .where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.scan_type == scan_type,
        )
        .group_by(QualityScanResult.category, QualityScanResult.severity, QualityScanResult.status)
    )).all()

    # Check which categories have auto-fix available
    fix_rows = (await db.execute(
        select(QualityScanResult.category)
        .where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.scan_type == scan_type,
            QualityScanResult.suggested_fix.isnot(None),
        )
        .group_by(QualityScanResult.category)
    )).scalars().all()
    has_fix_cats = set(fix_rows)

    # Distinct records affected
    total_affected = (await db.execute(
        select(func.count(func.distinct(QualityScanResult.record_index)))
        .where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.scan_type == scan_type,
        )
    )).scalar() or 0

    # Build category summaries
    cat_data: dict[str, dict] = {}
    for cat, sev, status, cnt in rows:
        d = cat_data.setdefault(cat, {
            "category": cat, "total": 0,
            "errors": 0, "warnings": 0, "info": 0,
            "unresolved": 0, "auto_fixed": 0, "manually_fixed": 0, "ignored": 0,
        })
        d["total"] += cnt
        if sev == "error":
            d["errors"] += cnt
        elif sev == "warning":
            d["warnings"] += cnt
        else:
            d["info"] += cnt
        if status in d:
            d[status] += cnt

    categories = [
        QualitySummaryCategory(**d, has_auto_fix=d["category"] in has_fix_cats)
        for d in cat_data.values()
    ]
    categories.sort(key=lambda c: (-c.errors, -c.warnings, c.category))

    total_issues = sum(c.total for c in categories)

    return QualitySummaryResponse(
        total_issues=total_issues,
        total_records_affected=total_affected,
        categories=categories,
        scan_type=scan_type,
    )


# ── Issues ───────────────────────────────────────────────────────────


@router.get("/{project_id}/quality/issues", response_model=QualityIssueListResponse)
async def list_quality_issues(
    project_id: str,
    scan_type: str = Query("bib"),
    category: str | None = Query(None),
    severity: str | None = Query(None),
    status: str | None = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Paginated, filterable issue list."""
    await require_project(project_id, db)

    q = select(QualityScanResult).where(
        QualityScanResult.project_id == project_id,
        QualityScanResult.scan_type == scan_type,
    )
    count_q = select(func.count()).select_from(QualityScanResult).where(
        QualityScanResult.project_id == project_id,
        QualityScanResult.scan_type == scan_type,
    )

    if category:
        q = q.where(QualityScanResult.category == category)
        count_q = count_q.where(QualityScanResult.category == category)
    if severity:
        q = q.where(QualityScanResult.severity == severity)
        count_q = count_q.where(QualityScanResult.severity == severity)
    if status:
        q = q.where(QualityScanResult.status == status)
        count_q = count_q.where(QualityScanResult.status == status)

    total = (await db.execute(count_q)).scalar() or 0
    issues = (await db.execute(
        q.order_by(QualityScanResult.record_index, QualityScanResult.category)
        .offset(offset).limit(limit)
    )).scalars().all()

    return QualityIssueListResponse(issues=issues, total=total, offset=offset, limit=limit)


@router.get("/{project_id}/quality/issues/{issue_id}", response_model=QualityIssueOut)
async def get_quality_issue(
    project_id: str,
    issue_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get a single issue with full detail."""
    await require_project(project_id, db)
    issue = await db.get(QualityScanResult, issue_id)
    if not issue or issue.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Issue not found."})
    return issue


# ── Fix ──────────────────────────────────────────────────────────────


@router.post("/{project_id}/quality/fix/{issue_id}", response_model=QualityIssueOut)
async def fix_quality_issue(
    project_id: str,
    issue_id: str,
    body: QualityFixRequest,
    db: AsyncSession = Depends(get_db),
):
    """Manually fix a single issue."""
    await require_project(project_id, db)
    issue = await db.get(QualityScanResult, issue_id)
    if not issue or issue.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Issue not found."})

    issue.status = "manually_fixed"
    issue.fixed_value = body.fixed_value
    issue.fixed_at = datetime.utcnow()
    issue.fixed_by = "manual"
    await db.flush()
    await db.refresh(issue)

    await audit_log(db, project_id, stage=3, level="info", tag="[quality-fix]",
                    message=f"Issue manually fixed: {issue.category} in record {issue.record_index}")

    return issue


@router.post("/{project_id}/quality/bulk-fix", response_model=QualityBulkFixResponse, status_code=202)
async def bulk_fix_quality(
    project_id: str,
    body: QualityBulkFixRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch bulk auto-fix for a category."""
    await require_project(project_id, db)

    await audit_log(db, project_id, stage=3, level="info", tag="[quality-fix]",
                    message=f"Bulk fix dispatched for category '{body.category}'")
    await db.flush()

    task = quality_bulk_fix_task.apply_async(
        args=[project_id, body.category],
        queue="analyze",
    )
    return QualityBulkFixResponse(task_id=task.id, message=f"Bulk fix started for '{body.category}'.")


# ── Ignore ───────────────────────────────────────────────────────────


@router.post("/{project_id}/quality/ignore/{issue_id}", response_model=QualityIssueOut)
async def ignore_quality_issue(
    project_id: str,
    issue_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Ignore a single issue."""
    await require_project(project_id, db)
    issue = await db.get(QualityScanResult, issue_id)
    if not issue or issue.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Issue not found."})

    issue.status = "ignored"
    issue.fixed_at = datetime.utcnow()
    issue.fixed_by = "ignored"
    await db.flush()
    await db.refresh(issue)
    return issue


@router.post("/{project_id}/quality/ignore-category")
async def ignore_quality_category(
    project_id: str,
    category: str = Query(...),
    scan_type: str = Query("bib"),
    db: AsyncSession = Depends(get_db),
):
    """Ignore all unresolved issues in a category."""
    await require_project(project_id, db)

    result = await db.execute(
        update(QualityScanResult)
        .where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.scan_type == scan_type,
            QualityScanResult.category == category,
            QualityScanResult.status == "unresolved",
        )
        .values(status="ignored", fixed_at=datetime.utcnow(), fixed_by="ignored")
    )
    count = result.rowcount

    await audit_log(db, project_id, stage=3, level="info", tag="[quality]",
                    message=f"Ignored {count} issues in category '{category}'")

    return {"category": category, "ignored_count": count}


# ── Stage Gate ───────────────────────────────────────────────────────


@router.post("/{project_id}/quality/approve", response_model=QualityApproveResponse)
async def approve_quality_pass(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Approve quality pass — stage gate for Stage 3.

    Checks that all error-level issues have been resolved or ignored.
    Marks stage_3_complete and advances current_stage.
    """
    project = await require_project(project_id, db)

    # Count unresolved errors
    unresolved_errors = (await db.execute(
        select(func.count()).select_from(QualityScanResult).where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.severity == "error",
            QualityScanResult.status == "unresolved",
        )
    )).scalar() or 0

    if unresolved_errors > 0:
        return QualityApproveResponse(
            approved=False,
            unresolved_errors=unresolved_errors,
            message=f"{unresolved_errors} unresolved error(s) remain. Resolve or ignore them first.",
        )

    project.stage_3_complete = True
    if (project.current_stage or 0) < 4:
        project.current_stage = 4

    await audit_log(db, project_id, stage=3, level="info", tag="[quality]",
                    message="Quality pass approved — Stage 3 complete")
    await db.flush()

    return QualityApproveResponse(
        approved=True,
        unresolved_errors=0,
        message="Quality pass approved. Proceed to Stage 4.",
    )


# ── Clear ────────────────────────────────────────────────────────────


@router.post("/{project_id}/quality/clear")
async def clear_quality_results(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete all quality scan results for a project."""
    project = await require_project(project_id, db)

    result = await db.execute(
        delete(QualityScanResult).where(QualityScanResult.project_id == project_id)
    )
    deleted = result.rowcount

    project.stage_3_complete = False

    await audit_log(db, project_id, stage=3, level="info", tag="[quality]",
                    message=f"Cleared {deleted} quality scan results")

    return {"deleted": deleted}
