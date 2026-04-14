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
from pathlib import Path

import pymarc
from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import MARCFile, QualityScanResult
from cerulean.utils.marc import record_to_dict

settings = get_settings()
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


@router.post("/{project_id}/quality/ignore-record/{record_index}")
async def ignore_record_issues(
    project_id: str,
    record_index: int,
    db: AsyncSession = Depends(get_db),
):
    """Ignore all unresolved issues for a specific record."""
    await require_project(project_id, db)
    result = await db.execute(
        update(QualityScanResult)
        .where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.record_index == record_index,
            QualityScanResult.status == "unresolved",
        )
        .values(status="ignored", fixed_at=datetime.utcnow(), fixed_by="ignored")
    )
    return {"record_index": record_index, "ignored_count": result.rowcount}


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


# ── Record View & Edit ───────────────────────────────────────────────


@router.get("/{project_id}/quality/record/{record_index}")
async def get_quality_record(
    project_id: str,
    record_index: int,
    file_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get a full MARC record with its quality issues annotated."""
    await require_project(project_id, db)

    # If no file_id, try to get it from the issue itself
    actual_file_id = file_id
    if not actual_file_id:
        issue_row = (await db.execute(
            select(QualityScanResult.file_id).where(
                QualityScanResult.project_id == project_id,
                QualityScanResult.record_index == record_index,
                QualityScanResult.file_id.isnot(None),
            ).limit(1)
        )).scalar_one_or_none()
        if issue_row:
            actual_file_id = issue_row

    # Load all indexed MARC files in scan order
    files_result = await db.execute(
        select(MARCFile).where(
            MARCFile.project_id == project_id,
            MARCFile.file_category == "marc",
            MARCFile.status == "indexed",
        ).order_by(MARCFile.sort_order, MARCFile.created_at)
    )
    all_files = files_result.scalars().all()
    if not all_files:
        raise HTTPException(404, detail={"error": "NO_FILE", "message": "No MARC file found."})

    # If we know the file_id, only search that file — compute local offset
    # by counting records in files that come before it in scan order
    record = None
    found_file_id = None
    if actual_file_id:
        # Count records in files before the target file
        offset = 0
        for f in all_files:
            if f.id == actual_file_id:
                break
            offset += f.record_count or 0
        local_idx = record_index - offset
        marc_file = await db.get(MARCFile, actual_file_id)
        if marc_file and local_idx >= 0:
            try:
                count = 0
                with open(marc_file.storage_path, "rb") as fh:
                    reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
                    for rec in reader:
                        if rec is None:
                            continue
                        if count == local_idx:
                            record = rec
                            found_file_id = actual_file_id
                            break
                        count += 1
            except Exception:
                pass
    else:
        # No file_id — iterate all files globally
        global_idx = 0
        for f in all_files:
            try:
                with open(f.storage_path, "rb") as fh:
                    reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
                    for rec in reader:
                        if rec is None:
                            continue
                        if global_idx == record_index:
                            record = rec
                            found_file_id = f.id
                            break
                        global_idx += 1
                if record:
                    break
            except Exception:
                continue

    if not record:
        raise HTTPException(404, detail={"error": "RECORD_NOT_FOUND", "message": f"No record at index {record_index}."})

    # Get issues for this record
    issues_result = await db.execute(
        select(QualityScanResult).where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.record_index == record_index,
        ).order_by(QualityScanResult.category)
    )
    issues = issues_result.scalars().all()

    # Build annotated field list
    issue_tags = {}
    for iss in issues:
        key = iss.tag or "LDR"
        issue_tags.setdefault(key, []).append({
            "id": iss.id,
            "category": iss.category,
            "severity": iss.severity,
            "description": iss.description,
            "status": iss.status,
            "original_value": iss.original_value,
            "suggested_fix": iss.suggested_fix,
        })

    record_dict = record_to_dict(record, record_index)

    return {
        "record_index": record_index,
        "file_id": found_file_id,
        "record": record_dict,
        "issues": [
            {
                "id": iss.id, "category": iss.category, "severity": iss.severity,
                "tag": iss.tag, "subfield": iss.subfield, "description": iss.description,
                "status": iss.status, "original_value": iss.original_value,
                "suggested_fix": iss.suggested_fix,
            }
            for iss in issues
        ],
        "issue_tags": issue_tags,
        "total_issues": len(issues),
    }


@router.post("/{project_id}/quality/record/{record_index}/edit")
async def edit_quality_record(
    project_id: str,
    record_index: int,
    body: dict,
    file_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Edit a MARC record field and save back to the file.

    Body: { "tag": "245", "subfield": "$a", "old_value": "...", "new_value": "..." }
    For leader edits: { "tag": "LDR", "position": 6, "new_value": "a" }
    For control fields: { "tag": "008", "new_value": "..." }
    """
    await require_project(project_id, db)

    # Find the file — prefer explicit file_id, then look up from issue data
    actual_file_id = file_id
    if not actual_file_id:
        issue_row = (await db.execute(
            select(QualityScanResult.file_id).where(
                QualityScanResult.project_id == project_id,
                QualityScanResult.record_index == record_index,
                QualityScanResult.file_id.isnot(None),
            ).limit(1)
        )).scalar_one_or_none()
        if issue_row:
            actual_file_id = issue_row

    marc_file = None
    local_index = record_index

    if actual_file_id:
        marc_file = await db.get(MARCFile, actual_file_id)
        # Compute local index by subtracting records from prior files
        files_result = await db.execute(
            select(MARCFile).where(
                MARCFile.project_id == project_id,
                MARCFile.file_category == "marc",
                MARCFile.status == "indexed",
            ).order_by(MARCFile.sort_order, MARCFile.created_at)
        )
        for f in files_result.scalars().all():
            if f.id == actual_file_id:
                break
            local_index -= f.record_count or 0
    else:
        result = await db.execute(
            select(MARCFile).where(
                MARCFile.project_id == project_id,
                MARCFile.file_category == "marc",
                MARCFile.status == "indexed",
            ).order_by(MARCFile.sort_order, MARCFile.created_at).limit(1)
        )
        marc_file = result.scalar_one_or_none()

    if not marc_file:
        raise HTTPException(404, detail={"error": "NO_FILE", "message": "No MARC file found."})

    tag = body.get("tag", "")
    new_value = body.get("new_value", "")
    subfield = body.get("subfield", "")
    position = body.get("position")

    # Read all records, modify the target, write back
    records = []
    try:
        with open(marc_file.storage_path, "rb") as fh:
            reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
            for rec in reader:
                records.append(rec)
    except Exception as e:
        raise HTTPException(500, detail={"error": "READ_ERROR", "message": str(e)})

    if local_index < 0 or local_index >= len(records) or records[local_index] is None:
        raise HTTPException(404, detail={"error": "RECORD_NOT_FOUND", "message": f"No record at local index {local_index} (global {record_index})."})

    record = records[local_index]
    edited = False

    if tag == "LDR" and position is not None:
        # Leader byte edit
        leader = list(record.leader)
        if 0 <= position < len(leader):
            leader[position] = new_value[0] if new_value else " "
            record.leader = "".join(leader)
            edited = True
    elif tag and tag < "010":
        # Control field
        for f in record.get_fields(tag):
            f.data = new_value
            edited = True
            break
    elif tag and subfield:
        # Data field subfield edit
        sub_code = subfield.lstrip("$")
        for f in record.get_fields(tag):
            for i, sf in enumerate(f.subfields):
                if sf.code == sub_code and (not body.get("old_value") or sf.value == body["old_value"]):
                    f.subfields[i] = pymarc.Subfield(code=sub_code, value=new_value)
                    edited = True
                    break
            if edited:
                break

    if not edited:
        raise HTTPException(400, detail={"error": "NO_MATCH", "message": "Could not find the field to edit."})

    # Write all records back to file
    try:
        with open(marc_file.storage_path, "wb") as fh:
            for rec in records:
                if rec is not None:
                    fh.write(rec.as_marc())
    except Exception as e:
        raise HTTPException(500, detail={"error": "WRITE_ERROR", "message": str(e)})

    # Mark related issues as manually fixed (filter by subfield if provided)
    fix_where = [
        QualityScanResult.project_id == project_id,
        QualityScanResult.record_index == record_index,
        QualityScanResult.tag == tag,
        QualityScanResult.status == "unresolved",
    ]
    if subfield:
        fix_where.append(QualityScanResult.subfield == subfield)
    await db.execute(
        update(QualityScanResult)
        .where(*fix_where)
        .values(status="manually_fixed", fixed_value=new_value, fixed_at=datetime.utcnow(), fixed_by="manual")
    )

    await audit_log(db, project_id, stage=3, level="info", tag="[quality-edit]",
                    message=f"Record {record_index} field {tag}{subfield} edited manually")

    return {"edited": True, "record_index": record_index, "tag": tag}


@router.get("/{project_id}/quality/bulk-fix/preview")
async def bulk_fix_preview(
    project_id: str,
    category: str = Query(...),
    sample_size: int = Query(5, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
):
    """Preview what a bulk fix would do — show before/after for sample records."""
    await require_project(project_id, db)

    # Get sample issues with suggested fixes
    result = await db.execute(
        select(QualityScanResult).where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.category == category,
            QualityScanResult.status == "unresolved",
            QualityScanResult.suggested_fix.isnot(None),
        ).limit(sample_size)
    )
    samples = result.scalars().all()

    # Count total fixable
    total = (await db.execute(
        select(func.count()).select_from(QualityScanResult).where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.category == category,
            QualityScanResult.status == "unresolved",
            QualityScanResult.suggested_fix.isnot(None),
        )
    )).scalar() or 0

    total_unresolved = (await db.execute(
        select(func.count()).select_from(QualityScanResult).where(
            QualityScanResult.project_id == project_id,
            QualityScanResult.category == category,
            QualityScanResult.status == "unresolved",
        )
    )).scalar() or 0

    return {
        "category": category,
        "total_fixable": total,
        "total_unresolved": total_unresolved,
        "samples": [
            {
                "record_index": s.record_index,
                "tag": s.tag,
                "subfield": s.subfield,
                "description": s.description,
                "before": s.original_value,
                "after": s.suggested_fix,
            }
            for s in samples
        ],
    }


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


# ── Clustering ─────────────────────────────────────────────────────

@router.post("/{project_id}/quality/cluster")
async def build_clusters(
    project_id: str,
    field: str,          # e.g. "245$a", "852$a", "942$c"
    db: AsyncSession = Depends(get_db),
):
    """Build clusters by grouping records on a field/subfield value.

    Returns a frequency distribution: {value: count} sorted by count desc.
    Useful for reviewing data distributions before reconciliation.
    """
    from pathlib import Path
    import pymarc
    from cerulean.core.config import get_settings

    settings = get_settings()
    await require_project(project_id, db)

    # Find the best MARC file
    project_dir = Path(settings.data_root) / project_id
    source = None
    for name in ["output.mrc", "Biblios-mapped-items.mrc", "merged_deduped.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            source = candidate
            break
    if not source:
        raw = project_dir / "raw"
        if raw.is_dir():
            files = sorted(raw.glob("*.mrc"))
            if files:
                source = files[0]
    if not source:
        raise HTTPException(404, detail="No MARC files found.")

    # Parse field spec
    if "$" in field:
        tag, sub = field.split("$", 1)
    else:
        tag, sub = field, None

    # Build frequency map
    clusters = {}
    total = 0
    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue
            total += 1
            values = []
            for f in record.get_fields(tag):
                if sub and hasattr(f, "subfields"):
                    for sf in f.subfields:
                        if sf.code == sub:
                            values.append(sf.value.strip())
                elif hasattr(f, "data"):
                    values.append(f.data.strip())
                elif not sub and hasattr(f, "subfields"):
                    values.append(" ".join(sf.value for sf in f.subfields).strip())

            if not values:
                values = ["(empty)"]

            for v in values:
                clusters[v] = clusters.get(v, 0) + 1

    # Sort by count descending
    sorted_clusters = sorted(clusters.items(), key=lambda x: -x[1])

    return {
        "field": field,
        "total_records": total,
        "unique_values": len(sorted_clusters),
        "clusters": [{"value": v, "count": c, "percent": round(c / total * 100, 1) if total else 0} for v, c in sorted_clusters],
    }
