"""
cerulean/api/routers/patrons.py
─────────────────────────────────────────────────────────────────────────────
Stage 6 — Patron Data Transformation API endpoints.

Upload & Parse (multi-file):
  GET   /projects/{id}/patrons/available-files — list Stage 1 files for selection
  POST  /projects/{id}/patrons/select-file     — select existing Stage 1 file as patron data
  POST  /projects/{id}/patrons/upload          — upload patron file
  GET   /projects/{id}/patrons/files           — list all patron file records
  DELETE /projects/{id}/patrons/files/{fid}    — delete one patron file, re-concat
  POST  /projects/{id}/patrons/reparse         — re-parse all (or one file by file_id)
  GET   /projects/{id}/patrons/preview         — preview combined parsed.csv
  GET   /projects/{id}/patrons/files/{fid}/preview — preview single file parsed data
  DELETE /projects/{id}/patrons/file           — delete ALL files, reset sub-steps

Column Mapping:
  POST  /projects/{id}/patrons/maps/ai-suggest     — dispatch AI column mapping
  GET   /projects/{id}/patrons/maps/ai-suggest/status — poll AI task status
  GET   /projects/{id}/patrons/maps                 — list column maps
  POST  /projects/{id}/patrons/maps                 — create manual column map
  PATCH /projects/{id}/patrons/maps/{mid}           — update column map
  DELETE /projects/{id}/patrons/maps/{mid}          — delete column map
  POST  /projects/{id}/patrons/maps/{mid}/approve   — approve single map
  POST  /projects/{id}/patrons/maps/approve-all     — batch approve above threshold

Value Reconciliation:
  POST  /projects/{id}/patrons/scan            — dispatch scan for controlled values
  GET   /projects/{id}/patrons/scan/status     — poll scan task status
  GET   /projects/{id}/patrons/values          — query scan results by header
  GET   /projects/{id}/patrons/koha-list       — fetch Koha control list values
  GET   /projects/{id}/patrons/rules           — list value rules
  POST  /projects/{id}/patrons/rules           — create rule
  PATCH /projects/{id}/patrons/rules/{rid}     — update rule
  DELETE /projects/{id}/patrons/rules/{rid}    — delete rule
  GET   /projects/{id}/patrons/validate        — cross-reference scan vs rules

Apply & Export:
  POST  /projects/{id}/patrons/apply           — dispatch apply task
  GET   /projects/{id}/patrons/apply/status    — poll apply task status
  GET   /projects/{id}/patrons/report          — fetch apply result
  GET   /projects/{id}/patrons/output-files           — list output files
  GET   /projects/{id}/patrons/output-files/{fn}      — get file contents as JSON
  GET   /projects/{id}/patrons/output-files/{fn}/download — download file
  PUT   /projects/{id}/patrons/descriptions/{cat} — save value descriptions
  POST  /projects/{id}/patrons/clear           — delete scan results + outputs
"""

import csv
import io
import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path

import httpx
from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import (
    MARCFile,
    PatronColumnMap,
    PatronFile,
    PatronScanResult,
    PatronValueRule,
)
from cerulean.schemas.patrons import (
    ApproveAllRequest,
    ApproveAllResponse,
    AvailablePatronFile,
    PatronAISuggestResponse,
    PatronApplyResponse,
    PatronApplyStatusResponse,
    PatronColumnMapCreate,
    PatronColumnMapOut,
    PatronColumnMapUpdate,
    PatronFileOut,
    PatronFileUploadResponse,
    PatronPreviewResponse,
    PatronReportOut,
    PatronRuleCreate,
    PatronRuleOut,
    PatronRuleUpdate,
    PatronScanResponse,
    PatronScanStatusResponse,
    PatronScanValueOut,
    PatronValidateResponse,
    PatronValidationIssue,
    SelectFileRequest,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.patrons import (
    PATRON_CONTROLLED_HEADERS,
    _concat_patron_files,
    patron_ai_map_task,
    patron_apply_task,
    patron_parse_task,
    patron_scan_task,
)

settings = get_settings()
router = APIRouter(prefix="/projects", tags=["patrons"])

_ALLOWED_EXTENSIONS = {".csv", ".tsv", ".txt", ".dat", ".prn", ".xlsx", ".xls", ".xml", ".mrc", ".marc"}


# ══════════════════════════════════════════════════════════════════════════
# 1-5. UPLOAD & PARSE
# ══════════════════════════════════════════════════════════════════════════


_PATRON_KEYWORDS = {"patron", "borrower", "user", "member", "reader", "cardholder"}


@router.get("/{project_id}/patrons/available-files", response_model=list[AvailablePatronFile])
async def list_available_patron_files(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all Stage 1 uploaded files, flagging likely patron files first."""
    await require_project(project_id, db)

    result = await db.execute(
        select(MARCFile)
        .where(MARCFile.project_id == project_id)
        .order_by(MARCFile.created_at)
    )
    files = result.scalars().all()

    def _is_patron_likely(filename: str) -> bool:
        name_lower = filename.lower()
        return any(kw in name_lower for kw in _PATRON_KEYWORDS)

    out = [
        AvailablePatronFile(
            file_id=f.id,
            filename=f.filename,
            file_format=f.file_format,
            file_size_bytes=f.file_size_bytes,
            record_count=f.record_count,
            status=f.status,
            is_patron_likely=_is_patron_likely(f.filename),
        )
        for f in files
    ]
    # Sort: likely patron files first, then the rest
    out.sort(key=lambda x: (not x.is_patron_likely, x.filename.lower()))
    return out


@router.post("/{project_id}/patrons/select-file", response_model=PatronFileUploadResponse, status_code=202)
async def select_existing_file(
    project_id: str,
    body: SelectFileRequest,
    db: AsyncSession = Depends(get_db),
):
    """Select an existing Stage 1 file as the patron data source. Adds to multi-file list."""
    await require_project(project_id, db)

    marc_file = await db.get(MARCFile, body.marc_file_id)
    if not marc_file or marc_file.project_id != project_id:
        raise HTTPException(404, detail={
            "error": "NOT_FOUND",
            "message": "File not found in this project.",
        })

    # Copy file from raw/ to patrons/raw/
    source_path = Path(marc_file.storage_path)
    if not source_path.is_file():
        raise HTTPException(404, detail={
            "error": "FILE_MISSING",
            "message": "Source file not found on disk.",
        })

    patron_dir = Path(settings.data_root) / project_id / "patrons" / "raw"
    patron_dir.mkdir(parents=True, exist_ok=True)
    dest = patron_dir / marc_file.filename
    shutil.copy2(str(source_path), str(dest))

    # Determine format from extension
    ext = Path(marc_file.filename).suffix.lower()
    format_map = {
        ".csv": "csv", ".tsv": "tsv", ".txt": "csv", ".dat": "fixed", ".prn": "fixed",
        ".xlsx": "xlsx", ".xls": "xls", ".xml": "xml", ".mrc": "patron_marc", ".marc": "patron_marc",
    }
    file_format = format_map.get(ext, "csv")

    # Create PatronFile record
    pf = PatronFile(
        id=str(uuid.uuid4()),
        project_id=project_id,
        filename=marc_file.filename,
        file_format=file_format,
        status="uploaded",
    )
    db.add(pf)
    await db.flush()
    await db.refresh(pf)

    await audit_log(db, project_id, stage=6, level="info", tag="[patron-upload]",
                    message=f"Patron file selected from Stage 1: {marc_file.filename}")
    await db.flush()

    # Dispatch parse task
    task = patron_parse_task.apply_async(
        args=[project_id, pf.id],
        queue="patrons",
    )

    return PatronFileUploadResponse(
        file_id=pf.id,
        filename=marc_file.filename,
        task_id=task.id,
        message="File selected from Stage 1 uploads. Parsing started.",
    )


@router.post("/{project_id}/patrons/upload", response_model=PatronFileUploadResponse, status_code=202)
async def upload_patron_file(
    project_id: str,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload a patron data file. Adds to multi-file list. Dispatches parse task."""
    await require_project(project_id, db)

    filename = file.filename or "patrons_upload"
    ext = Path(filename).suffix.lower()
    if ext not in _ALLOWED_EXTENSIONS:
        raise HTTPException(400, detail={
            "error": "INVALID_FORMAT",
            "message": f"Unsupported file type: {ext}. Allowed: {', '.join(sorted(_ALLOWED_EXTENSIONS))}",
        })

    # Determine format from extension
    format_map = {
        ".csv": "csv", ".tsv": "tsv", ".txt": "csv", ".dat": "fixed", ".prn": "fixed",
        ".xlsx": "xlsx", ".xls": "xls", ".xml": "xml", ".mrc": "patron_marc", ".marc": "patron_marc",
    }
    file_format = format_map.get(ext, "csv")

    # Store file
    project_dir = Path(settings.data_root) / project_id / "patrons" / "raw"
    project_dir.mkdir(parents=True, exist_ok=True)
    dest = project_dir / filename
    content = await file.read()
    dest.write_bytes(content)

    # Create PatronFile record
    pf = PatronFile(
        id=str(uuid.uuid4()),
        project_id=project_id,
        filename=filename,
        file_format=file_format,
        status="uploaded",
    )
    db.add(pf)
    await db.flush()
    await db.refresh(pf)

    await audit_log(db, project_id, stage=6, level="info", tag="[patron-upload]",
                    message=f"Patron file uploaded: {filename}")
    await db.flush()

    # Dispatch parse task
    task = patron_parse_task.apply_async(
        args=[project_id, pf.id],
        queue="patrons",
    )

    return PatronFileUploadResponse(
        file_id=pf.id,
        filename=filename,
        task_id=task.id,
        message="Patron file uploaded. Parsing started.",
    )


@router.get("/{project_id}/patrons/files", response_model=list[PatronFileOut])
async def list_patron_file_records(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all patron file records for this project."""
    await require_project(project_id, db)
    result = await db.execute(
        select(PatronFile)
        .where(PatronFile.project_id == project_id)
        .order_by(PatronFile.uploaded_at)
    )
    return result.scalars().all()


@router.delete("/{project_id}/patrons/files/{file_id}", status_code=200)
async def delete_single_patron_file(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a single patron file and its parsed intermediate, re-concat remaining."""
    await require_project(project_id, db)

    pf = await db.get(PatronFile, file_id)
    if not pf or pf.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Patron file not found."})

    filename = pf.filename
    await db.delete(pf)
    await db.flush()

    # Remove raw file and per-file parsed CSV
    patron_dir = Path(settings.data_root) / project_id / "patrons"
    raw_file = patron_dir / "raw" / filename
    if raw_file.is_file():
        os.remove(str(raw_file))
    per_file_csv = patron_dir / f"parsed_{file_id}.csv"
    if per_file_csv.is_file():
        os.remove(str(per_file_csv))

    # Re-concat remaining files
    _concat_patron_files(project_id)

    await audit_log(db, project_id, stage=6, level="info", tag="[patron]",
                    message=f"Patron file deleted: {filename}")
    await db.flush()

    return {"message": f"Patron file '{filename}' deleted. Remaining files re-concatenated."}


@router.post("/{project_id}/patrons/reparse", response_model=PatronFileUploadResponse, status_code=202)
async def reparse_patron_file(
    project_id: str,
    body: dict | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Re-parse patron file(s). Pass file_id to reparse one, omit to reparse all."""
    await require_project(project_id, db)

    file_id = (body or {}).get("file_id")

    if file_id:
        # Reparse single file
        pf = await db.get(PatronFile, file_id)
        if not pf or pf.project_id != project_id:
            raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Patron file not found."})

        if body and "parse_settings" in body:
            pf.parse_settings = body["parse_settings"]
        if body and "file_format" in body:
            pf.file_format = body["file_format"]

        pf.status = "uploaded"
        await db.flush()

        task = patron_parse_task.apply_async(
            args=[project_id, pf.id],
            queue="patrons",
        )

        return PatronFileUploadResponse(
            file_id=pf.id,
            filename=pf.filename,
            task_id=task.id,
            message="Re-parse started with updated settings.",
        )
    else:
        # Reparse all files
        result = await db.execute(
            select(PatronFile)
            .where(PatronFile.project_id == project_id)
        )
        files = result.scalars().all()
        if not files:
            raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "No patron files uploaded."})

        last_task_id = None
        last_file = files[-1]
        for pf in files:
            if body and "parse_settings" in body:
                pf.parse_settings = body["parse_settings"]
            if body and "file_format" in body:
                pf.file_format = body["file_format"]
            pf.status = "uploaded"

        await db.flush()

        for pf in files:
            task = patron_parse_task.apply_async(
                args=[project_id, pf.id],
                queue="patrons",
            )
            last_task_id = task.id

        return PatronFileUploadResponse(
            file_id=last_file.id,
            filename=last_file.filename,
            task_id=last_task_id,
            message=f"Re-parse started for {len(files)} file(s).",
        )


@router.get("/{project_id}/patrons/preview", response_model=PatronPreviewResponse)
async def preview_patron_data(
    project_id: str,
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Preview rows of parsed patron data with pagination."""
    await require_project(project_id, db)

    parsed_csv = Path(settings.data_root) / project_id / "patrons" / "parsed.csv"
    if not parsed_csv.is_file():
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Parsed data not found. Upload and parse first."})

    rows = []
    columns = []
    total = 0
    with open(str(parsed_csv), "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        for row in reader:
            if total >= offset and len(rows) < limit:
                rows.append(dict(row))
            total += 1

    return PatronPreviewResponse(rows=rows, total_rows=total, columns=columns)


@router.get("/{project_id}/patrons/files/{file_id}/preview", response_model=PatronPreviewResponse)
async def preview_patron_file(
    project_id: str,
    file_id: str,
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Preview rows from a single patron file's per-file parsed CSV."""
    await require_project(project_id, db)

    pf = await db.get(PatronFile, file_id)
    if not pf or pf.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Patron file not found."})

    per_file_csv = Path(settings.data_root) / project_id / "patrons" / f"parsed_{file_id}.csv"
    if not per_file_csv.is_file():
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Parsed data not available for this file."})

    rows = []
    columns = []
    total = 0
    with open(str(per_file_csv), "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        for row in reader:
            if total >= offset and len(rows) < limit:
                rows.append(dict(row))
            total += 1

    return PatronPreviewResponse(rows=rows, total_rows=total, columns=columns)


@router.delete("/{project_id}/patrons/file", status_code=200)
async def delete_patron_file(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete patron file, reset all sub-steps."""
    project = await require_project(project_id, db)

    # Delete all patron data
    await db.execute(delete(PatronScanResult).where(PatronScanResult.project_id == project_id))
    await db.execute(delete(PatronValueRule).where(PatronValueRule.project_id == project_id))
    await db.execute(delete(PatronColumnMap).where(PatronColumnMap.project_id == project_id))
    await db.execute(delete(PatronFile).where(PatronFile.project_id == project_id))

    # Remove files
    patron_dir = Path(settings.data_root) / project_id / "patrons"
    if patron_dir.is_dir():
        shutil.rmtree(str(patron_dir))

    # Reset project fields
    project.patron_scan_task_id = None
    project.patron_apply_task_id = None
    project.patron_ai_task_id = None
    project.stage_6_complete = False

    await db.flush()

    await audit_log(db, project_id, stage=6, level="info", tag="[patron]",
                    message="Patron file and all data deleted")
    await db.flush()

    return {"message": "Patron file and all associated data deleted."}


# ══════════════════════════════════════════════════════════════════════════
# 6-13. COLUMN MAPPING
# ══════════════════════════════════════════════════════════════════════════


@router.post("/{project_id}/patrons/maps/ai-suggest", response_model=PatronAISuggestResponse, status_code=202)
async def ai_suggest_maps(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch AI column mapping task."""
    project = await require_project(project_id, db)

    has_files = (await db.execute(
        select(PatronFile.id).where(PatronFile.project_id == project_id).limit(1)
    )).first()
    if not has_files:
        raise HTTPException(409, detail={
            "error": "NO_PATRON_FILE",
            "message": "Upload and parse a patron file first.",
        })

    await audit_log(db, project_id, stage=6, level="info", tag="[patron-ai]",
                    message="Patron AI column mapping dispatched")
    await db.flush()

    task = patron_ai_map_task.apply_async(
        args=[project_id],
        queue="patrons",
    )
    project.patron_ai_task_id = task.id
    await db.flush()

    return PatronAISuggestResponse(task_id=task.id, message="AI column mapping started.")


@router.get("/{project_id}/patrons/maps/ai-suggest/status")
async def ai_suggest_status(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Poll AI column mapping task status."""
    project = await require_project(project_id, db)

    task_id = project.patron_ai_task_id
    if not task_id:
        return {"task_id": None, "state": "IDLE"}

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

    return {"task_id": task_id, "state": state, "progress": progress, "result": result_data, "error": error}


@router.get("/{project_id}/patrons/maps", response_model=list[PatronColumnMapOut])
async def list_column_maps(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all patron column maps ordered by sort_order."""
    await require_project(project_id, db)
    result = await db.execute(
        select(PatronColumnMap)
        .where(PatronColumnMap.project_id == project_id)
        .order_by(PatronColumnMap.sort_order)
    )
    return result.scalars().all()


@router.post("/{project_id}/patrons/maps", response_model=PatronColumnMapOut, status_code=201)
async def create_column_map(
    project_id: str,
    body: PatronColumnMapCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a manual patron column map."""
    await require_project(project_id, db)

    cm = PatronColumnMap(
        id=str(uuid.uuid4()),
        project_id=project_id,
        source_column=body.source_column,
        target_header=body.target_header,
        ignored=body.ignored,
        transform_type=body.transform_type,
        transform_config=body.transform_config,
        is_controlled_list=body.is_controlled_list,
        approved=False,
        ai_suggested=False,
        source_label="manual",
        sort_order=body.sort_order,
    )
    db.add(cm)
    await db.flush()
    await db.refresh(cm)
    return cm


@router.patch("/{project_id}/patrons/maps/{map_id}", response_model=PatronColumnMapOut)
async def update_column_map(
    project_id: str,
    map_id: str,
    body: PatronColumnMapUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a patron column map."""
    await require_project(project_id, db)
    cm = await db.get(PatronColumnMap, map_id)
    if not cm or cm.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Column map not found."})

    update_data = body.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(cm, key, value)

    await db.flush()
    await db.refresh(cm)
    return cm


@router.delete("/{project_id}/patrons/maps/{map_id}", status_code=204)
async def delete_column_map(
    project_id: str,
    map_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a patron column map."""
    await require_project(project_id, db)
    cm = await db.get(PatronColumnMap, map_id)
    if not cm or cm.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Column map not found."})
    await db.delete(cm)
    await db.flush()


@router.post("/{project_id}/patrons/maps/{map_id}/approve", response_model=PatronColumnMapOut)
async def approve_column_map(
    project_id: str,
    map_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Approve a single patron column map."""
    await require_project(project_id, db)
    cm = await db.get(PatronColumnMap, map_id)
    if not cm or cm.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Column map not found."})

    cm.approved = True
    await db.flush()
    await db.refresh(cm)
    return cm


@router.post("/{project_id}/patrons/maps/approve-all", response_model=ApproveAllResponse)
async def approve_all_maps(
    project_id: str,
    body: ApproveAllRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Batch approve column maps above confidence threshold."""
    await require_project(project_id, db)
    min_conf = body.min_confidence if body else 0.85

    result = await db.execute(
        select(PatronColumnMap).where(
            PatronColumnMap.project_id == project_id,
            PatronColumnMap.approved == False,  # noqa: E712
            PatronColumnMap.ai_suggested == True,  # noqa: E712
        )
    )
    maps = result.scalars().all()

    approved_count = 0
    for m in maps:
        if m.ai_confidence is not None and m.ai_confidence >= min_conf:
            m.approved = True
            approved_count += 1

    await db.flush()
    return ApproveAllResponse(
        approved_count=approved_count,
        message=f"Approved {approved_count} maps with confidence >= {min_conf}.",
    )


# ══════════════════════════════════════════════════════════════════════════
# 14-22. VALUE RECONCILIATION
# ══════════════════════════════════════════════════════════════════════════


@router.post("/{project_id}/patrons/scan", response_model=PatronScanResponse, status_code=202)
async def start_patron_scan(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch patron scan task for controlled list values."""
    project = await require_project(project_id, db)

    has_files = (await db.execute(
        select(PatronFile.id).where(PatronFile.project_id == project_id).limit(1)
    )).first()
    if not has_files:
        raise HTTPException(409, detail={
            "error": "NO_PATRON_FILE",
            "message": "Upload and parse a patron file first.",
        })

    await audit_log(db, project_id, stage=6, level="info", tag="[patron-scan]",
                    message="Patron scan dispatched")
    await db.flush()

    task = patron_scan_task.apply_async(
        args=[project_id],
        queue="patrons",
    )
    project.patron_scan_task_id = task.id
    await db.flush()

    return PatronScanResponse(task_id=task.id, message="Patron scan started.")


@router.get("/{project_id}/patrons/scan/status", response_model=PatronScanStatusResponse)
async def patron_scan_status(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Poll patron scan task status."""
    project = await require_project(project_id, db)

    task_id = project.patron_scan_task_id
    if not task_id:
        return PatronScanStatusResponse(task_id=None, state="IDLE")

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

    return PatronScanStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


@router.get("/{project_id}/patrons/values", response_model=list[PatronScanValueOut])
async def list_patron_scan_values(
    project_id: str,
    header: str = Query(..., description="Koha header: categorycode, branchcode, title, lost"),
    db: AsyncSession = Depends(get_db),
):
    """Return scan results for a specific Koha header."""
    await require_project(project_id, db)

    result = await db.execute(
        select(PatronScanResult)
        .where(
            PatronScanResult.project_id == project_id,
            PatronScanResult.koha_header == header,
        )
        .order_by(PatronScanResult.record_count.desc())
    )
    return result.scalars().all()


@router.get("/{project_id}/patrons/koha-list")
async def patron_koha_list(
    project_id: str,
    header: str = Query(..., description="Koha header: categorycode, branchcode, title, lost"),
    db: AsyncSession = Depends(get_db),
):
    """Fetch Koha controlled list values via API for a patron header."""
    project = await require_project(project_id, db)

    if not project.koha_url or not project.koha_token_enc:
        raise HTTPException(409, detail={
            "error": "NO_KOHA_CONFIG",
            "message": "Koha URL and token must be configured on the project.",
        })

    ctrl = PATRON_CONTROLLED_HEADERS.get(header)
    if not ctrl:
        raise HTTPException(400, detail={
            "error": "UNKNOWN_HEADER",
            "message": f"Unknown controlled header: {header}. "
                       f"Valid: {', '.join(PATRON_CONTROLLED_HEADERS.keys())}",
        })

    from cerulean.tasks.push import _decrypt_token
    base_url = project.koha_url.rstrip("/")
    token = _decrypt_token(project.koha_token_enc)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    endpoint = f"{base_url}{ctrl['koha_endpoint']}"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(endpoint, headers=headers)

        if resp.status_code >= 400:
            raise HTTPException(502, detail={
                "error": "KOHA_API_ERROR",
                "message": f"Koha API returned {resp.status_code}: {resp.text[:500]}",
            })

        return {"header": header, "values": resp.json()}

    except httpx.HTTPError as exc:
        raise HTTPException(502, detail={
            "error": "KOHA_API_ERROR",
            "message": f"Koha API request failed: {exc}",
        })


@router.get("/{project_id}/patrons/rules", response_model=list[PatronRuleOut])
async def list_patron_rules(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all patron value rules ordered by sort_order."""
    await require_project(project_id, db)
    result = await db.execute(
        select(PatronValueRule)
        .where(PatronValueRule.project_id == project_id)
        .order_by(PatronValueRule.sort_order)
    )
    return result.scalars().all()


@router.post("/{project_id}/patrons/rules", response_model=PatronRuleOut, status_code=201)
async def create_patron_rule(
    project_id: str,
    body: PatronRuleCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a patron value rule."""
    await require_project(project_id, db)

    if body.koha_header not in PATRON_CONTROLLED_HEADERS:
        raise HTTPException(400, detail={
            "error": "UNKNOWN_HEADER",
            "message": f"Unknown controlled header: {body.koha_header}. "
                       f"Valid: {', '.join(PATRON_CONTROLLED_HEADERS.keys())}",
        })

    rule = PatronValueRule(
        id=str(uuid.uuid4()),
        project_id=project_id,
        koha_header=body.koha_header,
        operation=body.operation,
        source_values=body.source_values,
        target_value=body.target_value,
        split_conditions=body.split_conditions,
        delete_mode=body.delete_mode,
        sort_order=body.sort_order,
        active=body.active,
    )
    db.add(rule)
    await db.flush()
    await db.refresh(rule)
    return rule


@router.patch("/{project_id}/patrons/rules/{rule_id}", response_model=PatronRuleOut)
async def update_patron_rule(
    project_id: str,
    rule_id: str,
    body: PatronRuleUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a patron value rule."""
    await require_project(project_id, db)
    rule = await db.get(PatronValueRule, rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})

    update_data = body.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(rule, key, value)

    await db.flush()
    await db.refresh(rule)
    return rule


@router.delete("/{project_id}/patrons/rules/{rule_id}", status_code=204)
async def delete_patron_rule(
    project_id: str,
    rule_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a patron value rule."""
    await require_project(project_id, db)
    rule = await db.get(PatronValueRule, rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})
    await db.delete(rule)
    await db.flush()


@router.get("/{project_id}/patrons/validate", response_model=PatronValidateResponse)
async def validate_patron_rules(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Cross-reference patron scan results against defined rules."""
    await require_project(project_id, db)

    scan_result = await db.execute(
        select(PatronScanResult)
        .where(PatronScanResult.project_id == project_id)
    )
    scan_rows = scan_result.scalars().all()

    rules_result = await db.execute(
        select(PatronValueRule).where(
            PatronValueRule.project_id == project_id,
            PatronValueRule.active == True,  # noqa: E712
        )
    )
    rules = rules_result.scalars().all()

    covered: set[tuple[str, str]] = set()
    for rule in rules:
        for sv in (rule.source_values or []):
            covered.add((rule.koha_header, sv))

    total = len(scan_rows)
    matched = 0
    unmatched = 0
    issues: list[PatronValidationIssue] = []

    for row in scan_rows:
        if (row.koha_header, row.source_value) in covered:
            matched += 1
        else:
            unmatched += 1
            issues.append(PatronValidationIssue(
                koha_header=row.koha_header,
                source_value=row.source_value,
                record_count=row.record_count,
                issue="unmatched",
            ))

    return PatronValidateResponse(
        total_values=total,
        matched_values=matched,
        unmatched_values=unmatched,
        issues=issues,
    )


# ══════════════════════════════════════════════════════════════════════════
# 23-30. APPLY & EXPORT
# ══════════════════════════════════════════════════════════════════════════


@router.post("/{project_id}/patrons/apply", response_model=PatronApplyResponse, status_code=202)
async def start_patron_apply(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch patron apply task."""
    project = await require_project(project_id, db)

    has_files = (await db.execute(
        select(PatronFile.id).where(PatronFile.project_id == project_id).limit(1)
    )).first()
    if not has_files:
        raise HTTPException(409, detail={
            "error": "NO_PATRON_FILE",
            "message": "Upload and parse a patron file first.",
        })

    await audit_log(db, project_id, stage=6, level="info", tag="[patron-apply]",
                    message="Patron apply dispatched")
    await db.flush()

    task = patron_apply_task.apply_async(
        args=[project_id],
        queue="patrons",
    )
    project.patron_apply_task_id = task.id
    await db.flush()

    return PatronApplyResponse(task_id=task.id, message="Patron apply started.")


@router.get("/{project_id}/patrons/apply/status", response_model=PatronApplyStatusResponse)
async def patron_apply_status(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Poll patron apply task status."""
    project = await require_project(project_id, db)

    task_id = project.patron_apply_task_id
    if not task_id:
        return PatronApplyStatusResponse(task_id=None, state="IDLE")

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

    return PatronApplyStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


@router.get("/{project_id}/patrons/report", response_model=PatronReportOut)
async def patron_report(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Read patron apply task result from Celery backend."""
    project = await require_project(project_id, db)

    task_id = project.patron_apply_task_id
    if not task_id:
        return PatronReportOut(task_id=None, state="IDLE")

    async_result = AsyncResult(task_id, app=celery_app)
    state = async_result.state
    result_data = None
    error = None

    if state == "SUCCESS":
        result_data = async_result.result
    elif state == "FAILURE":
        error = str(async_result.info)

    return PatronReportOut(task_id=task_id, state=state, result=result_data, error=error)


@router.get("/{project_id}/patrons/output-files")
async def list_patron_output_files(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List patron output files (transformed CSV + controlled value CSVs)."""
    await require_project(project_id, db)
    patron_dir = Path(settings.data_root) / project_id / "patrons"
    files = []

    # Main output file
    transformed = patron_dir / "patrons_transformed.csv"
    if transformed.is_file():
        row_count = 0
        with open(str(transformed)) as f:
            reader = csv.reader(f)
            next(reader, None)
            for _ in reader:
                row_count += 1
        files.append({
            "filename": "patrons_transformed.csv",
            "type": "transformed",
            "row_count": row_count,
        })

    # Controlled value files
    controlled_dir = patron_dir / "controlled"
    if controlled_dir.is_dir():
        for csv_path in sorted(controlled_dir.glob("patron_*.csv")):
            row_count = 0
            with open(str(csv_path)) as f:
                reader = csv.reader(f)
                next(reader, None)
                for _ in reader:
                    row_count += 1
            cat = csv_path.stem.replace("patron_", "")
            files.append({
                "filename": csv_path.name,
                "category": cat,
                "type": "controlled_values",
                "value_count": row_count,
            })

    return {"files": files}


@router.get("/{project_id}/patrons/output-files/{filename}")
async def get_patron_output_file(
    project_id: str,
    filename: str,
    db: AsyncSession = Depends(get_db),
):
    """Return contents of a patron output file as JSON rows."""
    await require_project(project_id, db)

    if "/" in filename or "\\" in filename or not filename.endswith(".csv"):
        raise HTTPException(400, detail="Invalid filename")

    # Try transformed first, then controlled
    patron_dir = Path(settings.data_root) / project_id / "patrons"
    csv_path = patron_dir / filename
    if not csv_path.is_file():
        csv_path = patron_dir / "controlled" / filename
    if not csv_path.is_file():
        raise HTTPException(404, detail="File not found")

    rows = []
    cat = csv_path.stem.replace("patron_", "")
    descs = _load_descriptions(project_id)
    cat_descs = descs.get(cat, {})

    with open(str(csv_path)) as f:
        reader = csv.DictReader(f)
        for row in reader:
            entry = dict(row)
            val = row.get("value", "")
            if val and cat_descs:
                entry["description"] = cat_descs.get(val, "")
            rows.append(entry)

    return {"filename": filename, "rows": rows}


@router.get("/{project_id}/patrons/output-files/{filename}/download")
async def download_patron_output_file(
    project_id: str,
    filename: str,
    db: AsyncSession = Depends(get_db),
):
    """Download a patron output file."""
    await require_project(project_id, db)

    if "/" in filename or "\\" in filename or not filename.endswith(".csv"):
        raise HTTPException(400, detail="Invalid filename")

    patron_dir = Path(settings.data_root) / project_id / "patrons"
    csv_path = patron_dir / filename
    if not csv_path.is_file():
        csv_path = patron_dir / "controlled" / filename
    if not csv_path.is_file():
        raise HTTPException(404, detail="File not found")

    # For controlled value files, include descriptions
    cat = csv_path.stem.replace("patron_", "")
    descs = _load_descriptions(project_id)
    cat_descs = descs.get(cat, {})

    if cat_descs and filename.startswith("patron_"):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["value", "description", "record_count"])
        with open(str(csv_path)) as f:
            reader = csv.DictReader(f)
            for row in reader:
                val = row.get("value", "")
                writer.writerow([val, cat_descs.get(val, ""), row.get("record_count", 0)])

        return StreamingResponse(
            iter([output.getvalue().encode("utf-8")]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    return FileResponse(str(csv_path), media_type="text/csv", filename=filename)


@router.put("/{project_id}/patrons/descriptions/{category}")
async def save_patron_descriptions(
    project_id: str,
    category: str,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Save value descriptions for a patron controlled list category."""
    await require_project(project_id, db)
    new_descs = body.get("descriptions", {})
    if not isinstance(new_descs, dict):
        raise HTTPException(400, detail="descriptions must be a dict")

    descs = _load_descriptions(project_id)
    descs.setdefault(category, {})
    descs[category].update(new_descs)
    descs[category] = {k: v for k, v in descs[category].items() if v}
    _save_descriptions(project_id, descs)

    return {"category": category, "descriptions_saved": len(new_descs)}


@router.post("/{project_id}/patrons/clear", status_code=200)
async def clear_patron_scan(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete scan results + output files, preserving maps & rules."""
    project = await require_project(project_id, db)

    # Delete scan results
    await db.execute(
        delete(PatronScanResult)
        .where(PatronScanResult.project_id == project_id)
    )

    # Remove output files
    patron_dir = Path(settings.data_root) / project_id / "patrons"
    transformed = patron_dir / "patrons_transformed.csv"
    if transformed.is_file():
        os.remove(str(transformed))
    controlled_dir = patron_dir / "controlled"
    if controlled_dir.is_dir():
        shutil.rmtree(str(controlled_dir))

    # Reset task IDs
    project.patron_scan_task_id = None
    project.patron_apply_task_id = None
    project.stage_6_complete = False
    await db.flush()

    await audit_log(db, project_id, stage=6, level="info", tag="[patron]",
                    message="Patron scan results and output files cleared")
    await db.flush()

    return {"message": "Scan results and output files cleared. Maps and rules preserved."}


# ── Helpers ──────────────────────────────────────────────────────────────


def _descriptions_path(project_id: str) -> Path:
    return Path(settings.data_root) / project_id / "patrons" / "controlled" / "descriptions.json"


def _load_descriptions(project_id: str) -> dict:
    p = _descriptions_path(project_id)
    if p.is_file():
        try:
            return json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_descriptions(project_id: str, descs: dict) -> None:
    p = _descriptions_path(project_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(descs, indent=2))
