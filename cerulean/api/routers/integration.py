"""
cerulean/api/routers/integration.py
─────────────────────────────────────────────────────────────────────────────
Integration API for the Koha Migration Workbench and other external systems.

Authenticated via X-Cerulean-API-Key header (not JWT/OAuth).

GET    /integration/projects                        — list active projects
POST   /integration/project/resolve                 — create or connect to project
POST   /integration/projects/{id}/files             — push individual file
POST   /integration/projects/{id}/control-values    — push SQL file
POST   /integration/projects/{id}/push              — bulk push
PATCH  /integration/projects/{id}/metadata          — update project metadata
GET    /integration/projects/{id}/status             — poll processing status
"""

import os
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log
from cerulean.api.routers.api_keys import validate_api_key
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import (
    APIKey,
    ControlValueSet,
    IntegrationPushLog,
    MARCFile,
    PatronFile,
    Project,
)
from cerulean.utils.sql_parser import parse_insert_statements

settings = get_settings()
router = APIRouter(prefix="/integration", tags=["integration"])

MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB


# ── Auth dependency ──────────────────────────────────────────────────


async def _require_api_key(
    request: Request,
    db: AsyncSession = Depends(get_db),
    scope: str = "read",
) -> APIKey:
    """Extract and validate X-Cerulean-API-Key header."""
    raw_key = request.headers.get("X-Cerulean-API-Key", "").strip()
    if not raw_key:
        raise HTTPException(401, detail="Missing X-Cerulean-API-Key header")
    return await validate_api_key(raw_key, db, required_scope=scope)


async def _require_write_key(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> APIKey:
    return await _require_api_key(request, db, scope="write")


# ── List projects ────────────────────────────────────────────────────


@router.get("/projects")
async def list_projects(
    request: Request,
    active: bool = True,
    db: AsyncSession = Depends(get_db),
):
    """List projects (for 'connect to existing' dropdown)."""
    await _require_api_key(request, db, scope="read")
    q = select(Project)
    if active:
        q = q.where(Project.archived == False)  # noqa: E712
    q = q.order_by(Project.created_at.desc())
    result = await db.execute(q)
    projects = result.scalars().all()
    return {
        "projects": [
            {
                "project_id": p.id,
                "project_code": p.code,
                "library_name": p.library_name,
                "current_stage": p.current_stage,
                "source_ils": p.source_ils,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            }
            for p in projects
        ]
    }


# ── Project resolution ───────────────────────────────────────────────


@router.post("/project/resolve")
async def resolve_project(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create or connect to a project by code."""
    api_key = await _require_write_key(request, db)
    body = await request.json()

    library_name = (body.get("library_name") or "").strip()
    project_code = (body.get("project_code") or "").strip()
    source_ils = (body.get("source_ils") or "").strip() or None
    action = body.get("action", "create_or_connect")

    if not library_name or not project_code:
        raise HTTPException(400, detail="library_name and project_code are required")

    result = await db.execute(
        select(Project).where(Project.code == project_code)
    )
    existing = result.scalar_one_or_none()

    if action == "connect_only":
        if not existing:
            raise HTTPException(404, detail=f"Project '{project_code}' not found")
        project = existing
        status = "connected"
    elif action == "create_only":
        if existing:
            raise HTTPException(409, detail=f"Project '{project_code}' already exists")
        project = Project(
            code=project_code,
            library_name=library_name,
            source_ils=source_ils,
            owner_id=api_key.user_id,
            visibility="shared",
        )
        db.add(project)
        await db.flush()
        await db.refresh(project)
        status = "created"
    else:  # create_or_connect
        if existing:
            project = existing
            status = "connected"
        else:
            project = Project(
                code=project_code,
                library_name=library_name,
                source_ils=source_ils,
                owner_id=api_key.user_id,
                visibility="shared",
            )
            db.add(project)
            await db.flush()
            await db.refresh(project)
            status = "created"

    await audit_log(
        db, project.id, stage=1, level="info", tag="[integration]",
        message=f"Project {status} via integration API (key: {api_key.key_prefix})",
    )
    await db.flush()

    base_url = str(request.base_url).rstrip("/")
    return {
        "project_id": project.id,
        "project_code": project.code,
        "library_name": project.library_name,
        "status": status,
        "current_stage": project.current_stage,
        "cerulean_url": f"{base_url}/#/projects/{project.id}",
    }


# ── Push individual file ─────────────────────────────────────────────


@router.post("/projects/{project_id}/files")
async def push_file(
    project_id: str,
    request: Request,
    file: UploadFile = File(...),
    data_type: str = Form("bibs_items"),
    file_format: str = Form("marc21"),
    items_link_key: str = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Push an individual file (MARC, items CSV, patron CSV)."""
    api_key = await _require_write_key(request, db)

    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail="Project not found")

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(413, detail="File exceeds 2 GB limit")

    source_label = "workbench"
    file_id = str(uuid.uuid4())

    if data_type == "patrons" or file_format == "patron_csv":
        file_row, task_id = await _store_patron_file(
            project_id, file_id, file.filename, content, source_label, db,
        )
    else:
        file_row, task_id = await _store_marc_file(
            project_id, file_id, file.filename, file_format, content,
            source_label, db,
        )

    push_log = IntegrationPushLog(
        project_id=project_id,
        api_key_id=api_key.id,
        push_type="file",
        files_received={"filename": file.filename, "data_type": data_type, "size": len(content)},
        status="processing",
    )
    db.add(push_log)

    await audit_log(
        db, project_id, stage=1, level="info", tag="[integration]",
        message=f"File pushed: {file.filename} ({data_type}, {len(content):,} bytes, source: {source_label})",
    )
    await db.flush()

    return {
        "file_id": file_id,
        "filename": file.filename,
        "size_bytes": len(content),
        "data_type": data_type,
        "source": source_label,
        "ingest_task_id": task_id,
        "status": "queued",
    }


# ── Push control value SQL ───────────────────────────────────────────


@router.post("/projects/{project_id}/control-values")
async def push_control_values(
    project_id: str,
    request: Request,
    file: UploadFile = File(...),
    cv_type: str = Form("other"),
    db: AsyncSession = Depends(get_db),
):
    """Push a control value SQL file. Cerulean parses INSERT statements."""
    api_key = await _require_write_key(request, db)

    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail="Project not found")

    content = await file.read()
    sql_text = content.decode("utf-8", errors="replace")

    # Store raw SQL file
    project_dir = Path(settings.data_root) / project_id / "control_values"
    project_dir.mkdir(parents=True, exist_ok=True)
    raw_path = project_dir / f"{cv_type}_{file.filename}"
    raw_path.write_bytes(content)

    # Parse INSERT statements
    parsed = parse_insert_statements(sql_text)

    # Store parsed values
    stored = 0
    for entry in parsed:
        row = ControlValueSet(
            project_id=project_id,
            cv_type=entry["cv_type"] if cv_type == "other" else cv_type,
            code=entry["code"],
            description=entry["description"],
            source="workbench",
            raw_sql_path=str(raw_path),
        )
        db.add(row)
        stored += 1

    push_log = IntegrationPushLog(
        project_id=project_id,
        api_key_id=api_key.id,
        push_type="control_values",
        files_received={"filename": file.filename, "cv_type": cv_type, "parsed_count": stored},
        status="complete",
    )
    db.add(push_log)

    await audit_log(
        db, project_id, stage=1, level="info", tag="[integration]",
        message=f"Control values pushed: {file.filename} ({cv_type}, {stored} values parsed)",
    )
    await db.flush()

    return {
        "cv_type": cv_type,
        "raw_file_stored": True,
        "parsed_count": stored,
        "values": [{"code": e["code"], "description": e["description"]} for e in parsed[:50]],
    }


# ── Bulk push ────────────────────────────────────────────────────────


@router.post("/projects/{project_id}/push")
async def bulk_push(
    project_id: str,
    request: Request,
    marc_file: UploadFile = File(None),
    items_csv: UploadFile = File(None),
    patron_csv: UploadFile = File(None),
    sql_branches: UploadFile = File(None),
    sql_item_types: UploadFile = File(None),
    sql_ccodes: UploadFile = File(None),
    sql_locations: UploadFile = File(None),
    sql_patron_cats: UploadFile = File(None),
    sql_other: UploadFile = File(None),
    items_link_key: str = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Bulk push — everything in one multipart request."""
    api_key = await _require_write_key(request, db)

    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail="Project not found")

    if not marc_file and not patron_csv:
        raise HTTPException(400, detail="At least marc_file or patron_csv is required")

    source_label = "workbench"
    tasks = {"ingest": [], "index": [], "control_values": []}
    files_received = 0
    sql_files_received = 0

    # MARC file
    if marc_file and marc_file.filename:
        content = await marc_file.read()
        file_id = str(uuid.uuid4())
        _, task_id = await _store_marc_file(
            project_id, file_id, marc_file.filename, "marc21", content,
            source_label, db,
        )
        tasks["ingest"].append(task_id)
        files_received += 1

    # Items CSV
    if items_csv and items_csv.filename:
        content = await items_csv.read()
        file_id = str(uuid.uuid4())
        _, task_id = await _store_marc_file(
            project_id, file_id, items_csv.filename, "items_csv", content,
            source_label, db,
        )
        tasks["ingest"].append(task_id)
        files_received += 1

    # Patron CSV
    if patron_csv and patron_csv.filename:
        content = await patron_csv.read()
        file_id = str(uuid.uuid4())
        _, task_id = await _store_patron_file(
            project_id, file_id, patron_csv.filename, content,
            source_label, db,
        )
        tasks["ingest"].append(task_id)
        files_received += 1

    # SQL files
    sql_files = [
        (sql_branches, "branches"),
        (sql_item_types, "item_types"),
        (sql_ccodes, "collection_codes"),
        (sql_locations, "shelving_locations"),
        (sql_patron_cats, "patron_categories"),
        (sql_other, "other"),
    ]
    for sql_file, cv_type in sql_files:
        if sql_file and sql_file.filename:
            content = await sql_file.read()
            sql_text = content.decode("utf-8", errors="replace")
            parsed = parse_insert_statements(sql_text)

            project_dir = Path(settings.data_root) / project_id / "control_values"
            project_dir.mkdir(parents=True, exist_ok=True)
            raw_path = project_dir / f"{cv_type}_{sql_file.filename}"
            raw_path.write_bytes(content)

            for entry in parsed:
                db.add(ControlValueSet(
                    project_id=project_id,
                    cv_type=entry["cv_type"] if cv_type == "other" else cv_type,
                    code=entry["code"],
                    description=entry["description"],
                    source="workbench",
                    raw_sql_path=str(raw_path),
                ))
            sql_files_received += 1

    push_log = IntegrationPushLog(
        project_id=project_id,
        api_key_id=api_key.id,
        push_type="bulk",
        files_received={
            "files": files_received,
            "sql_files": sql_files_received,
        },
        status="processing",
    )
    db.add(push_log)

    await audit_log(
        db, project_id, stage=1, level="info", tag="[integration]",
        message=f"Bulk push: {files_received} file(s), {sql_files_received} SQL file(s) (source: {source_label})",
    )
    await db.flush()

    base_url = str(request.base_url).rstrip("/")
    return {
        "project_id": project_id,
        "files_received": files_received,
        "sql_files_received": sql_files_received,
        "tasks": tasks,
        "cerulean_url": f"{base_url}/#/projects/{project_id}",
    }


# ── Update metadata ──────────────────────────────────────────────────


@router.patch("/projects/{project_id}/metadata")
async def update_metadata(
    project_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Update project metadata (Koha URL, notes, etc.)."""
    await _require_write_key(request, db)

    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail="Project not found")

    body = await request.json()
    if "koha_url" in body:
        project.koha_url = body["koha_url"]
    if "source_ils" in body:
        project.source_ils = body["source_ils"]
    if "notes" in body:
        project.notes = body.get("notes")

    await db.flush()
    return {"project_id": project_id, "updated": True}


# ── Status polling ───────────────────────────────────────────────────


@router.get("/projects/{project_id}/status")
async def project_status(
    project_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Poll processing status for all files + control values."""
    await _require_api_key(request, db, scope="read")

    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail="Project not found")

    # Files
    file_result = await db.execute(
        select(MARCFile).where(MARCFile.project_id == project_id)
    )
    files = file_result.scalars().all()

    # Control values
    cv_result = await db.execute(
        select(ControlValueSet.cv_type, ControlValueSet.code)
        .where(ControlValueSet.project_id == project_id)
    )
    cv_rows = cv_result.all()
    cv_summary: dict[str, dict] = {}
    for cv_type, code in cv_rows:
        if cv_type not in cv_summary:
            cv_summary[cv_type] = {"status": "parsed", "count": 0}
        cv_summary[cv_type]["count"] += 1

    base_url = str(request.base_url).rstrip("/")
    return {
        "project_id": project.id,
        "project_code": project.code,
        "library_name": project.library_name,
        "current_stage": project.current_stage,
        "files": [
            {
                "file_id": f.id,
                "filename": f.filename,
                "data_type": f.file_category or "bibs_items",
                "record_count": f.record_count,
                "ingest_status": "complete" if f.status == "indexed" else f.status,
                "index_status": f.status,
                "source": f.source,
            }
            for f in files
        ],
        "control_values": cv_summary,
        "cerulean_url": f"{base_url}/#/projects/{project.id}",
    }


# ── File storage helpers ─────────────────────────────────────────────


async def _store_marc_file(
    project_id: str,
    file_id: str,
    filename: str,
    file_format: str,
    content: bytes,
    source: str,
    db: AsyncSession,
) -> tuple:
    """Store a MARC/items file and dispatch ingest task. Returns (MARCFile, task_id)."""
    suffix = Path(filename).suffix or ".mrc"
    project_dir = Path(settings.data_root) / project_id / "raw"
    project_dir.mkdir(parents=True, exist_ok=True)
    storage_path = str(project_dir / f"{file_id}{suffix}")

    with open(storage_path, "wb") as fh:
        fh.write(content)

    is_items = file_format in ("items_csv", "items_json")
    marc_file = MARCFile(
        id=file_id,
        project_id=project_id,
        filename=filename,
        file_format=file_format,
        file_category="items_csv" if is_items else "marc",
        file_size_bytes=len(content),
        storage_path=storage_path,
        status="uploaded",
        source=source,
    )
    db.add(marc_file)
    await db.flush()

    from cerulean.tasks.ingest import ingest_marc_task
    task = ingest_marc_task.apply_async(
        args=[project_id, file_id, storage_path],
        queue="ingest",
    )
    from cerulean.api.routers.tasks import register_task
    register_task(project_id, task.id, "ingest")

    return marc_file, task.id


async def _store_patron_file(
    project_id: str,
    file_id: str,
    filename: str,
    content: bytes,
    source: str,
    db: AsyncSession,
) -> tuple:
    """Store a patron file and dispatch parse task. Returns (PatronFile, task_id)."""
    project_dir = Path(settings.data_root) / project_id / "patrons"
    project_dir.mkdir(parents=True, exist_ok=True)
    storage_path = str(project_dir / f"{file_id}{Path(filename).suffix or '.csv'}")

    with open(storage_path, "wb") as fh:
        fh.write(content)

    patron_file = PatronFile(
        id=file_id,
        project_id=project_id,
        filename=filename,
        file_format="csv",
        status="uploaded",
        source=source,
    )
    db.add(patron_file)
    await db.flush()

    from cerulean.tasks.patrons import patron_parse_task
    task = patron_parse_task.apply_async(
        args=[project_id, file_id],
        queue="patrons",
    )
    from cerulean.api.routers.tasks import register_task
    register_task(project_id, task.id, "patron_parse")

    return patron_file, task.id
