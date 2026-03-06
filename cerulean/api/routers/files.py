"""
cerulean/api/routers/files.py
─────────────────────────────────────────────────────────────────────────────
POST /projects/{id}/files                       — upload MARC/CSV file
GET  /projects/{id}/files                       — list files for project
GET  /projects/{id}/files/{fid}/records/{n}     — fetch record N (0-indexed)
GET  /projects/{id}/files/{fid}/tags            — tag frequency histogram
"""

import os
import uuid
from pathlib import Path

import pymarc
from fastapi import APIRouter, Depends, HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import MARCFile, Project
from cerulean.schemas.projects import MARCFileOut, MARCFileUploadResponse, TagFrequencyOut
from cerulean.tasks.ingest import ingest_marc_task

settings = get_settings()
router = APIRouter(prefix="/projects", tags=["files"])

ALLOWED_EXTENSIONS = {".mrc", ".marc", ".mrk", ".txt", ".csv"}
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB


@router.post("/{project_id}/files", response_model=MARCFileUploadResponse, status_code=201)
async def upload_file(
    project_id: str,
    file: UploadFile,
    db: AsyncSession = Depends(get_db),
):
    """Upload a MARC or CSV file. Spawns ingest_marc_task immediately."""
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, detail={
            "error": "INVALID_FILE_TYPE",
            "message": f"File type '{suffix}' not supported. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        })

    # Save to data lake
    project_dir = Path(settings.data_root) / project_id / "raw"
    project_dir.mkdir(parents=True, exist_ok=True)

    file_id = str(uuid.uuid4())
    storage_path = str(project_dir / f"{file_id}{suffix}")

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(413, detail={"error": "FILE_TOO_LARGE", "message": "File exceeds 500 MB limit."})

    with open(storage_path, "wb") as fh:
        fh.write(content)

    # Create MARCFile row
    marc_file = MARCFile(
        id=file_id,
        project_id=project_id,
        filename=file.filename or f"upload{suffix}",
        storage_path=storage_path,
        file_size_bytes=len(content),
        status="uploaded",
    )
    db.add(marc_file)
    await db.flush()

    # Dispatch ingest task
    task = ingest_marc_task.apply_async(
        args=[project_id, file_id, storage_path],
        queue="ingest",
    )

    return MARCFileUploadResponse(
        file_id=file_id,
        filename=marc_file.filename,
        task_id=task.id,
        message="File uploaded. Indexing started.",
    )


@router.get("/{project_id}/files", response_model=list[MARCFileOut])
async def list_files(project_id: str, db: AsyncSession = Depends(get_db)):
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    result = await db.execute(
        select(MARCFile)
        .where(MARCFile.project_id == project_id)
        .order_by(MARCFile.sort_order, MARCFile.created_at)
    )
    return result.scalars().all()


@router.get("/{project_id}/files/{file_id}/records/{record_index}")
async def get_record(
    project_id: str,
    file_id: str,
    record_index: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch a single parsed MARC record by 0-based index.
    Returns a JSON representation of all fields and subfields.
    """
    marc_file = await _get_marc_file(project_id, file_id, db)

    if marc_file.status != "indexed":
        raise HTTPException(409, detail={"error": "NOT_READY", "message": "File is not yet indexed."})

    record = _read_record_at_index(marc_file.storage_path, record_index)
    if record is None:
        raise HTTPException(404, detail={"error": "RECORD_NOT_FOUND", "message": f"No record at index {record_index}."})

    return _record_to_dict(record, record_index)


@router.get("/{project_id}/files/{file_id}/tags", response_model=TagFrequencyOut)
async def get_tag_frequency(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return the pre-computed tag frequency histogram for a file."""
    marc_file = await _get_marc_file(project_id, file_id, db)

    if not marc_file.tag_frequency:
        raise HTTPException(409, detail={
            "error": "NOT_READY",
            "message": "Tag frequency analysis not yet complete. Check back shortly.",
        })

    return TagFrequencyOut(
        file_id=file_id,
        record_count=marc_file.record_count or 0,
        tags=marc_file.tag_frequency,
    )


# ── Helpers ────────────────────────────────────────────────────────────

async def _get_marc_file(project_id: str, file_id: str, db: AsyncSession) -> MARCFile:
    marc_file = await db.get(MARCFile, file_id)
    if not marc_file or marc_file.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "File not found."})
    return marc_file


def _read_record_at_index(storage_path: str, index: int) -> pymarc.Record | None:
    """Read a single record by 0-based index. O(n) scan — acceptable for spot-checks."""
    try:
        with open(storage_path, "rb") as fh:
            reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
            for i, record in enumerate(reader):
                if i == index:
                    return record
    except Exception:
        return None
    return None


def _record_to_dict(record: pymarc.Record, index: int) -> dict:
    """Serialise a pymarc Record to a JSON-friendly dict."""
    fields = []
    for field in record.fields:
        if field.is_control_field():
            fields.append({"tag": field.tag, "data": field.data})
        else:
            subfields = []
            for code, value in zip(field.subfields[0::2], field.subfields[1::2]):
                subfields.append({"code": code, "value": value})
            fields.append({
                "tag": field.tag,
                "ind1": field.indicator1,
                "ind2": field.indicator2,
                "subfields": subfields,
            })

    leader = record.leader if record.leader else ""
    title = record.title() or ""
    return {
        "index": index,
        "leader": leader,
        "title": title,
        "fields": fields,
    }
