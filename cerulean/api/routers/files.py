"""
cerulean/api/routers/files.py
─────────────────────────────────────────────────────────────────────────────
POST   /projects/{id}/files                       — upload MARC/CSV file
GET    /projects/{id}/files                       — list files for project
DELETE /projects/{id}/files/{fid}                 — delete a file
GET    /projects/{id}/files/{fid}/records/{n}     — fetch record N (0-indexed)
GET    /projects/{id}/files/{fid}/tags            — tag frequency histogram
"""

import logging
import os
import uuid
from pathlib import Path

import pymarc
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import MARCFile, Project
from cerulean.schemas.projects import MARCFileOut, MARCFileUploadResponse, TagFrequencyOut
from cerulean.tasks.ingest import (
    ingest_items_csv_task,
    ingest_items_json_task,
    ingest_items_mrc_task,
    ingest_marc_task,
)
from cerulean.utils.marc import record_to_dict

logger = logging.getLogger(__name__)
settings = get_settings()
router = APIRouter(prefix="/projects", tags=["files"])

ALLOWED_EXTENSIONS = {".mrc", ".marc", ".mrk", ".txt", ".csv", ".dat", ".tsv", ".xml", ".json"}
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB


@router.post("/{project_id}/files", response_model=MARCFileUploadResponse, status_code=201)
async def upload_file(
    project_id: str,
    file: UploadFile,
    mode: str = Query("auto", description="Duplicate handling: auto|replace|add"),
    category: str = Query("marc", description="File category: marc|items|items_csv"),
    db: AsyncSession = Depends(get_db),
):
    """Upload a MARC or CSV file. Spawns ingest_marc_task immediately.

    mode:
        auto    — if a file with the same name exists, return 409 DUPLICATE_FILENAME
        replace — delete existing file with same name, upload new one
        add     — upload as new file even if same name exists
    """
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, detail={
            "error": "INVALID_FILE_TYPE",
            "message": f"File type '{suffix}' not supported. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        })

    filename = file.filename or f"upload{suffix}"

    # Check for duplicate filename
    result = await db.execute(
        select(MARCFile).where(
            MARCFile.project_id == project_id,
            MARCFile.filename == filename,
        )
    )
    existing = result.scalars().all()

    if existing and mode == "auto":
        raise HTTPException(409, detail={
            "error": "DUPLICATE_FILENAME",
            "message": f"A file named '{filename}' already exists in this project.",
            "existing_file_id": existing[0].id,
            "existing_status": existing[0].status,
            "existing_record_count": existing[0].record_count,
        })

    if existing and mode == "replace":
        for old_file in existing:
            # Remove file from disk
            if old_file.storage_path and os.path.isfile(old_file.storage_path):
                os.remove(old_file.storage_path)
            await db.execute(
                delete(MARCFile).where(MARCFile.id == old_file.id)
            )
        await db.flush()

    # Save to data lake
    project_dir = Path(settings.data_root) / project_id / "raw"
    project_dir.mkdir(parents=True, exist_ok=True)

    file_id = str(uuid.uuid4())
    storage_path = str(project_dir / f"{file_id}{suffix}")

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(413, detail={"error": "FILE_TOO_LARGE", "message": "File exceeds 2 GB limit."})

    with open(storage_path, "wb") as fh:
        fh.write(content)

    # Derive format from extension
    format_map = {".mrc": "iso2709", ".marc": "iso2709", ".mrk": "mrk", ".txt": "mrk", ".csv": "csv", ".dat": "iso2709", ".tsv": "csv", ".xml": "marcxml", ".json": "json"}
    file_format = format_map.get(suffix, "iso2709")

    # Create MARCFile row
    marc_file = MARCFile(
        id=file_id,
        project_id=project_id,
        filename=filename,
        file_format=file_format,
        file_category=category,
        storage_path=storage_path,
        file_size_bytes=len(content),
        status="uploaded",
    )
    db.add(marc_file)
    await db.flush()

    # Dispatch ingest task — items get format-specific tasks
    if category in ("items", "items_csv"):
        if file_format == "json":
            task = ingest_items_json_task.apply_async(
                args=[project_id, file_id, storage_path],
                queue="ingest",
            )
        elif file_format == "iso2709":
            task = ingest_items_mrc_task.apply_async(
                args=[project_id, file_id, storage_path],
                queue="ingest",
            )
        else:
            task = ingest_items_csv_task.apply_async(
                args=[project_id, file_id, storage_path],
                queue="ingest",
            )
    else:
        task = ingest_marc_task.apply_async(
            args=[project_id, file_id, storage_path],
            queue="ingest",
        )

    replaced = len(existing) if existing and mode == "replace" else 0
    msg = f"File uploaded (replaced {replaced} existing). Indexing started." if replaced else "File uploaded. Indexing started."

    return MARCFileUploadResponse(
        file_id=file_id,
        filename=marc_file.filename,
        task_id=task.id,
        message=msg,
    )


@router.get("/{project_id}/files", response_model=list[MARCFileOut])
async def list_files(
    project_id: str,
    category: str | None = Query(None, description="Filter by file_category: marc|items_csv"),
    db: AsyncSession = Depends(get_db),
):
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    q = select(MARCFile).where(MARCFile.project_id == project_id)
    if category:
        q = q.where(MARCFile.file_category == category)
    q = q.order_by(MARCFile.sort_order, MARCFile.created_at)
    result = await db.execute(q)
    return result.scalars().all()


@router.delete("/{project_id}/files/{file_id}", status_code=204)
async def delete_file(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a file and its data from disk."""
    marc_file = await _get_marc_file(project_id, file_id, db)

    # Remove file from disk
    if marc_file.storage_path and os.path.isfile(marc_file.storage_path):
        os.remove(marc_file.storage_path)

    # Remove transformed output if it exists
    stem = Path(marc_file.filename).stem
    transformed_path = Path(settings.data_root) / project_id / "transformed" / f"{stem}_transformed.mrc"
    if transformed_path.is_file():
        os.remove(str(transformed_path))

    await db.execute(delete(MARCFile).where(MARCFile.id == file_id))
    await db.flush()


@router.post("/{project_id}/files/{file_id}/recategorize")
async def recategorize_file(
    project_id: str,
    file_id: str,
    category: str = Query(..., description="New category: marc|items|items_csv"),
    db: AsyncSession = Depends(get_db),
):
    """Change a file's category and re-dispatch the appropriate ingest task."""
    marc_file = await _get_marc_file(project_id, file_id, db)

    marc_file.file_category = category
    marc_file.status = "uploaded"
    marc_file.error_message = None
    marc_file.record_count = None
    marc_file.tag_frequency = None
    marc_file.subfield_frequency = None
    marc_file.column_headers = None
    await db.flush()

    # Dispatch correct ingest task
    file_format = marc_file.file_format or "csv"
    if category in ("items", "items_csv"):
        if file_format == "json":
            task = ingest_items_json_task.apply_async(
                args=[project_id, file_id, marc_file.storage_path], queue="ingest")
        elif file_format == "iso2709":
            task = ingest_items_mrc_task.apply_async(
                args=[project_id, file_id, marc_file.storage_path], queue="ingest")
        else:
            task = ingest_items_csv_task.apply_async(
                args=[project_id, file_id, marc_file.storage_path], queue="ingest")
    else:
        task = ingest_marc_task.apply_async(
            args=[project_id, file_id, marc_file.storage_path], queue="ingest")

    return {"file_id": file_id, "category": category, "task_id": task.id, "message": f"Re-categorized as {category}. Re-indexing started."}


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

    return record_to_dict(record, record_index)


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
        subfield_frequency=marc_file.subfield_frequency,
    )


@router.get("/{project_id}/search")
async def search_project_records(
    project_id: str,
    q: str = "",
    offset: int = 0,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """Full-text search across indexed MARC records (requires Elasticsearch)."""
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    from cerulean.core.search import get_es_client, search_records

    es = get_es_client()
    if es is None:
        raise HTTPException(501, detail={
            "error": "ES_NOT_CONFIGURED",
            "message": "Elasticsearch is not configured. Start with: docker compose --profile search up",
        })

    if not q:
        raise HTTPException(400, detail={"error": "MISSING_QUERY", "message": "Query parameter 'q' is required."})

    return search_records(es, project_id, q, offset=offset, limit=limit)


# ── Mark Ready to Load ────────────────────────────────────────────────


@router.post("/{project_id}/files/{file_id}/mark-ready")
async def mark_file_ready(
    project_id: str,
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Mark a MARC file as ready to push, skipping stages 4-6.

    Creates a symlink from the uploaded raw file to ``output.mrc`` in the
    project directory.  Stage 7's push file finder already looks for
    ``output.mrc``, so the file will immediately appear as a push candidate.

    Use this when the MARC data is clean and doesn't need field mapping,
    dedup, or reconciliation — common for well-structured exports.
    """
    from pathlib import Path
    from cerulean.core.config import get_settings

    settings = get_settings()
    marc_file = await db.get(MARCFile, file_id)
    if not marc_file or marc_file.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "File not found."})
    if marc_file.status != "indexed":
        raise HTTPException(409, detail={
            "error": "NOT_INDEXED",
            "message": "File must be fully indexed before marking ready.",
        })

    source = Path(marc_file.storage_path)
    if not source.is_file():
        raise HTTPException(404, detail={"error": "FILE_MISSING", "message": "Source file not found on disk."})

    project_dir = Path(settings.data_root) / project_id
    output = project_dir / "output.mrc"

    # Remove any existing output.mrc (symlink or file)
    if output.is_symlink() or output.is_file():
        output.unlink()

    # Symlink — no 1.2 GB copy needed
    output.symlink_to(source)

    return {
        "file_id": file_id,
        "filename": marc_file.filename,
        "output_path": "output.mrc",
        "record_count": marc_file.record_count,
        "message": f"'{marc_file.filename}' marked as ready to load. Available in Stage 7.",
    }


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
        logger.warning("Error reading record at index %d from %s", index, storage_path, exc_info=True)
        return None
    return None
