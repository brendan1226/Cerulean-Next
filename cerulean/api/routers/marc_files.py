"""
cerulean/api/routers/marc_files.py
─────────────────────────────────────────────────────────────────────────────
MARC file management tools — split and join.

POST /projects/{id}/marc-files/split   — split a file by criteria or count
POST /projects/{id}/marc-files/join    — merge multiple files into one
GET  /projects/{id}/marc-files/list    — list all MARC files in project dir
"""

import uuid
from pathlib import Path

import pymarc
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db

router = APIRouter(prefix="/projects", tags=["marc-files"])
settings = get_settings()


class SplitRequest(BaseModel):
    file_path: str | None = None
    mode: str = "count"         # "count" | "field"
    records_per_file: int = 10000       # for mode=count
    split_field: str | None = None      # for mode=field, e.g. "852$a"
    output_prefix: str = "split"

class JoinRequest(BaseModel):
    files: list[str]            # filenames to join
    output_filename: str = "joined.mrc"
    deduplicate_001: bool = False

class FileInfo(BaseModel):
    filename: str
    path: str
    size_bytes: int
    record_count: int | None = None


def _find_file(project_id: str, file_path: str | None) -> Path:
    project_dir = Path(settings.data_root) / project_id
    if file_path:
        for sub in ["", "raw", "transformed"]:
            candidate = (project_dir / sub / file_path) if sub else (project_dir / file_path)
            if candidate.is_file():
                return candidate
        raise HTTPException(404, detail=f"File not found: {file_path}")
    for name in ["output.mrc", "Biblios-mapped-items.mrc", "merged_deduped.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            return candidate
    raise HTTPException(404, detail="No MARC files found.")


def _count_records(path: Path) -> int:
    count = 0
    with open(str(path), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for rec in reader:
            if rec:
                count += 1
    return count


# ── List Files ───────────────────────────────────────────────────────

@router.get("/{project_id}/marc-files/list")
async def list_marc_files(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all .mrc files in the project directory tree."""
    await require_project(project_id, db)
    project_dir = Path(settings.data_root) / project_id

    files = []
    if project_dir.is_dir():
        for mrc in sorted(project_dir.rglob("*.mrc")):
            rel = str(mrc.relative_to(project_dir))
            files.append({
                "filename": mrc.name,
                "path": rel,
                "size_bytes": mrc.stat().st_size,
            })

    return {"files": files}


# ── Split ────────────────────────────────────────────────────────────

@router.post("/{project_id}/marc-files/split")
async def split_file(
    project_id: str,
    body: SplitRequest,
    db: AsyncSession = Depends(get_db),
):
    """Split a MARC file by record count or by field value."""
    await require_project(project_id, db)
    source = _find_file(project_id, body.file_path)
    project_dir = Path(settings.data_root) / project_id

    if body.mode == "count":
        return _split_by_count(source, project_dir, body, project_id, db)
    elif body.mode == "field":
        if not body.split_field:
            raise HTTPException(400, detail="split_field is required for mode=field")
        return await _split_by_field(source, project_dir, body, project_id, db)
    else:
        raise HTTPException(400, detail="mode must be 'count' or 'field'")


def _extract_value(record, field_spec):
    if "$" in field_spec:
        tag, sub = field_spec.split("$", 1)
    else:
        tag, sub = field_spec, None

    fields = record.get_fields(tag)
    for f in fields:
        if sub and hasattr(f, "subfields"):
            for sf in f.subfields:
                if sf.code == sub:
                    return sf.value
        elif hasattr(f, "data"):
            return f.data
    return "_no_value_"


def _split_by_count(source, project_dir, body, project_id, db):
    output_files = []
    file_num = 1
    count = 0
    writer = None
    current_path = None

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue

            if count % body.records_per_file == 0:
                if writer:
                    writer.close()
                    output_files.append({"filename": current_path.name, "records": count % body.records_per_file or body.records_per_file})
                current_path = project_dir / f"{body.output_prefix}_{file_num:04d}.mrc"
                writer = pymarc.MARCWriter(open(str(current_path), "wb"))
                file_num += 1

            writer.write(record)
            count += 1

    if writer:
        writer.close()
        remaining = count % body.records_per_file or body.records_per_file
        output_files.append({"filename": current_path.name, "records": remaining})

    return {
        "total_records": count,
        "files_created": len(output_files),
        "files": output_files,
    }


async def _split_by_field(source, project_dir, body, project_id, db):
    writers = {}
    counts = {}

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue

            value = _extract_value(record, body.split_field)
            safe_value = "".join(c if c.isalnum() or c in "-_" else "_" for c in value)[:50]
            key = safe_value or "_empty_"

            if key not in writers:
                path = project_dir / f"{body.output_prefix}_{key}.mrc"
                writers[key] = pymarc.MARCWriter(open(str(path), "wb"))
                counts[key] = 0

            writers[key].write(record)
            counts[key] += 1

    for w in writers.values():
        w.close()

    files = [{"filename": f"{body.output_prefix}_{k}.mrc", "field_value": k, "records": c} for k, c in sorted(counts.items(), key=lambda x: -x[1])]

    await audit_log(db, project_id, stage=3, level="info", tag="[split]",
                    message=f"Split by {body.split_field} → {len(files)} files, {sum(counts.values())} records")

    return {
        "total_records": sum(counts.values()),
        "files_created": len(files),
        "split_field": body.split_field,
        "files": files,
    }


# ── Join ─────────────────────────────────────────────────────────────

@router.post("/{project_id}/marc-files/join")
async def join_files(
    project_id: str,
    body: JoinRequest,
    db: AsyncSession = Depends(get_db),
):
    """Merge multiple MARC files into one."""
    await require_project(project_id, db)
    project_dir = Path(settings.data_root) / project_id

    if not body.files or len(body.files) < 2:
        raise HTTPException(400, detail="Specify at least 2 files to join.")

    output_path = project_dir / body.output_filename
    seen_001 = set()
    total = 0
    duplicates = 0

    with open(str(output_path), "wb") as fh_out:
        writer = pymarc.MARCWriter(fh_out)
        for fname in body.files:
            source = None
            for sub in ["", "raw", "transformed"]:
                candidate = (project_dir / sub / fname) if sub else (project_dir / fname)
                if candidate.is_file():
                    source = candidate
                    break
            if not source:
                continue

            with open(str(source), "rb") as fh_in:
                reader = pymarc.MARCReader(fh_in, to_unicode=True, force_utf8=True, utf8_handling="replace")
                for record in reader:
                    if record is None:
                        continue

                    if body.deduplicate_001:
                        ctrl = record.get_fields("001")
                        if ctrl:
                            val = ctrl[0].data
                            if val in seen_001:
                                duplicates += 1
                                continue
                            seen_001.add(val)

                    writer.write(record)
                    total += 1
        writer.close()

    await audit_log(db, project_id, stage=3, level="info", tag="[join]",
                    message=f"Joined {len(body.files)} files → {body.output_filename} ({total} records, {duplicates} duplicates skipped)")

    return {
        "total_records": total,
        "duplicates_skipped": duplicates,
        "output_file": body.output_filename,
        "files_joined": body.files,
    }
