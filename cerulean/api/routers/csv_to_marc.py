"""
cerulean/api/routers/csv_to_marc.py
─────────────────────────────────────────────────────────────────────────────
Convert CSV/spreadsheet data into MARC records.

POST /projects/{id}/csv-to-marc/preview    — upload CSV and preview columns
POST /projects/{id}/csv-to-marc/convert    — convert with column→tag mapping
"""

import csv
import io
import uuid
from pathlib import Path

import pymarc
from fastapi import APIRouter, Depends, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import MARCFile

router = APIRouter(prefix="/projects", tags=["csv-to-marc"])
settings = get_settings()


class ColumnMapping(BaseModel):
    column: str                     # CSV column name
    tag: str                        # MARC tag (e.g., "245")
    subfield: str = "a"             # subfield code
    indicators: list[str] = [" ", " "]

class ConvertRequest(BaseModel):
    mappings: list[ColumnMapping]
    leader: str = "     nam a22     a 4500"   # default leader for books
    output_filename: str = "converted.mrc"


@router.post("/{project_id}/csv-to-marc/preview")
async def preview_csv(
    project_id: str,
    file: UploadFile,
    db: AsyncSession = Depends(get_db),
):
    """Upload a CSV and preview columns + sample data for mapping."""
    await require_project(project_id, db)

    content = await file.read()
    text = content.decode("utf-8-sig")

    # Detect delimiter
    try:
        dialect = csv.Sniffer().sniff(text[:4096])
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    columns = reader.fieldnames or []

    samples = []
    for i, row in enumerate(reader):
        if i >= 5:
            break
        samples.append(dict(row))

    # Count total rows
    text_io = io.StringIO(text)
    total_rows = sum(1 for _ in text_io) - 1  # subtract header

    # Save temp file for conversion step
    project_dir = Path(settings.data_root) / project_id
    project_dir.mkdir(parents=True, exist_ok=True)
    temp_path = project_dir / "_csv_to_marc_temp.csv"
    temp_path.write_text(text, encoding="utf-8")

    return {
        "columns": columns,
        "samples": samples,
        "total_rows": total_rows,
        "delimiter": delimiter,
        "temp_file": str(temp_path),
    }


@router.post("/{project_id}/csv-to-marc/convert")
async def convert_csv_to_marc(
    project_id: str,
    body: ConvertRequest,
    db: AsyncSession = Depends(get_db),
):
    """Convert a previously uploaded CSV into MARC records using the provided mappings."""
    await require_project(project_id, db)

    if not body.mappings:
        raise HTTPException(400, detail="At least one column mapping is required.")

    project_dir = Path(settings.data_root) / project_id
    temp_path = project_dir / "_csv_to_marc_temp.csv"
    if not temp_path.is_file():
        raise HTTPException(404, detail="Upload a CSV first via the preview endpoint.")

    text = temp_path.read_text(encoding="utf-8")

    # Detect delimiter
    try:
        dialect = csv.Sniffer().sniff(text[:4096])
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    # Group mappings by tag for proper field construction
    # Multiple subfields with the same tag go into one field
    tag_groups = {}
    for m in body.mappings:
        key = (m.tag, tuple(m.indicators))
        if key not in tag_groups:
            tag_groups[key] = []
        tag_groups[key].append(m)

    # Write MARC
    raw_dir = project_dir / "raw"
    raw_dir.mkdir(exist_ok=True)
    file_id = str(uuid.uuid4())
    output_path = raw_dir / f"{file_id}.mrc"

    count = 0
    errors = 0
    with open(str(output_path), "wb") as fh:
        writer = pymarc.MARCWriter(fh)
        for row in reader:
            try:
                record = pymarc.Record()
                # Set leader
                leader = body.leader
                if len(leader) < 24:
                    leader = leader.ljust(24)
                record.leader = leader

                for (tag, indicators), mappings in tag_groups.items():
                    if int(tag) < 10:
                        # Control field — use first mapping's column value
                        val = row.get(mappings[0].column, "").strip()
                        if val:
                            record.add_field(pymarc.Field(tag=tag, data=val))
                    else:
                        # Data field — build subfields
                        subs = []
                        for m in mappings:
                            val = row.get(m.column, "").strip()
                            if val:
                                subs.append(pymarc.Subfield(code=m.subfield, value=val))
                        if subs:
                            record.add_field(pymarc.Field(
                                tag=tag,
                                indicators=list(indicators),
                                subfields=subs,
                            ))

                writer.write(record)
                count += 1
            except Exception:
                errors += 1
        writer.close()

    # Clean up temp file
    temp_path.unlink(missing_ok=True)

    # Register as MARCFile
    output_filename = body.output_filename or "converted.mrc"
    marc_file = MARCFile(
        id=file_id,
        project_id=project_id,
        filename=output_filename,
        file_format="iso2709",
        file_category="marc",
        file_size_bytes=output_path.stat().st_size,
        storage_path=str(output_path),
        record_count=count,
        status="uploaded",
    )
    db.add(marc_file)
    await db.flush()

    # Trigger ingest
    from cerulean.tasks.ingest import ingest_marc_task
    ingest_marc_task.apply_async(args=[project_id, file_id, str(output_path)], queue="ingest")

    await audit_log(db, project_id, stage=1, level="info", tag="[csv-to-marc]",
                    message=f"Converted CSV → {count} MARC records ({errors} errors) → {output_filename}")

    return {
        "file_id": file_id,
        "filename": output_filename,
        "records_created": count,
        "errors": errors,
        "message": f"Created {count} MARC records. Indexing started.",
    }
