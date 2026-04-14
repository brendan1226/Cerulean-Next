"""
cerulean/api/routers/marc_export.py
─────────────────────────────────────────────────────────────────────────────
MARC data export and record extraction tools.

POST /projects/{id}/marc-export/to-spreadsheet  — export selected fields as CSV/TSV
POST /projects/{id}/marc-export/extract-records  — extract records matching criteria
POST /projects/{id}/marc-export/to-json          — export as JSON
POST /projects/{id}/marc-export/from-json        — import from JSON
"""

import csv
import io
import json
import uuid
from pathlib import Path

import pymarc
from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import MARCFile

router = APIRouter(prefix="/projects", tags=["marc-export"])
settings = get_settings()


# ── Schemas ──────────────────────────────────────────────────────────

class SpreadsheetExportRequest(BaseModel):
    file_path: str | None = None
    fields: list[str]           # e.g. ["001", "245$a", "852$a", "952$p"]
    format: str = "csv"         # "csv" | "tsv"
    filter_tag: str | None = None
    filter_value: str | None = None
    max_records: int | None = None

class ExtractRecordsRequest(BaseModel):
    file_path: str | None = None
    criteria: list[dict]        # [{"tag": "942", "subfield": "c", "operator": "equals", "value": "DVD"}]
    logic: str = "and"          # "and" | "or"
    output_filename: str | None = None

class JsonExportRequest(BaseModel):
    file_path: str | None = None
    format: str = "pymarc"      # "pymarc" | "marc-in-json"


# ── Helpers ──────────────────────────────────────────────────────────

def _find_marc_file(project_id: str, file_path: str | None = None) -> Path:
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
    transformed = project_dir / "transformed"
    if transformed.is_dir():
        files = sorted(transformed.glob("*_transformed.mrc"))
        if files:
            return files[0]
    raw = project_dir / "raw"
    if raw.is_dir():
        files = sorted(raw.glob("*.mrc"))
        if files:
            return files[0]
    raise HTTPException(404, detail="No MARC files found in project.")


def _extract_field_value(record: pymarc.Record, field_spec: str) -> str:
    """Extract a field value from a record. Supports '001', '245$a', '852$h'."""
    if "$" in field_spec:
        tag, sub = field_spec.split("$", 1)
    else:
        tag = field_spec
        sub = None

    fields = record.get_fields(tag)
    if not fields:
        return ""

    values = []
    for f in fields:
        if sub and hasattr(f, "subfields"):
            for sf in f.subfields:
                if sf.code == sub:
                    values.append(sf.value)
        elif hasattr(f, "data"):
            values.append(f.data)
        elif not sub and hasattr(f, "subfields"):
            values.append(" ".join(sf.value for sf in f.subfields))

    return "; ".join(values) if values else ""


def _record_matches(record: pymarc.Record, criteria: list[dict], logic: str) -> bool:
    """Check if a record matches the given criteria."""
    results = []
    for c in criteria:
        tag = c.get("tag", "")
        sub = c.get("subfield")
        op = c.get("operator", "equals")
        val = c.get("value", "")

        field_spec = f"{tag}${sub}" if sub else tag
        actual = _extract_field_value(record, field_spec).lower()
        val_lower = val.lower()

        if op == "equals":
            results.append(actual == val_lower)
        elif op == "contains":
            results.append(val_lower in actual)
        elif op == "not_contains":
            results.append(val_lower not in actual)
        elif op == "starts_with":
            results.append(actual.startswith(val_lower))
        elif op == "exists":
            results.append(bool(actual))
        elif op == "missing":
            results.append(not actual)
        elif op == "regex":
            import re
            try:
                results.append(bool(re.search(val, _extract_field_value(record, field_spec))))
            except re.error:
                results.append(False)
        else:
            results.append(False)

    if not results:
        return True
    return all(results) if logic == "and" else any(results)


# ── Export to Spreadsheet ────────────────────────────────────────────

@router.post("/{project_id}/marc-export/to-spreadsheet")
async def export_to_spreadsheet(
    project_id: str,
    body: SpreadsheetExportRequest,
    db: AsyncSession = Depends(get_db),
):
    """Export selected MARC fields as a CSV/TSV spreadsheet."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    if not body.fields:
        raise HTTPException(400, detail="Specify at least one field to export.")

    delimiter = "\t" if body.format == "tsv" else ","
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=delimiter)
    writer.writerow(["record_index"] + body.fields)

    count = 0
    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for i, record in enumerate(reader):
            if record is None:
                continue

            # Apply filter if set
            if body.filter_tag and body.filter_value:
                actual = _extract_field_value(record, body.filter_tag)
                if body.filter_value.lower() not in actual.lower():
                    continue

            row = [str(i)]
            for field_spec in body.fields:
                row.append(_extract_field_value(record, field_spec))
            writer.writerow(row)
            count += 1

            if body.max_records and count >= body.max_records:
                break

    ext = "tsv" if body.format == "tsv" else "csv"
    filename = f"marc_export_{count}_records.{ext}"

    await audit_log(db, project_id, stage=3, level="info", tag="[export]",
                    message=f"Exported {count} records with fields {body.fields} to {ext}")

    return Response(
        content=buf.getvalue(),
        media_type="text/csv" if body.format == "csv" else "text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Extract Records ─────────────────────────────────────────────────

@router.post("/{project_id}/marc-export/extract-records")
async def extract_records(
    project_id: str,
    body: ExtractRecordsRequest,
    db: AsyncSession = Depends(get_db),
):
    """Extract records matching criteria into a new MARC file."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    if not body.criteria:
        raise HTTPException(400, detail="Specify at least one criterion.")

    project_dir = Path(settings.data_root) / project_id
    output_name = body.output_filename or f"extracted_{uuid.uuid4().hex[:8]}.mrc"
    if not output_name.endswith(".mrc"):
        output_name += ".mrc"
    output_path = project_dir / output_name

    matched = 0
    total = 0

    with open(str(source), "rb") as fh_in:
        reader = pymarc.MARCReader(fh_in, to_unicode=True, force_utf8=True, utf8_handling="replace")
        with open(str(output_path), "wb") as fh_out:
            writer = pymarc.MARCWriter(fh_out)
            for record in reader:
                if record is None:
                    continue
                total += 1
                if _record_matches(record, body.criteria, body.logic):
                    writer.write(record)
                    matched += 1
            writer.close()

    criteria_desc = ", ".join(f"{c.get('tag','')}{('$'+c['subfield']) if c.get('subfield') else ''} {c.get('operator','')} '{c.get('value','')[:20]}'" for c in body.criteria)
    await audit_log(db, project_id, stage=3, level="info", tag="[extract]",
                    message=f"Extracted {matched}/{total} records ({criteria_desc}) → {output_name}")

    return {
        "total_records": total,
        "matched_records": matched,
        "output_file": output_name,
        "criteria": body.criteria,
        "logic": body.logic,
    }


# ── JSON Export ──────────────────────────────────────────────────────

@router.post("/{project_id}/marc-export/to-json")
async def export_to_json(
    project_id: str,
    body: JsonExportRequest,
    db: AsyncSession = Depends(get_db),
):
    """Export MARC records as JSON."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    records_json = []
    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue
            records_json.append(record.as_dict())

    content = json.dumps(records_json, indent=2, ensure_ascii=False)
    filename = f"{source.stem}.json"
    safe_filename = filename.encode("ascii", "ignore").decode("ascii") or "export.json"

    await audit_log(db, project_id, stage=3, level="info", tag="[export]",
                    message=f"Exported {len(records_json)} records as JSON")

    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{safe_filename}"'},
    )


# ── JSON Import ──────────────────────────────────────────────────────

@router.post("/{project_id}/marc-export/from-json")
async def import_from_json(
    project_id: str,
    file: UploadFile,
    db: AsyncSession = Depends(get_db),
):
    """Import MARC records from a JSON file (pymarc format)."""
    await require_project(project_id, db)

    content = await file.read()
    try:
        data = json.loads(content.decode("utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise HTTPException(400, detail=f"Invalid JSON: {exc}")

    if not isinstance(data, list):
        raise HTTPException(400, detail="Expected a JSON array of record objects.")

    project_dir = Path(settings.data_root) / project_id
    project_dir.mkdir(parents=True, exist_ok=True)

    output_name = file.filename.replace(".json", ".mrc") if file.filename else "imported.mrc"
    raw_dir = project_dir / "raw"
    raw_dir.mkdir(exist_ok=True)

    file_id = str(uuid.uuid4())
    output_path = raw_dir / f"{file_id}.mrc"

    count = 0
    with open(str(output_path), "wb") as fh:
        writer = pymarc.MARCWriter(fh)
        for rec_dict in data:
            try:
                record = pymarc.Record()
                if "leader" in rec_dict:
                    record.leader = rec_dict["leader"]
                for field_data in rec_dict.get("fields", []):
                    for tag, value in field_data.items():
                        if isinstance(value, str):
                            record.add_field(pymarc.Field(tag=tag, data=value))
                        elif isinstance(value, dict):
                            ind1 = value.get("ind1", " ")
                            ind2 = value.get("ind2", " ")
                            subs = []
                            for sf in value.get("subfields", []):
                                for code, val in sf.items():
                                    subs.append(pymarc.Subfield(code=code, value=val))
                            record.add_field(pymarc.Field(tag=tag, indicators=[ind1, ind2], subfields=subs))
                writer.write(record)
                count += 1
            except Exception:
                continue
        writer.close()

    # Register as a MARCFile
    marc_file = MARCFile(
        id=file_id,
        project_id=project_id,
        filename=output_name,
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
    ingest_marc_task.apply_async(
        args=[project_id, file_id, str(output_path)],
        queue="ingest",
    )

    await audit_log(db, project_id, stage=1, level="info", tag="[import]",
                    message=f"Imported {count} records from JSON → {output_name}")

    return {
        "file_id": file_id,
        "filename": output_name,
        "records_imported": count,
        "message": f"Imported {count} records. Indexing started.",
    }
