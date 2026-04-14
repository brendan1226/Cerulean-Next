"""
cerulean/api/routers/batch_edit.py
─────────────────────────────────────────────────────────────────────────────
MarcEdit-style batch editing operations on MARC files.

POST /projects/{id}/batch-edit/find-replace     — find/replace across all records
POST /projects/{id}/batch-edit/add-field        — add a field to all records
POST /projects/{id}/batch-edit/delete-field     — delete a field from all records
POST /projects/{id}/batch-edit/regex            — regex substitution on a subfield
POST /projects/{id}/batch-edit/preview          — preview changes without applying
"""

import re
import uuid
from pathlib import Path

import pymarc
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db

router = APIRouter(prefix="/projects", tags=["batch-edit"])
settings = get_settings()


# ── Schemas ──────────────────────────────────────────────────────────

class FindReplaceRequest(BaseModel):
    file_path: str | None = None        # relative filename or auto-detect
    tag: str | None = None              # limit to this tag (e.g., "245"), None = all tags
    subfield: str | None = None         # limit to this subfield code (e.g., "a"), None = all
    find: str                           # text to find
    replace: str                        # replacement text
    case_sensitive: bool = False
    whole_field: bool = False           # replace entire field value vs substring
    max_records: int | None = None      # limit processing (None = all)

class AddFieldRequest(BaseModel):
    file_path: str | None = None
    tag: str                            # e.g., "590"
    indicators: list[str] = [" ", " "]
    subfields: list[dict]               # [{"code": "a", "value": "Local note"}]
    condition_tag: str | None = None    # only add if this tag exists/doesn't exist
    condition_exists: bool = True       # True = add if tag exists, False = add if missing

class DeleteFieldRequest(BaseModel):
    file_path: str | None = None
    tag: str                            # e.g., "9XX" or "852"
    subfield: str | None = None         # if set, delete only this subfield (not entire field)
    value_contains: str | None = None   # only delete if field/subfield contains this text

class RegexRequest(BaseModel):
    file_path: str | None = None
    tag: str
    subfield: str | None = None
    pattern: str                        # regex pattern
    replacement: str                    # replacement (supports \1, \2 groups)
    flags: str = ""                     # "i" for case-insensitive, "g" for global (default)

class BatchEditResult(BaseModel):
    records_processed: int
    records_modified: int
    modifications: int
    output_file: str
    preview_samples: list[dict] = []


# ── Helpers ──────────────────────────────────────────────────────────

def _find_marc_file(project_id: str, file_path: str | None = None) -> Path:
    """Find the best MARC file to edit."""
    project_dir = Path(settings.data_root) / project_id
    if file_path:
        candidate = project_dir / file_path
        if candidate.is_file():
            return candidate
        # Try in raw/
        for sub in ["", "raw", "transformed"]:
            candidate = project_dir / sub / file_path if sub else project_dir / file_path
            if candidate.is_file():
                return candidate
        raise HTTPException(404, detail=f"File not found: {file_path}")

    # Auto-detect: prefer output > merged > transformed > raw
    for name in ["output.mrc", "Biblios-mapped-items.mrc", "merged_deduped.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            return candidate
    # Look in transformed/
    transformed = project_dir / "transformed"
    if transformed.is_dir():
        files = sorted(transformed.glob("*_transformed.mrc"))
        if files:
            return files[0]
    # Look in raw/
    raw = project_dir / "raw"
    if raw.is_dir():
        files = sorted(raw.glob("*.mrc"))
        if files:
            return files[0]
    raise HTTPException(404, detail="No MARC files found in project.")


def _write_records(records: list[pymarc.Record], output_path: Path):
    """Write records to a MARC file."""
    with open(str(output_path), "wb") as fh:
        writer = pymarc.MARCWriter(fh)
        for rec in records:
            writer.write(rec)
        writer.close()


# ── Find & Replace ──────────────────────────────────────────────────

@router.post("/{project_id}/batch-edit/find-replace", response_model=BatchEditResult)
async def find_replace(
    project_id: str,
    body: FindReplaceRequest,
    db: AsyncSession = Depends(get_db),
):
    """Find and replace text across all records in a MARC file."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    records = []
    modified_count = 0
    total_mods = 0
    samples = []

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue
            rec_modified = False
            for field in record.get_fields():
                if body.tag and field.tag != body.tag:
                    continue
                if hasattr(field, "subfields") and field.subfields:
                    new_subs = []
                    for sf in field.subfields:
                        if body.subfield and sf.code != body.subfield:
                            new_subs.append(sf)
                            continue
                        old_val = sf.value
                        if body.whole_field:
                            flags = 0 if body.case_sensitive else re.IGNORECASE
                            if re.search(re.escape(body.find), old_val, flags):
                                new_val = body.replace
                            else:
                                new_val = old_val
                        else:
                            if body.case_sensitive:
                                new_val = old_val.replace(body.find, body.replace)
                            else:
                                new_val = re.sub(re.escape(body.find), body.replace, old_val, flags=re.IGNORECASE)
                        if new_val != old_val:
                            rec_modified = True
                            total_mods += 1
                            if len(samples) < 5:
                                samples.append({"record": len(records), "tag": field.tag, "sub": sf.code, "before": old_val[:80], "after": new_val[:80]})
                        new_subs.append(pymarc.Subfield(code=sf.code, value=new_val))
                    field.subfields = new_subs
                elif hasattr(field, "data"):
                    old_val = field.data
                    if body.case_sensitive:
                        new_val = old_val.replace(body.find, body.replace)
                    else:
                        new_val = re.sub(re.escape(body.find), body.replace, old_val, flags=re.IGNORECASE)
                    if new_val != old_val:
                        field.data = new_val
                        rec_modified = True
                        total_mods += 1

            if rec_modified:
                modified_count += 1
            records.append(record)

    # Write output
    output = source.parent / f"{source.stem}_edited{source.suffix}"
    _write_records(records, output)

    await audit_log(db, project_id, stage=3, level="info", tag="[batch-edit]",
                    message=f"Find/replace: '{body.find}' → '{body.replace}' in {body.tag or 'all'} — {total_mods} changes in {modified_count}/{len(records)} records")

    return BatchEditResult(
        records_processed=len(records),
        records_modified=modified_count,
        modifications=total_mods,
        output_file=output.name,
        preview_samples=samples,
    )


# ── Add Field ────────────────────────────────────────────────────────

@router.post("/{project_id}/batch-edit/add-field", response_model=BatchEditResult)
async def add_field(
    project_id: str,
    body: AddFieldRequest,
    db: AsyncSession = Depends(get_db),
):
    """Add a field to all records (optionally conditional on another field existing)."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    records = []
    modified_count = 0
    samples = []

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue

            # Check condition
            should_add = True
            if body.condition_tag:
                has_tag = bool(record.get_fields(body.condition_tag))
                should_add = has_tag if body.condition_exists else not has_tag

            if should_add:
                subs = []
                for sf in body.subfields:
                    subs.append(pymarc.Subfield(code=sf["code"], value=sf["value"]))
                ind1 = body.indicators[0] if len(body.indicators) > 0 else " "
                ind2 = body.indicators[1] if len(body.indicators) > 1 else " "
                new_field = pymarc.Field(tag=body.tag, indicators=[ind1, ind2], subfields=subs)
                record.add_ordered_field(new_field)
                modified_count += 1
                if len(samples) < 3:
                    samples.append({"record": len(records), "tag": body.tag, "added": str(new_field)[:100]})

            records.append(record)

    output = source.parent / f"{source.stem}_edited{source.suffix}"
    _write_records(records, output)

    sub_desc = ", ".join(f"${s['code']}={s['value'][:20]}" for s in body.subfields)
    await audit_log(db, project_id, stage=3, level="info", tag="[batch-edit]",
                    message=f"Add field {body.tag} ({sub_desc}) to {modified_count}/{len(records)} records")

    return BatchEditResult(
        records_processed=len(records),
        records_modified=modified_count,
        modifications=modified_count,
        output_file=output.name,
        preview_samples=samples,
    )


# ── Delete Field ─────────────────────────────────────────────────────

@router.post("/{project_id}/batch-edit/delete-field", response_model=BatchEditResult)
async def delete_field(
    project_id: str,
    body: DeleteFieldRequest,
    db: AsyncSession = Depends(get_db),
):
    """Delete a field (or subfield) from all records."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    records = []
    modified_count = 0
    total_mods = 0
    samples = []

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue

            rec_modified = False
            fields_to_remove = []

            for field in record.get_fields(body.tag):
                # Check value filter
                if body.value_contains:
                    field_text = str(field)
                    if body.value_contains.lower() not in field_text.lower():
                        continue

                if body.subfield and hasattr(field, "subfields"):
                    # Delete specific subfield only
                    new_subs = [sf for sf in field.subfields if sf.code != body.subfield]
                    if len(new_subs) < len(field.subfields):
                        field.subfields = new_subs
                        rec_modified = True
                        total_mods += 1
                else:
                    # Delete entire field
                    fields_to_remove.append(field)
                    total_mods += 1

            for f in fields_to_remove:
                record.remove_field(f)
                rec_modified = True

            if rec_modified:
                modified_count += 1
                if len(samples) < 3:
                    samples.append({"record": len(records), "tag": body.tag, "deleted": len(fields_to_remove)})

            records.append(record)

    output = source.parent / f"{source.stem}_edited{source.suffix}"
    _write_records(records, output)

    await audit_log(db, project_id, stage=3, level="info", tag="[batch-edit]",
                    message=f"Delete {body.tag}{('$' + body.subfield) if body.subfield else ''} — {total_mods} deletions in {modified_count}/{len(records)} records")

    return BatchEditResult(
        records_processed=len(records),
        records_modified=modified_count,
        modifications=total_mods,
        output_file=output.name,
        preview_samples=samples,
    )


# ── Regex ────────────────────────────────────────────────────────────

@router.post("/{project_id}/batch-edit/regex", response_model=BatchEditResult)
async def regex_replace(
    project_id: str,
    body: RegexRequest,
    db: AsyncSession = Depends(get_db),
):
    """Apply a regex substitution to a field/subfield across all records."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    try:
        flags = re.IGNORECASE if "i" in body.flags else 0
        compiled = re.compile(body.pattern, flags)
    except re.error as exc:
        raise HTTPException(400, detail=f"Invalid regex: {exc}")

    records = []
    modified_count = 0
    total_mods = 0
    samples = []

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue

            rec_modified = False
            for field in record.get_fields(body.tag):
                if hasattr(field, "subfields") and field.subfields:
                    new_subs = []
                    for sf in field.subfields:
                        if body.subfield and sf.code != body.subfield:
                            new_subs.append(sf)
                            continue
                        old_val = sf.value
                        new_val = compiled.sub(body.replacement, old_val)
                        if new_val != old_val:
                            rec_modified = True
                            total_mods += 1
                            if len(samples) < 5:
                                samples.append({"record": len(records), "tag": field.tag, "sub": sf.code, "before": old_val[:80], "after": new_val[:80]})
                        new_subs.append(pymarc.Subfield(code=sf.code, value=new_val))
                    field.subfields = new_subs
                elif hasattr(field, "data"):
                    old_val = field.data
                    new_val = compiled.sub(body.replacement, old_val)
                    if new_val != old_val:
                        field.data = new_val
                        rec_modified = True
                        total_mods += 1

            if rec_modified:
                modified_count += 1
            records.append(record)

    output = source.parent / f"{source.stem}_edited{source.suffix}"
    _write_records(records, output)

    await audit_log(db, project_id, stage=3, level="info", tag="[batch-edit]",
                    message=f"Regex {body.tag}: s/{body.pattern}/{body.replacement}/ — {total_mods} changes in {modified_count}/{len(records)} records")

    return BatchEditResult(
        records_processed=len(records),
        records_modified=modified_count,
        modifications=total_mods,
        output_file=output.name,
        preview_samples=samples,
    )


# ── Call Number Generation ───────────────────────────────────────────

class CallNumberRequest(BaseModel):
    file_path: str | None = None
    scheme: str = "dewey"           # "dewey" | "lc"
    source_tag: str = "082"         # where to read classification from
    source_sub: str = "a"
    target_tag: str = "952"         # where to write call number
    target_sub: str = "o"
    prefix_tag: str | None = None
    prefix_sub: str | None = None
    skip_if_exists: bool = True

@router.post("/{project_id}/batch-edit/call-numbers", response_model=BatchEditResult)
async def generate_call_numbers(
    project_id: str,
    body: CallNumberRequest,
    db: AsyncSession = Depends(get_db),
):
    """Generate call numbers from classification fields (082/050) into target (952$o)."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    records = []
    modified_count = 0
    total_mods = 0
    samples = []

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue

            if body.skip_if_exists:
                existing = record.get_fields(body.target_tag)
                has_target = any(
                    sf.code == body.target_sub and sf.value.strip()
                    for f in existing if hasattr(f, "subfields")
                    for sf in f.subfields
                )
                if has_target:
                    records.append(record)
                    continue

            class_value = ""
            for f in record.get_fields(body.source_tag):
                if hasattr(f, "subfields"):
                    for sf in f.subfields:
                        if sf.code == body.source_sub:
                            class_value = sf.value.strip()
                            break
                elif hasattr(f, "data"):
                    class_value = f.data.strip()
                if class_value:
                    break

            if not class_value:
                records.append(record)
                continue

            call_number = class_value
            author_fields = record.get_fields("100")
            if author_fields and hasattr(author_fields[0], "subfields"):
                for sf in author_fields[0].subfields:
                    if sf.code == "a":
                        surname = sf.value.strip().rstrip(",").strip()
                        if surname:
                            call_number += " " + surname[0].upper()
                        break

            if body.prefix_tag and body.prefix_sub:
                for f in record.get_fields(body.prefix_tag):
                    if hasattr(f, "subfields"):
                        for sf in f.subfields:
                            if sf.code == body.prefix_sub:
                                call_number = sf.value.strip() + " " + call_number
                                break

            target_fields = record.get_fields(body.target_tag)
            if target_fields and hasattr(target_fields[0], "subfields"):
                target_fields[0].subfields.append(pymarc.Subfield(code=body.target_sub, value=call_number))
            else:
                record.add_ordered_field(pymarc.Field(
                    tag=body.target_tag, indicators=[" ", " "],
                    subfields=[pymarc.Subfield(code=body.target_sub, value=call_number)],
                ))

            modified_count += 1
            total_mods += 1
            if len(samples) < 5:
                samples.append({"record": len(records), "tag": body.target_tag, "sub": body.target_sub, "before": "", "after": call_number[:80]})
            records.append(record)

    output = source.parent / f"{source.stem}_edited{source.suffix}"
    _write_records(records, output)

    await audit_log(db, project_id, stage=3, level="info", tag="[batch-edit]",
                    message=f"Call numbers: {body.source_tag}${body.source_sub} → {body.target_tag}${body.target_sub} — {total_mods} generated")

    return BatchEditResult(
        records_processed=len(records),
        records_modified=modified_count,
        modifications=total_mods,
        output_file=output.name,
        preview_samples=samples,
    )
