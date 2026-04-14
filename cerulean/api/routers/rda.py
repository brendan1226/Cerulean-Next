"""
cerulean/api/routers/rda.py
─────────────────────────────────────────────────────────────────────────────
RDA Helper — auto-generate RDA fields (336/337/338) from MARC data.

POST /projects/{id}/rda/scan    — scan records for missing RDA fields
POST /projects/{id}/rda/apply   — auto-generate 336/337/338 fields
"""

import re
from pathlib import Path

import pymarc
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db

router = APIRouter(prefix="/projects", tags=["rda"])
settings = get_settings()


# ── RDA Content/Media/Carrier mappings based on Leader/006/007 ───────

# Leader position 6 (Type of record) → 336 content type
_CONTENT_TYPE_MAP = {
    "a": ("text", "txt", "rdacontent"),
    "t": ("text", "txt", "rdacontent"),
    "c": ("notated music", "ntm", "rdacontent"),
    "d": ("notated music", "ntm", "rdacontent"),
    "e": ("cartographic image", "cri", "rdacontent"),
    "f": ("cartographic image", "cri", "rdacontent"),
    "g": ("two-dimensional moving image", "tdi", "rdacontent"),
    "i": ("spoken word", "spw", "rdacontent"),
    "j": ("performed music", "prm", "rdacontent"),
    "k": ("still image", "sti", "rdacontent"),
    "m": ("computer program", "cop", "rdacontent"),
    "o": ("other", "xxx", "rdacontent"),
    "p": ("text", "txt", "rdacontent"),  # mixed materials — default to text
    "r": ("three-dimensional form", "tdf", "rdacontent"),
}

# Leader position 6 → 337 media type
_MEDIA_TYPE_MAP = {
    "a": ("unmediated", "n", "rdamedia"),
    "t": ("unmediated", "n", "rdamedia"),
    "c": ("unmediated", "n", "rdamedia"),
    "d": ("unmediated", "n", "rdamedia"),
    "e": ("unmediated", "n", "rdamedia"),
    "f": ("unmediated", "n", "rdamedia"),
    "g": ("video", "v", "rdamedia"),
    "i": ("audio", "s", "rdamedia"),
    "j": ("audio", "s", "rdamedia"),
    "k": ("unmediated", "n", "rdamedia"),
    "m": ("computer", "c", "rdamedia"),
    "o": ("unmediated", "n", "rdamedia"),
    "p": ("unmediated", "n", "rdamedia"),
    "r": ("unmediated", "n", "rdamedia"),
}

# Leader position 6 + 7 → 338 carrier type
_CARRIER_TYPE_MAP = {
    "am": ("volume", "nc", "rdacarrier"),
    "as": ("volume", "nc", "rdacarrier"),
    "ab": ("volume", "nc", "rdacarrier"),
    "ai": ("online resource", "cr", "rdacarrier"),
    "tm": ("volume", "nc", "rdacarrier"),
    "cm": ("volume", "nc", "rdacarrier"),
    "dm": ("volume", "nc", "rdacarrier"),
    "em": ("sheet", "nb", "rdacarrier"),
    "fm": ("sheet", "nb", "rdacarrier"),
    "gm": ("videodisc", "vd", "rdacarrier"),
    "im": ("audio disc", "sd", "rdacarrier"),
    "jm": ("audio disc", "sd", "rdacarrier"),
    "km": ("sheet", "nb", "rdacarrier"),
    "mm": ("online resource", "cr", "rdacarrier"),
    "om": ("volume", "nc", "rdacarrier"),
    "pm": ("volume", "nc", "rdacarrier"),
    "rm": ("object", "nr", "rdacarrier"),
}


class RdaScanResult(BaseModel):
    total_records: int
    missing_336: int
    missing_337: int
    missing_338: int
    has_all_rda: int
    missing_any: int

class RdaApplyRequest(BaseModel):
    file_path: str | None = None
    overwrite_existing: bool = False
    generate_336: bool = True
    generate_337: bool = True
    generate_338: bool = True

class RdaApplyResult(BaseModel):
    records_processed: int
    records_modified: int
    fields_added: int
    output_file: str


def _find_marc_file(project_id: str, file_path: str | None = None) -> Path:
    project_dir = Path(settings.data_root) / project_id
    if file_path:
        for sub in ["", "raw", "transformed"]:
            candidate = (project_dir / sub / file_path) if sub else (project_dir / file_path)
            if candidate.is_file():
                return candidate
    for name in ["output.mrc", "Biblios-mapped-items.mrc", "merged_deduped.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            return candidate
    raw = project_dir / "raw"
    if raw.is_dir():
        files = sorted(raw.glob("*.mrc"))
        if files:
            return files[0]
    raise HTTPException(404, detail="No MARC files found.")


def _get_rda_fields(record: pymarc.Record) -> dict:
    """Determine 336/337/338 values from leader bytes."""
    leader = record.leader or ""
    type_of_record = leader[6] if len(leader) > 6 else "a"
    bib_level = leader[7] if len(leader) > 7 else "m"

    content = _CONTENT_TYPE_MAP.get(type_of_record, ("text", "txt", "rdacontent"))
    media = _MEDIA_TYPE_MAP.get(type_of_record, ("unmediated", "n", "rdamedia"))

    carrier_key = type_of_record + bib_level
    carrier = _CARRIER_TYPE_MAP.get(carrier_key,
              _CARRIER_TYPE_MAP.get(type_of_record + "m", ("volume", "nc", "rdacarrier")))

    return {
        "336": content,  # (term, code, source)
        "337": media,
        "338": carrier,
    }


@router.post("/{project_id}/rda/scan", response_model=RdaScanResult)
async def rda_scan(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Scan records for missing RDA 336/337/338 fields."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id)

    total = 0
    missing_336 = 0
    missing_337 = 0
    missing_338 = 0
    has_all = 0

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue
            total += 1
            has_336 = bool(record.get_fields("336"))
            has_337 = bool(record.get_fields("337"))
            has_338 = bool(record.get_fields("338"))
            if not has_336:
                missing_336 += 1
            if not has_337:
                missing_337 += 1
            if not has_338:
                missing_338 += 1
            if has_336 and has_337 and has_338:
                has_all += 1

    missing_any = total - has_all

    return RdaScanResult(
        total_records=total,
        missing_336=missing_336,
        missing_337=missing_337,
        missing_338=missing_338,
        has_all_rda=has_all,
        missing_any=missing_any,
    )


@router.post("/{project_id}/rda/apply", response_model=RdaApplyResult)
async def rda_apply(
    project_id: str,
    body: RdaApplyRequest,
    db: AsyncSession = Depends(get_db),
):
    """Auto-generate RDA 336/337/338 fields based on leader bytes."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    records = []
    modified_count = 0
    fields_added = 0

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue

            rda = _get_rda_fields(record)
            rec_modified = False

            for tag, generate in [("336", body.generate_336), ("337", body.generate_337), ("338", body.generate_338)]:
                if not generate:
                    continue
                existing = record.get_fields(tag)
                if existing and not body.overwrite_existing:
                    continue
                if existing and body.overwrite_existing:
                    for f in existing:
                        record.remove_field(f)

                term, code, source_vocab = rda[tag]
                field = pymarc.Field(
                    tag=tag,
                    indicators=[" ", " "],
                    subfields=[
                        pymarc.Subfield(code="a", value=term),
                        pymarc.Subfield(code="b", value=code),
                        pymarc.Subfield(code="2", value=source_vocab),
                    ],
                )
                record.add_ordered_field(field)
                fields_added += 1
                rec_modified = True

            if rec_modified:
                modified_count += 1
            records.append(record)

    output = source.parent / f"{source.stem}_rda{source.suffix}"
    with open(str(output), "wb") as fh:
        writer = pymarc.MARCWriter(fh)
        for rec in records:
            writer.write(rec)
        writer.close()

    await audit_log(db, project_id, stage=3, level="info", tag="[rda]",
                    message=f"RDA apply: {fields_added} fields added to {modified_count}/{len(records)} records → {output.name}")

    return RdaApplyResult(
        records_processed=len(records),
        records_modified=modified_count,
        fields_added=fields_added,
        output_file=output.name,
    )
