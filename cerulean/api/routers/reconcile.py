"""
cerulean/api/routers/reconcile.py
─────────────────────────────────────────────────────────────────────────────
Stage 8 — Items API endpoints.

POST  /projects/{id}/reconcile/confirm-source   — resolve and store source file
POST  /projects/{id}/reconcile/scan             — dispatch scan task
GET   /projects/{id}/reconcile/scan/status      — poll scan task status
GET   /projects/{id}/reconcile/values           — query scan results by vocab
GET   /projects/{id}/reconcile/koha-list        — fetch Koha controlled vocab list
GET   /projects/{id}/reconcile/rules            — list rules
POST  /projects/{id}/reconcile/rules            — create rule
PATCH /projects/{id}/reconcile/rules/{rid}      — update rule
DELETE /projects/{id}/reconcile/rules/{rid}     — delete rule
GET   /projects/{id}/reconcile/validate         — cross-ref scan results vs rules
POST  /projects/{id}/reconcile/apply            — dispatch apply task
GET   /projects/{id}/reconcile/apply/status     — poll apply task status
GET   /projects/{id}/reconcile/report           — read apply task result
POST  /projects/{id}/reconcile/clear            — delete scan results + output file
"""

import csv
import io
import json
import os
from pathlib import Path

import httpx
import pymarc
from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import ReconciliationRule, ReconciliationScanResult
from cerulean.schemas.reconcile import (
    ConfirmSourceRequest,
    ConfirmSourceResponse,
    ReconcileApplyResponse,
    ReconcileApplyStatusResponse,
    ReconcileReportOut,
    ReconcileRuleCreate,
    ReconcileRuleOut,
    ReconcileRuleUpdate,
    ReconcileScanResponse,
    ReconcileScanStatusResponse,
    ScanValueOut,
    ValidateResponse,
    ValidationIssue,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.reconcile import (
    VOCAB_SUBFIELD_MAP,
    reconciliation_apply_task,
    reconciliation_scan_task,
)

settings = get_settings()
router = APIRouter(prefix="/projects", tags=["reconcile"])


# ── 1. Confirm source file ──────────────────────────────────────────────


@router.post("/{project_id}/reconcile/confirm-source", response_model=ConfirmSourceResponse)
async def confirm_source(
    project_id: str,
    body: ConfirmSourceRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Resolve source file for items stage, count records, store in project."""
    project = await require_project(project_id, db)
    project_dir = Path(settings.data_root) / project_id

    # Resolve source file — priority: explicit override > deduped > merged > transformed/*
    source_file = None
    if body and body.source_file:
        candidate = project_dir / body.source_file
        if candidate.is_file():
            source_file = str(candidate)
    if not source_file:
        for name in ["merged_deduped.mrc", "output.mrc", "merged.mrc"]:
            candidate = project_dir / name
            if candidate.is_file():
                source_file = str(candidate)
                break
    if not source_file:
        transformed_dir = project_dir / "transformed"
        if transformed_dir.is_dir():
            transformed = sorted(transformed_dir.glob("*_transformed.mrc"))
            if transformed:
                # Use first transformed file (single-file projects) or merged
                source_file = str(transformed[0])

    if not source_file:
        raise HTTPException(409, detail={
            "error": "NO_SOURCE_FILE",
            "message": "No MARC output files found. Complete Stage 6 (Transform) first.",
        })

    # Quick record count
    import pymarc
    record_count = 0
    with open(source_file, "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True,
                                    utf8_handling="replace")
        for rec in reader:
            if rec is not None:
                record_count += 1

    project.reconcile_source_file = source_file
    await db.flush()

    await audit_log(db, project_id, stage=8, level="info", tag="[items]",
                    message=f"Source file confirmed: {Path(source_file).name} ({record_count:,} records)")
    await db.flush()

    return ConfirmSourceResponse(
        source_file=Path(source_file).name,
        record_count=record_count,
        message=f"Source file confirmed with {record_count:,} records.",
    )


# ── 2. Scan ─────────────────────────────────────────────────────────────


@router.post("/{project_id}/reconcile/scan", response_model=ReconcileScanResponse, status_code=202)
async def start_scan(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch items scan task. Precondition: stage_6_complete."""
    project = await require_project(project_id, db)

    # Allow scan if source file is confirmed OR if output.mrc exists (Quick Path).
    # The old gate required stage_6_complete, but Quick Path skips stages 4-6.
    if not project.reconcile_source_file:
        raise HTTPException(409, detail={
            "error": "NO_SOURCE_FILE",
            "message": "Confirm a source file first via POST /reconcile/confirm-source.",
        })

    await audit_log(db, project_id, stage=8, level="info", tag="[items-scan]",
                    message="Items scan dispatched")
    await db.flush()

    task = reconciliation_scan_task.apply_async(
        args=[project_id],
        queue="reconcile",
    )
    from cerulean.api.routers.tasks import register_task
    register_task(project_id, task.id, "reconcile_scan")
    project.reconcile_scan_task_id = task.id
    await db.flush()

    return ReconcileScanResponse(
        task_id=task.id,
        message="Items scan started.",
    )


# ── 3. Scan status ──────────────────────────────────────────────────────


@router.get("/{project_id}/reconcile/scan/status", response_model=ReconcileScanStatusResponse)
async def scan_status(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Poll scan task status."""
    project = await require_project(project_id, db)

    task_id = project.reconcile_scan_task_id
    if not task_id:
        return ReconcileScanStatusResponse(task_id=None, state="IDLE")

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

    return ReconcileScanStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


# ── 4. Query scan values ────────────────────────────────────────────────


@router.get("/{project_id}/reconcile/values", response_model=list[ScanValueOut])
async def list_scan_values(
    project_id: str,
    vocab: str = Query(..., description="Vocab category: itype, loc, ccode, etc."),
    db: AsyncSession = Depends(get_db),
):
    """Return scan results for a specific vocab category."""
    await require_project(project_id, db)

    result = await db.execute(
        select(ReconciliationScanResult)
        .where(
            ReconciliationScanResult.project_id == project_id,
            ReconciliationScanResult.vocab_category == vocab,
        )
        .order_by(ReconciliationScanResult.record_count.desc())
    )
    return result.scalars().all()


# ── 5. Koha controlled vocab list ───────────────────────────────────────


@router.get("/{project_id}/reconcile/koha-list")
async def koha_vocab_list(
    project_id: str,
    vocab: str = Query(..., description="Vocab category: itype, loc, ccode, etc."),
    db: AsyncSession = Depends(get_db),
):
    """Fetch Koha controlled vocabulary list via REST API.

    Reuses project's encrypted Koha token for authentication.
    """
    project = await require_project(project_id, db)

    if not project.koha_url or not project.koha_token_enc:
        raise HTTPException(409, detail={
            "error": "NO_KOHA_CONFIG",
            "message": "Koha URL and token must be configured on the project.",
        })

    # Decrypt token
    from cerulean.tasks.push import _decrypt_token
    base_url = project.koha_url.rstrip("/")
    token = _decrypt_token(project.koha_token_enc)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Map vocab category to Koha API endpoint
    endpoint = _koha_endpoint_for_vocab(vocab, base_url)
    if not endpoint:
        raise HTTPException(400, detail={
            "error": "UNKNOWN_VOCAB",
            "message": f"Unknown vocab category: {vocab}",
        })

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(endpoint, headers=headers)

        if resp.status_code >= 400:
            raise HTTPException(502, detail={
                "error": "KOHA_API_ERROR",
                "message": f"Koha API returned {resp.status_code}: {resp.text[:500]}",
            })

        return {"vocab": vocab, "values": resp.json()}

    except httpx.HTTPError as exc:
        raise HTTPException(502, detail={
            "error": "KOHA_API_ERROR",
            "message": f"Koha API request failed: {exc}",
        })


# ── 6-9. Rules CRUD ─────────────────────────────────────────────────────


@router.get("/{project_id}/reconcile/rules", response_model=list[ReconcileRuleOut])
async def list_rules(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all item rules ordered by sort_order."""
    await require_project(project_id, db)
    result = await db.execute(
        select(ReconciliationRule)
        .where(ReconciliationRule.project_id == project_id)
        .order_by(ReconciliationRule.sort_order)
    )
    return result.scalars().all()


@router.post("/{project_id}/reconcile/rules", response_model=ReconcileRuleOut, status_code=201)
async def create_rule(
    project_id: str,
    body: ReconcileRuleCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create an item rule. Auto-sets marc_tag and marc_subfield."""
    await require_project(project_id, db)

    sf_code = VOCAB_SUBFIELD_MAP.get(body.vocab_category)
    if not sf_code:
        raise HTTPException(400, detail={
            "error": "UNKNOWN_VOCAB",
            "message": f"Unknown vocab category: {body.vocab_category}. "
                       f"Valid: {', '.join(VOCAB_SUBFIELD_MAP.keys())}",
        })

    rule = ReconciliationRule(
        project_id=project_id,
        vocab_category=body.vocab_category,
        marc_tag="952",
        marc_subfield=sf_code,
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


@router.patch("/{project_id}/reconcile/rules/{rule_id}", response_model=ReconcileRuleOut)
async def update_rule(
    project_id: str,
    rule_id: str,
    body: ReconcileRuleUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update an item rule."""
    await require_project(project_id, db)
    rule = await db.get(ReconciliationRule, rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})

    update_data = body.model_dump(exclude_unset=True)

    # If vocab_category changed, update marc_subfield too
    if "vocab_category" in update_data:
        sf_code = VOCAB_SUBFIELD_MAP.get(update_data["vocab_category"])
        if not sf_code:
            raise HTTPException(400, detail={
                "error": "UNKNOWN_VOCAB",
                "message": f"Unknown vocab category: {update_data['vocab_category']}",
            })
        update_data["marc_subfield"] = sf_code

    for key, value in update_data.items():
        setattr(rule, key, value)

    await db.flush()
    await db.refresh(rule)
    return rule


@router.delete("/{project_id}/reconcile/rules/{rule_id}", status_code=204)
async def delete_rule(
    project_id: str,
    rule_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete an item rule."""
    await require_project(project_id, db)
    rule = await db.get(ReconciliationRule, rule_id)
    if not rule or rule.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Rule not found."})
    await db.delete(rule)
    await db.flush()


# ── 10. Validate ─────────────────────────────────────────────────────────


@router.get("/{project_id}/reconcile/validate", response_model=ValidateResponse)
async def validate_rules(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Cross-reference scan results against defined rules.

    Returns list of unmatched values (warning only — does not block apply).
    """
    await require_project(project_id, db)

    # Get all scan results
    scan_result = await db.execute(
        select(ReconciliationScanResult)
        .where(ReconciliationScanResult.project_id == project_id)
    )
    scan_rows = scan_result.scalars().all()

    # Get all active rules
    rules_result = await db.execute(
        select(ReconciliationRule)
        .where(
            ReconciliationRule.project_id == project_id,
            ReconciliationRule.active == True,  # noqa: E712
        )
    )
    rules = rules_result.scalars().all()

    # Build set of (category, value) covered by rules
    covered: set[tuple[str, str]] = set()
    for rule in rules:
        for sv in (rule.source_values or []):
            covered.add((rule.vocab_category, sv))

    total = len(scan_rows)
    matched = 0
    unmatched = 0
    issues: list[ValidationIssue] = []

    for row in scan_rows:
        if (row.vocab_category, row.source_value) in covered:
            matched += 1
        else:
            unmatched += 1
            issues.append(ValidationIssue(
                vocab_category=row.vocab_category,
                source_value=row.source_value,
                record_count=row.record_count,
                issue="unmatched",
            ))

    return ValidateResponse(
        total_values=total,
        matched_values=matched,
        unmatched_values=unmatched,
        issues=issues,
    )


# ── 11. Apply ────────────────────────────────────────────────────────────


@router.post("/{project_id}/reconcile/apply", response_model=ReconcileApplyResponse, status_code=202)
async def start_apply(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch items apply task."""
    project = await require_project(project_id, db)

    if not project.reconcile_source_file:
        raise HTTPException(409, detail={
            "error": "NO_SOURCE_FILE",
            "message": "Confirm a source file first.",
        })

    await audit_log(db, project_id, stage=8, level="info", tag="[items-apply]",
                    message="Items apply dispatched")
    await db.flush()

    task = reconciliation_apply_task.apply_async(
        args=[project_id],
        queue="reconcile",
    )
    project.reconcile_apply_task_id = task.id
    await db.flush()

    return ReconcileApplyResponse(
        task_id=task.id,
        message="Items apply started.",
    )


# ── 12. Apply status ────────────────────────────────────────────────────


@router.get("/{project_id}/reconcile/apply/status", response_model=ReconcileApplyStatusResponse)
async def apply_status(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Poll apply task status."""
    project = await require_project(project_id, db)

    task_id = project.reconcile_apply_task_id
    if not task_id:
        return ReconcileApplyStatusResponse(task_id=None, state="IDLE")

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

    return ReconcileApplyStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


# ── 13. Report ───────────────────────────────────────────────────────────


@router.get("/{project_id}/reconcile/report", response_model=ReconcileReportOut)
async def reconcile_report(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Read apply task result from Celery backend."""
    project = await require_project(project_id, db)

    task_id = project.reconcile_apply_task_id
    if not task_id:
        return ReconcileReportOut(task_id=None, state="IDLE")

    async_result = AsyncResult(task_id, app=celery_app)
    state = async_result.state
    result_data = None
    error = None

    if state == "SUCCESS":
        result_data = async_result.result
    elif state == "FAILURE":
        error = str(async_result.info)

    return ReconcileReportOut(
        task_id=task_id, state=state,
        result=result_data, error=error,
    )


# ── 14. Category files (post-apply value lists) ─────────────────────────


@router.get("/{project_id}/reconcile/files")
async def list_category_files(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List per-category CSV files produced by the apply step."""
    await require_project(project_id, db)
    items_dir = Path(settings.data_root) / project_id / "items"
    if not items_dir.is_dir():
        return {"files": []}

    files = []
    for csv_path in sorted(items_dir.glob("items_*.csv")):
        # Read header + count rows
        row_count = 0
        with open(str(csv_path)) as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for _ in reader:
                row_count += 1
        cat = csv_path.stem.replace("items_", "")
        files.append({
            "category": cat,
            "filename": csv_path.name,
            "value_count": row_count,
        })
    return {"files": files}


@router.get("/{project_id}/reconcile/files/{filename}")
async def get_category_file(
    project_id: str,
    filename: str,
    db: AsyncSession = Depends(get_db),
):
    """Return contents of a category CSV file as JSON rows."""
    await require_project(project_id, db)
    # Sanitise filename
    if "/" in filename or "\\" in filename or not filename.endswith(".csv"):
        raise HTTPException(400, detail="Invalid filename")
    csv_path = Path(settings.data_root) / project_id / "items" / filename
    if not csv_path.is_file():
        raise HTTPException(404, detail="File not found")

    rows = []
    cat = csv_path.stem.replace("items_", "")
    descs = _load_descriptions(project_id)
    cat_descs = descs.get(cat, {})
    with open(str(csv_path)) as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = row.get("value", "")
            rows.append({
                "value": val,
                "record_count": int(row.get("record_count", 0)),
                "description": cat_descs.get(val, ""),
            })
    return {"category": cat, "filename": filename, "rows": rows}


@router.get("/{project_id}/reconcile/files/{filename}/download")
async def download_category_file(
    project_id: str,
    filename: str,
    db: AsyncSession = Depends(get_db),
):
    """Download a category CSV file with descriptions included."""
    await require_project(project_id, db)
    if "/" in filename or "\\" in filename or not filename.endswith(".csv"):
        raise HTTPException(400, detail="Invalid filename")
    csv_path = Path(settings.data_root) / project_id / "items" / filename
    if not csv_path.is_file():
        raise HTTPException(404, detail="File not found")

    cat = csv_path.stem.replace("items_", "")
    descs = _load_descriptions(project_id)
    cat_descs = descs.get(cat, {})

    # Rebuild CSV with description column
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


@router.put("/{project_id}/reconcile/descriptions/{category}")
async def save_descriptions(
    project_id: str,
    category: str,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Save value descriptions for a vocab category.

    Body: {"descriptions": {"BK": "Books", "DVD": "DVDs", ...}}
    """
    await require_project(project_id, db)
    new_descs = body.get("descriptions", {})
    if not isinstance(new_descs, dict):
        raise HTTPException(400, detail="descriptions must be a dict")

    descs = _load_descriptions(project_id)
    descs.setdefault(category, {})
    descs[category].update(new_descs)
    # Remove empty descriptions
    descs[category] = {k: v for k, v in descs[category].items() if v}
    _save_descriptions(project_id, descs)

    return {"category": category, "descriptions_saved": len(new_descs)}


# ── 15. Sample records for a category value ──────────────────────────────


@router.get("/{project_id}/reconcile/sample")
async def sample_records(
    project_id: str,
    vocab: str = Query(..., description="Vocab category, e.g. itype"),
    value: str = Query(..., description="Value to search for"),
    limit: int = Query(5, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
):
    """Return a few sample records that have the given value in the vocab subfield."""
    from cerulean.tasks.reconcile import VOCAB_SUBFIELD_MAP
    from cerulean.utils.marc import record_to_dict

    project = await require_project(project_id, db)

    sf_code = VOCAB_SUBFIELD_MAP.get(vocab)
    if not sf_code:
        raise HTTPException(400, detail=f"Unknown vocab category: {vocab}")

    # Use the reconciled file if it exists, else the source file
    project_dir = Path(settings.data_root) / project_id
    marc_path = project_dir / "Biblios-mapped-items.mrc"
    if not marc_path.is_file():
        marc_path = Path(project.reconcile_source_file) if project.reconcile_source_file else None
    if not marc_path or not marc_path.is_file():
        return {"records": [], "total_matched": 0}

    matched = []
    total_matched = 0
    with open(str(marc_path), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for idx, record in enumerate(reader):
            if record is None:
                continue
            # Check if any 952 has the subfield with the target value
            for f952 in record.get_fields("952"):
                for sf in f952.subfields:
                    if sf.code == sf_code and sf.value and sf.value.strip() == value:
                        total_matched += 1
                        if len(matched) < limit:
                            matched.append(record_to_dict(record, idx))
                        break
                else:
                    continue
                break
            if total_matched > 1000:
                break  # Don't scan the entire file for count

    return {"records": matched, "total_matched": total_matched, "vocab": vocab, "value": value}


# ── 16. Clear ────────────────────────────────────────────────────────────


@router.post("/{project_id}/reconcile/clear", status_code=200)
async def clear_scan(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete scan results and output file, preserving rules."""
    project = await require_project(project_id, db)

    # Delete scan results
    await db.execute(
        delete(ReconciliationScanResult)
        .where(ReconciliationScanResult.project_id == project_id)
    )

    # Remove output file and category CSVs if they exist
    output_path = Path(settings.data_root) / project_id / "Biblios-mapped-items.mrc"
    if output_path.is_file():
        os.remove(str(output_path))
    items_dir = Path(settings.data_root) / project_id / "items"
    if items_dir.is_dir():
        import shutil
        shutil.rmtree(str(items_dir))

    # Reset task IDs
    project.reconcile_scan_task_id = None
    project.reconcile_apply_task_id = None
    project.stage_8_complete = False
    await db.flush()

    await audit_log(db, project_id, stage=8, level="info", tag="[items]",
                    message="Items scan results and output cleared")
    await db.flush()

    return {"message": "Scan results and output file cleared. Rules preserved."}


# ── Helpers ──────────────────────────────────────────────────────────────


def _descriptions_path(project_id: str) -> Path:
    return Path(settings.data_root) / project_id / "items" / "descriptions.json"


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


def _koha_endpoint_for_vocab(vocab: str, base_url: str) -> str | None:
    """Map vocab category to Koha REST API endpoint URL."""
    if vocab == "itype":
        return f"{base_url}/api/v1/item_types"
    elif vocab in ("homebranch", "holdingbranch"):
        return f"{base_url}/api/v1/libraries"
    elif vocab in ("loc", "ccode", "not_loan", "withdrawn", "damaged"):
        # Koha authorized values API — category param
        av_category = vocab.upper()
        return f"{base_url}/api/v1/authorised_value_categories/{av_category}/authorised_values"
    return None
