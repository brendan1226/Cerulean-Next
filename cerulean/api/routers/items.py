"""
cerulean/api/routers/items.py
─────────────────────────────────────────────────────────────────────────────
Item CSV → 952 subfield mapping API endpoints.

Column Mapping:
  GET    /projects/{id}/items/maps              — list ItemColumnMaps
  POST   /projects/{id}/items/maps              — create manual mapping
  PATCH  /projects/{id}/items/maps/{mid}        — update mapping
  DELETE /projects/{id}/items/maps/{mid}        — delete mapping
  POST   /projects/{id}/items/maps/{mid}/approve — approve single map
  POST   /projects/{id}/items/maps/approve-all  — batch approve above threshold

AI Suggest:
  POST   /projects/{id}/items/maps/ai-suggest        — dispatch AI task
  GET    /projects/{id}/items/maps/ai-suggest/status  — poll AI task status

Preview:
  GET    /projects/{id}/items/preview           — sample rows from CSV

Match Config:
  GET    /projects/{id}/items/match-config      — get match config
  PUT    /projects/{id}/items/match-config      — update match config
"""

import csv
import uuid

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import ItemColumnMap, MARCFile, Project
from cerulean.schemas.items import (
    ApproveAllRequest,
    ApproveAllResponse,
    ItemAISuggestResponse,
    ItemAISuggestStatusResponse,
    ItemColumnMapCreate,
    ItemColumnMapOut,
    ItemColumnMapUpdate,
    ItemMatchConfig,
    ItemMatchConfigUpdate,
    ItemPreviewResponse,
    RenameColumnRequest,
    RenameColumnResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.items import items_ai_map_task

settings = get_settings()
router = APIRouter(prefix="/projects", tags=["items"])


# ── Helpers ──────────────────────────────────────────────────────────────


async def _get_items_csv_file(project_id: str, db: AsyncSession) -> MARCFile | None:
    """Return the first items_csv file for a project (if any)."""
    result = await db.execute(
        select(MARCFile)
        .where(MARCFile.project_id == project_id, MARCFile.file_category == "items_csv")
        .order_by(MARCFile.created_at)
        .limit(1)
    )
    return result.scalar_one_or_none()


# ══════════════════════════════════════════════════════════════════════════
# COLUMN MAPPING CRUD
# ══════════════════════════════════════════════════════════════════════════


@router.get("/{project_id}/items/maps", response_model=list[ItemColumnMapOut])
async def list_item_maps(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all item column maps ordered by sort_order."""
    await require_project(project_id, db)
    result = await db.execute(
        select(ItemColumnMap)
        .where(ItemColumnMap.project_id == project_id)
        .order_by(ItemColumnMap.sort_order)
    )
    return result.scalars().all()


@router.post("/{project_id}/items/maps", response_model=ItemColumnMapOut, status_code=201)
async def create_item_map(
    project_id: str,
    body: ItemColumnMapCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a manual item column map."""
    await require_project(project_id, db)

    cm = ItemColumnMap(
        id=str(uuid.uuid4()),
        project_id=project_id,
        source_column=body.source_column,
        target_subfield=body.target_subfield,
        ignored=body.ignored,
        transform_type=body.transform_type,
        transform_config=body.transform_config,
        approved=False,
        ai_suggested=False,
        source_label="manual",
        sort_order=body.sort_order,
    )
    db.add(cm)
    await db.flush()
    await db.refresh(cm)
    return cm


@router.patch("/{project_id}/items/maps/{map_id}", response_model=ItemColumnMapOut)
async def update_item_map(
    project_id: str,
    map_id: str,
    body: ItemColumnMapUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update an item column map."""
    await require_project(project_id, db)
    cm = await db.get(ItemColumnMap, map_id)
    if not cm or cm.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Column map not found."})

    update_data = body.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(cm, key, value)

    await db.flush()
    await db.refresh(cm)
    return cm


@router.delete("/{project_id}/items/maps/{map_id}", status_code=204)
async def delete_item_map(
    project_id: str,
    map_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete an item column map."""
    await require_project(project_id, db)
    cm = await db.get(ItemColumnMap, map_id)
    if not cm or cm.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Column map not found."})
    await db.delete(cm)
    await db.flush()


@router.post("/{project_id}/items/maps/{map_id}/approve", response_model=ItemColumnMapOut)
async def approve_item_map(
    project_id: str,
    map_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Approve a single item column map."""
    await require_project(project_id, db)
    cm = await db.get(ItemColumnMap, map_id)
    if not cm or cm.project_id != project_id:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": "Column map not found."})

    cm.approved = True
    await db.flush()
    await db.refresh(cm)
    return cm


@router.post("/{project_id}/items/maps/approve-all", response_model=ApproveAllResponse)
async def approve_all_item_maps(
    project_id: str,
    body: ApproveAllRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Batch approve item column maps above confidence threshold."""
    await require_project(project_id, db)
    min_conf = body.min_confidence if body else 0.85

    result = await db.execute(
        select(ItemColumnMap).where(
            ItemColumnMap.project_id == project_id,
            ItemColumnMap.approved == False,  # noqa: E712
            ItemColumnMap.ai_suggested == True,  # noqa: E712
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
# AI SUGGEST
# ══════════════════════════════════════════════════════════════════════════


@router.post("/{project_id}/items/maps/ai-suggest", response_model=ItemAISuggestResponse, status_code=202)
async def ai_suggest_item_maps(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch AI column mapping task for items CSV."""
    await require_project(project_id, db)

    items_file = await _get_items_csv_file(project_id, db)
    if not items_file:
        raise HTTPException(409, detail={
            "error": "NO_ITEMS_CSV",
            "message": "Upload an items CSV file first.",
        })

    await audit_log(db, project_id, stage=2, level="info", tag="[items-ai]",
                    message="Items AI column mapping dispatched")
    await db.flush()

    task = items_ai_map_task.apply_async(
        args=[project_id],
        queue="ingest",
    )

    return ItemAISuggestResponse(task_id=task.id, message="AI column mapping started.")


@router.get("/{project_id}/items/maps/ai-suggest/status", response_model=ItemAISuggestStatusResponse)
async def ai_suggest_item_status(
    project_id: str,
    task_id: str = Query(..., description="Celery task ID to poll"),
    db: AsyncSession = Depends(get_db),
):
    """Poll AI column mapping task status."""
    await require_project(project_id, db)

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

    return ItemAISuggestStatusResponse(
        task_id=task_id, state=state, progress=progress, result=result_data, error=error,
    )


# ══════════════════════════════════════════════════════════════════════════
# PREVIEW
# ══════════════════════════════════════════════════════════════════════════


@router.get("/{project_id}/items/preview", response_model=ItemPreviewResponse)
async def preview_items_csv(
    project_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Return sample rows from the items CSV file."""
    await require_project(project_id, db)

    items_file = await _get_items_csv_file(project_id, db)
    if not items_file:
        raise HTTPException(404, detail={"error": "NO_ITEMS_CSV", "message": "No items CSV file found."})

    rows: list[dict] = []
    columns: list[str] = []
    total_rows = items_file.record_count or 0

    try:
        csv.field_size_limit(10 * 1024 * 1024)
        stored_headers = items_file.column_headers or []
        with open(items_file.storage_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            # If headers are col_1..col_N, the file has no header row — use fieldnames param
            if stored_headers and stored_headers[0].startswith("col_"):
                reader = csv.DictReader(fh, fieldnames=stored_headers)
            else:
                reader = csv.DictReader(fh)
            columns = list(reader.fieldnames or [])
            for i, row in enumerate(reader):
                if i < offset:
                    continue
                if len(rows) >= limit:
                    break
                rows.append(dict(row))
    except Exception:
        raise HTTPException(500, detail={"error": "READ_ERROR", "message": "Failed to read items CSV."})

    return ItemPreviewResponse(rows=rows, total_rows=total_rows, columns=columns)


# ══════════════════════════════════════════════════════════════════════════
# RENAME COLUMN
# ══════════════════════════════════════════════════════════════════════════


@router.post("/{project_id}/items/rename-column", response_model=RenameColumnResponse)
async def rename_column(
    project_id: str,
    body: RenameColumnRequest,
    db: AsyncSession = Depends(get_db),
):
    """Rename a column header in the items CSV metadata.

    Updates column_headers, tag_frequency keys, subfield_frequency keys,
    and any ItemColumnMap.source_column references.
    """
    await require_project(project_id, db)

    items_file = await _get_items_csv_file(project_id, db)
    if not items_file:
        raise HTTPException(404, detail={"error": "NO_ITEMS_CSV", "message": "No items CSV file found."})

    headers = items_file.column_headers or []
    if body.old_name not in headers:
        raise HTTPException(400, detail={
            "error": "COLUMN_NOT_FOUND",
            "message": f"Column '{body.old_name}' not found in headers.",
        })
    if body.new_name in headers and body.new_name != body.old_name:
        raise HTTPException(400, detail={
            "error": "DUPLICATE_NAME",
            "message": f"Column '{body.new_name}' already exists.",
        })

    # Update column_headers list
    new_headers = [body.new_name if h == body.old_name else h for h in headers]
    items_file.column_headers = new_headers

    # Update tag_frequency keys
    if items_file.tag_frequency and body.old_name in items_file.tag_frequency:
        tf = dict(items_file.tag_frequency)
        tf[body.new_name] = tf.pop(body.old_name)
        items_file.tag_frequency = tf

    # Update subfield_frequency keys
    if items_file.subfield_frequency and body.old_name in items_file.subfield_frequency:
        sf = dict(items_file.subfield_frequency)
        sf[body.new_name] = sf.pop(body.old_name)
        items_file.subfield_frequency = sf

    # Update ItemColumnMap references
    result = await db.execute(
        select(ItemColumnMap).where(
            ItemColumnMap.project_id == project_id,
            ItemColumnMap.source_column == body.old_name,
        )
    )
    maps = result.scalars().all()
    for m in maps:
        m.source_column = body.new_name

    # Update project key_column if it references the old name
    project = await db.get(Project, project_id)
    if project and project.items_csv_key_column == body.old_name:
        project.items_csv_key_column = body.new_name

    await db.flush()

    return RenameColumnResponse(
        old_name=body.old_name,
        new_name=body.new_name,
        maps_updated=len(maps),
        message=f"Renamed '{body.old_name}' to '{body.new_name}'.",
    )


# ══════════════════════════════════════════════════════════════════════════
# MATCH CONFIG
# ══════════════════════════════════════════════════════════════════════════


@router.get("/{project_id}/items/match-config", response_model=ItemMatchConfig)
async def get_match_config(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get items CSV match configuration."""
    project = await require_project(project_id, db)
    return ItemMatchConfig(
        items_csv_match_tag=project.items_csv_match_tag,
        items_csv_key_column=project.items_csv_key_column,
    )


@router.put("/{project_id}/items/match-config", response_model=ItemMatchConfig)
async def update_match_config(
    project_id: str,
    body: ItemMatchConfigUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update items CSV match configuration."""
    project = await require_project(project_id, db)

    update_data = body.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(project, key, value)

    await db.flush()
    await db.refresh(project)
    return ItemMatchConfig(
        items_csv_match_tag=project.items_csv_match_tag,
        items_csv_key_column=project.items_csv_key_column,
    )
