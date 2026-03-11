"""
cerulean/api/routers/transform.py
─────────────────────────────────────────────────────────────────────────────
POST /projects/{id}/transform/start      — dispatch transform_pipeline_task
POST /projects/{id}/transform/merge      — dispatch merge_pipeline_task
GET  /projects/{id}/transform/status     — poll Celery task status
GET  /projects/{id}/transform/manifest   — list transform/merge manifests
"""

import uuid
from datetime import datetime
from pathlib import Path

import pymarc
import redis
from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import FieldMap, MARCFile, TransformManifest
from cerulean.schemas.transform import (
    MergeStartRequest,
    MergeStartResponse,
    TransformManifestOut,
    TransformStartRequest,
    TransformStartResponse,
    TransformStatusResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.transform import merge_pipeline_task, transform_pipeline_task
from cerulean.utils.marc import record_to_dict

settings = get_settings()
_redis = redis.from_url(settings.redis_url)

router = APIRouter(prefix="/projects", tags=["transform"])


# ── Endpoints ─────────────────────────────────────────────────────────


@router.post("/{project_id}/transform/start", response_model=TransformStartResponse, status_code=202)
async def start_transform(
    project_id: str,
    body: TransformStartRequest,
    db: AsyncSession = Depends(get_db),
):
    """Validate preconditions and dispatch transform_pipeline_task."""
    await require_project(project_id, db)

    # Must have at least one approved map
    result = await db.execute(
        select(FieldMap)
        .where(FieldMap.project_id == project_id, FieldMap.approved == True)  # noqa: E712
        .limit(1)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(409, detail={
            "error": "NO_APPROVED_MAPS",
            "message": "No approved field maps. Complete Stage 2 first.",
        })

    # Must have indexed files
    files_q = select(MARCFile).where(
        MARCFile.project_id == project_id, MARCFile.status == "indexed",
    )
    if body.file_ids:
        files_q = files_q.where(MARCFile.id.in_(body.file_ids))
    result = await db.execute(files_q.limit(1))
    if not result.scalar_one_or_none():
        raise HTTPException(409, detail={
            "error": "NO_INDEXED_FILES",
            "message": "No indexed MARC files found.",
        })

    # Create manifest
    manifest_id = str(uuid.uuid4())
    manifest = TransformManifest(
        id=manifest_id,
        project_id=project_id,
        task_type="transform",
        status="running",
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await audit_log(db, project_id, stage=3, level="info", tag="[transform]",
                    message="Transform pipeline dispatched")
    await db.flush()

    # Dispatch
    task = transform_pipeline_task.apply_async(
        args=[project_id, manifest_id],
        kwargs={"file_ids": body.file_ids, "dry_run": body.dry_run},
        queue="transform",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    return TransformStartResponse(
        task_id=task.id, manifest_id=manifest_id,
        message="Transform pipeline started.",
    )


@router.post("/{project_id}/transform/merge", response_model=MergeStartResponse, status_code=202)
async def start_merge(
    project_id: str,
    body: MergeStartRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch merge_pipeline_task."""
    await require_project(project_id, db)

    manifest_id = str(uuid.uuid4())
    manifest = TransformManifest(
        id=manifest_id,
        project_id=project_id,
        task_type="merge",
        status="running",
        items_csv_path=body.items_csv_path,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await audit_log(db, project_id, stage=3, level="info", tag="[merge]",
                    message="Merge pipeline dispatched")
    await db.flush()

    task = merge_pipeline_task.apply_async(
        args=[project_id, manifest_id],
        kwargs={
            "file_ids": body.file_ids,
            "items_csv_path": body.items_csv_path,
            "items_csv_match_tag": body.items_csv_match_tag,
            "items_csv_key_column": body.items_csv_key_column,
        },
        queue="transform",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    return MergeStartResponse(
        task_id=task.id, manifest_id=manifest_id,
        message="Merge pipeline started.",
    )


@router.get("/{project_id}/transform/status", response_model=TransformStatusResponse)
async def transform_status(
    project_id: str,
    task_id: str | None = Query(None, description="Celery task_id to poll"),
    db: AsyncSession = Depends(get_db),
):
    """Poll Celery task status. Falls back to most recent manifest if no task_id given."""
    await require_project(project_id, db)

    if not task_id:
        result = await db.execute(
            select(TransformManifest)
            .where(TransformManifest.project_id == project_id)
            .order_by(TransformManifest.started_at.desc())
            .limit(1)
        )
        manifest = result.scalar_one_or_none()
        if not manifest or not manifest.celery_task_id:
            return TransformStatusResponse(
                task_id=None, state="IDLE", progress=None, result=None, error=None,
            )
        task_id = manifest.celery_task_id

    async_result = AsyncResult(task_id, app=celery_app)
    state = async_result.state
    progress = None
    result_data = None
    error = None

    # Check if paused via per-task Redis flag
    is_paused = _redis.exists(f"cerulean:pause:task:{task_id}")
    if is_paused and state in ("PROGRESS", "PAUSED", "STARTED"):
        state = "PAUSED"

    if state == "PROGRESS":
        progress = async_result.info
    elif state == "PAUSED":
        progress = async_result.info if isinstance(async_result.info, dict) else None
    elif state == "SUCCESS":
        result_data = async_result.result
    elif state == "FAILURE":
        error = str(async_result.info)

    return TransformStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


@router.get("/{project_id}/transform/manifest", response_model=list[TransformManifestOut])
async def list_manifests(
    project_id: str,
    task_type: str | None = Query(None, description="Filter: transform | merge"),
    db: AsyncSession = Depends(get_db),
):
    """List all transform/merge manifests for a project."""
    await require_project(project_id, db)

    q = select(TransformManifest).where(TransformManifest.project_id == project_id)
    if task_type:
        q = q.where(TransformManifest.task_type == task_type)
    q = q.order_by(TransformManifest.started_at.desc())

    result = await db.execute(q)
    return result.scalars().all()


@router.get("/{project_id}/transform/preview")
async def preview_transformed(
    project_id: str,
    record_index: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Preview a transformed record alongside the original for comparison."""
    await require_project(project_id, db)

    # Find the latest successful transform manifest
    result = await db.execute(
        select(TransformManifest)
        .where(TransformManifest.project_id == project_id,
               TransformManifest.task_type == "transform",
               TransformManifest.status == "complete")
        .order_by(TransformManifest.started_at.desc())
        .limit(1)
    )
    manifest = result.scalar_one_or_none()
    if not manifest:
        raise HTTPException(404, detail={"error": "NO_TRANSFORM", "message": "No completed transform found."})

    # Get the first transformed file
    transformed_dir = Path(settings.data_root) / project_id / "transformed"
    transformed_files = sorted(transformed_dir.glob("*_transformed.mrc")) if transformed_dir.exists() else []
    if not transformed_files:
        raise HTTPException(404, detail={"error": "NO_FILES", "message": "No transformed files found."})

    # Also find the original file for comparison
    original_files = []
    if manifest.file_ids:
        orig_result = await db.execute(
            select(MARCFile).where(MARCFile.id.in_(manifest.file_ids))
            .order_by(MARCFile.sort_order, MARCFile.created_at)
        )
        original_files = [f.storage_path for f in orig_result.scalars().all()]

    def read_record(path, valid_idx):
        """Read the N-th valid (non-None) record from a MARC file."""
        try:
            with open(path, "rb") as fh:
                reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
                count = 0
                for rec in reader:
                    if rec is None:
                        continue
                    if count == valid_idx:
                        return record_to_dict(rec, valid_idx)
                    count += 1
        except Exception:
            pass
        return None

    # Count total records across all transformed files
    total_records = manifest.total_records or 0

    # Find which file and local index this global index maps to
    transformed_record = None
    original_record = None
    global_idx = 0
    for tf_idx, tf_path in enumerate(transformed_files):
        try:
            with open(str(tf_path), "rb") as fh:
                reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
                local_valid = 0
                for rec in reader:
                    if rec is None:
                        continue
                    if global_idx == record_index:
                        transformed_record = record_to_dict(rec, record_index)
                        # Read the corresponding original
                        if tf_idx < len(original_files):
                            original_record = read_record(original_files[tf_idx], local_valid)
                        break
                    global_idx += 1
                    local_valid += 1
            if transformed_record:
                break
        except Exception:
            continue

    if not transformed_record:
        raise HTTPException(404, detail={"error": "RECORD_NOT_FOUND", "message": f"No record at index {record_index}."})

    return {
        "record_index": record_index,
        "total_records": total_records,
        "original": original_record,
        "transformed": transformed_record,
    }


@router.post("/{project_id}/transform/clear")
async def clear_transform_results(project_id: str, db: AsyncSession = Depends(get_db)):
    """Delete all transformed files and manifests so the user can start fresh."""
    project = await require_project(project_id, db)

    # Delete transformed files on disk
    project_dir = Path(settings.data_root) / project_id / "transformed"
    deleted_files = 0
    if project_dir.is_dir():
        for f in project_dir.glob("*_transformed.mrc"):
            f.unlink()
            deleted_files += 1

    # Also delete merged.mrc if present
    merged = Path(settings.data_root) / project_id / "merged.mrc"
    if merged.is_file():
        merged.unlink()
        deleted_files += 1

    # Delete transform/merge manifests
    result = await db.execute(
        delete(TransformManifest).where(TransformManifest.project_id == project_id)
    )
    manifests_deleted = result.rowcount

    # Reset stage 3 completion
    project.stage_3_complete = False
    if (project.current_stage or 0) > 3:
        project.current_stage = 3

    await audit_log(db, project_id, stage=3, level="info", tag="[transform]",
                    message=f"Cleared previous results: {deleted_files} file(s), {manifests_deleted} manifest(s)")

    return {
        "deleted_files": deleted_files,
        "manifests_deleted": manifests_deleted,
    }


@router.post("/{project_id}/transform/pause")
async def pause_transform(project_id: str, db: AsyncSession = Depends(get_db)):
    """Set pause flag — running transform will pause at the next record."""
    await require_project(project_id, db)
    # Find the currently running transform task
    manifest = (await db.execute(
        select(TransformManifest)
        .where(TransformManifest.project_id == project_id, TransformManifest.status == "running")
        .order_by(TransformManifest.started_at.desc()).limit(1)
    )).scalar_one_or_none()
    if manifest and manifest.celery_task_id:
        _redis.set(f"cerulean:pause:task:{manifest.celery_task_id}", "1")
    return {"status": "paused"}


@router.post("/{project_id}/transform/resume")
async def resume_transform(project_id: str, db: AsyncSession = Depends(get_db)):
    """Clear pause flag — transform resumes processing."""
    await require_project(project_id, db)
    manifest = (await db.execute(
        select(TransformManifest)
        .where(TransformManifest.project_id == project_id, TransformManifest.status == "running")
        .order_by(TransformManifest.started_at.desc()).limit(1)
    )).scalar_one_or_none()
    if manifest and manifest.celery_task_id:
        _redis.delete(f"cerulean:pause:task:{manifest.celery_task_id}")
    return {"status": "resumed"}
