"""
cerulean/api/routers/evergreen.py
─────────────────────────────────────────────────────────────────────────────
Stage 13 — Evergreen ILS Integration.

PATCH /projects/{id}/evergreen/config       — set Evergreen DB connection
GET   /projects/{id}/evergreen/counts       — get Evergreen record counts
POST  /projects/{id}/evergreen/push-bibs    — push MARC records to Evergreen
POST  /projects/{id}/evergreen/pingest      — trigger parallel ingest
GET   /projects/{id}/evergreen/test         — test DB connection
"""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.database import get_db
from cerulean.models import PushManifest
from cerulean.tasks.evergreen import (
    evergreen_counts_task,
    evergreen_pingest_task,
    evergreen_push_bibs_task,
    evergreen_remap_task,
    evergreen_service_restart_task,
)

router = APIRouter(prefix="/projects", tags=["evergreen"])


# ── Schemas ──────────────────────────────────────────────────────────────


class EvergreenConfigUpdate(BaseModel):
    evergreen_db_host: str
    evergreen_db_port: int = 5432
    evergreen_db_name: str = "evergreen"
    evergreen_db_user: str = "evergreen"
    evergreen_db_password: str = ""


class EvergreenPushRequest(BaseModel):
    batch_size: int = 500
    trigger_mode: str = "indexing_only"   # "all_on" | "indexing_only" | "all_off"
    remap_metarecords: bool = True        # populate metabib.metarecord_source_map after insert


class EvergreenPingestRequest(BaseModel):
    processes: int = 2                    # --max-child
    batch_size: int = 25                  # --batch-size (small batches are faster)
    delay_symspell: bool = True
    skip_browse: bool = False
    skip_attrs: bool = False
    skip_search: bool = False
    skip_facets: bool = False
    skip_display: bool = False


class EvergreenRemapRequest(BaseModel):
    start_id: int = 0


class EvergreenRestartRequest(BaseModel):
    container: str = "evergreen-test"


class EvergreenTaskResponse(BaseModel):
    task_id: str
    manifest_id: str
    message: str


# ── Config ───────────────────────────────────────────────────────────────


@router.patch("/{project_id}/evergreen/config")
async def update_evergreen_config(
    project_id: str,
    body: EvergreenConfigUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Set the Evergreen PostgreSQL connection for this project."""
    project = await require_project(project_id, db)
    project.evergreen_db_host = body.evergreen_db_host
    project.evergreen_db_port = body.evergreen_db_port
    project.evergreen_db_name = body.evergreen_db_name
    project.evergreen_db_user = body.evergreen_db_user
    project.evergreen_db_password = body.evergreen_db_password
    await db.flush()
    await audit_log(db, project_id, stage=13, level="info", tag="[config]",
                    message=f"Evergreen DB: {body.evergreen_db_host}:{body.evergreen_db_port}/{body.evergreen_db_name}")
    return {
        "evergreen_db_host": project.evergreen_db_host,
        "evergreen_db_port": project.evergreen_db_port,
        "evergreen_db_name": project.evergreen_db_name,
        "evergreen_db_user": project.evergreen_db_user,
    }


# ── Connection Test ──────────────────────────────────────────────────────


@router.get("/{project_id}/evergreen/test")
async def test_evergreen_connection(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Test the Evergreen PostgreSQL connection."""
    await require_project(project_id, db)
    try:
        result = evergreen_counts_task.apply_async(
            args=[project_id], queue="push"
        ).get(timeout=15)
        return result
    except Exception as exc:
        return {"success": False, "error": str(exc)[:500]}


# ── Counts ───────────────────────────────────────────────────────────────


@router.get("/{project_id}/evergreen/counts")
async def evergreen_counts(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get record counts from the Evergreen database."""
    await require_project(project_id, db)
    try:
        result = evergreen_counts_task.apply_async(
            args=[project_id], queue="push"
        ).get(timeout=30)
        return result
    except Exception as exc:
        return {"success": False, "error": str(exc)[:500]}


# ── Push Bibs ────────────────────────────────────────────────────────────


@router.post("/{project_id}/evergreen/push-bibs", response_model=EvergreenTaskResponse, status_code=202)
async def push_bibs_to_evergreen(
    project_id: str,
    body: EvergreenPushRequest,
    db: AsyncSession = Depends(get_db),
):
    """Push MARC records to Evergreen via direct PostgreSQL INSERT.

    Converts MARC to MARCXML and inserts into biblio.record_entry.
    Optionally disables indexing triggers for speed (requires pingest after).
    """
    project = await require_project(project_id, db)
    if not project.evergreen_db_host:
        raise HTTPException(409, detail={"error": "NO_EVERGREEN_CONFIG",
                                         "message": "Configure Evergreen DB connection first."})

    manifest = PushManifest(
        project_id=project_id,
        task_type="evergreen_push",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    task = evergreen_push_bibs_task.apply_async(
        args=[project_id, manifest.id],
        kwargs={
            "batch_size": body.batch_size,
            "trigger_mode": body.trigger_mode,
            "remap_metarecords": body.remap_metarecords,
        },
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    await audit_log(db, project_id, stage=13, level="info", tag="[evergreen]",
                    message=(
                        f"Evergreen push dispatched (batch={body.batch_size}, "
                        f"trigger_mode={body.trigger_mode}, remap={body.remap_metarecords})"
                    ))

    return EvergreenTaskResponse(
        task_id=task.id,
        manifest_id=manifest.id,
        message="Evergreen bib push dispatched.",
    )


# ── Pingest ──────────────────────────────────────────────────────────────


@router.post("/{project_id}/evergreen/pingest", response_model=EvergreenTaskResponse, status_code=202)
async def run_pingest(
    project_id: str,
    body: EvergreenPingestRequest,
    db: AsyncSession = Depends(get_db),
):
    """Trigger Evergreen's parallel ingest (search index rebuild).

    After bulk loading records with triggers disabled, pingest.pl must
    be run to populate the metabib.* search index tables.
    """
    project = await require_project(project_id, db)
    if not project.evergreen_db_host:
        raise HTTPException(409, detail={"error": "NO_EVERGREEN_CONFIG"})

    manifest = PushManifest(
        project_id=project_id,
        task_type="evergreen_pingest",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    task = evergreen_pingest_task.apply_async(
        args=[project_id, manifest.id],
        kwargs={
            "processes": body.processes,
            "batch_size": body.batch_size,
            "delay_symspell": body.delay_symspell,
            "skip_browse": body.skip_browse,
            "skip_attrs": body.skip_attrs,
            "skip_search": body.skip_search,
            "skip_facets": body.skip_facets,
            "skip_display": body.skip_display,
        },
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    await audit_log(db, project_id, stage=13, level="info", tag="[evergreen]",
                    message=f"Evergreen pingest dispatched (processes={body.processes})")

    return EvergreenTaskResponse(
        task_id=task.id,
        manifest_id=manifest.id,
        message="Evergreen pingest dispatched.",
    )


# ── Metarecord Remap ─────────────────────────────────────────────────────


@router.post("/{project_id}/evergreen/remap", response_model=EvergreenTaskResponse, status_code=202)
async def run_metarecord_remap(
    project_id: str,
    body: EvergreenRemapRequest,
    db: AsyncSession = Depends(get_db),
):
    """Populate metabib.metarecord_source_map via remap_metarecord_for_bib().

    When bibs are inserted with the indexing trigger disabled, the
    metarecord source map is not populated and records become invisible
    to OPAC search. This task repairs that by calling
    metabib.remap_metarecord_for_bib(id, fingerprint, deleted) for every
    bib with id > start_id.
    """
    project = await require_project(project_id, db)
    if not project.evergreen_db_host:
        raise HTTPException(409, detail={"error": "NO_EVERGREEN_CONFIG"})

    manifest = PushManifest(
        project_id=project_id,
        task_type="evergreen_remap",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    task = evergreen_remap_task.apply_async(
        args=[project_id, manifest.id],
        kwargs={"start_id": body.start_id},
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    await audit_log(db, project_id, stage=13, level="info", tag="[evergreen]",
                    message=f"Metarecord remap dispatched (start_id={body.start_id})")

    return EvergreenTaskResponse(
        task_id=task.id,
        manifest_id=manifest.id,
        message="Metarecord remap dispatched.",
    )


# ── Service Restart ──────────────────────────────────────────────────────


@router.post("/{project_id}/evergreen/restart-services", response_model=EvergreenTaskResponse, status_code=202)
async def restart_evergreen_services(
    project_id: str,
    body: EvergreenRestartRequest,
    db: AsyncSession = Depends(get_db),
):
    """Restart Evergreen's OpenSRF stack (memcached + osrf_control + apache2).

    Required after a metarecord remap — OpenSRF caches old metarecord
    state and won't reflect new records until memcached, the OpenSRF
    services, and apache2's mod_perl workers are all restarted.
    """
    await require_project(project_id, db)

    manifest = PushManifest(
        project_id=project_id,
        task_type="evergreen_restart",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    task = evergreen_service_restart_task.apply_async(
        args=[project_id, manifest.id],
        kwargs={"container": body.container},
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    await audit_log(db, project_id, stage=13, level="info", tag="[evergreen]",
                    message=f"Service restart dispatched (container={body.container})")

    return EvergreenTaskResponse(
        task_id=task.id,
        manifest_id=manifest.id,
        message="Service restart dispatched.",
    )
