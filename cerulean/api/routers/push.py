"""
cerulean/api/routers/push.py
─────────────────────────────────────────────────────────────────────────────
Stage 7 — Push to Koha API endpoints.

POST  /projects/{id}/push/preflight   — dispatch preflight check
POST  /projects/{id}/push/start       — dispatch selected push tasks
GET   /projects/{id}/push/status      — poll Celery task status
GET   /projects/{id}/push/manifests   — list push manifests
GET   /projects/{id}/push/log         — alias for manifests (by started_at desc)
GET   /projects/{id}/push/koha-refs   — fetch reference data from Koha
GET   /projects/{id}/push/needed-refs — scan project data for needed ref values
POST  /projects/{id}/push/libraries   — push missing libraries to Koha
GET   /projects/{id}/push/files       — list push-ready MARC files
GET   /projects/{id}/push/files/download — download a push-ready file
GET   /projects/{id}/push/files/preview  — preview records from a push-ready file
"""

import base64
import os
from datetime import datetime
from pathlib import Path

import httpx
import pymarc
from celery.result import AsyncResult
from cryptography.fernet import Fernet
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import (
    PatronScanResult,
    PushManifest,
    ReconciliationScanResult,
)
from cerulean.schemas.push import (
    ItemTypePushItem,
    KohaRefDataResponse,
    TurboIndexRequest,
    TurboIndexResponse,
    LibraryPushItem,
    NeededRefDataResponse,
    NeededRefValue,
    PatronCategoryPushItem,
    PreflightRequest,
    PreflightResponse,
    PushItemTypeResult,
    PushItemTypesRequest,
    PushItemTypesResponse,
    PushLibrariesRequest,
    PushLibrariesResponse,
    PushLibraryResult,
    PushManifestOut,
    PushPatronCategoriesRequest,
    PushPatronCategoriesResponse,
    PushPatronCategoryResult,
    PushStartRequest,
    PushStartResponse,
    PushStatusResponse,
)
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.push import (
    es_reindex_task,
    push_bulk_api_task,
    push_bulkmarc_task,
    push_bulkmarcimport_task,
    push_circ_task,
    push_fastmarc_plugin_task,
    push_holds_task,
    push_patrons_task,
    push_preflight_task,
    turboindex_task,
)
from cerulean.utils.marc import record_to_dict

settings = get_settings()
router = APIRouter(prefix="/projects", tags=["push"])


# ── Endpoints ────────────────────────────────────────────────────────────


@router.post("/{project_id}/push/preflight", response_model=PreflightResponse, status_code=202)
async def preflight(
    project_id: str,
    body: PreflightRequest | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch preflight check. Precondition: koha_url must be set."""
    project = await require_project(project_id, db)

    if not project.koha_url:
        raise HTTPException(409, detail={
            "error": "NO_KOHA_URL",
            "message": "Set koha_url on the project before running preflight.",
        })

    manifest = PushManifest(
        project_id=project_id,
        task_type="preflight",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await audit_log(db, project_id, stage=7, level="info", tag="[preflight]",
                    message="Preflight check dispatched")
    await db.flush()
    await db.refresh(manifest)

    task = push_preflight_task.apply_async(
        args=[project_id, manifest.id],
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    return PreflightResponse(task_id=task.id, message="Preflight check started.")


@router.post("/{project_id}/push/start", response_model=PushStartResponse, status_code=202)
async def start_push(
    project_id: str,
    body: PushStartRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch selected push tasks. Precondition: MARC output files exist."""
    project = await require_project(project_id, db)

    # Check that transformed or merged files exist
    project_dir = Path(settings.data_root) / project_id
    has_output = (
        (project_dir / "Biblios-mapped-items.mrc").is_file()
        or (project_dir / "merged_deduped.mrc").is_file()
        or (project_dir / "output.mrc").is_file()
        or (project_dir / "merged.mrc").is_file()
        or (
            (project_dir / "transformed").is_dir()
            and any((project_dir / "transformed").glob("*_transformed.mrc"))
        )
    )
    if not has_output and body.push_bibs:
        raise HTTPException(409, detail={
            "error": "NO_OUTPUT_FILES",
            "message": "No transformed or merged MARC files found. Complete Stage 6 first.",
        })

    # Reject if any of the requested task types are already running
    requested_types = []
    if body.push_bibs:
        requested_types.append("bulkmarc")
    if body.push_patrons:
        requested_types.append("patrons")
    if body.push_holds:
        requested_types.append("holds")
    if body.push_circ:
        requested_types.append("circ")
    if body.reindex:
        requested_types.append("reindex")

    if requested_types:
        already_running = (
            await db.execute(
                select(PushManifest).where(
                    PushManifest.project_id == project_id,
                    PushManifest.status == "running",
                    PushManifest.task_type.in_(requested_types),
                )
            )
        ).scalars().all()
        if already_running:
            running_types = [m.task_type for m in already_running]
            raise HTTPException(409, detail={
                "error": "TASK_ALREADY_RUNNING",
                "message": f"Already running: {', '.join(running_types)}. Cancel or wait for completion.",
            })

    task_ids: dict[str, str] = {}

    # Map of task_type → (task_func, needs_dry_run)
    tasks_to_dispatch: list[tuple[str, object, bool]] = []
    if body.push_bibs:
        bib_method = (body.bib_options.method if body.bib_options else "rest_api") or "rest_api"
        if bib_method == "bulkmarcimport":
            tasks_to_dispatch.append(("bulkmarc", push_bulkmarcimport_task, True))
        elif bib_method == "bulk_api":
            tasks_to_dispatch.append(("bulkmarc", push_bulk_api_task, True))
        elif bib_method == "plugin_fast":
            tasks_to_dispatch.append(("bulkmarc", push_fastmarc_plugin_task, True))
        else:
            tasks_to_dispatch.append(("bulkmarc", push_bulkmarc_task, True))
    if body.push_patrons:
        tasks_to_dispatch.append(("patrons", push_patrons_task, True))
    if body.push_holds:
        tasks_to_dispatch.append(("holds", push_holds_task, True))
    if body.push_circ:
        tasks_to_dispatch.append(("circ", push_circ_task, True))
    if body.reindex:
        tasks_to_dispatch.append(("reindex", es_reindex_task, False))

    if not tasks_to_dispatch:
        raise HTTPException(400, detail={
            "error": "NO_TASKS_SELECTED",
            "message": "Select at least one push task to start.",
        })

    for task_type, task_func, needs_dry_run in tasks_to_dispatch:
        manifest = PushManifest(
            project_id=project_id,
            task_type=task_type,
            status="running",
            dry_run=body.dry_run if needs_dry_run else False,
            started_at=datetime.utcnow(),
        )
        db.add(manifest)
        await db.flush()
        await db.refresh(manifest)

        task_kwargs = {}
        if needs_dry_run:
            task_kwargs["dry_run"] = body.dry_run
        if task_type == "bulkmarc" and body.bib_options and body.bib_options.method in ("bulkmarcimport", "bulk_api", "plugin_fast"):
            task_kwargs["bib_options"] = body.bib_options.model_dump()
        if task_type == "reindex" and body.reindex_engine:
            task_kwargs["reindex_engine"] = body.reindex_engine

        task = task_func.apply_async(
            args=[project_id, manifest.id],
            kwargs=task_kwargs if task_kwargs else {},
            queue="push",
        )
        manifest.celery_task_id = task.id
        task_ids[task_type] = task.id

    await audit_log(db, project_id, stage=7, level="info", tag="[push]",
                    message=f"Push started: {', '.join(task_ids.keys())} (dry_run={body.dry_run})")
    await db.flush()

    return PushStartResponse(
        task_ids=task_ids,
        message=f"Push tasks dispatched: {', '.join(task_ids.keys())}.",
    )


@router.get("/{project_id}/push/status", response_model=PushStatusResponse)
async def push_status(
    project_id: str,
    task_id: str | None = Query(None, description="Celery task_id to poll"),
    db: AsyncSession = Depends(get_db),
):
    """Poll Celery task status."""
    await require_project(project_id, db)

    if not task_id:
        # Find most recent manifest
        result = await db.execute(
            select(PushManifest)
            .where(PushManifest.project_id == project_id)
            .order_by(PushManifest.started_at.desc())
            .limit(1)
        )
        manifest = result.scalar_one_or_none()
        if not manifest or not manifest.celery_task_id:
            return PushStatusResponse(task_id=None, state="IDLE")
        task_id = manifest.celery_task_id

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

    return PushStatusResponse(
        task_id=task_id, state=state, progress=progress,
        result=result_data, error=error,
    )


@router.get("/{project_id}/push/manifests", response_model=list[PushManifestOut])
async def list_manifests(
    project_id: str,
    task_type: str | None = Query(None, description="Filter: preflight|bulkmarc|patrons|holds|circ|reindex"),
    db: AsyncSession = Depends(get_db),
):
    """List all push manifests for a project."""
    await require_project(project_id, db)

    q = select(PushManifest).where(PushManifest.project_id == project_id)
    if task_type:
        q = q.where(PushManifest.task_type == task_type)
    q = q.order_by(PushManifest.started_at.desc())

    result = await db.execute(q)
    return result.scalars().all()


@router.get("/{project_id}/push/manifests/{manifest_id}", response_model=PushManifestOut)
async def get_manifest(
    project_id: str,
    manifest_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return a single push manifest with its full result_data.

    Used by the UI to live-poll job status, message streams, counters,
    and per-slice breakdowns while a push/reindex is running.
    """
    await require_project(project_id, db)
    m = (await db.execute(
        select(PushManifest).where(
            PushManifest.project_id == project_id,
            PushManifest.id == manifest_id,
        )
    )).scalar_one_or_none()
    if not m:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": f"Manifest {manifest_id} not found"})
    return m


@router.get("/{project_id}/push/log", response_model=list[PushManifestOut])
async def push_log(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Alias — list manifests ordered by started_at desc."""
    await require_project(project_id, db)

    result = await db.execute(
        select(PushManifest)
        .where(PushManifest.project_id == project_id)
        .order_by(PushManifest.started_at.desc())
    )
    return result.scalars().all()


# ── Reference Data (Setup tab) ────────────────────────────────────────────


def _koha_headers(project) -> tuple[str, dict[str, str]]:
    """Build (base_url, headers) from a Project ORM object."""
    from cerulean.tasks.push import _rewrite_localhost

    base_url = _rewrite_localhost(project.koha_url.rstrip("/"))

    # Decrypt token
    key = settings.fernet_key.strip() if settings.fernet_key else ""
    if key and not key.startswith("#") and project.koha_token_enc:
        f = Fernet(key.encode())
        token = f.decrypt(project.koha_token_enc.encode()).decode()
    else:
        token = project.koha_token_enc or ""

    auth_type = getattr(project, "koha_auth_type", "basic") or "basic"
    if auth_type == "bearer":
        auth_value = f"Bearer {token}"
    else:
        b64 = base64.b64encode(token.encode()).decode()
        auth_value = f"Basic {b64}"

    return base_url, {"Authorization": auth_value, "Accept": "application/json"}


@router.get("/{project_id}/push/koha-refs", response_model=KohaRefDataResponse)
async def get_koha_refs(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Fetch reference data from Koha (libraries, patron categories, item types, authorised values)."""
    project = await require_project(project_id, db)
    if not project.koha_url:
        raise HTTPException(409, detail={"error": "NO_KOHA_URL", "message": "Set koha_url first."})

    base_url, headers = _koha_headers(project)

    async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
        # Fire all GETs in parallel
        responses = await _fetch_koha_refs(client, base_url, headers)

    return KohaRefDataResponse(
        libraries=responses["libraries"],
        patron_categories=responses["patron_categories"],
        item_types=responses["item_types"],
        authorised_values=responses["authorised_values"],
    )


async def _fetch_koha_refs(client: httpx.AsyncClient, base_url: str, headers: dict) -> dict:
    """GET all reference data endpoints from Koha, return parsed dicts."""
    endpoints = {
        "libraries": "/api/v1/libraries",
        "patron_categories": "/api/v1/patron_categories",
        "item_types": "/api/v1/item_types",
        "av_LOC": "/api/v1/authorised_value_categories/LOC/authorised_values",
        "av_CCODE": "/api/v1/authorised_value_categories/CCODE/authorised_values",
        "av_LOST": "/api/v1/authorised_value_categories/LOST/authorised_values",
    }

    import asyncio
    tasks = {
        key: client.get(f"{base_url}{path}", headers=headers)
        for key, path in endpoints.items()
    }
    results_raw = await asyncio.gather(*tasks.values(), return_exceptions=True)
    results = dict(zip(tasks.keys(), results_raw))

    def _parse(key: str) -> list[dict]:
        resp = results.get(key)
        if isinstance(resp, Exception) or resp is None:
            return []
        if resp.status_code >= 400:
            return []
        try:
            return resp.json()
        except Exception:
            return []

    return {
        "libraries": _parse("libraries"),
        "patron_categories": _parse("patron_categories"),
        "item_types": _parse("item_types"),
        "authorised_values": {
            "LOC": _parse("av_LOC"),
            "CCODE": _parse("av_CCODE"),
            "LOST": _parse("av_LOST"),
        },
    }


@router.get("/{project_id}/push/needed-refs", response_model=NeededRefDataResponse)
async def get_needed_refs(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Scan project data to determine what reference values are needed."""
    await require_project(project_id, db)

    # Patron scan results: branchcode, categorycode
    patron_q = (
        select(PatronScanResult.koha_header, PatronScanResult.source_value,
               func.sum(PatronScanResult.record_count).label("cnt"))
        .where(PatronScanResult.project_id == project_id)
        .where(PatronScanResult.koha_header.in_(["branchcode", "categorycode"]))
        .group_by(PatronScanResult.koha_header, PatronScanResult.source_value)
    )
    patron_rows = (await db.execute(patron_q)).all()

    # Reconciliation scan results: itype, loc, ccode
    recon_q = (
        select(ReconciliationScanResult.vocab_category, ReconciliationScanResult.source_value,
               func.sum(ReconciliationScanResult.record_count).label("cnt"))
        .where(ReconciliationScanResult.project_id == project_id)
        .where(ReconciliationScanResult.vocab_category.in_(["itype", "loc", "ccode"]))
        .group_by(ReconciliationScanResult.vocab_category, ReconciliationScanResult.source_value)
    )
    recon_rows = (await db.execute(recon_q)).all()

    # Build response
    libraries = []
    patron_categories = []
    item_types = []
    locations = []
    ccodes = []

    for header, value, cnt in patron_rows:
        item = NeededRefValue(value=value, record_count=int(cnt))
        if header == "branchcode":
            libraries.append(item)
        elif header == "categorycode":
            patron_categories.append(item)

    for category, value, cnt in recon_rows:
        item = NeededRefValue(value=value, record_count=int(cnt))
        if category == "itype":
            item_types.append(item)
        elif category == "loc":
            locations.append(item)
        elif category == "ccode":
            ccodes.append(item)

    return NeededRefDataResponse(
        libraries=sorted(libraries, key=lambda x: -x.record_count),
        patron_categories=sorted(patron_categories, key=lambda x: -x.record_count),
        item_types=sorted(item_types, key=lambda x: -x.record_count),
        locations=sorted(locations, key=lambda x: -x.record_count),
        ccodes=sorted(ccodes, key=lambda x: -x.record_count),
    )


@router.post("/{project_id}/push/libraries", response_model=PushLibrariesResponse)
async def push_libraries(
    project_id: str,
    body: PushLibrariesRequest,
    db: AsyncSession = Depends(get_db),
):
    """Push missing libraries to Koha via POST /api/v1/libraries."""
    project = await require_project(project_id, db)
    if not project.koha_url:
        raise HTTPException(409, detail={"error": "NO_KOHA_URL", "message": "Set koha_url first."})

    base_url, headers = _koha_headers(project)
    results = []
    success_count = 0
    failed_count = 0

    async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
        for lib in body.libraries:
            try:
                resp = await client.post(
                    f"{base_url}/api/v1/libraries",
                    json={"library_id": lib.library_id, "name": lib.name},
                    headers=headers,
                )
                if resp.status_code < 300:
                    success_count += 1
                    results.append(PushLibraryResult(library_id=lib.library_id, success=True))
                else:
                    failed_count += 1
                    error_text = resp.text[:200]
                    results.append(PushLibraryResult(
                        library_id=lib.library_id, success=False,
                        error=f"HTTP {resp.status_code}: {error_text}",
                    ))
            except httpx.HTTPError as exc:
                failed_count += 1
                results.append(PushLibraryResult(
                    library_id=lib.library_id, success=False, error=str(exc),
                ))

    await audit_log(db, project_id, stage=7, level="info", tag="[setup]",
                    message=f"Pushed {success_count}/{len(body.libraries)} libraries to Koha")

    return PushLibrariesResponse(
        total=len(body.libraries),
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )


@router.post("/{project_id}/push/patron-categories", response_model=PushPatronCategoriesResponse)
async def push_patron_categories(
    project_id: str,
    body: PushPatronCategoriesRequest,
    db: AsyncSession = Depends(get_db),
):
    """Push missing patron categories to Koha.

    Tries REST API first (POST /api/v1/patron_categories).
    Falls back to SQL via docker exec koha-mysql if REST returns 404.
    """
    import asyncio
    import subprocess

    project = await require_project(project_id, db)
    if not project.koha_url:
        raise HTTPException(409, detail={"error": "NO_KOHA_URL", "message": "Set koha_url first."})

    base_url, headers = _koha_headers(project)
    results = []
    success_count = 0
    failed_count = 0
    use_sql = False

    # Try REST API first with the first category
    if body.categories:
        first = body.categories[0]
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            try:
                resp = await client.post(
                    f"{base_url}/api/v1/patron_categories",
                    json={
                        "patron_category_id": first.category_id,
                        "name": first.name,
                        "enrolmentperiod": first.enrolmentperiod,
                    },
                    headers=headers,
                )
                if resp.status_code == 404:
                    use_sql = True  # endpoint doesn't exist, fall back to SQL
                elif resp.status_code < 300:
                    success_count += 1
                    results.append(PushPatronCategoryResult(category_id=first.category_id, success=True))
                else:
                    failed_count += 1
                    results.append(PushPatronCategoryResult(
                        category_id=first.category_id, success=False,
                        error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                    ))
            except httpx.HTTPError:
                use_sql = True

    if use_sql:
        # Fall back to SQL via docker exec — must run on the worker (has docker CLI)
        from cerulean.tasks.push import push_patron_categories_sql_task
        categories_data = [
            {"category_id": cat.category_id, "name": cat.name, "enrolmentperiod": cat.enrolmentperiod}
            for cat in body.categories
        ]
        celery_result = push_patron_categories_sql_task.apply_async(
            args=[project_id, categories_data],
            queue="push",
        )
        try:
            task_result = celery_result.get(timeout=60)  # wait up to 60s
        except Exception as exc:
            raise HTTPException(500, detail={
                "error": "TASK_FAILED",
                "message": f"SQL push failed: {exc}",
            })

        success_count = task_result.get("success_count", 0)
        failed_count = task_result.get("failed_count", 0)
        results = [
            PushPatronCategoryResult(
                category_id=r["category_id"], success=r["success"],
                error=r.get("error"),
            )
            for r in task_result.get("results", [])
        ]
    elif len(body.categories) > 1:
        # REST API worked for the first one, continue with the rest
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            for cat in body.categories[1:]:
                try:
                    resp = await client.post(
                        f"{base_url}/api/v1/patron_categories",
                        json={
                            "patron_category_id": cat.category_id,
                            "name": cat.name,
                            "enrolmentperiod": cat.enrolmentperiod,
                        },
                        headers=headers,
                    )
                    if resp.status_code < 300:
                        success_count += 1
                        results.append(PushPatronCategoryResult(category_id=cat.category_id, success=True))
                    else:
                        failed_count += 1
                        results.append(PushPatronCategoryResult(
                            category_id=cat.category_id, success=False,
                            error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                        ))
                except httpx.HTTPError as exc:
                    failed_count += 1
                    results.append(PushPatronCategoryResult(
                        category_id=cat.category_id, success=False, error=str(exc),
                    ))

    method = "SQL (docker exec)" if use_sql else "REST API"
    await audit_log(db, project_id, stage=7, level="info", tag="[setup]",
                    message=f"Pushed {success_count}/{len(body.categories)} patron categories to Koha via {method}")

    return PushPatronCategoriesResponse(
        total=len(body.categories),
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )


@router.post("/{project_id}/push/item-types", response_model=PushItemTypesResponse)
async def push_item_types(
    project_id: str,
    body: PushItemTypesRequest,
    db: AsyncSession = Depends(get_db),
):
    """Push missing item types to Koha.

    Tries REST API first (POST /api/v1/item_types).
    Falls back to SQL via docker exec koha-mysql if REST returns 404.
    """
    project = await require_project(project_id, db)
    if not project.koha_url:
        raise HTTPException(409, detail={"error": "NO_KOHA_URL", "message": "Set koha_url first."})

    base_url, headers = _koha_headers(project)
    results = []
    success_count = 0
    failed_count = 0
    use_sql = False

    # Try REST API first with the first item type
    if body.item_types:
        first = body.item_types[0]
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            try:
                resp = await client.post(
                    f"{base_url}/api/v1/item_types",
                    json={
                        "item_type_id": first.item_type_id,
                        "description": first.description,
                    },
                    headers=headers,
                )
                if resp.status_code == 404:
                    use_sql = True  # endpoint doesn't exist, fall back to SQL
                elif resp.status_code < 300:
                    success_count += 1
                    results.append(PushItemTypeResult(item_type_id=first.item_type_id, success=True))
                else:
                    failed_count += 1
                    results.append(PushItemTypeResult(
                        item_type_id=first.item_type_id, success=False,
                        error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                    ))
            except httpx.HTTPError:
                use_sql = True

    if use_sql:
        # Fall back to SQL via Celery task (has docker CLI / DB access)
        from cerulean.tasks.push import push_item_types_sql_task
        item_types_data = [
            {"item_type_id": it.item_type_id, "description": it.description}
            for it in body.item_types
        ]
        celery_result = push_item_types_sql_task.apply_async(
            args=[project_id, item_types_data],
            queue="push",
        )
        try:
            task_result = celery_result.get(timeout=60)
        except Exception as exc:
            raise HTTPException(500, detail={
                "error": "TASK_FAILED",
                "message": f"SQL push failed: {exc}",
            })

        success_count = task_result.get("success_count", 0)
        failed_count = task_result.get("failed_count", 0)
        results = [
            PushItemTypeResult(
                item_type_id=r["item_type_id"], success=r["success"],
                error=r.get("error"),
            )
            for r in task_result.get("results", [])
        ]
    elif len(body.item_types) > 1:
        # REST API worked for the first one, continue with the rest
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            for it in body.item_types[1:]:
                try:
                    resp = await client.post(
                        f"{base_url}/api/v1/item_types",
                        json={
                            "item_type_id": it.item_type_id,
                            "description": it.description,
                        },
                        headers=headers,
                    )
                    if resp.status_code < 300:
                        success_count += 1
                        results.append(PushItemTypeResult(item_type_id=it.item_type_id, success=True))
                    else:
                        failed_count += 1
                        results.append(PushItemTypeResult(
                            item_type_id=it.item_type_id, success=False,
                            error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                        ))
                except httpx.HTTPError as exc:
                    failed_count += 1
                    results.append(PushItemTypeResult(
                        item_type_id=it.item_type_id, success=False, error=str(exc),
                    ))

    method = "SQL (docker exec)" if use_sql else "REST API"
    await audit_log(db, project_id, stage=7, level="info", tag="[setup]",
                    message=f"Pushed {success_count}/{len(body.item_types)} item types to Koha via {method}")

    return PushItemTypesResponse(
        total=len(body.item_types),
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )


# ── Migration mode (stop/start Koha daemons) ────────────────────────────


@router.get("/{project_id}/push/migration-mode")
async def migration_mode_status_endpoint(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Check which Koha daemons are running/stopped.

    Dispatches via Celery worker (which has the Docker socket).
    """
    await require_project(project_id, db)
    from cerulean.tasks.push import migration_mode_status_task
    try:
        result = migration_mode_status_task.apply_async(
            args=[project_id], queue="push"
        ).get(timeout=30)
        return result
    except Exception as exc:
        raise HTTPException(500, detail={"error": "MIGRATION_MODE_FAILED", "message": str(exc)})


@router.post("/{project_id}/push/migration-mode/enable")
async def migration_mode_enable_endpoint(
    project_id: str,
    buffer_pool_mb: int = Query(2048, ge=128, le=16384,
                                 description="InnoDB buffer pool size in MB (migration fast mode)"),
    db: AsyncSession = Depends(get_db),
):
    """Stop non-essential Koha daemons and tune DB for migration.

    Stops: ES indexer, Zebra indexer, Zebra search, SIP, Z39.50.
    Keeps: background workers (process import/reindex jobs), Plack (REST API).
    DB tuning: sets innodb_flush_log_at_trx_commit=2, grows buffer pool.
    """
    await require_project(project_id, db)
    from cerulean.tasks.push import migration_mode_enable_task
    try:
        result = migration_mode_enable_task.apply_async(
            args=[project_id],
            kwargs={"buffer_pool_mb": buffer_pool_mb},
            queue="push",
        ).get(timeout=60)
    except Exception as exc:
        raise HTTPException(500, detail={"error": "MIGRATION_MODE_FAILED", "message": str(exc)})
    await audit_log(db, project_id, stage=7, level="info", tag="[migration-mode]",
                    message=(
                        f"Migration mode ENABLED — daemons stopped, "
                        f"DB tuned (buffer_pool={buffer_pool_mb}MB, flush=2)"
                    ))
    return result


@router.post("/{project_id}/push/migration-mode/disable")
async def migration_mode_disable_endpoint(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Restart non-essential Koha daemons after migration.

    Restarts everything that was stopped by migration-mode/enable.
    """
    await require_project(project_id, db)
    from cerulean.tasks.push import migration_mode_disable_task
    try:
        result = migration_mode_disable_task.apply_async(
            args=[project_id], queue="push"
        ).get(timeout=60)
    except Exception as exc:
        raise HTTPException(500, detail={"error": "MIGRATION_MODE_FAILED", "message": str(exc)})
    await audit_log(db, project_id, stage=7, level="info", tag="[migration-mode]",
                    message="Migration mode DISABLED — Koha daemons restarted")
    return result


# ── TurboIndex (plugin) reindex ──────────────────────────────────────────


@router.post("/{project_id}/push/turboindex", response_model=TurboIndexResponse, status_code=202)
async def turboindex_start(
    project_id: str,
    body: TurboIndexRequest,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch a TurboIndex reindex job.

    Runs the turboindex plugin (POST /api/v1/contrib/turboindex/biblios/reindex)
    to rebuild the Elasticsearch biblio index in a single parallel pass.
    Use this after a fastmarcimport push, which defers index writes.
    """
    project = await require_project(project_id, db)
    if not project.koha_url:
        raise HTTPException(409, detail={"error": "NO_KOHA_URL", "message": "Set koha_url first."})

    # Reject if a reindex is already running
    already_running = (
        await db.execute(
            select(PushManifest).where(
                PushManifest.project_id == project_id,
                PushManifest.status == "running",
                PushManifest.task_type == "reindex",
            )
        )
    ).scalars().all()
    if already_running:
        raise HTTPException(409, detail={
            "error": "TASK_ALREADY_RUNNING",
            "message": "A reindex is already running. Wait for it to finish.",
        })

    manifest = PushManifest(
        project_id=project_id,
        task_type="reindex",
        status="running",
        dry_run=False,
        started_at=datetime.utcnow(),
    )
    db.add(manifest)
    await db.flush()
    await db.refresh(manifest)

    task = turboindex_task.apply_async(
        args=[project_id, manifest.id],
        kwargs={
            "reset": body.reset,
            "processes": body.processes,
            "commit": body.commit,
            "force_merge": body.force_merge,
        },
        queue="push",
    )
    manifest.celery_task_id = task.id
    await db.flush()

    await audit_log(db, project_id, stage=7, level="info", tag="[turboindex]",
                    message=(
                        f"TurboIndex dispatched (processes={body.processes}, "
                        f"reset={body.reset}, commit={body.commit}, "
                        f"force_merge={body.force_merge})"
                    ))

    return TurboIndexResponse(
        task_id=task.id,
        manifest_id=manifest.id,
        message="TurboIndex reindex dispatched.",
    )


# ── UTF-8 preflight endpoints ────────────────────────────────────────────


def _best_marc_path(project_id: str) -> Path | None:
    """Resolve the single best push-ready MARC file for a project."""
    project_dir = Path(settings.data_root) / project_id
    for name in ["Biblios-mapped-items.mrc", "merged_deduped.mrc", "output.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            return candidate
    transformed_dir = project_dir / "transformed"
    if transformed_dir.is_dir():
        transformed = sorted(transformed_dir.glob("*_transformed.mrc"))
        if transformed:
            return transformed[0]
    return None


@router.get("/{project_id}/push/utf8-scan")
async def utf8_scan(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Scan the push-ready MARC file for records that will fail strict UTF-8
    decode downstream in Koha.

    Returns a count of bad records plus sample error info.  Safe to run
    repeatedly — does not modify any files.
    """
    await require_project(project_id, db)
    from cerulean.utils.marc import scan_utf8_issues

    marc_path = _best_marc_path(project_id)
    if not marc_path:
        raise HTTPException(409, detail={
            "error": "NO_MARC_FILE",
            "message": "No push-ready MARC file found.",
        })

    result = scan_utf8_issues(str(marc_path))
    return {
        "filename": marc_path.name,
        "total_records": result["total_records"],
        "bad_record_count": result["bad_record_count"],
        "bad_record_indices": result["bad_record_indices"][:50],  # cap list
        "nonascii_record_count": result.get("nonascii_record_count", 0),
        "samples": result["samples"],
    }


@router.post("/{project_id}/push/utf8-repair")
async def utf8_repair(
    project_id: str,
    mode: str = Query("transliterate", pattern="^(skip|transliterate|replace)$"),
    db: AsyncSession = Depends(get_db),
):
    """Repair UTF-8 hazards in the push-ready MARC file.

    Rewrites the push-ready file in place (original is preserved as a
    ``.pre-utf8-repair`` sibling).

    Modes:
        skip           — drop bad records entirely
        transliterate  — ASCII-fold non-ASCII in affected fields (default)
        replace        — substitute undecodable bytes with U+FFFD
    """
    await require_project(project_id, db)
    from cerulean.utils.marc import repair_utf8

    marc_path = _best_marc_path(project_id)
    if not marc_path:
        raise HTTPException(409, detail={
            "error": "NO_MARC_FILE",
            "message": "No push-ready MARC file found.",
        })

    backup_path = marc_path.with_suffix(marc_path.suffix + ".pre-utf8-repair")
    tmp_path = marc_path.with_suffix(marc_path.suffix + ".utf8-fixed")
    try:
        result = repair_utf8(str(marc_path), str(tmp_path), mode=mode)
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink()
        raise HTTPException(500, detail={"error": "REPAIR_FAILED", "message": str(exc)})

    # Atomic swap: original -> backup, tmp -> original
    if backup_path.exists():
        backup_path.unlink()
    marc_path.rename(backup_path)
    tmp_path.rename(marc_path)

    await audit_log(db, project_id, stage=7, level="info", tag="[utf8]",
                    message=(
                        f"UTF-8 repair ({mode}): {result['bad_record_count']} bad records, "
                        f"kept={result['kept']}, skipped={result.get('skipped', 0)}, "
                        f"repaired_fields={result.get('repaired_fields', 0)}"
                    ))

    return {
        "filename": marc_path.name,
        "backup": backup_path.name,
        **result,
    }


# ── File review endpoints ────────────────────────────────────────────────


def _list_push_files(project_id: str) -> list[dict]:
    """Return push-ready files: best MARC file + controlled value CSVs."""
    project_dir = Path(settings.data_root) / project_id
    files: list[dict] = []

    # Find the single best MARC file (highest priority wins)
    source_labels = {
        "Biblios-mapped-items.mrc": "Items",
        "merged_deduped.mrc": "Deduped",
        "output.mrc": "Build Output",
        "merged.mrc": "Merged",
    }
    best_marc = None
    for name in ["Biblios-mapped-items.mrc", "merged_deduped.mrc", "output.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            best_marc = candidate
            break

    if not best_marc:
        # Fall back to transformed files
        transformed_dir = project_dir / "transformed"
        if transformed_dir.is_dir():
            transformed = sorted(transformed_dir.glob("*_transformed.mrc"))
            if transformed:
                best_marc = transformed[0]

    if best_marc:
        stat = best_marc.stat()
        files.append({
            "filename": best_marc.name,
            "path": str(best_marc),
            "size": stat.st_size,
            "modified": datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z",
            "source": source_labels.get(best_marc.name, "Transformed"),
            "type": "marc",
        })

    # Include controlled value CSVs from items stage
    items_dir = project_dir / "items"
    if items_dir.is_dir():
        for csv_path in sorted(items_dir.glob("items_*.csv")):
            stat = csv_path.stat()
            cat = csv_path.stem.replace("items_", "")
            files.append({
                "filename": csv_path.name,
                "path": str(csv_path),
                "size": stat.st_size,
                "modified": datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z",
                "source": cat,
                "type": "controlled_values",
            })

    return files


@router.get("/{project_id}/push/files")
async def list_push_files(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List MARC files available for push, with sizes and record counts."""
    await require_project(project_id, db)
    files = _list_push_files(project_id)

    # Quick record count per file
    for f in files:
        try:
            count = 0
            with open(f["path"], "rb") as fh:
                reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True,
                                            utf8_handling="replace")
                for rec in reader:
                    if rec is not None:
                        count += 1
            f["record_count"] = count
        except Exception:
            f["record_count"] = None

    # Don't expose internal paths to the frontend
    for f in files:
        del f["path"]

    return {"files": files}


@router.get("/{project_id}/push/files/download")
async def download_push_file(
    project_id: str,
    filename: str = Query(..., description="Filename to download"),
    db: AsyncSession = Depends(get_db),
):
    """Download a push-ready MARC file."""
    await require_project(project_id, db)

    # Resolve safely — only allow known filenames
    files = _list_push_files(project_id)
    match = next((f for f in files if f["filename"] == filename), None)
    if not match or not os.path.isfile(match["path"]):
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": f"File '{filename}' not found."})

    return FileResponse(
        match["path"],
        media_type="application/marc",
        filename=filename,
    )


@router.get("/{project_id}/push/files/preview")
async def preview_push_file(
    project_id: str,
    filename: str = Query(..., description="Filename to preview"),
    record_index: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Preview a record from a push-ready MARC file."""
    await require_project(project_id, db)

    files = _list_push_files(project_id)
    match = next((f for f in files if f["filename"] == filename), None)
    if not match or not os.path.isfile(match["path"]):
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": f"File '{filename}' not found."})

    total = 0
    record_data = None
    try:
        with open(match["path"], "rb") as fh:
            reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True,
                                        utf8_handling="replace")
            valid_idx = 0
            for rec in reader:
                if rec is None:
                    continue
                if valid_idx == record_index:
                    record_data = record_to_dict(rec, record_index)
                valid_idx += 1
    except Exception as exc:
        raise HTTPException(500, detail={"error": "READ_ERROR", "message": str(exc)})

    total = valid_idx
    if not record_data:
        raise HTTPException(404, detail={
            "error": "RECORD_NOT_FOUND",
            "message": f"No record at index {record_index} (file has {total} records).",
        })

    return {
        "filename": filename,
        "record_index": record_index,
        "total_records": total,
        "record": record_data,
    }
