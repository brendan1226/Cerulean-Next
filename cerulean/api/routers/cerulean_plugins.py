"""
cerulean/api/routers/cerulean_plugins.py
─────────────────────────────────────────────────────────────────────────────
Installer API for Cerulean plugins (.cpz archives).

Distinct from the existing ``plugins`` router which handles Koha .kpz
files pushed AT a Koha instance. These endpoints manage plugins that
extend Cerulean itself — custom transforms and quality checks today,
and eventually UI tabs (Phase C).

All endpoints require an authenticated user (standard AuthMiddleware).
Any authenticated user can install/manage plugins per the trust model
set in Phase A planning.

POST   /cerulean-plugins/upload        — install a .cpz
GET    /cerulean-plugins                — list installed plugins
POST   /cerulean-plugins/{slug}/enable — move to enabled/ (restart required)
POST   /cerulean-plugins/{slug}/disable — move to disabled/
DELETE /cerulean-plugins/{slug}         — uninstall (keeps archive in available/)
"""

from __future__ import annotations

import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.database import get_db
from cerulean.core.plugins.loader import (
    available_dir,
    disabled_dir,
    enabled_dir,
    ensure_directories,
)
from cerulean.core.plugins.manifest import ManifestError, parse_manifest
from cerulean.models import CeruleanPlugin

router = APIRouter(prefix="/cerulean-plugins", tags=["cerulean-plugins"])

# 50 MB — plugin archives are source code, not datasets
MAX_CPZ_SIZE = 50 * 1024 * 1024


@router.get("")
async def list_plugins(db: AsyncSession = Depends(get_db)):
    """Return every installed Cerulean plugin with its status."""
    result = await db.execute(
        select(CeruleanPlugin).order_by(CeruleanPlugin.installed_at.desc())
    )
    rows = result.scalars().all()
    return [_row_to_dict(r) for r in rows]


@router.post("/upload")
async def upload_plugin(
    request: Request,
    file: UploadFile,
    db: AsyncSession = Depends(get_db),
):
    """Install (or upgrade) a plugin from an uploaded .cpz file.

    A restart of web + worker is required before the plugin's hooks
    take effect — the response includes ``restart_required=true`` as a
    reminder so the UI can show a banner.
    """
    ensure_directories()

    filename = file.filename or "plugin.cpz"
    if not filename.lower().endswith(".cpz"):
        raise HTTPException(400, detail={
            "error": "WRONG_EXTENSION",
            "message": "Cerulean plugins must have a .cpz extension.",
        })

    content = await file.read()
    if len(content) > MAX_CPZ_SIZE:
        raise HTTPException(413, detail={
            "error": "FILE_TOO_LARGE",
            "message": f"Plugin archives are capped at {MAX_CPZ_SIZE // (1024 * 1024)} MB.",
        })

    # 1. Stash the archive in available/ so the user can roll back later.
    available_dir().mkdir(parents=True, exist_ok=True)
    archive_path = available_dir() / filename
    archive_path.write_bytes(content)

    # 2. Extract into a temp dir first so a malformed manifest never
    #    leaves a half-extracted plugin on disk.
    import tempfile
    with tempfile.TemporaryDirectory(prefix="cerulean-cpz-") as tmp:
        tmp_dir = Path(tmp)
        try:
            with zipfile.ZipFile(archive_path, "r") as zf:
                _safe_extract(zf, tmp_dir)
        except zipfile.BadZipFile:
            archive_path.unlink(missing_ok=True)
            raise HTTPException(400, detail={
                "error": "BAD_ARCHIVE",
                "message": "Uploaded file is not a valid zip archive.",
            })

        manifest_path = tmp_dir / "manifest.yaml"
        if not manifest_path.is_file():
            archive_path.unlink(missing_ok=True)
            raise HTTPException(400, detail={
                "error": "NO_MANIFEST",
                "message": "Archive is missing manifest.yaml at the root.",
            })

        try:
            manifest = parse_manifest(manifest_path.read_text(encoding="utf-8"))
        except ManifestError as exc:
            archive_path.unlink(missing_ok=True)
            raise HTTPException(400, detail={
                "error": "BAD_MANIFEST",
                "message": str(exc),
            })

        # 3. Move validated tree into enabled/ — replacing any prior
        #    install of the same slug. The old archive stays in
        #    available/ for rollback.
        dest = enabled_dir() / manifest.slug
        if dest.exists():
            shutil.rmtree(dest)
        # Also purge a disabled/ copy if the slug was previously disabled
        disabled_copy = disabled_dir() / manifest.slug
        if disabled_copy.exists():
            shutil.rmtree(disabled_copy)

        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(tmp_dir, dest)

    # 4. Upsert the DB row.
    result = await db.execute(
        select(CeruleanPlugin).where(CeruleanPlugin.slug == manifest.slug)
    )
    row = result.scalar_one_or_none()
    user_id = getattr(request.state, "user_id", None)

    if row:
        row.name = manifest.name
        row.version = manifest.version
        row.author = manifest.author
        row.description = manifest.description
        row.runtime = manifest.runtime
        row.manifest = manifest.model_dump()
        row.install_path = str(dest)
        row.archive_filename = archive_path.name
        row.status = "enabled"
        row.error_message = None
        row.updated_at = datetime.utcnow()
    else:
        row = CeruleanPlugin(
            slug=manifest.slug,
            name=manifest.name,
            version=manifest.version,
            author=manifest.author,
            description=manifest.description,
            runtime=manifest.runtime,
            manifest=manifest.model_dump(),
            install_path=str(dest),
            archive_filename=archive_path.name,
            status="enabled",
            installed_by=user_id,
        )
        db.add(row)
    await db.flush()
    await db.refresh(row)

    return {
        "plugin": _row_to_dict(row),
        "restart_required": True,
        "message": f"Plugin '{manifest.slug}' installed. Restart web and worker containers to activate.",
    }


@router.post("/{slug}/enable")
async def enable_plugin(slug: str, db: AsyncSession = Depends(get_db)):
    row = await _load_plugin_row(slug, db)
    if row.status == "enabled":
        return {"plugin": _row_to_dict(row), "restart_required": False}

    ensure_directories()
    src = disabled_dir() / slug
    dest = enabled_dir() / slug
    if not src.is_dir():
        raise HTTPException(409, detail={
            "error": "PLUGIN_NOT_ON_DISK",
            "message": f"Plugin directory missing at {src}. Reinstall from the archive.",
        })
    if dest.exists():
        shutil.rmtree(dest)
    shutil.move(str(src), str(dest))

    row.install_path = str(dest)
    row.status = "enabled"
    row.error_message = None
    row.updated_at = datetime.utcnow()
    await db.flush()
    await db.refresh(row)
    return {
        "plugin": _row_to_dict(row),
        "restart_required": True,
        "message": f"Plugin '{slug}' enabled. Restart web and worker containers.",
    }


@router.post("/{slug}/disable")
async def disable_plugin(slug: str, db: AsyncSession = Depends(get_db)):
    row = await _load_plugin_row(slug, db)
    if row.status == "disabled":
        return {"plugin": _row_to_dict(row), "restart_required": False}

    ensure_directories()
    src = enabled_dir() / slug
    dest = disabled_dir() / slug
    if src.is_dir():
        if dest.exists():
            shutil.rmtree(dest)
        shutil.move(str(src), str(dest))
        row.install_path = str(dest)

    row.status = "disabled"
    row.updated_at = datetime.utcnow()
    await db.flush()
    await db.refresh(row)
    return {
        "plugin": _row_to_dict(row),
        "restart_required": True,
        "message": f"Plugin '{slug}' disabled. Restart web and worker containers to clear its hooks.",
    }


@router.delete("/{slug}")
async def uninstall_plugin(slug: str, db: AsyncSession = Depends(get_db)):
    row = await _load_plugin_row(slug, db)
    # Remove both enabled and disabled copies; leave the archive in
    # available/ as a rollback option.
    for d in (enabled_dir() / slug, disabled_dir() / slug):
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
    await db.delete(row)
    await db.flush()
    return {
        "slug": slug,
        "restart_required": True,
        "message": f"Plugin '{slug}' uninstalled. Archive kept in available/ for rollback.",
    }


# ── Helpers ──────────────────────────────────────────────────────────

async def _load_plugin_row(slug: str, db: AsyncSession) -> CeruleanPlugin:
    result = await db.execute(
        select(CeruleanPlugin).where(CeruleanPlugin.slug == slug)
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise HTTPException(404, detail={"error": "NOT_FOUND", "message": f"Plugin '{slug}' not installed."})
    return row


def _row_to_dict(r: CeruleanPlugin) -> dict:
    return {
        "id": r.id,
        "slug": r.slug,
        "name": r.name,
        "version": r.version,
        "author": r.author,
        "description": r.description,
        "runtime": r.runtime,
        "status": r.status,
        "error_message": r.error_message,
        "install_path": r.install_path,
        "archive_filename": r.archive_filename,
        "manifest": r.manifest,
        "installed_at": r.installed_at.isoformat() if r.installed_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def _safe_extract(zf: zipfile.ZipFile, target_dir: Path) -> None:
    """Prevent path-traversal on extraction — never let a member write
    outside the target directory."""
    target = target_dir.resolve()
    for member in zf.namelist():
        out = (target / member).resolve()
        if not str(out).startswith(str(target) + os.sep) and out != target:
            raise HTTPException(400, detail={
                "error": "UNSAFE_PATH",
                "message": f"Archive contains a path escape: {member!r}",
            })
    zf.extractall(target_dir)
