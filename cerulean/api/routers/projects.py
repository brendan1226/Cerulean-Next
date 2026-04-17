"""
cerulean/api/routers/projects.py
─────────────────────────────────────────────────────────────────────────────
GET  /projects           — list projects (filtered by ownership/visibility)
POST /projects           — create project
GET  /projects/{id}      — project detail
PATCH /projects/{id}     — update project config
"""

import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import Project, User
from cerulean.schemas.projects import ProjectCreate, ProjectOut, ProjectUpdate

router = APIRouter(prefix="/projects", tags=["projects"])


def _encrypt_token(token: str | None) -> str | None:
    """Encrypt a Koha API token with Fernet. Returns None if no token."""
    if not token:
        return None
    from cryptography.fernet import Fernet
    from cerulean.core.config import get_settings
    settings = get_settings()
    key = settings.fernet_key.strip() if settings.fernet_key else ""
    if not key or key.startswith("#"):
        return token  # dev fallback — no encryption
    f = Fernet(key.encode())
    return f.encrypt(token.encode()).decode()


@router.get("", response_model=list[ProjectOut])
async def list_projects(
    include_archived: bool = False,
    request: Request = None,
    db: AsyncSession = Depends(get_db),
):
    """List projects visible to the current user.

    A user sees:
    - Their own projects (any visibility)
    - All shared projects
    - Legacy projects (no owner — pre-auth migration)
    """
    user_id = getattr(request.state, "user_id", None) if request else None

    stmt = select(Project).order_by(Project.created_at.desc())
    if not include_archived:
        stmt = stmt.where(Project.archived == False)  # noqa: E712

    # Apply ownership filter when auth is active
    if user_id:
        stmt = stmt.where(
            or_(
                Project.owner_id == user_id,
                Project.visibility == "shared",
                Project.owner_id.is_(None),  # legacy projects
            )
        )

    result = await db.execute(stmt)
    projects = result.scalars().all()

    # Eagerly load owner names for the response
    owner_ids = {p.owner_id for p in projects if p.owner_id}
    owners = {}
    if owner_ids:
        owner_result = await db.execute(
            select(User).where(User.id.in_(owner_ids))
        )
        owners = {u.id: u for u in owner_result.scalars().all()}

    # Attach owner info for serialization
    for p in projects:
        p._owner_name = owners[p.owner_id].name if p.owner_id and p.owner_id in owners else None
        p._owner_email = owners[p.owner_id].email if p.owner_id and p.owner_id in owners else None

    return projects


@router.post("", response_model=ProjectOut, status_code=201)
async def create_project(
    body: ProjectCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    # Check code uniqueness
    existing = await db.execute(select(Project).where(Project.code == body.code))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail={"error": "DUPLICATE_CODE", "message": f"Project code '{body.code}' already exists."})

    user_id = getattr(request.state, "user_id", None)

    project = Project(
        code=body.code,
        library_name=body.library_name,
        koha_url=body.koha_url,
        koha_token_enc=_encrypt_token(body.koha_token),
        owner_id=user_id,
        visibility=body.visibility if hasattr(body, "visibility") and body.visibility else "private",
    )
    db.add(project)
    await db.flush()
    await db.refresh(project)
    return project


@router.get("/{project_id}", response_model=ProjectOut)
async def get_project(
    project_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    # Access control
    user_id = getattr(request.state, "user_id", None)
    if project.owner_id and project.visibility == "private" and user_id and user_id != project.owner_id:
        raise HTTPException(status_code=403, detail={"error": "FORBIDDEN", "message": "Not your project."})

    return project


@router.patch("/{project_id}", response_model=ProjectOut)
async def update_project(
    project_id: str,
    body: ProjectUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    # Only owner (or legacy) can update
    user_id = getattr(request.state, "user_id", None)
    if project.owner_id and user_id and user_id != project.owner_id:
        raise HTTPException(status_code=403, detail={"error": "FORBIDDEN", "message": "Only the project owner can update."})

    if body.library_name is not None:
        project.library_name = body.library_name
    if body.koha_url is not None:
        project.koha_url = body.koha_url
    if body.koha_token is not None:
        project.koha_token_enc = _encrypt_token(body.koha_token)
    if body.koha_auth_type is not None:
        if body.koha_auth_type not in ("basic", "bearer"):
            raise HTTPException(400, detail={"error": "INVALID_AUTH_TYPE", "message": "koha_auth_type must be 'basic' or 'bearer'."})
        project.koha_auth_type = body.koha_auth_type
    if body.source_ils is not None:
        project.source_ils = body.source_ils
    if body.archived is not None:
        project.archived = body.archived
    if body.visibility is not None:
        if body.visibility not in ("private", "shared"):
            raise HTTPException(400, detail={"error": "INVALID_VISIBILITY", "message": "visibility must be 'private' or 'shared'."})
        project.visibility = body.visibility

    await db.flush()
    await db.refresh(project)
    return project


# ── Generic project file download ────────────────────────────────────


@router.get("/{project_id}/download")
async def download_project_file(
    project_id: str,
    filename: str = Query(..., description="Filename relative to project dir"),
    db: AsyncSession = Depends(get_db),
):
    """Download any file from the project data directory by name.

    Only serves files inside the project root or known subdirectories.
    Path traversal is rejected.
    """
    await require_project(project_id, db)
    settings = get_settings()
    project_dir = Path(settings.data_root) / project_id

    if ".." in filename or filename.startswith("/"):
        raise HTTPException(400, detail="Invalid filename")

    allowed_prefixes = ("", "transformed/", "items/", "patrons/", "raw/", "versions/")
    if not any(filename == "" or filename.startswith(p) for p in allowed_prefixes):
        raise HTTPException(400, detail="Invalid file path")

    file_path = (project_dir / filename).resolve()
    if not str(file_path).startswith(str(project_dir.resolve())):
        raise HTTPException(400, detail="Invalid file path")

    if not file_path.is_file():
        raise HTTPException(404, detail=f"File not found: {filename}")

    safe_name = file_path.name.encode("ascii", "ignore").decode("ascii") or "download"
    media = "application/marc" if safe_name.endswith(".mrc") else "application/octet-stream"

    return FileResponse(path=str(file_path), media_type=media, filename=safe_name)
