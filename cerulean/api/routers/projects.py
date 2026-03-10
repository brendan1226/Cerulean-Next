"""
cerulean/api/routers/projects.py
─────────────────────────────────────────────────────────────────────────────
GET  /projects           — list all projects
POST /projects           — create project
GET  /projects/{id}      — project detail
PATCH /projects/{id}     — update project config
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.database import get_db
from cerulean.models import Project
from cerulean.schemas.projects import ProjectCreate, ProjectOut, ProjectUpdate

router = APIRouter(prefix="/projects", tags=["projects"])


def _encrypt_token(token: str | None) -> str | None:
    """Encrypt a Koha API token with Fernet. Returns None if no token."""
    if not token:
        return None
    from cryptography.fernet import Fernet
    from cerulean.core.config import get_settings
    settings = get_settings()
    if not settings.fernet_key:
        return token  # dev fallback — no encryption
    f = Fernet(settings.fernet_key.encode())
    return f.encrypt(token.encode()).decode()


@router.get("", response_model=list[ProjectOut])
async def list_projects(
    include_archived: bool = False,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Project).order_by(Project.created_at.desc())
    if not include_archived:
        stmt = stmt.where(Project.archived == False)  # noqa: E712
    result = await db.execute(stmt)
    return result.scalars().all()


@router.post("", response_model=ProjectOut, status_code=201)
async def create_project(body: ProjectCreate, db: AsyncSession = Depends(get_db)):
    # Check code uniqueness
    existing = await db.execute(select(Project).where(Project.code == body.code))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail={"error": "DUPLICATE_CODE", "message": f"Project code '{body.code}' already exists."})

    project = Project(
        code=body.code,
        library_name=body.library_name,
        koha_url=body.koha_url,
        koha_token_enc=_encrypt_token(body.koha_token),
    )
    db.add(project)
    await db.flush()
    await db.refresh(project)
    return project


@router.get("/{project_id}", response_model=ProjectOut)
async def get_project(project_id: str, db: AsyncSession = Depends(get_db)):
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail={"error": "NOT_FOUND", "message": "Project not found."})
    return project


@router.patch("/{project_id}", response_model=ProjectOut)
async def update_project(project_id: str, body: ProjectUpdate, db: AsyncSession = Depends(get_db)):
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail={"error": "NOT_FOUND", "message": "Project not found."})

    if body.library_name is not None:
        project.library_name = body.library_name
    if body.koha_url is not None:
        project.koha_url = body.koha_url
    if body.koha_token is not None:
        project.koha_token_enc = _encrypt_token(body.koha_token)
    if body.source_ils is not None:
        project.source_ils = body.source_ils
    if body.archived is not None:
        project.archived = body.archived

    await db.flush()
    await db.refresh(project)
    return project
