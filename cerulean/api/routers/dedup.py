"""
cerulean/api/routers/dedup.py
─────────────────────────────────────────────────────────────────────────────
Stage 4 — Dedup API stubs. Full implementation is next sprint.
"""

from fastapi import APIRouter

router = APIRouter(prefix="/projects", tags=["dedup"])


@router.get("/{project_id}/dedup/rules")
async def list_dedup_rules(project_id: str):
    return []


@router.post("/{project_id}/dedup/rules", status_code=201)
async def create_dedup_rule(project_id: str):
    return {"message": "Dedup rules — coming in Stage 4 sprint"}


@router.post("/{project_id}/dedup/scan", status_code=202)
async def scan_dedup(project_id: str):
    return {"message": "Dedup scan — coming in Stage 4 sprint"}


@router.post("/{project_id}/dedup/apply", status_code=202)
async def apply_dedup(project_id: str):
    return {"message": "Dedup apply — coming in Stage 4 sprint"}


@router.get("/{project_id}/dedup/clusters")
async def list_dedup_clusters(project_id: str):
    return []
