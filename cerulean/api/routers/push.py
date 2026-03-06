"""
cerulean/api/routers/push.py
─────────────────────────────────────────────────────────────────────────────
Stage 5 — Push to Koha API stubs. Full implementation is next sprint.
"""

from fastapi import APIRouter

router = APIRouter(prefix="/projects", tags=["push"])


@router.post("/{project_id}/push/preflight", status_code=202)
async def preflight(project_id: str):
    return {"message": "Push preflight — coming in Stage 5 sprint"}


@router.post("/{project_id}/push/start", status_code=202)
async def start_push(project_id: str):
    return {"message": "Push start — coming in Stage 5 sprint"}


@router.get("/{project_id}/push/status")
async def push_status(project_id: str):
    return {"status": "idle"}


@router.get("/{project_id}/push/log")
async def push_log(project_id: str):
    return []
