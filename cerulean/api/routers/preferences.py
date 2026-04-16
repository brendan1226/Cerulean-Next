"""
cerulean/api/routers/preferences.py
─────────────────────────────────────────────────────────────────────────────
Per-user preference endpoints.

GET   /users/me/preferences          — registry metadata + the caller's values
PATCH /users/me/preferences          — set a single preference { key, value }
POST  /users/me/preferences/reset    — reset every preference to registry defaults
GET   /users/{user_id}/preferences   — read-only view of another user's values (any authenticated user; used by admin view)
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.auth import get_current_user
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.core.features import get_feature, registry_payload
from cerulean.core.preferences import (
    UnknownPreferenceKey,
    get_user_prefs,
    set_user_pref,
)
from cerulean.models import User, UserPreference

router = APIRouter(prefix="/users", tags=["preferences"])

# Fixed id used only when Google OAuth is not configured (local dev). A real
# User row is created lazily so preferences persist across restarts and so
# FK constraints are satisfied, but the record never appears in production.
_DEV_USER_ID = "00000000-0000-0000-0000-000000000dev"


async def _resolve_user(
    request: Request, db: AsyncSession,
) -> User:
    """Return the authenticated user, or a synthetic dev user in dev mode.

    In production (OAuth configured) this is equivalent to get_current_user.
    In dev (no GOOGLE_CLIENT_ID) it transparently creates/returns a single
    shared dev user so the preferences UI is usable without signing in.
    """
    user_id = getattr(request.state, "user_id", None)
    if user_id:
        user = await db.get(User, user_id)
        if user and user.is_active:
            return user
        raise HTTPException(401, detail="User not found or deactivated")

    settings = get_settings()
    if settings.google_client_id:
        raise HTTPException(401, detail="Not authenticated")

    # Dev mode — lazily create a single shared dev user
    user = await db.get(User, _DEV_USER_ID)
    if user is None:
        user = User(
            id=_DEV_USER_ID,
            email="dev@localhost",
            name="Dev User",
            google_sub="dev-local",
        )
        db.add(user)
        await db.flush()
    return user


class PreferenceUpdate(BaseModel):
    key: str
    value: Any


@router.get("/me/preferences")
async def get_my_preferences(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return the preferences registry plus the caller's current values.

    Response shape::

        {
          "registry": { "categories": [ ... ] },
          "values":   { "ai.data_health_report": false, ... }
        }
    """
    user = await _resolve_user(request, db)
    values = await get_user_prefs(db, user.id)
    return {"registry": registry_payload(), "values": values}


@router.patch("/me/preferences")
async def update_my_preference(
    request: Request,
    payload: PreferenceUpdate = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Set a single preference value. Validates key against the registry."""
    user = await _resolve_user(request, db)
    try:
        await set_user_pref(db, user.id, payload.key, payload.value)
    except UnknownPreferenceKey:
        raise HTTPException(404, detail={"error": "UNKNOWN_KEY", "key": payload.key})
    except ValueError as exc:
        raise HTTPException(400, detail={"error": "INVALID_VALUE", "message": str(exc)})
    return {"key": payload.key, "value": payload.value}


@router.post("/me/preferences/reset")
async def reset_my_preferences(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Delete every stored preference for the caller. Subsequent reads fall
    back to registry defaults (all AI features off)."""
    user = await _resolve_user(request, db)
    await db.execute(delete(UserPreference).where(UserPreference.user_id == user.id))
    await db.flush()
    return {"reset": True}


@router.get("/{user_id}/preferences")
async def get_user_preferences_readonly(
    user_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Read-only view of another user's preference values.

    Per spec: admins can view but not change another user's AI preferences.
    Since the platform has no explicit admin role beyond OAuth domain
    membership, any authenticated user can view — this is purely advisory
    and is never used to gate AI calls.
    """
    values = await get_user_prefs(db, user_id)
    return {"user_id": user_id, "values": values}
