"""
cerulean/core/preferences.py
─────────────────────────────────────────────────────────────────────────────
Read + write helpers for user preferences.

Two families of helpers:

* async (web layer)
    - ``get_user_pref(db, user_id, key)``
    - ``get_user_prefs(db, user_id)``
    - ``set_user_pref(db, user_id, key, value)``
    - ``require_preference(key)``  — FastAPI dependency, returns 403 when off

* sync (Celery tasks)
    - ``pref_enabled_sync(session, user_id, key)``

Unknown keys raise :class:`UnknownPreferenceKey` — every toggle must be
declared in :mod:`cerulean.core.features`.
"""

from __future__ import annotations

from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from cerulean.core.auth import get_current_user
from cerulean.core.database import get_db
from cerulean.core.features import all_feature_keys, default_value, get_feature
from cerulean.models import User, UserPreference


class UnknownPreferenceKey(Exception):
    """Raised when code references a preference key that isn't in the registry."""


# ── Async helpers (web layer) ────────────────────────────────────────────

async def get_user_pref(db: AsyncSession, user_id: str, key: str) -> object:
    """Return the user's value for ``key``, falling back to the registry default."""
    if get_feature(key) is None:
        raise UnknownPreferenceKey(key)
    result = await db.execute(
        select(UserPreference.value).where(
            UserPreference.user_id == user_id,
            UserPreference.key == key,
        )
    )
    row = result.scalar_one_or_none()
    return row if row is not None else default_value(key)


async def get_user_prefs(db: AsyncSession, user_id: str) -> dict[str, object]:
    """Return a flat ``{key: value}`` dict of every registered preference
    for ``user_id``. Missing rows fall back to registry defaults."""
    result = await db.execute(
        select(UserPreference.key, UserPreference.value).where(
            UserPreference.user_id == user_id
        )
    )
    stored = {key: value for key, value in result.all()}
    return {k: stored.get(k, default_value(k)) for k in all_feature_keys()}


async def set_user_pref(
    db: AsyncSession, user_id: str, key: str, value: object
) -> UserPreference:
    """Upsert a preference row. Validates the key against the registry and
    (for bool features) the value type."""
    feature = get_feature(key)
    if feature is None:
        raise UnknownPreferenceKey(key)

    # Type coercion / validation
    if feature.value_type == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"Preference {key!r} expects a boolean, got {type(value).__name__}")
    elif feature.value_type == "string":
        if not isinstance(value, str):
            raise ValueError(f"Preference {key!r} expects a string, got {type(value).__name__}")
        if feature.options and value not in feature.options:
            raise ValueError(
                f"Preference {key!r} must be one of {feature.options}, got {value!r}"
            )

    result = await db.execute(
        select(UserPreference).where(
            UserPreference.user_id == user_id,
            UserPreference.key == key,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        row = UserPreference(user_id=user_id, key=key, value=value)
        db.add(row)
    else:
        row.value = value
    await db.flush()
    return row


# ── FastAPI dependency ───────────────────────────────────────────────────

def require_preference(key: str):
    """FastAPI dependency that 403s if ``key`` is disabled for the caller.

    Usage::

        @router.post("/projects/{pid}/ai/something",
                     dependencies=[Depends(require_preference("ai.data_health_report"))])
        async def ...

    Or, if you need the user object too::

        async def ...(user: User = Depends(require_preference("ai.data_health_report")))

    The dependency short-circuits to a 403 when the feature is off; it
    returns the authenticated ``User`` when enabled.
    """
    if get_feature(key) is None:
        raise UnknownPreferenceKey(key)

    async def _checker(
        user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db),
    ) -> User:
        value = await get_user_pref(db, user.id, key)
        if not value:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "PREFERENCE_DISABLED",
                    "feature_key": key,
                    "message": (
                        f"Feature '{key}' is disabled. Enable it in your "
                        f"preferences to use this."
                    ),
                },
            )
        return user

    return _checker


# ── Sync helper (Celery tasks) ───────────────────────────────────────────

def pref_enabled_sync(session: Session, user_id: str | None, key: str) -> bool:
    """Return the user's preference value as a bool (tasks-only helper).

    Celery tasks use a sync psycopg2 session. When ``user_id`` is None
    (legacy tasks not yet wired to the triggering user) this returns the
    registry default so AI features stay opt-in by default — tasks that
    rely on AI output must pass through the user id from the request.
    """
    if get_feature(key) is None:
        raise UnknownPreferenceKey(key)
    if not user_id:
        return bool(default_value(key))
    row = session.execute(
        select(UserPreference.value).where(
            UserPreference.user_id == user_id,
            UserPreference.key == key,
        )
    ).scalar_one_or_none()
    return bool(row if row is not None else default_value(key))
