"""
cerulean/api/routers/api_keys.py
─────────────────────────────────────────────────────────────────────────────
Admin API Key management for integration authentication.

GET    /admin/api-keys          — list all keys (hash masked)
POST   /admin/api-keys          — generate a new key (shown once)
DELETE /admin/api-keys/{id}     — revoke a key
"""

import hashlib
import secrets
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.database import get_db
from cerulean.models import APIKey

router = APIRouter(prefix="/admin/api-keys", tags=["admin"])

_KEY_PREFIX_LIVE = "cn_live_"
_KEY_PREFIX_TEST = "cn_test_"


def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def _generate_key(prefix: str = _KEY_PREFIX_LIVE) -> str:
    return f"{prefix}{secrets.token_hex(24)}"


@router.get("")
async def list_api_keys(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """List all API keys (key_hash masked, only prefix shown)."""
    result = await db.execute(
        select(APIKey).order_by(APIKey.created_at.desc())
    )
    keys = result.scalars().all()
    return [
        {
            "id": k.id,
            "label": k.label,
            "key_prefix": k.key_prefix,
            "scope": k.scope,
            "revoked": k.revoked,
            "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None,
            "created_at": k.created_at.isoformat() if k.created_at else None,
        }
        for k in keys
    ]


@router.post("", status_code=201)
async def create_api_key(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Generate a new API key. The full key is returned ONCE — store it securely."""
    body = await request.json()
    label = (body.get("label") or "").strip()
    if not label:
        raise HTTPException(400, detail="label is required")
    scope = body.get("scope", "write")
    if scope not in ("read", "write", "admin"):
        raise HTTPException(400, detail="scope must be read, write, or admin")
    env = body.get("environment", "live")
    prefix = _KEY_PREFIX_TEST if env == "test" else _KEY_PREFIX_LIVE

    raw_key = _generate_key(prefix)
    key_hash = _hash_key(raw_key)

    user_id = getattr(request.state, "user_id", None) or "_system"

    row = APIKey(
        user_id=user_id,
        key_hash=key_hash,
        key_prefix=raw_key[:16] + "…",
        label=label,
        scope=scope,
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)

    return {
        "id": row.id,
        "key": raw_key,
        "key_prefix": row.key_prefix,
        "label": row.label,
        "scope": row.scope,
        "message": "Store this key securely — it will not be shown again.",
    }


@router.delete("/{key_id}", status_code=200)
async def revoke_api_key(
    key_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Revoke an API key. It can no longer be used for authentication."""
    key = await db.get(APIKey, key_id)
    if not key:
        raise HTTPException(404, detail="API key not found")
    key.revoked = True
    await db.flush()
    return {"id": key_id, "revoked": True, "message": "Key revoked."}


# ── Helpers for the integration auth middleware ──────────────────────


async def validate_api_key(raw_key: str, db: AsyncSession, required_scope: str = "read") -> APIKey:
    """Validate an API key and return the APIKey row. Raises 401/403."""
    key_hash = _hash_key(raw_key)
    result = await db.execute(
        select(APIKey).where(APIKey.key_hash == key_hash)
    )
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(401, detail="Invalid API key")
    if key.revoked:
        raise HTTPException(401, detail="API key has been revoked")

    scope_order = {"read": 0, "write": 1, "admin": 2}
    if scope_order.get(key.scope, 0) < scope_order.get(required_scope, 0):
        raise HTTPException(403, detail=f"API key scope '{key.scope}' insufficient — requires '{required_scope}'")

    # Update last_used_at
    await db.execute(
        update(APIKey)
        .where(APIKey.id == key.id)
        .values(last_used_at=datetime.utcnow())
    )
    return key
