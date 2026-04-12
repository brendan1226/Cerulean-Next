"""
cerulean/core/auth.py
---------------------------------------------------------------------
Google OAuth client, JWT creation, and get_current_user dependency.

All @bywatersolutions.com users get full access; no roles or permissions.
"""

from datetime import datetime, timedelta

from authlib.integrations.starlette_client import OAuth
from fastapi import Depends, HTTPException, Request
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import User

settings = get_settings()

# ── Google OAuth client ─────────────────────────────────────────────────

oauth = OAuth()
oauth.register(
    name="google",
    client_id=settings.google_client_id,
    client_secret=settings.google_client_secret,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)


# ── JWT helpers ─────────────────────────────────────────────────────────

def create_access_token(user_id: str) -> str:
    """Issue a JWT for the given user id."""
    expire = datetime.utcnow() + timedelta(minutes=settings.jwt_expiry_minutes)
    return jwt.encode(
        {"sub": user_id, "exp": expire},
        settings.secret_key,
        algorithm=settings.jwt_algorithm,
    )


def decode_token(token: str) -> str | None:
    """Decode a JWT and return the user id (sub), or None on failure."""
    try:
        payload = jwt.decode(
            token, settings.secret_key, algorithms=[settings.jwt_algorithm]
        )
        return payload.get("sub")
    except JWTError:
        return None


# ── FastAPI dependency ──────────────────────────────────────────────────

async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> User:
    """Return the authenticated User for the current request.

    Reads user_id from request.state (set by AuthMiddleware) and fetches
    the full User object from the database.
    """
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")

    user = await db.get(User, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or deactivated")

    return user


# ── Domain validation ──────────────────────────────────────────────────

def validate_email_domain(email: str) -> bool:
    """Check that the email is from the allowed domain."""
    return email.lower().endswith(f"@{settings.google_allowed_domain}")
