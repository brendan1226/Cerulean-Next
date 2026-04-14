"""
cerulean/api/routers/auth.py
---------------------------------------------------------------------
Google OAuth login flow + /me endpoint.

GET  /auth/google/login     — redirect to Google consent screen
GET  /auth/google/callback  — exchange code for user info, issue JWT
GET  /auth/me               — return current user info (validates token)
"""

from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.auth import (
    _ensure_oauth_registered,
    create_access_token,
    get_current_user,
    oauth,
    validate_email_domain,
)
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import User

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/google/login")
async def google_login(request: Request):
    """Redirect the browser to Google's OAuth consent screen."""
    _ensure_oauth_registered()
    settings = get_settings()
    callback_url = str(request.url_for("google_callback"))
    # Behind a reverse proxy, the URL may be http:// — force https:// in production
    if callback_url.startswith("http://") and request.headers.get("x-forwarded-proto") == "https":
        callback_url = callback_url.replace("http://", "https://", 1)
    return await oauth.google.authorize_redirect(
        request,
        callback_url,
        # Only hint the domain if there's a single one (multi-domain: let user pick)
        **({'hd': settings.google_allowed_domain} if ',' not in settings.google_allowed_domain else {}),
    )


@router.get("/google/callback", name="google_callback")
async def google_callback(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Handle the redirect back from Google.

    Exchanges the authorization code for user info, validates the domain,
    upserts a User row, issues a JWT, and returns an HTML page that stores
    the token in sessionStorage and redirects to /.
    """
    _ensure_oauth_registered()
    settings = get_settings()
    token = await oauth.google.authorize_access_token(request)
    user_info = token.get("userinfo")
    if not user_info:
        return HTMLResponse(
            "<h2>Login failed</h2><p>Could not retrieve user info from Google.</p>",
            status_code=400,
        )

    email = user_info["email"]
    if not validate_email_domain(email):
        from cerulean.core.logging import get_logger
        get_logger(__name__).warn(f"Login rejected: {email} (domain not allowed)", email=email)
        return HTMLResponse(
            f"<h2>Access denied</h2>"
            f"<p>Only @{settings.google_allowed_domain} accounts are allowed.</p>"
            f"<p>You signed in as <b>{email}</b>.</p>"
            f"<p><a href='/'>Try again</a></p>",
            status_code=403,
        )

    # Upsert user
    result = await db.execute(
        select(User).where(User.google_sub == user_info["sub"])
    )
    user = result.scalar_one_or_none()

    if user:
        user.name = user_info.get("name", user.name)
        user.picture = user_info.get("picture")
        user.email = email
        user.last_login = datetime.utcnow()
    else:
        user = User(
            email=email,
            name=user_info.get("name", email.split("@")[0]),
            picture=user_info.get("picture"),
            google_sub=user_info["sub"],
            last_login=datetime.utcnow(),
        )
        db.add(user)

    await db.flush()
    await db.refresh(user)

    # Log the authentication event
    from cerulean.core.logging import get_logger
    logger = get_logger(__name__)
    is_new = user.last_login is None or (datetime.utcnow() - user.created_at).total_seconds() < 5
    logger.info(
        f"User authenticated: {email} ({user.name})"
        f"{' [NEW USER]' if is_new else ''}",
        email=email,
        user_id=user.id,
    )

    access_token = create_access_token(user.id)

    # Return HTML that stores the JWT and redirects to the SPA.
    # This avoids CORS complexity since the callback is a full-page navigation.
    user_json = (
        f'{{"id":"{user.id}","email":"{user.email}",'
        f'"name":"{user.name}","picture":"{user.picture or ""}"}}'
    )
    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><title>Signing in...</title></head>
<body>
<p>Signing in...</p>
<script>
  sessionStorage.setItem('cerulean_token', '{access_token}');
  sessionStorage.setItem('cerulean_user', '{user_json}');
  window.location.href = '/';
</script>
</body></html>""")


@router.get("/me")
async def auth_me(user: User = Depends(get_current_user)):
    """Return the current authenticated user. Used on page load to validate the stored token."""
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "picture": user.picture,
    }


@router.get("/server-logs")
async def get_server_logs(
    filter: str = "",
    lines: int = 100,
):
    """Read recent Docker container logs. Returns parsed log lines."""
    import subprocess

    try:
        cmd = ["docker", "logs", "--tail", str(min(lines, 500)), "cerulean-web-1"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        all_lines = (result.stdout + result.stderr).strip().split("\n")

        if filter == "auth":
            all_lines = [l for l in all_lines if "authenticated" in l or "rejected" in l or "NEW USER" in l or "Login" in l]
        elif filter == "error":
            all_lines = [l for l in all_lines if "error" in l.lower() or "traceback" in l.lower() or "500" in l]

        return {"lines": all_lines[-lines:], "total": len(all_lines)}
    except FileNotFoundError:
        # Docker CLI not available inside container — read from log file fallback
        return {"lines": ["Docker CLI not available. View logs via: docker compose logs web"], "total": 1}
    except Exception as exc:
        return {"lines": [f"Error reading logs: {str(exc)[:200]}"], "total": 1}


@router.get("/users")
async def list_users(db: AsyncSession = Depends(get_db)):
    """List all registered users with login info."""
    result = await db.execute(
        select(User).order_by(User.last_login.desc().nullslast())
    )
    users = result.scalars().all()
    return [
        {
            "id": u.id,
            "email": u.email,
            "name": u.name,
            "picture": u.picture,
            "is_active": u.is_active,
            "last_login": u.last_login.isoformat() if u.last_login else None,
            "created_at": u.created_at.isoformat() if u.created_at else None,
        }
        for u in users
    ]
