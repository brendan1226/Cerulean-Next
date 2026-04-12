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
    return await oauth.google.authorize_redirect(
        request,
        callback_url,
        hd=settings.google_allowed_domain,
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
