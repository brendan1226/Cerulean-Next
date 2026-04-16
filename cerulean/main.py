"""
cerulean/main.py
─────────────────────────────────────────────────────────────────────────────
FastAPI application factory. All routers registered here.
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

from cerulean.core.auth import decode_token
from cerulean.core.config import get_settings
from cerulean.core.logging import configure_logging, get_logger

settings = get_settings()
configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Cerulean Next starting up", debug=settings.debug)
    # Load system settings from DB into the running config
    try:
        from cerulean.api.routers.settings import load_settings_from_db
        await load_settings_from_db()
        logger.info("System settings loaded from database")
    except Exception as exc:
        logger.warn(f"Could not load DB settings (first run?): {exc}")

    # Load Cerulean plugins — restart-required model. Failures on
    # individual plugins are logged here and persisted to the DB row by
    # the installer flow; they never take down the host process.
    try:
        from cerulean.core.plugins.loader import load_all_enabled_plugins
        summary = load_all_enabled_plugins()
        if summary.successes:
            logger.info(
                f"Loaded {len(summary.successes)} Cerulean plugin(s): "
                + ", ".join(r.slug or "?" for r in summary.successes)
            )
        if summary.failures:
            for r in summary.failures:
                logger.warn(
                    f"Plugin load failed: slug={r.slug} path={r.install_path} "
                    f"error={r.error}"
                )
            # Best-effort status update so the admin page shows the error.
            try:
                await _persist_plugin_load_errors(summary.failures)
            except Exception as persist_exc:
                logger.warn(f"Could not persist plugin load errors: {persist_exc}")
    except Exception as exc:
        logger.warn(f"Plugin loader skipped: {exc}")

    yield
    logger.info("Cerulean Next shutting down")


async def _persist_plugin_load_errors(failures):
    """Stamp each failed load onto its cerulean_plugins row so the UI can
    show the error. Safe even when the table doesn't exist yet (fresh
    install before the first migration)."""
    from cerulean.core.database import get_db
    from cerulean.models import CeruleanPlugin
    from sqlalchemy import select
    async for db in get_db():
        for r in failures:
            if not r.slug:
                continue
            result = await db.execute(
                select(CeruleanPlugin).where(CeruleanPlugin.slug == r.slug)
            )
            row = result.scalar_one_or_none()
            if row:
                row.status = "error"
                row.error_message = (r.error or "")[:2000]
        await db.commit()
        return


app = FastAPI(
    title="Cerulean Next",
    description="BWS MARC Migration Platform API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)


# ── Middleware (order matters: last added = outermost) ────────────────

# SessionMiddleware is required by authlib for OAuth state storage.
app.add_middleware(SessionMiddleware, secret_key=settings.secret_key)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Auth middleware — gates all /api/v1/ endpoints ────────────────────

_PUBLIC_PATHS = {
    "/api/v1/auth/google/login",
    "/api/v1/auth/google/callback",
    "/api/health",
    "/api/docs",
    "/api/redoc",
    "/api/openapi.json",
}


class AuthMiddleware(BaseHTTPMiddleware):
    """Verify JWT on all /api/v1/ requests (except public auth paths).

    Sets request.state.user_id for downstream use by get_current_user().
    """

    async def dispatch(self, request, call_next):
        path = request.url.path

        # Allow public paths, non-API paths (frontend, vendor, health), OAI-PMH
        if path in _PUBLIC_PATHS or not path.startswith("/api/v1/") or path.startswith("/api/v1/oai/"):
            return await call_next(request)

        # Skip auth entirely when Google OAuth is not configured (dev mode)
        if not settings.google_client_id:
            return await call_next(request)

        # Extract and validate JWT
        auth_header = request.headers.get("Authorization", "")
        token = auth_header.removeprefix("Bearer ").strip()
        if not token:
            return JSONResponse(
                status_code=401,
                content={"detail": "Not authenticated"},
            )

        user_id = decode_token(token)
        if not user_id:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
            )

        request.state.user_id = user_id

        # Log significant user actions (POST/PATCH/DELETE, not GETs)
        if request.method in ("POST", "PATCH", "PUT", "DELETE"):
            _log_user_action(user_id, request.method, path)

        return await call_next(request)


app.add_middleware(AuthMiddleware)

# User email cache for logging (avoids DB lookup on every request)
_user_email_cache: dict[str, str] = {}

def _log_user_action(user_id: str, method: str, path: str):
    """Log significant user actions to /tmp/cerulean.log."""
    try:
        # Skip noisy endpoints
        if "/health" in path or "/tasks/active" in path or "/status" in path:
            return

        email = _user_email_cache.get(user_id, user_id[:8] + "...")
        from datetime import datetime
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        # Make path readable
        action = f"{method} {path}"

        with open("/tmp/cerulean.log", "a") as f:
            f.write(f"[{ts}] {email}: {action}\n")
    except Exception:
        pass


def _cache_user_email(user_id: str, email: str):
    """Cache user email for action logging."""
    _user_email_cache[user_id] = email


# ── Routers ───────────────────────────────────────
from cerulean.api.routers import auth, batch_edit, cerulean_plugins, csv_to_marc, holds, macros, marc_export, marc_files, marc_sql, oai, plugins, preferences, projects, files, maps, templates, quality, versions, dedup, reconcile, patrons, items, push, sandbox, log, suggestions, transform, reference, tasks, aspen, evergreen  # noqa: E402
from cerulean.api.routers import rda as rda_router  # noqa: E402
from cerulean.api.routers import settings as settings_router  # noqa: E402 — avoid shadowing config `settings`

app.include_router(auth.router,               prefix="/api/v1")
app.include_router(preferences.router,        prefix="/api/v1")
app.include_router(settings_router.router,    prefix="/api/v1")
app.include_router(plugins.router,            prefix="/api/v1")
app.include_router(cerulean_plugins.router,   prefix="/api/v1")
app.include_router(batch_edit.router,         prefix="/api/v1")
app.include_router(holds.router,             prefix="/api/v1")
app.include_router(csv_to_marc.router,       prefix="/api/v1")
app.include_router(marc_export.router,       prefix="/api/v1")
app.include_router(marc_files.router,        prefix="/api/v1")
app.include_router(macros.router,             prefix="/api/v1")
app.include_router(marc_sql.router,          prefix="/api/v1")
app.include_router(rda_router.router,        prefix="/api/v1")
app.include_router(oai.router,               prefix="/api/v1")
app.include_router(projects.router,    prefix="/api/v1")
app.include_router(files.router,       prefix="/api/v1")
app.include_router(maps.router,        prefix="/api/v1")
app.include_router(templates.router,   prefix="/api/v1")
app.include_router(quality.router,     prefix="/api/v1")
app.include_router(versions.router,   prefix="/api/v1")
app.include_router(dedup.router,       prefix="/api/v1")
app.include_router(reconcile.router,   prefix="/api/v1")
app.include_router(patrons.router,     prefix="/api/v1")
app.include_router(items.router,       prefix="/api/v1")
app.include_router(push.router,        prefix="/api/v1")
app.include_router(sandbox.router,     prefix="/api/v1")
app.include_router(log.router,         prefix="/api/v1")
app.include_router(suggestions.router, prefix="/api/v1")
app.include_router(transform.router,   prefix="/api/v1")
app.include_router(reference.router,   prefix="/api/v1")
app.include_router(tasks.router,       prefix="/api/v1")
app.include_router(aspen.router,       prefix="/api/v1")
app.include_router(evergreen.router,   prefix="/api/v1")


@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


# ── Static files ──────────────────────────────────
_FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
_VENDOR_DIR = _FRONTEND_DIR / "vendor"
if _VENDOR_DIR.is_dir():
    app.mount("/vendor", StaticFiles(directory=str(_VENDOR_DIR)), name="vendor")


@app.get("/help/manual")
async def serve_user_manual():
    _docs = Path(__file__).resolve().parent.parent / "docs"
    # Prefer PDF, then Word document, then markdown
    pdf = _docs / "Cerulean-Next-User-Manual.pdf"
    if pdf.is_file():
        return FileResponse(
            path=str(pdf),
            filename="Cerulean-Next-User-Manual.pdf",
            media_type="application/pdf",
        )
    docx = _docs / "Cerulean-Next-User-Manual.docx"
    if docx.is_file():
        return FileResponse(
            path=str(docx),
            filename="Cerulean-Next-User-Manual.docx",
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
    md = _docs / "USER-MANUAL.md"
    if md.is_file():
        return FileResponse(path=str(md), filename="Cerulean-Next-User-Manual.md", media_type="text/markdown")
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse("Manual not found", status_code=404)


@app.get("/help/connect-local-koha")
async def serve_connect_local_koha():
    guide = Path(__file__).resolve().parent.parent / "docs" / "CONNECT-LOCAL-KOHA.md"
    if not guide.is_file():
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("Guide not found", status_code=404)
    return FileResponse(path=str(guide), filename="Connect-Local-Koha.md", media_type="text/markdown")


@app.get("/help/plugin-authoring")
async def serve_plugin_authoring():
    guide = Path(__file__).resolve().parent.parent / "docs" / "PLUGIN-AUTHORING.md"
    if not guide.is_file():
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("Guide not found", status_code=404)
    return FileResponse(path=str(guide), filename="Plugin-Authoring.md", media_type="text/markdown")


@app.get("/")
async def serve_frontend():
    return FileResponse(_FRONTEND_DIR / "index.html")
