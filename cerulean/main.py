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
    yield
    logger.info("Cerulean Next shutting down")


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

        # Allow public paths, non-API paths (frontend, vendor, health)
        if path in _PUBLIC_PATHS or not path.startswith("/api/v1/"):
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
        return await call_next(request)


app.add_middleware(AuthMiddleware)


# ── Routers ───────────────────────────────────────
from cerulean.api.routers import auth, projects, files, maps, templates, quality, versions, dedup, reconcile, patrons, items, push, sandbox, log, suggestions, transform, reference, tasks, aspen, evergreen  # noqa: E402

app.include_router(auth.router,        prefix="/api/v1")
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


@app.get("/")
async def serve_frontend():
    return FileResponse(_FRONTEND_DIR / "index.html")
