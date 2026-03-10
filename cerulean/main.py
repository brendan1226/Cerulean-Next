"""
cerulean/main.py
─────────────────────────────────────────────────────────────────────────────
FastAPI application factory. All routers registered here.
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────
from cerulean.api.routers import projects, files, maps, templates, dedup, push, sandbox, log, suggestions, transform, reference  # noqa: E402

app.include_router(projects.router,    prefix="/api/v1")
app.include_router(files.router,       prefix="/api/v1")
app.include_router(maps.router,        prefix="/api/v1")
app.include_router(templates.router,   prefix="/api/v1")
app.include_router(dedup.router,       prefix="/api/v1")
app.include_router(push.router,        prefix="/api/v1")
app.include_router(sandbox.router,     prefix="/api/v1")
app.include_router(log.router,         prefix="/api/v1")
app.include_router(suggestions.router, prefix="/api/v1")
app.include_router(transform.router,   prefix="/api/v1")
app.include_router(reference.router,   prefix="/api/v1")


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
