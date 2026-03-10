"""
cerulean/api/routers/reference.py
─────────────────────────────────────────────────────────────────────────────
GET /reference/koha-fields          — static Koha MARC field reference
GET /reference/transform-presets    — available transform preset functions
"""

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

from cerulean.core.transform_presets import get_preset_registry

router = APIRouter(prefix="/reference", tags=["reference"])

_KOHA_FIELDS_PATH = Path(__file__).resolve().parent.parent.parent.parent / "frontend" / "koha_marc_fields.json"


@router.get("/koha-fields")
async def get_koha_fields():
    """Return the static Koha MARC field reference JSON."""
    return FileResponse(_KOHA_FIELDS_PATH, media_type="application/json")


@router.get("/transform-presets")
async def get_transform_presets():
    """Return available transform presets grouped by category."""
    return get_preset_registry()
