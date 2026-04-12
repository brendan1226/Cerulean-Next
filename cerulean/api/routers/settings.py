"""
cerulean/api/routers/settings.py
---------------------------------------------------------------------
System-wide settings API. Reads/writes the system_settings table.

GET  /settings          — list all settings (secrets masked)
PUT  /settings          — bulk update settings
POST /settings/reload   — reload settings into running config
"""

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.database import get_db
from cerulean.models import SystemSetting

router = APIRouter(prefix="/settings", tags=["settings"])

# Settings that can be managed via the GUI.
# key → (description, is_secret, default_value)
_MANAGED_SETTINGS = {
    "google_client_id": ("Google OAuth Client ID", False, ""),
    "google_client_secret": ("Google OAuth Client Secret", True, ""),
    "google_allowed_domain": ("Allowed email domain for login", False, "bywatersolutions.com"),
    "jwt_expiry_minutes": ("JWT token expiry (minutes)", False, "480"),
    "anthropic_api_key": ("Anthropic API key for AI features", True, ""),
    "anthropic_model": ("Claude model ID", False, "claude-sonnet-4-20250514"),
}


class SettingOut(BaseModel):
    key: str
    value: str
    is_secret: bool
    description: str | None


class SettingsUpdate(BaseModel):
    settings: dict[str, str]  # key → value


@router.get("", response_model=list[SettingOut])
async def list_settings(db: AsyncSession = Depends(get_db)):
    """List all managed settings. Secret values are masked."""
    result = await db.execute(select(SystemSetting))
    existing = {s.key: s for s in result.scalars().all()}

    out = []
    for key, (desc, is_secret, default) in _MANAGED_SETTINGS.items():
        row = existing.get(key)
        value = row.value if row else ""
        # Mask secrets — show only last 4 chars
        if is_secret and value:
            value = "****" + value[-4:]
        out.append(SettingOut(
            key=key,
            value=value,
            is_secret=is_secret,
            description=desc,
        ))
    return out


@router.put("")
async def update_settings(
    body: SettingsUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Bulk update system settings. Only keys in _MANAGED_SETTINGS are accepted."""
    updated = []
    for key, value in body.settings.items():
        if key not in _MANAGED_SETTINGS:
            continue
        # Strip surrounding quotes (frontend may JSON-encode the value)
        value = value.strip().strip('"').strip("'")
        # Skip masked values (user didn't change the secret)
        if value.startswith("****"):
            continue

        desc, is_secret, _ = _MANAGED_SETTINGS[key]
        existing = await db.get(SystemSetting, key)
        if existing:
            existing.value = value
        else:
            db.add(SystemSetting(
                key=key, value=value, is_secret=is_secret, description=desc,
            ))
        updated.append(key)

    await db.flush()

    # Reload settings into the running config
    await _apply_settings_to_config(db)

    return {"updated": updated, "message": f"Updated {len(updated)} settings."}


async def _apply_settings_to_config(db: AsyncSession):
    """Push DB settings into the running Settings singleton.

    This allows settings changed via the GUI to take effect without
    a container restart.
    """
    from cerulean.core.config import get_settings
    settings = get_settings()

    result = await db.execute(select(SystemSetting))
    for row in result.scalars().all():
        if hasattr(settings, row.key) and row.value:
            try:
                current = getattr(settings, row.key)
                # Cast to the correct type
                if isinstance(current, int):
                    setattr(settings, row.key, int(row.value))
                elif isinstance(current, float):
                    setattr(settings, row.key, float(row.value))
                elif isinstance(current, bool):
                    setattr(settings, row.key, row.value.lower() in ("true", "1", "yes"))
                else:
                    setattr(settings, row.key, row.value)
            except (ValueError, TypeError):
                pass

    # Re-register the Google OAuth client with new credentials
    if settings.google_client_id:
        from cerulean.core.auth import oauth
        oauth.register(
            name="google",
            client_id=settings.google_client_id,
            client_secret=settings.google_client_secret,
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
            overwrite=True,
        )


async def load_settings_from_db():
    """Called at startup to merge DB settings into the config singleton."""
    from cerulean.core.database import AsyncSessionLocal
    async with AsyncSessionLocal() as db:
        await _apply_settings_to_config(db)
