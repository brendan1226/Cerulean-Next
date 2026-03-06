"""
cerulean/core/config.py
─────────────────────────────────────────────────────────────────────────────
Application settings loaded from environment / .env file.
All settings are typed and validated at startup. No raw os.getenv() calls
should appear anywhere else in the codebase — import settings from here.
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
	extra="ignore",
    )

    # ── Database ──────────────────────────────────
    database_url: str = "postgresql+asyncpg://cerulean:cerulean@postgres:5432/cerulean"

    # ── Redis / Celery ────────────────────────────
    redis_url: str = "redis://redis:6379/0"
    celery_result_backend: str = "redis://redis:6379/1"

    # ── Security ──────────────────────────────────
    secret_key: str = "dev-secret-change-me"
    fernet_key: str = ""          # encrypted Koha tokens at rest
    jwt_algorithm: str = "HS256"
    jwt_expiry_minutes: int = 60

    # ── AI ────────────────────────────────────────
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-20250514"
    ai_max_sample_records: int = 50
    ai_min_confidence: float = 0.70

    # ── File storage ──────────────────────────────
    data_root: str = "/data/projects"
    s3_endpoint_url: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = "cerulean"

    # ── App behaviour ─────────────────────────────
    debug: bool = False
    log_level: str = "info"

    @property
    def use_s3(self) -> bool:
        return bool(self.s3_endpoint_url)


@lru_cache
def get_settings() -> Settings:
    """Return cached Settings instance. Use as FastAPI dependency."""
    return Settings()
