"""
cerulean/core/plugins/manifest.py
─────────────────────────────────────────────────────────────────────────────
Manifest schema + parser for Cerulean Plugin (.cpz) archives.

The manifest lives at ``manifest.yaml`` at the root of every .cpz.
It declares what the plugin is, how to run it, and which extension points
it contributes. Unknown keys are rejected so plugin authors get fast
feedback when they typo a field.

See docs/PLUGIN-AUTHORING.md for the human-readable reference.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SUPPORTED_MANIFEST_VERSIONS = {1}

RUNTIME_PYTHON = "python"
RUNTIME_SUBPROCESS = "subprocess"
SUPPORTED_RUNTIMES = {RUNTIME_PYTHON, RUNTIME_SUBPROCESS}

EXT_TRANSFORM = "transform"
EXT_QUALITY_CHECK = "quality_check"
EXT_CELERY_TASK = "celery_task"
EXT_API_ENDPOINT = "api_endpoint"
EXT_DB_STORE = "db_store"
SUPPORTED_EXTENSIONS = {
    EXT_TRANSFORM, EXT_QUALITY_CHECK,
    EXT_CELERY_TASK, EXT_API_ENDPOINT, EXT_DB_STORE,
}

SUPPORTED_PERMISSIONS = {
    "files:read",
    "files:write",
    "project:read",
    # Deliberately short list in Phase A — more land as Phase B opens up
    # richer context surfaces.
}

# Slug constraints: url-safe, lowercase, separates words with hyphens so
# the plugin name can become the last segment of a URL route cleanly.
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-]{1,62}[a-z0-9]$")


class ManifestError(Exception):
    """Raised for any manifest validation failure."""


class ExtensionPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal[
        "transform", "quality_check",
        "celery_task", "api_endpoint", "db_store",
    ]
    key: str = Field(min_length=1, max_length=100)
    label: str = Field(min_length=1, max_length=200)
    description: str | None = None
    # Python runtime: dotted path to the callable, e.g. ``mash.transforms:mash``
    # Subprocess runtime: optional, overrides the manifest-level entry/args
    handler: str | None = None
    # Phase B metadata — queue hint for celery_task, etc.
    metadata: dict[str, Any] = Field(default_factory=dict)


class PluginManifest(BaseModel):
    """Parsed, validated representation of ``manifest.yaml``.

    Plugins that fail validation never reach the registry — the loader
    short-circuits and records the error on the DB row.
    """

    model_config = ConfigDict(extra="forbid")

    manifest_version: int
    slug: str
    name: str
    version: str
    runtime: Literal["python", "subprocess"]

    author: str | None = None
    description: str | None = None

    # Python runtime: ``module:function`` (called once at load to register hooks)
    # Subprocess runtime: relative path to the executable from the plugin root
    entry: str

    # Subprocess-only — template strings substituted at invocation time:
    # {input_path}, {output_path}, {config_json}
    args: list[str] = Field(default_factory=list)
    timeout_sec: int = Field(default=300, ge=1, le=3600)

    permissions: list[str] = Field(default_factory=list)
    extension_points: list[ExtensionPoint] = Field(default_factory=list)

    @field_validator("manifest_version")
    @classmethod
    def _check_version(cls, v: int) -> int:
        if v not in SUPPORTED_MANIFEST_VERSIONS:
            raise ValueError(
                f"manifest_version {v} is not supported. "
                f"Supported: {sorted(SUPPORTED_MANIFEST_VERSIONS)}"
            )
        return v

    @field_validator("slug")
    @classmethod
    def _check_slug(cls, v: str) -> str:
        if not _SLUG_RE.match(v):
            raise ValueError(
                f"slug {v!r} is invalid. Must be 3–64 chars, lowercase a-z / 0-9 / "
                f"hyphens, starting + ending with an alphanumeric."
            )
        return v

    @field_validator("permissions")
    @classmethod
    def _check_permissions(cls, v: list[str]) -> list[str]:
        for p in v:
            if p not in SUPPORTED_PERMISSIONS:
                raise ValueError(
                    f"permission {p!r} is not recognised. "
                    f"Supported: {sorted(SUPPORTED_PERMISSIONS)}"
                )
        return v

    @field_validator("version")
    @classmethod
    def _check_version_string(cls, v: str) -> str:
        # Don't enforce strict semver — many plugin authors will use "0.1"
        # or "2026-04-16". Just check it's not obviously junk.
        if not v.strip():
            raise ValueError("version must be non-empty")
        if len(v) > 50:
            raise ValueError("version too long (max 50 chars)")
        return v.strip()

    @model_validator(mode="after")
    def _check_runtime_specific(self) -> "PluginManifest":
        if self.runtime == RUNTIME_PYTHON:
            # Python entry must be module:function
            if ":" not in self.entry:
                raise ValueError(
                    "python runtime: entry must be 'module:function' "
                    f"(got {self.entry!r})"
                )
            if self.args:
                raise ValueError(
                    "python runtime: 'args' is only valid for subprocess plugins"
                )
        elif self.runtime == RUNTIME_SUBPROCESS:
            # Subprocess entry must look like a relative path
            if self.entry.startswith("/"):
                raise ValueError(
                    "subprocess runtime: entry must be relative to the plugin "
                    f"root directory (got absolute path {self.entry!r})"
                )
            # Flag common template-string typos early so plugin authors
            # don't discover them only on first invocation.
            allowed_placeholders = {"{input_path}", "{output_path}", "{config_json}"}
            for a in self.args:
                placeholders = set(re.findall(r"\{[^}]+\}", a))
                unknown = placeholders - allowed_placeholders
                if unknown:
                    raise ValueError(
                        f"unknown placeholder(s) in args: {sorted(unknown)}. "
                        f"Allowed: {sorted(allowed_placeholders)}"
                    )
        # Extension keys unique within this manifest
        seen: set[tuple[str, str]] = set()
        for ep in self.extension_points:
            k = (ep.type, ep.key)
            if k in seen:
                raise ValueError(
                    f"duplicate extension point {ep.type}:{ep.key!r} in manifest"
                )
            seen.add(k)
            # Phase B: api_endpoint is Python-only (needs APIRouter object)
            if ep.type == EXT_API_ENDPOINT and self.runtime != RUNTIME_PYTHON:
                raise ValueError(
                    f"extension point {ep.type}:{ep.key!r} requires "
                    f"runtime 'python' (got {self.runtime!r})"
                )
            # Phase B: db_store is Python-only (needs SQLAlchemy model)
            if ep.type == EXT_DB_STORE and self.runtime != RUNTIME_PYTHON:
                raise ValueError(
                    f"extension point {ep.type}:{ep.key!r} requires "
                    f"runtime 'python' (got {self.runtime!r})"
                )
        return self


# ── Loading ───────────────────────────────────────────────────────────

def parse_manifest(raw: str | bytes | dict) -> PluginManifest:
    """Parse + validate manifest content. Raises :class:`ManifestError`.

    Accepts a YAML string/bytes or an already-loaded dict (for tests)."""
    if isinstance(raw, dict):
        data = raw
    else:
        try:
            data = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise ManifestError(f"manifest is not valid YAML: {exc}") from exc
        if not isinstance(data, dict):
            raise ManifestError("manifest must be a YAML mapping at the top level")
    try:
        return PluginManifest(**data)
    except Exception as exc:
        # Pydantic errors are already readable; wrap for a stable exception type.
        raise ManifestError(str(exc)) from exc


def load_manifest_file(path: Path | str) -> PluginManifest:
    """Read + parse a ``manifest.yaml`` from disk."""
    p = Path(path)
    if not p.is_file():
        raise ManifestError(f"manifest file not found: {path}")
    return parse_manifest(p.read_text(encoding="utf-8"))
