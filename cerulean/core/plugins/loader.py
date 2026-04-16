"""
cerulean/core/plugins/loader.py
─────────────────────────────────────────────────────────────────────────────
Startup scan: discover every enabled plugin on disk and register its
hooks. One broken plugin does NOT take down the host process — errors
are collected and returned so the caller (app lifespan / worker init)
can log them and update the DB row status.

Called from:
    * cerulean/main.py lifespan  (web process)
    * cerulean/tasks/celery_app.py on worker start
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from cerulean.core.config import get_settings
from cerulean.core.plugins.extension_points import (
    clear_all,
    unregister_plugin_quality_checks,
    unregister_plugin_transforms,
)
from cerulean.core.plugins.manifest import (
    ManifestError,
    PluginManifest,
    RUNTIME_PYTHON,
    RUNTIME_SUBPROCESS,
    load_manifest_file,
)
from cerulean.core.plugins.runtime_python import (
    PythonRuntimeError,
    load_python_plugin,
)


@dataclass
class LoadResult:
    """Outcome of loading a single plugin. The caller uses this to
    update the ``cerulean_plugins`` row status + error_message."""
    slug: str | None
    install_path: str
    manifest: PluginManifest | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class LoadSummary:
    """Aggregate outcome of scanning the whole enabled/ directory."""
    results: list[LoadResult]

    @property
    def successes(self) -> list[LoadResult]:
        return [r for r in self.results if r.ok]

    @property
    def failures(self) -> list[LoadResult]:
        return [r for r in self.results if not r.ok]


def plugins_root() -> Path:
    """Absolute path to the plugin storage root. Derived from the
    configured ``DATA_ROOT`` so dev and prod stay consistent with other
    project data (raw MARC files, transforms, etc.)."""
    return Path(get_settings().data_root) / "plugins"


def enabled_dir() -> Path:
    return plugins_root() / "enabled"


def available_dir() -> Path:
    return plugins_root() / "available"


def disabled_dir() -> Path:
    return plugins_root() / "disabled"


def ensure_directories() -> None:
    """Create the plugin storage dirs on first use. Safe to call every
    startup — idempotent."""
    for d in (enabled_dir(), available_dir(), disabled_dir(),
              plugins_root() / "run"):
        d.mkdir(parents=True, exist_ok=True)


# ── The actual scan ──────────────────────────────────────────────────

def load_all_enabled_plugins(
    *,
    clear_first: bool = True,
    enabled_root: Path | None = None,
) -> LoadSummary:
    """Walk every sub-directory of enabled/, register each plugin's hooks,
    and return a summary the caller can log / persist.

    ``clear_first=True`` wipes the in-process registries first, which is
    the right behavior on fresh process start. Set False if you're doing
    a targeted reload of a single plugin — in that case call
    :func:`load_plugin_dir` directly.

    ``enabled_root`` overrides the default ``{DATA_ROOT}/plugins/enabled``
    location. Tests use this; production always uses the default.
    """
    root = enabled_root if enabled_root is not None else enabled_dir()
    if enabled_root is None:
        # Only attempt to create the standard dir tree for the default path.
        # Tests pass their own prepared directory so they don't need mkdir.
        try:
            ensure_directories()
        except OSError:
            # First startup on a fresh host where DATA_ROOT isn't writable
            # yet — log and carry on; no plugins can possibly be enabled
            # anyway.
            return LoadSummary(results=[])
    if clear_first:
        clear_all()

    results: list[LoadResult] = []
    if not root.is_dir():
        return LoadSummary(results=results)

    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("."):
            continue
        results.append(load_plugin_dir(entry))
    return LoadSummary(results=results)


def load_plugin_dir(plugin_path: Path) -> LoadResult:
    """Load a single plugin from an extracted directory."""
    manifest_path = plugin_path / "manifest.yaml"
    if not manifest_path.is_file():
        return LoadResult(
            slug=None, install_path=str(plugin_path), manifest=None,
            error=f"manifest.yaml missing at {manifest_path}",
        )

    try:
        manifest = load_manifest_file(manifest_path)
    except ManifestError as exc:
        return LoadResult(
            slug=None, install_path=str(plugin_path), manifest=None,
            error=f"invalid manifest: {exc}",
        )

    try:
        _invoke_runtime(plugin_path, manifest)
    except Exception as exc:
        # Clean up anything the plugin partially registered before it blew
        # up so the registries don't hold dangling hooks.
        unregister_plugin_transforms(manifest.slug)
        unregister_plugin_quality_checks(manifest.slug)
        return LoadResult(
            slug=manifest.slug, install_path=str(plugin_path),
            manifest=manifest, error=str(exc),
        )

    return LoadResult(
        slug=manifest.slug, install_path=str(plugin_path), manifest=manifest,
    )


def _invoke_runtime(plugin_path: Path, manifest: PluginManifest) -> None:
    """Dispatch to the correct runtime loader. Subprocess plugins don't
    have a load-time hook — their entry script is invoked per transform
    call — so we just validate the file exists and register a thunk."""
    if manifest.runtime == RUNTIME_PYTHON:
        load_python_plugin(plugin_path, manifest)
    elif manifest.runtime == RUNTIME_SUBPROCESS:
        # Import here to avoid a circular import with runtime_subprocess
        # which may pull in pymarc etc.
        from cerulean.core.plugins.runtime_subprocess import (
            register_subprocess_plugin,
        )
        register_subprocess_plugin(plugin_path, manifest)
    else:
        raise RuntimeError(f"unknown runtime: {manifest.runtime}")
