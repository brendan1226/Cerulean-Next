"""
cerulean/core/plugins/extension_points.py
─────────────────────────────────────────────────────────────────────────────
Global registries that plugin-contributed hooks land in at startup.

One registry per extension-point type. Each registered entry is
namespaced by plugin slug so two plugins can expose the same ``key``
without collision. The rest of Cerulean reads these registries to
surface plugin-provided transforms / checks alongside the built-ins.

Registries are process-local — they rebuild from scratch every time the
web / worker starts and the loader re-registers enabled plugins. That's
deliberate: no hot-reload means a restart is always required after
install/uninstall/upgrade, which keeps Celery workers consistent with
the web process.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

# ─── Transform registry ──────────────────────────────────────────────

@dataclass
class PluginTransform:
    """A transform hook contributed by a plugin.

    The runtime adapter is what actually executes the hook (Python
    in-process or subprocess invocation); the registry only cares about
    the plugin/key addressing and the display metadata.
    """
    plugin_slug: str
    key: str                     # preset_key from the manifest
    label: str
    description: str | None
    runtime: str                 # "python" | "subprocess"
    # Populated by the runtime registrar. For python plugins this is the
    # actual callable; for subprocess plugins it's a closure that wraps
    # the subprocess invocation so callers don't need to know the
    # difference.
    callable: Callable[[str, dict], str] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PluginQualityCheck:
    """A quality-check hook contributed by a plugin."""
    plugin_slug: str
    check_id: str
    label: str
    description: str | None
    runtime: str
    callable: Callable[..., list[dict]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


# Module-level registries. Keyed by "<plugin_slug>:<key>" to guarantee
# uniqueness across plugins. The leading prefix is how the transform
# pipeline will dispatch: preset_key "plugin:<slug>:<key>".

_TRANSFORM_REGISTRY: dict[str, PluginTransform] = {}
_QUALITY_CHECK_REGISTRY: dict[str, PluginQualityCheck] = {}


def _compose_key(slug: str, key: str) -> str:
    return f"{slug}:{key}"


# ─── Transform API ────────────────────────────────────────────────────

def register_transform(entry: PluginTransform) -> None:
    """Add a transform hook to the registry. Replaces any prior entry
    with the same ``(plugin_slug, key)`` — the loader uses this during
    reload so a re-enabled plugin picks up the new callable cleanly."""
    _TRANSFORM_REGISTRY[_compose_key(entry.plugin_slug, entry.key)] = entry


def get_transform(plugin_slug: str, key: str) -> PluginTransform | None:
    return _TRANSFORM_REGISTRY.get(_compose_key(plugin_slug, key))


def all_transforms() -> list[PluginTransform]:
    return list(_TRANSFORM_REGISTRY.values())


def unregister_plugin_transforms(plugin_slug: str) -> int:
    """Remove every transform contributed by ``plugin_slug``. Returns
    the number of entries dropped."""
    prefix = f"{plugin_slug}:"
    keys = [k for k in _TRANSFORM_REGISTRY if k.startswith(prefix)]
    for k in keys:
        _TRANSFORM_REGISTRY.pop(k, None)
    return len(keys)


# ─── Quality-check API ────────────────────────────────────────────────

def register_quality_check(entry: PluginQualityCheck) -> None:
    _QUALITY_CHECK_REGISTRY[_compose_key(entry.plugin_slug, entry.check_id)] = entry


def get_quality_check(plugin_slug: str, check_id: str) -> PluginQualityCheck | None:
    return _QUALITY_CHECK_REGISTRY.get(_compose_key(plugin_slug, check_id))


def all_quality_checks() -> list[PluginQualityCheck]:
    return list(_QUALITY_CHECK_REGISTRY.values())


def unregister_plugin_quality_checks(plugin_slug: str) -> int:
    prefix = f"{plugin_slug}:"
    keys = [k for k in _QUALITY_CHECK_REGISTRY if k.startswith(prefix)]
    for k in keys:
        _QUALITY_CHECK_REGISTRY.pop(k, None)
    return len(keys)


# ─── Reset (mostly for tests) ────────────────────────────────────────

def clear_all() -> None:
    """Forget every registered hook. Used by tests and by the loader
    when reloading the whole plugin set."""
    _TRANSFORM_REGISTRY.clear()
    _QUALITY_CHECK_REGISTRY.clear()
