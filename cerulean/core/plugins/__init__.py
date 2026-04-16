"""
cerulean/core/plugins/__init__.py
─────────────────────────────────────────────────────────────────────────────
Public API for Cerulean's plugin system (.cpz).

This is the entry point the rest of Cerulean imports. Implementation
lives in sibling modules (manifest, loader, runtime_*, extension_points).
"""

from cerulean.core.plugins.extension_points import (
    PluginQualityCheck,
    PluginTransform,
    all_quality_checks,
    all_transforms,
    clear_all,
    get_quality_check,
    get_transform,
    register_quality_check,
    register_transform,
    unregister_plugin_quality_checks,
    unregister_plugin_transforms,
)
from cerulean.core.plugins.manifest import (
    EXT_QUALITY_CHECK,
    EXT_TRANSFORM,
    RUNTIME_PYTHON,
    RUNTIME_SUBPROCESS,
    SUPPORTED_PERMISSIONS,
    ManifestError,
    PluginManifest,
    load_manifest_file,
    parse_manifest,
)

__all__ = [
    # Manifest
    "PluginManifest",
    "ManifestError",
    "parse_manifest",
    "load_manifest_file",
    "RUNTIME_PYTHON",
    "RUNTIME_SUBPROCESS",
    "EXT_TRANSFORM",
    "EXT_QUALITY_CHECK",
    "SUPPORTED_PERMISSIONS",
    # Extension points
    "PluginTransform",
    "PluginQualityCheck",
    "register_transform",
    "register_quality_check",
    "get_transform",
    "get_quality_check",
    "all_transforms",
    "all_quality_checks",
    "unregister_plugin_transforms",
    "unregister_plugin_quality_checks",
    "clear_all",
]
