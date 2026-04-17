"""
cerulean/core/plugins/__init__.py
─────────────────────────────────────────────────────────────────────────────
Public API for Cerulean's plugin system (.cpz).

This is the entry point the rest of Cerulean imports. Implementation
lives in sibling modules (manifest, loader, runtime_*, extension_points).
"""

from cerulean.core.plugins.extension_points import (
    PluginAPIEndpoint,
    PluginCeleryTask,
    PluginDBStore,
    PluginQualityCheck,
    PluginTransform,
    PluginUITab,
    all_api_endpoints,
    all_celery_tasks,
    all_db_stores,
    all_quality_checks,
    all_transforms,
    all_ui_tabs,
    clear_all,
    get_api_endpoint,
    get_celery_task,
    get_db_store,
    get_quality_check,
    get_transform,
    get_ui_tab,
    register_api_endpoint,
    register_celery_task,
    register_db_store,
    register_quality_check,
    register_transform,
    register_ui_tab,
    unregister_plugin_api_endpoints,
    unregister_plugin_celery_tasks,
    unregister_plugin_db_stores,
    unregister_plugin_quality_checks,
    unregister_plugin_transforms,
    unregister_plugin_ui_tabs,
)
from cerulean.core.plugins.manifest import (
    EXT_API_ENDPOINT,
    EXT_CELERY_TASK,
    EXT_DB_STORE,
    EXT_QUALITY_CHECK,
    EXT_TRANSFORM,
    EXT_UI_TAB,
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
    "EXT_CELERY_TASK",
    "EXT_API_ENDPOINT",
    "EXT_DB_STORE",
    "EXT_UI_TAB",
    "SUPPORTED_PERMISSIONS",
    # Extension points — Phase A
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
    # Extension points — Phase B
    "PluginCeleryTask",
    "PluginAPIEndpoint",
    "PluginDBStore",
    "register_celery_task",
    "register_api_endpoint",
    "register_db_store",
    "get_celery_task",
    "get_api_endpoint",
    "get_db_store",
    "all_celery_tasks",
    "all_api_endpoints",
    "all_db_stores",
    "unregister_plugin_celery_tasks",
    "unregister_plugin_api_endpoints",
    "unregister_plugin_db_stores",
    # Extension points — Phase C
    "PluginUITab",
    "register_ui_tab",
    "get_ui_tab",
    "all_ui_tabs",
    "unregister_plugin_ui_tabs",
    # Reset
    "clear_all",
]
