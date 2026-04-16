"""
cerulean/core/plugins/runtime_python.py
─────────────────────────────────────────────────────────────────────────────
In-process runtime for plugins whose ``runtime: python``.

Loading a python plugin means:
    1. Prepend its ``src/`` directory to sys.path so the plugin's module
       resolves without the plugin author needing to worry about package
       naming collisions.
    2. Import the module named in ``entry`` (left half of ``module:function``).
    3. Look up the function (right half).
    4. Call it with a ``PluginContext`` — the plugin registers its hooks
       from inside that call.

Plugins are trusted code. They share our event loop, DB connections,
and Celery workers. This is the same trust model Koha uses for its
Perl plugins.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path

from cerulean.core.plugins.context import PluginContext
from cerulean.core.plugins.manifest import PluginManifest


class PythonRuntimeError(Exception):
    """Raised when a python plugin fails to import or setup."""


def load_python_plugin(install_path: Path | str, manifest: PluginManifest) -> None:
    """Import the plugin and run its setup function.

    Any exception the plugin raises is wrapped in :class:`PythonRuntimeError`
    so the loader can mark the DB row as errored without crashing the
    host process.
    """
    plugin_root = Path(install_path).resolve()
    if not plugin_root.is_dir():
        raise PythonRuntimeError(f"plugin install_path not a directory: {plugin_root}")

    src_dir = plugin_root / "src"
    search_dir = src_dir if src_dir.is_dir() else plugin_root
    search_path = str(search_dir)

    if "://" in manifest.entry or not manifest.entry.count(":") == 1:
        raise PythonRuntimeError(
            f"python entry must be 'module:function', got {manifest.entry!r}"
        )
    module_name, function_name = manifest.entry.split(":", 1)

    # We prepend to sys.path so this plugin's module wins over any same-
    # named installed package, then pop it again once import is done so
    # later plugins don't accidentally resolve against this one's tree.
    sys.path.insert(0, search_path)
    try:
        # Evict any cached version so re-enabling a plugin picks up new
        # code on the next worker restart without manual bookkeeping.
        _evict_module(module_name)
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            raise PythonRuntimeError(
                f"failed to import {module_name!r} from {search_path}: {exc}"
            ) from exc

        setup = getattr(module, function_name, None)
        if not callable(setup):
            raise PythonRuntimeError(
                f"{module_name}.{function_name} is not callable"
            )

        ctx = PluginContext(manifest=manifest)
        try:
            setup(ctx)
        except Exception as exc:
            raise PythonRuntimeError(
                f"plugin setup raised: {exc}"
            ) from exc
    finally:
        # Remove the prepended path whether or not import succeeded
        try:
            sys.path.remove(search_path)
        except ValueError:
            pass


def _evict_module(module_name: str) -> None:
    """Drop a module (and its submodules) from sys.modules so the next
    import reloads from disk. Needed when a plugin is re-enabled after
    its source changed on disk."""
    to_drop = [
        name for name in sys.modules
        if name == module_name or name.startswith(module_name + ".")
    ]
    for name in to_drop:
        sys.modules.pop(name, None)
