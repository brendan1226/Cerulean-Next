"""
cerulean/core/plugins/context.py
─────────────────────────────────────────────────────────────────────────────
PluginContext — the stable surface plugin Python code imports.

Phase A keeps the context deliberately narrow: the extension-point
registrars (``register_transform``, ``register_quality_check``) plus a
few identification helpers. Phase B will open up file access, task
dispatch, and DB store. Plugins that only need transforms or quality
checks don't have to wait for Phase B to ship.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from cerulean.core.plugins.extension_points import (
    PluginQualityCheck,
    PluginTransform,
    register_quality_check,
    register_transform,
)
from cerulean.core.plugins.manifest import PluginManifest


@dataclass
class PluginContext:
    """Passed to a python plugin's ``setup(ctx)`` function at load time.

    The plugin registers its hooks through ``ctx.register_transform`` /
    ``ctx.register_quality_check`` rather than importing the module-
    level registrars directly — this lets us add cross-cutting logging,
    permission checks, or deprecation warnings later without every
    plugin needing to change its imports.
    """

    manifest: PluginManifest

    @property
    def slug(self) -> str:
        return self.manifest.slug

    @property
    def version(self) -> str:
        return self.manifest.version

    def register_transform(
        self,
        key: str,
        callable: Callable[[str, dict], str],
        label: str | None = None,
        description: str | None = None,
    ) -> None:
        """Register a transform hook. ``label`` defaults to whatever the
        manifest's extension_point entry declared for this key.

        The manifest is still the source of truth: a plugin that tries
        to register a key that isn't declared in its manifest raises —
        this keeps plugins honest about what they expose."""
        ep = self._find_extension("transform", key)
        if ep is None:
            raise KeyError(
                f"plugin {self.slug!r} tried to register transform {key!r} "
                f"which is not declared in its manifest.extension_points"
            )
        register_transform(PluginTransform(
            plugin_slug=self.slug,
            key=key,
            label=label or ep.label,
            description=description if description is not None else ep.description,
            runtime=self.manifest.runtime,
            callable=callable,
        ))

    def register_quality_check(
        self,
        check_id: str,
        callable: Callable[..., list[dict]],
        label: str | None = None,
        description: str | None = None,
    ) -> None:
        ep = self._find_extension("quality_check", check_id)
        if ep is None:
            raise KeyError(
                f"plugin {self.slug!r} tried to register quality_check "
                f"{check_id!r} which is not declared in its manifest"
            )
        register_quality_check(PluginQualityCheck(
            plugin_slug=self.slug,
            check_id=check_id,
            label=label or ep.label,
            description=description if description is not None else ep.description,
            runtime=self.manifest.runtime,
            callable=callable,
        ))

    def _find_extension(self, type_: str, key: str):
        for ep in self.manifest.extension_points:
            if ep.type == type_ and ep.key == key:
                return ep
        return None
