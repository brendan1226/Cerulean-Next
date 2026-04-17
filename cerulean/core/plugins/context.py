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
    PluginAPIEndpoint,
    PluginCeleryTask,
    PluginDBStore,
    PluginQualityCheck,
    PluginTransform,
    register_api_endpoint,
    register_celery_task,
    register_db_store,
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

    # ── Phase B extension point registrars ─────────────────────────────

    def register_celery_task(
        self,
        key: str,
        callable: Callable,
        label: str | None = None,
        description: str | None = None,
        queue: str = "default",
    ) -> None:
        """Register a Celery task hook.

        The callable should be a Celery task function (decorated with
        ``@celery_app.task``). The ``queue`` parameter is a routing hint
        stored in metadata — the worker must be configured to listen on it.
        """
        ep = self._find_extension("celery_task", key)
        if ep is None:
            raise KeyError(
                f"plugin {self.slug!r} tried to register celery_task {key!r} "
                f"which is not declared in its manifest"
            )
        register_celery_task(PluginCeleryTask(
            plugin_slug=self.slug,
            key=key,
            label=label or ep.label,
            description=description if description is not None else ep.description,
            runtime=self.manifest.runtime,
            callable=callable,
            metadata={"queue": queue, **(ep.metadata or {})},
        ))

    def register_api_endpoint(
        self,
        key: str,
        router,
        label: str | None = None,
        description: str | None = None,
    ) -> None:
        """Register an API router (Python-only).

        The router is mounted at ``/api/v1/plugins/<slug>/<key>`` during
        the lifespan startup. Requires a restart to take effect.
        """
        ep = self._find_extension("api_endpoint", key)
        if ep is None:
            raise KeyError(
                f"plugin {self.slug!r} tried to register api_endpoint {key!r} "
                f"which is not declared in its manifest"
            )
        register_api_endpoint(PluginAPIEndpoint(
            plugin_slug=self.slug,
            key=key,
            label=label or ep.label,
            description=description if description is not None else ep.description,
            runtime=self.manifest.runtime,
            router=router,
            metadata=ep.metadata or {},
        ))

    def register_db_store(
        self,
        key: str,
        model_class,
        label: str | None = None,
        description: str | None = None,
    ) -> None:
        """Register a DB model (Python-only).

        The model's table is auto-created at load time via
        ``metadata.create_all``. Table names MUST use a ``cpz_<slug>_``
        prefix to avoid collisions with core tables.
        """
        ep = self._find_extension("db_store", key)
        if ep is None:
            raise KeyError(
                f"plugin {self.slug!r} tried to register db_store {key!r} "
                f"which is not declared in its manifest"
            )
        table_name = getattr(model_class, "__tablename__", "")
        expected_prefix = f"cpz_{self.slug.replace('-', '_')}_"
        if not table_name.startswith(expected_prefix):
            raise ValueError(
                f"plugin {self.slug!r} db_store table {table_name!r} must "
                f"start with {expected_prefix!r} to avoid collisions"
            )
        register_db_store(PluginDBStore(
            plugin_slug=self.slug,
            key=key,
            label=label or ep.label,
            description=description if description is not None else ep.description,
            model_class=model_class,
            metadata=ep.metadata or {},
        ))

    def _find_extension(self, type_: str, key: str):
        for ep in self.manifest.extension_points:
            if ep.type == type_ and ep.key == key:
                return ep
        return None
