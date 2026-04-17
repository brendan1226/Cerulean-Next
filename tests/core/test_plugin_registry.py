"""Unit tests for cerulean/core/plugins/extension_points.py."""

import pytest

from cerulean.core.plugins import (
    PluginAPIEndpoint,
    PluginCeleryTask,
    PluginDBStore,
    PluginQualityCheck,
    PluginTransform,
    all_api_endpoints,
    all_celery_tasks,
    all_db_stores,
    all_quality_checks,
    all_transforms,
    clear_all,
    get_api_endpoint,
    get_celery_task,
    get_db_store,
    get_quality_check,
    get_transform,
    register_api_endpoint,
    register_celery_task,
    register_db_store,
    register_quality_check,
    register_transform,
    unregister_plugin_api_endpoints,
    unregister_plugin_celery_tasks,
    unregister_plugin_db_stores,
    unregister_plugin_quality_checks,
    unregister_plugin_transforms,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Isolate each test — the registries are module-level globals."""
    clear_all()
    yield
    clear_all()


# ── Transforms ──────────────────────────────────────────────────────

class TestTransformRegistry:
    def test_register_then_lookup(self):
        entry = PluginTransform(
            plugin_slug="bibliomasher", key="mash",
            label="Mash", description=None, runtime="python",
            callable=lambda v, c: v.upper(),
        )
        register_transform(entry)

        got = get_transform("bibliomasher", "mash")
        assert got is entry
        assert get_transform("bibliomasher", "not-a-key") is None
        assert get_transform("unknown-plugin", "mash") is None

    def test_lookup_returns_none_when_empty(self):
        assert get_transform("x", "y") is None
        assert all_transforms() == []

    def test_two_plugins_can_share_a_key(self):
        a = PluginTransform(plugin_slug="one", key="upper",
                            label="Upper", description=None, runtime="python")
        b = PluginTransform(plugin_slug="two", key="upper",
                            label="Upper", description=None, runtime="python")
        register_transform(a)
        register_transform(b)
        assert get_transform("one", "upper") is a
        assert get_transform("two", "upper") is b
        assert len(all_transforms()) == 2

    def test_re_register_replaces_same_key(self):
        v1 = PluginTransform(plugin_slug="p", key="k",
                             label="v1", description=None, runtime="python")
        v2 = PluginTransform(plugin_slug="p", key="k",
                             label="v2", description=None, runtime="python")
        register_transform(v1)
        register_transform(v2)
        assert len(all_transforms()) == 1
        assert get_transform("p", "k") is v2

    def test_unregister_plugin_removes_all_its_transforms(self):
        register_transform(PluginTransform(plugin_slug="a", key="x",
                                           label="X", description=None, runtime="python"))
        register_transform(PluginTransform(plugin_slug="a", key="y",
                                           label="Y", description=None, runtime="python"))
        register_transform(PluginTransform(plugin_slug="b", key="z",
                                           label="Z", description=None, runtime="python"))
        removed = unregister_plugin_transforms("a")
        assert removed == 2
        assert get_transform("a", "x") is None
        assert get_transform("a", "y") is None
        assert get_transform("b", "z") is not None

    def test_unregister_unknown_plugin_noop(self):
        register_transform(PluginTransform(plugin_slug="a", key="x",
                                           label="X", description=None, runtime="python"))
        assert unregister_plugin_transforms("nonexistent") == 0
        assert len(all_transforms()) == 1


# ── Quality checks ──────────────────────────────────────────────────

class TestQualityCheckRegistry:
    def test_register_then_lookup(self):
        qc = PluginQualityCheck(
            plugin_slug="bibliomasher", check_id="no-245",
            label="Missing 245", description=None, runtime="python",
        )
        register_quality_check(qc)
        assert get_quality_check("bibliomasher", "no-245") is qc

    def test_clear_all_resets_both_registries(self):
        register_transform(PluginTransform(plugin_slug="a", key="x",
                                           label="X", description=None, runtime="python"))
        register_quality_check(PluginQualityCheck(plugin_slug="a", check_id="y",
                                                   label="Y", description=None, runtime="python"))
        assert all_transforms() and all_quality_checks()
        clear_all()
        assert all_transforms() == []
        assert all_quality_checks() == []

    def test_unregister_plugin_quality_checks_is_scoped(self):
        register_quality_check(PluginQualityCheck(plugin_slug="a", check_id="x",
                                                   label="X", description=None, runtime="python"))
        register_quality_check(PluginQualityCheck(plugin_slug="b", check_id="y",
                                                   label="Y", description=None, runtime="python"))
        removed = unregister_plugin_quality_checks("a")
        assert removed == 1
        assert get_quality_check("b", "y") is not None


# ── Phase B: Celery tasks ─────────────────────────────────────────

class TestCeleryTaskRegistry:
    def test_register_then_lookup(self):
        entry = PluginCeleryTask(
            plugin_slug="my-plugin", key="my-task",
            label="My Task", description=None, runtime="python",
            callable=lambda c: {},
        )
        register_celery_task(entry)
        assert get_celery_task("my-plugin", "my-task") is entry
        assert get_celery_task("my-plugin", "other") is None

    def test_all_celery_tasks(self):
        register_celery_task(PluginCeleryTask(
            plugin_slug="a", key="x", label="X", description=None, runtime="python",
        ))
        register_celery_task(PluginCeleryTask(
            plugin_slug="b", key="y", label="Y", description=None, runtime="python",
        ))
        assert len(all_celery_tasks()) == 2

    def test_unregister_scoped(self):
        register_celery_task(PluginCeleryTask(
            plugin_slug="a", key="x", label="X", description=None, runtime="python",
        ))
        register_celery_task(PluginCeleryTask(
            plugin_slug="b", key="y", label="Y", description=None, runtime="python",
        ))
        assert unregister_plugin_celery_tasks("a") == 1
        assert len(all_celery_tasks()) == 1
        assert get_celery_task("b", "y") is not None


# ── Phase B: API endpoints ────────────────────────────────────────

class TestAPIEndpointRegistry:
    def test_register_then_lookup(self):
        entry = PluginAPIEndpoint(
            plugin_slug="my-plugin", key="routes",
            label="Routes", description=None, runtime="python",
            router="fake-router-obj",
        )
        register_api_endpoint(entry)
        assert get_api_endpoint("my-plugin", "routes") is entry
        assert get_api_endpoint("my-plugin", "other") is None

    def test_unregister_scoped(self):
        register_api_endpoint(PluginAPIEndpoint(
            plugin_slug="a", key="x", label="X", description=None, runtime="python",
        ))
        register_api_endpoint(PluginAPIEndpoint(
            plugin_slug="b", key="y", label="Y", description=None, runtime="python",
        ))
        assert unregister_plugin_api_endpoints("a") == 1
        assert len(all_api_endpoints()) == 1


# ── Phase B: DB stores ────────────────────────────────────────────

class TestDBStoreRegistry:
    def test_register_then_lookup(self):
        entry = PluginDBStore(
            plugin_slug="my-plugin", key="data",
            label="Data", description=None, model_class="FakeModel",
        )
        register_db_store(entry)
        assert get_db_store("my-plugin", "data") is entry
        assert get_db_store("my-plugin", "other") is None

    def test_unregister_scoped(self):
        register_db_store(PluginDBStore(
            plugin_slug="a", key="x", label="X", description=None,
        ))
        register_db_store(PluginDBStore(
            plugin_slug="b", key="y", label="Y", description=None,
        ))
        assert unregister_plugin_db_stores("a") == 1
        assert len(all_db_stores()) == 1


# ── Phase B: clear_all includes new registries ────────────────────

# ── Phase C: UI tabs ──────────────────────────────────────────────

from cerulean.core.plugins import (
    PluginUITab,
    all_ui_tabs,
    get_ui_tab,
    register_ui_tab,
    unregister_plugin_ui_tabs,
)

class TestUITabRegistry:
    def test_register_then_lookup(self):
        entry = PluginUITab(
            plugin_slug="my-plugin", key="metrics",
            label="Metrics", description=None, runtime="python",
            metadata={"context": ["stage:3"], "api_endpoint": "metrics-api"},
        )
        register_ui_tab(entry)
        assert get_ui_tab("my-plugin", "metrics") is entry
        assert get_ui_tab("my-plugin", "other") is None

    def test_all_ui_tabs(self):
        register_ui_tab(PluginUITab(
            plugin_slug="a", key="x", label="X", description=None, runtime="python",
        ))
        register_ui_tab(PluginUITab(
            plugin_slug="b", key="y", label="Y", description=None, runtime="python",
        ))
        assert len(all_ui_tabs()) == 2

    def test_unregister_scoped(self):
        register_ui_tab(PluginUITab(
            plugin_slug="a", key="x", label="X", description=None, runtime="python",
        ))
        register_ui_tab(PluginUITab(
            plugin_slug="b", key="y", label="Y", description=None, runtime="python",
        ))
        assert unregister_plugin_ui_tabs("a") == 1
        assert len(all_ui_tabs()) == 1
        assert get_ui_tab("b", "y") is not None


class TestClearAllComplete:
    def test_clear_all_wipes_all_six_registries(self):
        register_transform(PluginTransform(plugin_slug="a", key="x",
                                           label="X", description=None, runtime="python"))
        register_quality_check(PluginQualityCheck(plugin_slug="a", check_id="y",
                                                   label="Y", description=None, runtime="python"))
        register_celery_task(PluginCeleryTask(
            plugin_slug="a", key="t", label="T", description=None, runtime="python",
        ))
        register_api_endpoint(PluginAPIEndpoint(
            plugin_slug="a", key="e", label="E", description=None, runtime="python",
        ))
        register_db_store(PluginDBStore(
            plugin_slug="a", key="d", label="D", description=None,
        ))
        register_ui_tab(PluginUITab(
            plugin_slug="a", key="u", label="U", description=None, runtime="python",
        ))
        clear_all()
        assert all_transforms() == []
        assert all_quality_checks() == []
        assert all_celery_tasks() == []
        assert all_api_endpoints() == []
        assert all_db_stores() == []
        assert all_ui_tabs() == []
