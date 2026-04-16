"""Unit tests for cerulean/core/plugins/extension_points.py."""

import pytest

from cerulean.core.plugins import (
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
