"""End-to-end loader tests: drop a real plugin on disk, load it, verify
the hooks land in the registry and actually execute.

These also serve as the reference for how plugin authors should lay out
their packages — when anything in the loader contract changes, these
tests change with it so the docs stay honest.
"""

from pathlib import Path

import pytest

from cerulean.core.plugins import clear_all, get_transform
from cerulean.core.plugins.loader import (
    LoadResult,
    LoadSummary,
    load_all_enabled_plugins,
    load_plugin_dir,
)


@pytest.fixture(autouse=True)
def _clean():
    clear_all()
    yield
    clear_all()


# ── Helpers ──────────────────────────────────────────────────────────

PY_PLUGIN_SETUP = '''
def setup(ctx):
    def upper(value, config):
        return value.upper()
    ctx.register_transform("upper", upper)
'''


def _make_python_plugin(tmp_path: Path, slug: str = "upper-demo") -> Path:
    plugin_dir = tmp_path / "enabled" / slug
    (plugin_dir / "src").mkdir(parents=True)
    (plugin_dir / "manifest.yaml").write_text(f"""
manifest_version: 1
slug: {slug}
name: Upper Demo
version: '0.1'
runtime: python
entry: upper_demo.plugin:setup
extension_points:
  - type: transform
    key: upper
    label: Uppercase
""")
    # The module name in ``entry`` becomes ``upper_demo`` (underscores), so
    # the plugin's source file must match.
    (plugin_dir / "src" / "upper_demo").mkdir()
    (plugin_dir / "src" / "upper_demo" / "__init__.py").write_text("")
    (plugin_dir / "src" / "upper_demo" / "plugin.py").write_text(PY_PLUGIN_SETUP)
    return plugin_dir


def _make_broken_plugin(tmp_path: Path, slug: str = "broken") -> Path:
    plugin_dir = tmp_path / "enabled" / slug
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "manifest.yaml").write_text(f"""
manifest_version: 1
slug: {slug}
name: Broken
version: '0.1'
runtime: python
entry: broken_plugin.plugin:setup
""")
    (plugin_dir / "broken_plugin").mkdir()
    (plugin_dir / "broken_plugin" / "__init__.py").write_text("")
    (plugin_dir / "broken_plugin" / "plugin.py").write_text(
        "def setup(ctx):\n    raise RuntimeError('intentional boom')\n"
    )
    return plugin_dir


# ── Python runtime, happy path ───────────────────────────────────────

class TestPythonPluginLoad:
    def test_registers_transform(self, tmp_path):
        _make_python_plugin(tmp_path)
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert len(summary.results) == 1
        assert summary.results[0].ok, summary.results[0].error
        entry = get_transform("upper-demo", "upper")
        assert entry is not None
        # Hook is callable and produces the expected output
        assert entry.callable("hello", {}) == "HELLO"

    def test_load_summary_tracks_successes_and_failures(self, tmp_path):
        _make_python_plugin(tmp_path, slug="good")
        _make_broken_plugin(tmp_path, slug="bad")

        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert len(summary.results) == 2
        assert len(summary.successes) == 1
        assert len(summary.failures) == 1
        assert summary.failures[0].slug == "bad"
        assert "intentional boom" in (summary.failures[0].error or "")

    def test_broken_plugin_does_not_leak_registrations(self, tmp_path):
        # A plugin that partially registers then fails should NOT leave
        # dangling entries in the registry.
        bad_dir = tmp_path / "enabled" / "leaky"
        (bad_dir / "leaky" ).mkdir(parents=True)
        (bad_dir / "manifest.yaml").write_text("""
manifest_version: 1
slug: leaky
name: Leaky
version: '0.1'
runtime: python
entry: leaky.plugin:setup
extension_points:
  - type: transform
    key: ok
    label: OK
""")
        (bad_dir / "leaky" / "__init__.py").write_text("")
        (bad_dir / "leaky" / "plugin.py").write_text(
            "def setup(ctx):\n"
            "    ctx.register_transform('ok', lambda v, c: v)\n"
            "    raise RuntimeError('die after registering')\n"
        )
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert len(summary.failures) == 1
        # Registration was rolled back
        assert get_transform("leaky", "ok") is None


# ── Error paths ──────────────────────────────────────────────────────

class TestLoaderErrorPaths:
    def test_empty_enabled_dir(self, tmp_path):
        (tmp_path / "enabled").mkdir()
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert summary.results == []

    def test_missing_enabled_dir(self, tmp_path):
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "does-not-exist")
        assert summary.results == []

    def test_missing_manifest(self, tmp_path):
        plugin_dir = tmp_path / "enabled" / "no-manifest"
        plugin_dir.mkdir(parents=True)
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert len(summary.failures) == 1
        assert "manifest.yaml missing" in summary.failures[0].error

    def test_invalid_manifest(self, tmp_path):
        plugin_dir = tmp_path / "enabled" / "bad-manifest"
        plugin_dir.mkdir(parents=True)
        (plugin_dir / "manifest.yaml").write_text("manifest_version: 999")
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert len(summary.failures) == 1
        assert "invalid manifest" in summary.failures[0].error

    def test_plugin_that_registers_unknown_key_fails(self, tmp_path):
        plugin_dir = tmp_path / "enabled" / "undeclared"
        (plugin_dir / "undeclared_plugin").mkdir(parents=True)
        (plugin_dir / "manifest.yaml").write_text("""
manifest_version: 1
slug: undeclared
name: Undeclared
version: '0.1'
runtime: python
entry: undeclared_plugin.plugin:setup
extension_points:
  - type: transform
    key: declared
    label: Declared
""")
        (plugin_dir / "undeclared_plugin" / "__init__.py").write_text("")
        (plugin_dir / "undeclared_plugin" / "plugin.py").write_text(
            "def setup(ctx):\n"
            "    ctx.register_transform('different-key', lambda v, c: v)\n"
        )
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert len(summary.failures) == 1
        assert "not declared" in summary.failures[0].error


# ── Clearing behavior ────────────────────────────────────────────────

class TestClearFirst:
    def test_clear_first_true_resets_registry(self, tmp_path):
        # Load once
        _make_python_plugin(tmp_path, slug="first")
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert summary.successes

        # Rename the plugin on disk — second load should NOT still have the old entry
        (tmp_path / "enabled" / "first").rename(tmp_path / "enabled" / "renamed")
        # Rebuild manifest with new slug so it actually loads
        manifest_file = tmp_path / "enabled" / "renamed" / "manifest.yaml"
        manifest_file.write_text(manifest_file.read_text().replace("slug: first", "slug: renamed"))

        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled", clear_first=True)
        assert get_transform("first", "upper") is None
        assert get_transform("renamed", "upper") is not None
