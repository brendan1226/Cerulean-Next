"""Unit tests for cerulean/core/plugins/manifest.py.

The manifest is the contract between plugin authors and Cerulean.
Every one of these tests exists to keep that contract stable — a
breakage here means shipping plugins suddenly stop loading.
"""

import pytest

from cerulean.core.plugins import (
    ManifestError,
    PluginManifest,
    parse_manifest,
)


# ── Fixtures ─────────────────────────────────────────────────────────

def _python_manifest(**overrides):
    base = {
        "manifest_version": 1,
        "slug": "demo",
        "name": "Demo",
        "version": "0.1.0",
        "runtime": "python",
        "entry": "demo.plugin:setup",
    }
    base.update(overrides)
    return base


def _subprocess_manifest(**overrides):
    base = {
        "manifest_version": 1,
        "slug": "demo",
        "name": "Demo",
        "version": "0.1.0",
        "runtime": "subprocess",
        "entry": "./run.sh",
        "args": ["--in", "{input_path}", "--out", "{output_path}"],
    }
    base.update(overrides)
    return base


# ── Happy path ───────────────────────────────────────────────────────

class TestHappyPath:
    def test_python_runtime_parses(self):
        m = parse_manifest(_python_manifest())
        assert isinstance(m, PluginManifest)
        assert m.slug == "demo"
        assert m.runtime == "python"

    def test_subprocess_runtime_parses(self):
        m = parse_manifest(_subprocess_manifest())
        assert m.runtime == "subprocess"
        assert m.args[0] == "--in"

    def test_parses_yaml_string(self):
        raw = """
        manifest_version: 1
        slug: yaml-demo
        name: YAML Demo
        version: '1.0'
        runtime: python
        entry: yd.plugin:setup
        """
        m = parse_manifest(raw)
        assert m.slug == "yaml-demo"

    def test_extension_points_parse(self):
        m = parse_manifest(_python_manifest(extension_points=[
            {"type": "transform", "key": "upcase", "label": "Uppercase"},
            {"type": "quality_check", "key": "missing-245", "label": "Missing 245"},
        ]))
        assert len(m.extension_points) == 2
        assert m.extension_points[0].type == "transform"


# ── Validation: top-level fields ─────────────────────────────────────

class TestValidation:
    def test_rejects_unsupported_manifest_version(self):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(manifest_version=999))

    @pytest.mark.parametrize("slug", [
        "AB",                # too short and uppercase
        "a",                 # too short
        "-bad",              # starts with hyphen
        "bad-",              # ends with hyphen
        "Has_Underscore",    # underscores not allowed
        "spaces here",       # spaces
        "x" * 100,           # too long
    ])
    def test_rejects_bad_slug(self, slug):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(slug=slug))

    @pytest.mark.parametrize("slug", [
        "bib",
        "bibliomasher",
        "my-plugin",
        "plugin-v2",
        "a1-b2-c3",
    ])
    def test_accepts_good_slug(self, slug):
        m = parse_manifest(_python_manifest(slug=slug))
        assert m.slug == slug

    def test_rejects_unknown_runtime(self):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(runtime="ruby"))

    def test_rejects_unknown_permissions(self):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(permissions=["root:access"]))

    def test_accepts_known_permissions(self):
        m = parse_manifest(_python_manifest(permissions=["files:read", "files:write"]))
        assert "files:read" in m.permissions

    def test_rejects_extra_top_level_keys(self):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(unknown_field="anything"))

    def test_rejects_empty_version(self):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(version="   "))


# ── Validation: runtime-specific ─────────────────────────────────────

class TestRuntimeSpecific:
    def test_python_entry_must_be_module_function(self):
        with pytest.raises(ManifestError, match="module:function"):
            parse_manifest(_python_manifest(entry="no_colon"))

    def test_python_runtime_rejects_args(self):
        with pytest.raises(ManifestError, match="args"):
            parse_manifest(_python_manifest(args=["--something"]))

    def test_subprocess_entry_cannot_be_absolute(self):
        with pytest.raises(ManifestError, match="absolute"):
            parse_manifest(_subprocess_manifest(entry="/etc/passwd"))

    def test_subprocess_args_reject_unknown_placeholders(self):
        with pytest.raises(ManifestError, match="placeholder"):
            parse_manifest(_subprocess_manifest(args=["--host", "{hostname}"]))

    def test_subprocess_args_accept_known_placeholders(self):
        m = parse_manifest(_subprocess_manifest(args=[
            "--in", "{input_path}",
            "--out", "{output_path}",
            "--config", "{config_json}",
        ]))
        assert m.args.count("{input_path}") == 1

    def test_subprocess_accepts_empty_args(self):
        # Some plugins read from stdin / use fixed paths — no args is fine
        m = parse_manifest(_subprocess_manifest(args=[]))
        assert m.args == []

    def test_timeout_bounds_enforced(self):
        with pytest.raises(ManifestError):
            parse_manifest(_subprocess_manifest(timeout_sec=0))
        with pytest.raises(ManifestError):
            parse_manifest(_subprocess_manifest(timeout_sec=99999))


# ── Validation: extension points ─────────────────────────────────────

class TestExtensionPoints:
    def test_rejects_unknown_extension_type(self):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(extension_points=[
                {"type": "nothing", "key": "x", "label": "X"},
            ]))

    def test_rejects_duplicate_extension_keys(self):
        with pytest.raises(ManifestError, match="duplicate"):
            parse_manifest(_python_manifest(extension_points=[
                {"type": "transform", "key": "x", "label": "X"},
                {"type": "transform", "key": "x", "label": "X2"},
            ]))

    def test_same_key_different_type_is_ok(self):
        # Transform "x" and quality_check "x" should coexist
        m = parse_manifest(_python_manifest(extension_points=[
            {"type": "transform", "key": "x", "label": "X"},
            {"type": "quality_check", "key": "x", "label": "X check"},
        ]))
        assert len(m.extension_points) == 2

    def test_rejects_empty_key(self):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(extension_points=[
                {"type": "transform", "key": "", "label": "Empty"},
            ]))

    def test_extension_point_rejects_extra_fields(self):
        with pytest.raises(ManifestError):
            parse_manifest(_python_manifest(extension_points=[
                {"type": "transform", "key": "x", "label": "X", "mystery": 1},
            ]))


# ── Malformed YAML ───────────────────────────────────────────────────

class TestMalformedYaml:
    def test_rejects_non_yaml(self):
        with pytest.raises(ManifestError):
            parse_manifest(":\n:\n:bad")

    def test_rejects_top_level_list(self):
        with pytest.raises(ManifestError, match="mapping"):
            parse_manifest("- not\n- a\n- mapping")

    def test_rejects_empty_string(self):
        with pytest.raises(ManifestError):
            parse_manifest("")


# ── Phase B extension types ────────────────────────────────────────

class TestPhaseBExtensionTypes:
    def test_celery_task_python_accepted(self):
        m = parse_manifest(_python_manifest(extension_points=[
            {"type": "celery_task", "key": "my-task", "label": "My Task"},
        ]))
        assert m.extension_points[0].type == "celery_task"

    def test_celery_task_subprocess_accepted(self):
        m = parse_manifest(_subprocess_manifest(extension_points=[
            {"type": "celery_task", "key": "my-task", "label": "My Task"},
        ]))
        assert m.extension_points[0].type == "celery_task"

    def test_api_endpoint_python_accepted(self):
        m = parse_manifest(_python_manifest(extension_points=[
            {"type": "api_endpoint", "key": "routes", "label": "Routes"},
        ]))
        assert m.extension_points[0].type == "api_endpoint"

    def test_api_endpoint_subprocess_rejected(self):
        with pytest.raises(ManifestError, match="requires runtime 'python'"):
            parse_manifest(_subprocess_manifest(extension_points=[
                {"type": "api_endpoint", "key": "routes", "label": "Routes"},
            ]))

    def test_db_store_python_accepted(self):
        m = parse_manifest(_python_manifest(extension_points=[
            {"type": "db_store", "key": "data", "label": "Data"},
        ]))
        assert m.extension_points[0].type == "db_store"

    def test_db_store_subprocess_rejected(self):
        with pytest.raises(ManifestError, match="requires runtime 'python'"):
            parse_manifest(_subprocess_manifest(extension_points=[
                {"type": "db_store", "key": "data", "label": "Data"},
            ]))

    def test_celery_task_metadata_passthrough(self):
        m = parse_manifest(_python_manifest(extension_points=[
            {"type": "celery_task", "key": "job", "label": "Job",
             "metadata": {"queue": "analyze"}},
        ]))
        assert m.extension_points[0].metadata == {"queue": "analyze"}

    def test_mixed_phase_a_and_b(self):
        m = parse_manifest(_python_manifest(extension_points=[
            {"type": "transform", "key": "up", "label": "Upper"},
            {"type": "celery_task", "key": "job", "label": "Job"},
            {"type": "api_endpoint", "key": "api", "label": "API"},
        ]))
        assert len(m.extension_points) == 3
