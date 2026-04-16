"""Subprocess runtime tests — spawn a real shell script plugin to
exercise the full invocation contract.

These won't run on Windows (POSIX shell, chmod +x). That's fine for
now — production + CI run on Linux.
"""

import json
import os
import stat
import sys
from pathlib import Path

import pytest

from cerulean.core.plugins import clear_all, get_transform
from cerulean.core.plugins.loader import load_all_enabled_plugins


pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="subprocess runtime tests assume POSIX shell",
)


@pytest.fixture(autouse=True)
def _clean():
    clear_all()
    yield
    clear_all()


# ── Fixtures ─────────────────────────────────────────────────────────

UPPER_SCRIPT = """#!/bin/bash
set -eu
# Trivial shell plugin: reads input file, uppercases it, writes output.
IN=""
OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --in)  IN="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --config) shift 2 ;;
    *) shift ;;
  esac
done
tr '[:lower:]' '[:upper:]' < "$IN" > "$OUT"
"""

EXIT_ONE_SCRIPT = """#!/bin/bash
exit 1
"""

SLOW_SCRIPT = """#!/bin/bash
sleep 30
"""


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _make_subprocess_plugin(
    tmp_path: Path,
    slug: str = "upper-sh",
    script: str = UPPER_SCRIPT,
    timeout_sec: int = 30,
) -> Path:
    plugin_dir = tmp_path / "enabled" / slug
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "manifest.yaml").write_text(f"""
manifest_version: 1
slug: {slug}
name: Upper SH
version: '0.1'
runtime: subprocess
entry: ./run.sh
args: ["--in", "{{input_path}}", "--out", "{{output_path}}", "--config", "{{config_json}}"]
timeout_sec: {timeout_sec}
extension_points:
  - type: transform
    key: upper
    label: Uppercase (sh)
""")
    _write_executable(plugin_dir / "run.sh", script)
    return plugin_dir


# ── Happy path ───────────────────────────────────────────────────────

class TestSubprocessPlugin:
    def test_registers_and_invokes(self, tmp_path):
        _make_subprocess_plugin(tmp_path)
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert summary.failures == []

        entry = get_transform("upper-sh", "upper")
        assert entry is not None
        assert entry.runtime == "subprocess"
        assert entry.callable("hello world", {}) == "HELLO WORLD"

    def test_multiple_invocations_work_independently(self, tmp_path):
        _make_subprocess_plugin(tmp_path)
        load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        entry = get_transform("upper-sh", "upper")
        assert entry.callable("abc", {}) == "ABC"
        assert entry.callable("xyz", {}) == "XYZ"
        assert entry.callable("", {}) == ""


# ── Error resilience ─────────────────────────────────────────────────

class TestSubprocessFailureModes:
    def test_nonzero_exit_falls_back_to_input(self, tmp_path):
        _make_subprocess_plugin(tmp_path, slug="exiter", script=EXIT_ONE_SCRIPT)
        load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        entry = get_transform("exiter", "upper")
        # Exit 1 → transform returns the input unchanged; pipeline stays safe
        assert entry.callable("unchanged", {}) == "unchanged"

    def test_timeout_falls_back_to_input(self, tmp_path):
        _make_subprocess_plugin(
            tmp_path, slug="slowpoke", script=SLOW_SCRIPT, timeout_sec=1,
        )
        load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        entry = get_transform("slowpoke", "upper")
        # Timeout → returns the input string; not a raised exception
        assert entry.callable("stays", {}) == "stays"

    def test_missing_executable_fails_load(self, tmp_path):
        plugin_dir = tmp_path / "enabled" / "missing-bin"
        plugin_dir.mkdir(parents=True)
        (plugin_dir / "manifest.yaml").write_text("""
manifest_version: 1
slug: missing-bin
name: Missing Bin
version: '0.1'
runtime: subprocess
entry: ./does-not-exist.sh
args: []
extension_points:
  - type: transform
    key: x
    label: X
""")
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert len(summary.failures) == 1
        assert "not found" in summary.failures[0].error

    def test_non_executable_entry_fails_load(self, tmp_path):
        plugin_dir = tmp_path / "enabled" / "not-executable"
        plugin_dir.mkdir(parents=True)
        (plugin_dir / "manifest.yaml").write_text("""
manifest_version: 1
slug: not-executable
name: Not Exec
version: '0.1'
runtime: subprocess
entry: ./plain.sh
args: []
extension_points:
  - type: transform
    key: x
    label: X
""")
        # Write file WITHOUT chmod +x
        (plugin_dir / "plain.sh").write_text("#!/bin/bash\necho hi\n")
        summary = load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
        assert len(summary.failures) == 1
        assert "not executable" in summary.failures[0].error


# ── Environment isolation ────────────────────────────────────────────

class TestSubprocessEnvSafety:
    def test_secrets_not_leaked_into_plugin_env(self, tmp_path):
        """Verify our minimal env contract — ANTHROPIC_API_KEY etc.
        should not be visible inside the subprocess.

        We test by writing a plugin that dumps its env into the output
        file and checking the dangerous keys are absent.
        """
        script = """#!/bin/bash
set -e
IN=""; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --in) IN="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --config) shift 2 ;;
    *) shift ;;
  esac
done
env > "$OUT"
"""
        # Set a fake dangerous env var for the test
        os.environ["ANTHROPIC_API_KEY"] = "sk-test-should-not-leak"
        try:
            _make_subprocess_plugin(tmp_path, slug="env-dumper", script=script)
            load_all_enabled_plugins(enabled_root=tmp_path / "enabled")
            entry = get_transform("env-dumper", "upper")
            out = entry.callable("irrelevant", {})
        finally:
            os.environ.pop("ANTHROPIC_API_KEY", None)

        assert "ANTHROPIC_API_KEY" not in out
        assert "sk-test-should-not-leak" not in out
        # Sanity: the allowed CERULEAN_PLUGIN marker IS present
        assert "CERULEAN_PLUGIN=1" in out
