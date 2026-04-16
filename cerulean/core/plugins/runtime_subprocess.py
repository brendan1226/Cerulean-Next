"""
cerulean/core/plugins/runtime_subprocess.py
─────────────────────────────────────────────────────────────────────────────
Runtime adapter for plugins whose ``runtime: subprocess``.

Per cerulean_ai_spec §5 — we treat the plugin as a black-box executable
with a stable CLI contract:

    <entry> <...manifest.args with placeholders substituted>

Template placeholders:
    {input_path}    path to a temp file containing the input value,
                    one string per line (UTF-8).
    {output_path}   path where Cerulean will read the transformed
                    output from, one string per line.
    {config_json}   path to a JSON file containing {} for now — Phase B
                    will populate it with project metadata.

For transforms we invoke the subprocess once per transform call with a
single-line input file. This is fine for sub-second latency on small
files and avoids keeping a long-lived process. If that becomes a
bottleneck we can add a ``http`` runtime later.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import uuid
from pathlib import Path

from cerulean.core.plugins.extension_points import (
    PluginQualityCheck,
    PluginTransform,
    register_quality_check,
    register_transform,
)
from cerulean.core.plugins.manifest import (
    EXT_QUALITY_CHECK,
    EXT_TRANSFORM,
    PluginManifest,
)


class SubprocessRuntimeError(Exception):
    """Raised when a subprocess plugin invocation fails in a way that
    can't be gracefully degraded (e.g. executable missing). Per-call
    errors are surfaced as a returned error string instead of raising
    so the transform pipeline can fall through to the original value."""


# ── Registration ─────────────────────────────────────────────────────

def register_subprocess_plugin(plugin_path: Path, manifest: PluginManifest) -> None:
    """Validate the subprocess entry exists + register a thunk for each
    declared extension point that shells out on invocation."""
    plugin_root = plugin_path.resolve()
    entry_path = (plugin_root / manifest.entry).resolve()
    if not entry_path.is_file():
        raise SubprocessRuntimeError(
            f"subprocess entry {manifest.entry!r} not found at {entry_path}"
        )
    if not os.access(str(entry_path), os.X_OK):
        raise SubprocessRuntimeError(
            f"subprocess entry {manifest.entry!r} is not executable — "
            f"run `chmod +x {manifest.entry}` before packaging the plugin"
        )

    for ep in manifest.extension_points:
        if ep.type == EXT_TRANSFORM:
            register_transform(PluginTransform(
                plugin_slug=manifest.slug, key=ep.key,
                label=ep.label, description=ep.description,
                runtime=manifest.runtime,
                callable=_make_transform_thunk(plugin_root, manifest, ep.key),
            ))
        elif ep.type == EXT_QUALITY_CHECK:
            register_quality_check(PluginQualityCheck(
                plugin_slug=manifest.slug, check_id=ep.key,
                label=ep.label, description=ep.description,
                runtime=manifest.runtime,
                callable=_make_quality_check_thunk(plugin_root, manifest, ep.key),
            ))


def _make_transform_thunk(
    plugin_root: Path,
    manifest: PluginManifest,
    key: str,
):
    """Closure that applies the subprocess plugin to a single value."""
    def thunk(value: str, config: dict | None = None) -> str:
        return run_subprocess_transform(plugin_root, manifest, key, value, config or {})
    return thunk


def _make_quality_check_thunk(
    plugin_root: Path,
    manifest: PluginManifest,
    check_id: str,
):
    """Closure that invokes the subprocess plugin as a quality check."""
    def thunk(record_bytes: bytes, config: dict | None = None) -> list[dict]:
        return run_subprocess_quality_check(
            plugin_root, manifest, check_id, record_bytes, config or {},
        )
    return thunk


# ── Invocation ───────────────────────────────────────────────────────

def run_subprocess_transform(
    plugin_root: Path,
    manifest: PluginManifest,
    key: str,
    value: str,
    config: dict,
) -> str:
    """One round-trip: write the input file, run the executable, read
    the output file. Timeouts + non-zero exits fall back to the input
    value so a single bad plugin call never corrupts the pipeline."""
    with tempfile.TemporaryDirectory(prefix=f"cerulean-plugin-{manifest.slug}-") as tmp:
        tmp_dir = Path(tmp)
        input_path = tmp_dir / "input.txt"
        output_path = tmp_dir / "output.txt"
        config_path = tmp_dir / "config.json"

        input_path.write_text(value, encoding="utf-8")
        # Include the extension-point key so a single binary can serve
        # multiple transforms if it wants to.
        config_path.write_text(json.dumps({"key": key, **config}), encoding="utf-8")
        output_path.write_text("", encoding="utf-8")

        cmd = _build_command(plugin_root, manifest, input_path, output_path, config_path)
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(plugin_root),
                capture_output=True,
                text=True,
                timeout=manifest.timeout_sec,
                env=_minimal_env(),
                check=False,
            )
        except subprocess.TimeoutExpired:
            return value  # timeout → fall through, pipeline stays safe
        except FileNotFoundError:
            # Entry disappeared between register time and now — degrade.
            return value
        except Exception:
            return value

        if proc.returncode != 0:
            return value

        try:
            return output_path.read_text(encoding="utf-8")
        except Exception:
            return value


def run_subprocess_quality_check(
    plugin_root: Path,
    manifest: PluginManifest,
    check_id: str,
    record_bytes: bytes,
    config: dict,
) -> list[dict]:
    """Run a quality check. Input is raw MARC bytes (ISO 2709), output
    is expected to be a JSON array of issue dicts — same shape quality
    checks in the built-in scanner produce."""
    with tempfile.TemporaryDirectory(prefix=f"cerulean-plugin-{manifest.slug}-") as tmp:
        tmp_dir = Path(tmp)
        input_path = tmp_dir / "input.mrc"
        output_path = tmp_dir / "output.json"
        config_path = tmp_dir / "config.json"

        input_path.write_bytes(record_bytes)
        config_path.write_text(
            json.dumps({"check_id": check_id, **config}), encoding="utf-8",
        )
        output_path.write_text("[]", encoding="utf-8")

        cmd = _build_command(plugin_root, manifest, input_path, output_path, config_path)
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(plugin_root),
                capture_output=True,
                text=True,
                timeout=manifest.timeout_sec,
                env=_minimal_env(),
                check=False,
            )
        except Exception:
            return []

        if proc.returncode != 0:
            return []

        try:
            parsed = json.loads(output_path.read_text(encoding="utf-8"))
            if isinstance(parsed, list):
                return [i for i in parsed if isinstance(i, dict)]
        except Exception:
            pass
        return []


# ── Helpers ──────────────────────────────────────────────────────────

def _build_command(
    plugin_root: Path,
    manifest: PluginManifest,
    input_path: Path,
    output_path: Path,
    config_path: Path,
) -> list[str]:
    """Substitute placeholders in manifest.args and prepend the entry."""
    entry = str((plugin_root / manifest.entry).resolve())
    mapping = {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "config_json": str(config_path),
    }
    args: list[str] = []
    for a in manifest.args:
        for placeholder, real in mapping.items():
            a = a.replace("{" + placeholder + "}", real)
        args.append(a)
    return [entry, *args]


def _minimal_env() -> dict[str, str]:
    """Deliberately small env so plugins don't inherit secrets (Anthropic
    API keys, database URLs, Koha tokens). We give them just enough to
    find language interpreters via PATH."""
    return {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "LANG": os.environ.get("LANG", "en_US.UTF-8"),
        "LC_ALL": os.environ.get("LC_ALL", "C.UTF-8"),
        # Allow plugin authors to opt in to a single env var they need
        # without inheriting everything.
        "CERULEAN_PLUGIN": "1",
    }
