# Cerulean Plugin Authoring Guide

Cerulean plugins extend the platform with custom transforms and quality
checks. A plugin is a `.cpz` archive (just a zip) containing a manifest
and the plugin's code. Two runtimes are supported — **Python** (in-
process, tightest integration) and **Subprocess** (any language — Perl,
Node, Go, Ruby, compiled binaries — so long as it can read a file, write
a file, and exit 0).

> **Handing existing code to an AI agent?** See the sibling doc
> [PLUGIN-FROM-EXISTING-CODE.md](PLUGIN-FROM-EXISTING-CODE.md) — it's a
> prescriptive briefing (decision tree, copy-paste templates, packaging
> checklist, verification loop) you can paste into Claude Code / Cursor
> alongside the author's source files.

This guide covers the manifest format, both runtimes, and ships two
copy-paste starting points. See the full examples under
[`examples/plugins/`](../examples/plugins/).

> **Distinct from Koha plugins.** Cerulean's existing "Plugins" page
> manages `.kpz` files you push AT a Koha instance. **Cerulean Plugins**
> are `.cpz` files that extend Cerulean itself.

---

## TL;DR — The 10-line Python plugin

```
my-plugin/
├── manifest.yaml
└── src/
    └── myplugin/
        ├── __init__.py
        └── plugin.py
```

**`manifest.yaml`**
```yaml
manifest_version: 1
slug: myplugin
name: My Plugin
version: 0.1.0
runtime: python
entry: myplugin.plugin:setup
extension_points:
  - type: transform
    key: shout
    label: Shout (uppercase + !)
```

**`src/myplugin/plugin.py`**
```python
def setup(ctx):
    def shout(value, config):
        return (value or "").upper() + "!"
    ctx.register_transform("shout", shout)
```

**`src/myplugin/__init__.py`** is empty.

**Package it**: `cd my-plugin && zip -r ../myplugin-0.1.0.cpz .`
**Install**: upload on the **Cerulean Plugins** page, then
`docker compose restart web worker worker-push` on the host.
In Step 5 Field Mapping the transform dropdown shows **"Shout (myplugin)"**
under a **Plugin** category.

---

## Manifest reference

| Field | Required | Notes |
|-------|----------|-------|
| `manifest_version` | ✓ | Currently `1`. Any other value is rejected. |
| `slug` | ✓ | URL-safe, 3–64 chars, lowercase a-z / 0-9 / hyphens. Must be unique across installed plugins — uploading a new version of the same slug replaces the previous install. |
| `name` | ✓ | Human-readable name shown in the UI. |
| `version` | ✓ | Free-form string up to 50 chars (`"0.1"`, `"2026-04-16"`, `"1.0.0-rc1"` all fine). |
| `runtime` | ✓ | `python` or `subprocess`. |
| `entry` | ✓ | For `python`: `module:function` (dotted Python path). For `subprocess`: a relative path to the executable inside the archive. |
| `args` | — | Subprocess only. List of CLI arguments. Placeholders: `{input_path}`, `{output_path}`, `{config_json}`. |
| `timeout_sec` | — | Subprocess only. Default `300`, max `3600`. Each invocation is killed if it exceeds. |
| `author` | — | String, shown in the UI. |
| `description` | — | Short summary, shown in the UI. |
| `permissions` | — | List of strings. Phase A: `files:read`, `files:write`, `project:read`. |
| `extension_points` | — | List of `{type, key, label, description?}`. See below. |

### Extension points

Every hook your plugin exposes must be declared in `extension_points` —
the registration code must match what the manifest says it will register.
Mismatches fail the plugin load with a clear error.

| `type` | What it hooks into | Input to the callable | Expected output |
|--------|-------------------|-----------------------|-----------------|
| `transform` | Step 5 Field Mapping — transform dropdown | `(value: str, config: dict)` | `str` (returned as the transformed value) |
| `quality_check` | Step 3 Quality scanner | `(record_bytes: bytes, config: dict)` (raw MARC record bytes) | `list[dict]` (issue objects) |

### Permissions (informational in Phase A)

Declared permissions are recorded in the DB and shown to admins at
install time. Phase A does not enforce them in-process — the plugin is
trusted code inside a trusted-user install flow. Phase B adds
enforcement hooks to the `PluginContext` when we open up richer file /
DB / task access.

---

## Python runtime

### What you get

- Your plugin is imported **in-process** at web / worker startup.
- Your `setup(ctx)` function is called once with a `PluginContext`.
- You register hooks through `ctx.register_transform(...)` and
  `ctx.register_quality_check(...)` — never by importing the registry
  module directly (we may add deprecation warnings and permission
  checks at that layer later).

### `PluginContext` API (Phase A)

```python
ctx.slug              # str — your plugin's slug
ctx.version           # str — from the manifest
ctx.manifest          # PluginManifest — the parsed manifest
ctx.register_transform(key, callable, label=None, description=None)
ctx.register_quality_check(check_id, callable, label=None, description=None)
```

Phase B opens up file access, Celery task dispatch, DB store, and audit
logging on the same context.

### Callable contracts

Transform:
```python
def my_transform(value: str, config: dict) -> str:
    # value is the current subfield string
    # config is a dict of options (empty in Phase A)
    return transformed
```

Quality check:
```python
import pymarc, io

def my_check(record_bytes: bytes, config: dict) -> list[dict]:
    record = next(pymarc.MARCReader(io.BytesIO(record_bytes),
                                    to_unicode=True, force_utf8=True))
    issues = []
    if not record.get_fields("245"):
        issues.append({
            "severity": "error",
            "tag": "245",
            "description": "Bib record has no title (245 missing)",
        })
    return issues
```

Issue dict fields the scanner consumes:
`severity` (`error` / `warning` / `info`), `tag`, `subfield`,
`description`, `original_value`, `suggested_fix`.

### Imports and dependencies

Phase A plugins can `import` anything available in Cerulean's worker
Python environment. If you need a third-party package we don't already
have (`pymarc`, `pydantic`, etc.), bundle a pinned wheel inside your
archive and add the path to `sys.path` in your `setup()` — **or** open
an issue to request the package be added to the base image.

---

## Subprocess runtime

For any-language plugins. Cerulean invokes your executable once per
transform call with template-substituted args.

### The contract

- **Input**: Cerulean writes a file containing the input value at
  `{input_path}`. For transforms, a single UTF-8 string. For quality
  checks, raw ISO 2709 MARC bytes.
- **Config**: `{config_json}` is a JSON file containing at minimum
  `{"key": "<your_extension_key>"}` so one executable can serve multiple
  extension points.
- **Output**: you write the result to `{output_path}`. Transforms: a
  single UTF-8 string. Quality checks: a JSON array of issue dicts.
- **Exit code**: `0` for success, anything else falls back to the input
  unchanged (never raises in the pipeline).
- **Timeout**: configurable via `timeout_sec`; defaults to 300 seconds.
  Exceeding the timeout also falls back to the input.
- **Environment**: you get a minimal `env` (PATH, LANG, LC_ALL,
  `CERULEAN_PLUGIN=1`). Host secrets (Anthropic API keys, DB URLs, Koha
  tokens) are **not** leaked.
- **Working directory**: the plugin root inside `enabled/<slug>/`, so
  `./mash.pl` and relative paths in the manifest resolve predictably.

### Example — Perl bibliomasher

**`manifest.yaml`**
```yaml
manifest_version: 1
slug: bibliomasher
name: Bibliomasher
version: 0.1.0
runtime: subprocess
entry: ./mash.pl
args: ["--in", "{input_path}", "--out", "{output_path}", "--config", "{config_json}"]
timeout_sec: 60
extension_points:
  - type: transform
    key: mash
    label: Bibliomasher
```

**`mash.pl`** (don't forget `chmod +x mash.pl` before packaging)
```perl
#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;

my ($in, $out, $config);
GetOptions("in=s" => \$in, "out=s" => \$out, "config=s" => \$config) or die;

open my $fh, "<:utf8", $in or die "open $in: $!";
my $value = do { local $/; <$fh> };
close $fh;

# ... your mashing logic ...
$value =~ s/\s+$//;
$value = uc $value;

open my $out_fh, ">:utf8", $out or die "open $out: $!";
print $out_fh $value;
close $out_fh;
```

Zip + upload the same as a Python plugin.

---

## Installing, enabling, uninstalling

Via the **Cerulean Plugins** sidebar entry:

1. **Upload** a `.cpz` → Cerulean validates the manifest, extracts into
   `{DATA_ROOT}/plugins/enabled/<slug>/`, and stashes the archive in
   `available/` for rollback.
2. **Restart** `web` + `worker` + `worker-push` so each Python process
   picks up the new hooks. (No hot-reload — same model as Koha.)
3. **Disable** moves the extracted tree to `disabled/`; the hooks clear
   on the next restart.
4. **Uninstall** removes the extracted tree entirely. The archive stays
   in `available/` so you can re-install a known-good version later.

Every install / enable / disable / uninstall writes an AuditEvent
visible in the system log.

---

## Debugging tips

- **Plugin load errors** appear on the Cerulean Plugins page under the
  plugin row, and in the server log at startup. Tailing
  `/tmp/cerulean.log` is the fastest way to see them during iteration.
- **Transform not in the dropdown?** Confirm three things: (1) plugin
  status is `enabled` on the page, (2) the manifest declares the
  transform's key in `extension_points`, (3) your `setup()` actually
  calls `ctx.register_transform(...)` with the same key. A mismatch
  between manifest and code surfaces as a load error; missing from both
  means the dropdown simply doesn't include the plugin.
- **Subprocess plugin failing silently?** The pipeline falls back to
  the input on any non-zero exit. Run your executable by hand with the
  expected input file and print the stderr; the minimal env means
  missing interpreters (`perl`, `node`, etc.) show up immediately.
- **Testing locally** — Cerulean looks at `{DATA_ROOT}/plugins/enabled/`
  at startup. During development you can drop an un-zipped plugin
  directory straight into `enabled/` and restart, skipping the upload
  step.

---

## Security model

Plugins are **trusted code**. Cerulean's threat model for Phase A is:

- Any authenticated user can install a plugin (there is no separate
  admin role today — everyone in your allowed OAuth domain is staff).
- Python plugins share our process, DB connections, and Celery workers.
  We cannot sandbox them.
- Subprocess plugins run in their own process with a minimal env and
  a scoped working directory — still arbitrary executables, just
  isolated from Cerulean's memory.
- Every install / enable / disable / uninstall / registration writes
  an AuditEvent.

Before accepting a third-party plugin, **read the source and the
manifest permissions**. Same discipline you'd apply to a Koha `.kpz`.

---

## Phase B extension points

Phase B adds three extension-point types alongside the Phase A types
(`transform`, `quality_check`).

### `celery_task` — background jobs

Register a Celery task callable. Supported by both Python and subprocess
runtimes.

**Manifest:**

```yaml
extension_points:
  - type: celery_task
    key: reindex
    label: Reindex Records
    metadata:
      queue: default    # optional queue hint
```

**Python `setup()`:**

```python
from cerulean.tasks.celery_app import celery_app

def setup(ctx):
    @celery_app.task(bind=True, name=f"cerulean.plugins.{ctx.slug}.reindex")
    def reindex(self, project_id, **kwargs):
        # Your async work here
        return {"status": "done", "records": 42}

    ctx.register_celery_task("reindex", reindex, queue="default")
```

**Subprocess:** the entry binary receives a `config.json` with `{"key": "reindex", ...}` and writes a JSON result to `output.json`.

### `api_endpoint` — custom API routes (Python only)

Register a FastAPI `APIRouter`. Mounted at `/api/v1/plugins/<slug>/<key>`.

**Manifest:**

```yaml
extension_points:
  - type: api_endpoint
    key: stats
    label: Custom Stats
```

**Python `setup()`:**

```python
from fastapi import APIRouter

def setup(ctx):
    router = APIRouter()

    @router.get("/summary")
    async def summary():
        return {"plugin": ctx.slug, "message": "hello from plugin"}

    ctx.register_api_endpoint("stats", router)
```

After restart, the endpoint is at `GET /api/v1/plugins/<slug>/stats/summary`.

### `db_store` — plugin-owned database tables (Python only)

Register a SQLAlchemy declarative model. The table is auto-created at
startup via `create_all(checkfirst=True)`. Table name **MUST** use a
`cpz_<slug>_` prefix (hyphens in slug become underscores).

**Manifest:**

```yaml
extension_points:
  - type: db_store
    key: cache
    label: Plugin Cache
```

**Python `setup()`:**

```python
from sqlalchemy import Column, String, Integer, DateTime, func
from sqlalchemy.orm import DeclarativeBase

class PluginBase(DeclarativeBase):
    pass

class CacheEntry(PluginBase):
    __tablename__ = "cpz_my_plugin_cache"
    id = Column(String(36), primary_key=True)
    key = Column(String(200), nullable=False)
    value = Column(String(2000))
    created_at = Column(DateTime, server_default=func.now())

def setup(ctx):
    ctx.register_db_store("cache", CacheEntry)
```

To query from a Celery task, use the standard sync engine pattern:

```python
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from cerulean.core.config import get_settings

settings = get_settings()
engine = create_engine(settings.database_url.replace("+asyncpg", "+psycopg2"))

with Session(engine) as db:
    rows = db.execute(select(CacheEntry)).scalars().all()
```
