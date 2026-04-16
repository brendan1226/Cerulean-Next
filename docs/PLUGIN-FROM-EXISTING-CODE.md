# Wrapping Existing Code as a Cerulean Plugin — Agent Brief

**Audience:** an AI coding agent (Claude Code, Cursor, Copilot Workspace,
etc.) handed somebody's existing code (a Perl script, a Python module, a
compiled binary, a Node CLI, anything) with the task: **"Make this work
as a Cerulean plugin."**

You produce a `.cpz` archive that the user uploads on the **Cerulean
Plugins** page. Every plugin is a zip with one manifest and whatever
code the plugin needs.

Read this doc first, then use [PLUGIN-AUTHORING.md](PLUGIN-AUTHORING.md)
as the detailed spec reference when you need it.

---

## 0. What you should have from the user

Before starting, confirm you have:

1. **The code itself** — source files, a binary, or a directory.
2. **One-line description of what it does** — e.g. "strips trailing
   period and comma", "validates that 245$a matches a title format",
   "mashes bib records the BWS way".
3. **The input/output shape** — does it operate on a single string
   value (a MARC subfield)? On whole MARC records? Something else?

If any of those are unclear, ask the user before wrapping. Guessing the
I/O contract wrong means the plugin won't do what they expect.

---

## 1. Decision tree

### Pick the runtime

| If the code is… | Runtime |
|-----------------|---------|
| Pure Python (script, module, class) | `python` |
| Perl | `subprocess` |
| Node / JavaScript (CLI) | `subprocess` |
| Ruby | `subprocess` |
| Go / Rust / compiled binary | `subprocess` |
| Shell script | `subprocess` |
| Python that imports packages not in Cerulean's image (`nltk`, `tensorflow`, etc.) | `subprocess` (Cerulean's Python env is frozen) |

### Pick the extension point

| If the code operates on… | Extension type |
|--------------------------|----------------|
| A single string value (e.g. "cleans up a subfield") | `transform` |
| A whole MARC record, emitting a list of issues | `quality_check` |

If the code operates on whole files, batches of records, or wants to
talk to external services — Phase A's extension points don't fit;
tell the user and wait for Phase B (custom tasks + API endpoints) or
wrap it as a `transform` that calls out for each value (slow but works).

---

## 2. Wrapping templates

Pick the ONE template that matches your (runtime × extension point)
combo and copy it verbatim. Every `TODO:` marker is a spot where the
author's code gets dropped in.

### 2A — `python` + `transform`

**Directory layout:**
```
<slug>/
├── manifest.yaml
└── src/
    └── <slug_underscore>/
        ├── __init__.py           # empty file
        └── plugin.py
```

The slug goes in `manifest.yaml` with hyphens (e.g. `bibliomasher`);
the Python package name uses underscores (e.g. `bibliomasher`). They
must match after substituting `_` for `-`.

**`manifest.yaml`:**
```yaml
manifest_version: 1
slug: TODO-slug-here                    # url-safe, 3–64 chars, lowercase/digits/hyphens
name: TODO Human-Readable Name
version: TODO-0.1.0
author: TODO author name / email
description: TODO one-line description
runtime: python
entry: TODO_package:setup               # e.g. bibliomasher.plugin:setup
extension_points:
  - type: transform
    key: TODO-key                       # short identifier, e.g. "mash"
    label: TODO Label in the dropdown
    description: TODO what this transform does
```

**`src/<slug_underscore>/plugin.py`:**
```python
# TODO: if the author's code is importable as a module, import it here:
# from .authors_code import do_the_thing


def setup(ctx):
    """Register every hook declared in manifest.yaml.

    Cerulean calls this once at web / worker startup. The key passed to
    ctx.register_transform MUST match the `key` under extension_points
    in the manifest — mismatches fail the plugin load with a clear
    error visible on the Cerulean Plugins page.
    """

    def my_transform(value: str, config: dict) -> str:
        # TODO: replace with the author's logic.
        # Contract: receives a single string, returns a single string.
        # If the input is empty or None, return it unchanged — lots of
        # MARC subfields are empty and the transform pipeline would
        # rather pass them through than crash.
        if not value:
            return value
        # TODO: call into the author's code here
        return value  # placeholder

    ctx.register_transform("TODO-key", my_transform)
```

### 2B — `python` + `quality_check`

Same directory layout as 2A. Manifest has `type: quality_check` instead.

**`plugin.py`:**
```python
import io
import pymarc


def setup(ctx):
    def my_check(record_bytes: bytes, config: dict) -> list[dict]:
        """Return a list of issue dicts — one per problem found in this
        record. Empty list means the record is clean.

        Each issue dict can include:
            severity        "error" | "warning" | "info"
            tag             MARC tag, e.g. "245"
            subfield        subfield code, e.g. "a"
            description     one-sentence human summary
            original_value  optional, the value that triggered
            suggested_fix   optional, a proposed replacement string
        """
        record = next(pymarc.MARCReader(
            io.BytesIO(record_bytes),
            to_unicode=True, force_utf8=True, utf8_handling="replace",
        ))

        issues: list[dict] = []

        # TODO: replace with the author's logic. Example template —
        # flag records missing a 245 title field:
        if not record.get_fields("245"):
            issues.append({
                "severity": "error",
                "tag": "245",
                "description": "Bib record has no title (245 missing)",
            })

        return issues

    ctx.register_quality_check("TODO-key", my_check)
```

### 2C — `subprocess` + `transform`

**Directory layout:**
```
<slug>/
├── manifest.yaml
└── <entry_executable>          # e.g. mash.pl, mash.sh, mash (compiled binary)
```

**`manifest.yaml`:**
```yaml
manifest_version: 1
slug: TODO-slug-here
name: TODO Human-Readable Name
version: TODO-0.1.0
author: TODO
description: TODO
runtime: subprocess
entry: ./TODO-executable                # relative path — MUST start with `./` or be a bare filename
args: ["--in", "{input_path}", "--out", "{output_path}", "--config", "{config_json}"]
timeout_sec: 300                        # seconds per invocation; max 3600
extension_points:
  - type: transform
    key: TODO-key
    label: TODO Label in the dropdown
    description: TODO what this transform does
```

**Executable contract** (what the executable must do — adapt to the
author's language):
1. Parse `--in`, `--out`, `--config` from argv.
2. Read the single UTF-8 string value from the file at `--in`.
3. Apply the author's transform.
4. Write the transformed string as UTF-8 to the file at `--out`.
5. Exit `0` on success. Any non-zero exit, timeout, or missing output
   file causes Cerulean to pass the input through unchanged.

**Bash wrapper skeleton** (use this when the author's code is a
standalone executable that reads stdin and writes stdout — put it in
`run.sh` and chmod +x):
```bash
#!/usr/bin/env bash
set -eu
IN=""; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --in)     IN="$2";  shift 2 ;;
    --out)    OUT="$2"; shift 2 ;;
    --config) shift 2 ;;      # ignored in this wrapper
    *) shift ;;
  esac
done
# TODO: replace "the-authors-binary" with the real call
./the-authors-binary < "$IN" > "$OUT"
```

**Perl wrapper skeleton:**
```perl
#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;
my ($in, $out, $config);
GetOptions("in=s" => \$in, "out=s" => \$out, "config=s" => \$config) or die;
open my $fh, "<:encoding(UTF-8)", $in or die "open $in: $!";
my $value = do { local $/; <$fh> };
close $fh;

# TODO: replace with the author's logic — $value is the input string,
# assign the transformed string back to $value before writing.
# $value = ...author's code...;

open my $out_fh, ">:encoding(UTF-8)", $out or die "open $out: $!";
print $out_fh $value;
close $out_fh;
exit 0;
```

**Node wrapper skeleton** (`run.js`, chmod +x, with shebang
`#!/usr/bin/env node` and parse `process.argv`).

### 2D — `subprocess` + `quality_check`

Same manifest shape as 2C except:
- `extension_points[0].type: quality_check`
- Input file contains **raw ISO 2709 MARC bytes** (one record), not a string.
- Output file must contain a **JSON array of issue dicts** (see 2B for
  the shape). Empty array = no issues.

---

## 3. Packaging checklist

Before you hand the `.cpz` back:

- [ ] Manifest `slug` is url-safe, lowercase, 3–64 chars, hyphens OK,
      starts and ends with an alphanumeric.
- [ ] Python: package directory name matches `slug.replace("-", "_")`
      and `__init__.py` exists (even if empty).
- [ ] Python: `entry` is `package:function_name` (exactly one `:`).
- [ ] Python: `setup(ctx)` accepts exactly one argument named `ctx`.
- [ ] Python: every `register_*` key matches a `key` declared in the
      manifest's `extension_points`. Mismatches fail the plugin load.
- [ ] Subprocess: `entry` starts with `./` or is a bare filename
      (relative, NOT absolute).
- [ ] Subprocess: the entry file is **executable**
      (`chmod +x ./entry`). Archives preserve the exec bit.
- [ ] Subprocess: `args` only uses `{input_path}`, `{output_path}`,
      `{config_json}` placeholders — any other `{…}` string is
      rejected at install time.
- [ ] No absolute paths in the code or manifest.
- [ ] No attempts to read env vars that aren't in the allow-list
      (`PATH`, `LANG`, `LC_ALL`, `CERULEAN_PLUGIN`) — subprocess
      plugins run with a scrubbed environment.
- [ ] Dependencies: if the plugin needs Python packages that aren't
      in Cerulean's image (`requirements.txt`), switch to `subprocess`
      runtime with its own interpreter instead.
- [ ] `manifest.yaml` is at the **root** of the archive (not inside a
      wrapper directory — `zip -r foo.cpz <slug>/*` from the parent
      puts it one level too deep; `cd <slug> && zip -r ../foo.cpz .`
      is the correct invocation).

**Zip the archive:**
```bash
cd <slug>
zip -r ../<slug>-<version>.cpz .
```

---

## 4. Verification loop — confirm it actually loads

Before handing the `.cpz` to the user, **smoke-test it** from a Python
REPL or script. The loader is designed to be test-friendly — you can
drop an extracted plugin into a temp directory and invoke the real
registration path without touching the DB or a running server.

```python
import shutil, tempfile
from pathlib import Path

from cerulean.core.plugins import clear_all, get_transform, get_quality_check
from cerulean.core.plugins.loader import load_all_enabled_plugins

clear_all()
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    (tmp / "enabled").mkdir()
    # Extract the .cpz (or copy your source dir directly)
    shutil.copytree("<your plugin source dir>", tmp / "enabled" / "<slug>")

    summary = load_all_enabled_plugins(enabled_root=tmp / "enabled")
    print("Successes:", [r.slug for r in summary.successes])
    print("Failures:", [(r.slug, r.error) for r in summary.failures])
    assert summary.failures == [], "plugin failed to load"

    # For transforms — fetch and invoke
    entry = get_transform("<slug>", "<key>")
    assert entry is not None, "transform didn't register"
    print("Sample output:", entry.callable("hello world", {}))
```

If `summary.failures` is non-empty, the `error` string tells you what's
wrong:

| Error fragment | Likely cause | Fix |
|----------------|--------------|-----|
| `manifest.yaml missing` | Archive wasn't zipped from the right cwd | Re-zip with `cd <slug> && zip -r ../out.cpz .` |
| `invalid manifest` | YAML / pydantic validation failure | Read the rest of the error — it's pydantic output, very specific |
| `failed to import …` | `entry` module path wrong OR package name doesn't match slug | Check `src/<slug_underscore>/__init__.py` exists; confirm `entry: pkg:fn` |
| `not callable` | The function named in `entry` isn't defined | Confirm `def setup(ctx):` exists in the right file |
| `plugin setup raised: …` | The plugin's setup function threw | Read the wrapped exception; usually an import inside setup() |
| `not declared in its manifest` | Registered a key that isn't in `extension_points` | Either add the key to the manifest or rename the registered key |
| `subprocess entry … not found` | Entry file missing or path typo | Check manifest `entry` matches the filename at the plugin root |
| `is not executable` | Forgot `chmod +x` before zipping | `chmod +x <entry>`, re-zip |

Once the smoke test passes and `entry.callable("sample input", {})` /
`entry.callable(sample_record_bytes, {})` returns what you expect, the
`.cpz` is ready to hand off.

---

## 5. Things that are NOT possible in Phase A

Tell the user explicitly if the code they handed you needs any of
these — the plugin system will be extended in Phase B/C:

- Reading the project's other MARC files from inside the plugin.
- Dispatching new Celery background tasks.
- Adding new HTTP endpoints under `/api/v1/`.
- Creating custom sidebar tabs / UI pages.
- Persisting plugin-specific data in the DB.
- Calling Cerulean's own `anthropic` client (use the subprocess runtime
  and have the plugin carry its own API key in a config file — but
  flag this to the user as a data-sharing concern).

If the code needs any of the above, the right move today is to wrap
whatever sliver of it IS a pure transform or quality check, then flag
the remainder to the user as "Phase B work".

---

## 6. Handing off

When done, give the user:

1. The `.cpz` file (or a path to it).
2. The manifest's `slug` and `version`.
3. A one-line summary of what the plugin contributes (e.g. "registers
   one `transform:bibliomasher:mash` hook").
4. The smoke-test output from Section 4 so they can see it loaded
   cleanly on your end.
5. A reminder that after uploading, they need to run
   `docker compose restart web worker worker-push` — Cerulean does
   not hot-reload plugin code.

Keep the response tight — the user has the plugin and the restart
command, they don't need a lecture.
