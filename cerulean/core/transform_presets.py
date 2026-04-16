"""
cerulean/core/transform_presets.py
─────────────────────────────────────────────────────────────────────────────
Static registry of transform presets for field mapping.

Each preset is a pure function (value: str) -> str that performs a common
data transformation on MARC field values.
"""

import re
from html.parser import HTMLParser
from io import StringIO


# ══════════════════════════════════════════════════════════════════════════
# PRESET FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════


def _date_mdy_to_dmy(value: str) -> str:
    """MM/DD/YYYY → DD/MM/YYYY"""
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", value.strip())
    if m:
        return f"{m.group(2)}/{m.group(1)}/{m.group(3)}"
    return value


def _date_dmy_to_iso(value: str) -> str:
    """DD/MM/YYYY → YYYY-MM-DD"""
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", value.strip())
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return value


def _date_ymd_to_iso(value: str) -> str:
    """YYYYMMDD → YYYY-MM-DD"""
    m = re.match(r"(\d{4})(\d{2})(\d{2})", value.strip())
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return value


def _date_marc_to_iso(value: str) -> str:
    """Handle common MARC date patterns → YYYY-MM-DD.

    Supports: YYYYMMDD, YYYY-MM-DD (passthrough), YYYY (year only).
    """
    v = value.strip()
    # Already ISO
    if re.match(r"\d{4}-\d{2}-\d{2}$", v):
        return v
    # YYYYMMDD
    m = re.match(r"(\d{4})(\d{2})(\d{2})", v)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Year only
    m = re.match(r"(\d{4})$", v)
    if m:
        return f"{m.group(1)}-01-01"
    return value


def _case_upper(value: str) -> str:
    return value.upper()


def _case_lower(value: str) -> str:
    return value.lower()


def _case_title(value: str) -> str:
    return value.title()


def _case_sentence(value: str) -> str:
    if not value:
        return value
    return value[0].upper() + value[1:]


def _text_trim(value: str) -> str:
    return value.strip()


def _text_normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _text_strip_punctuation(value: str) -> str:
    return re.sub(r"[.,:;/]+$", "", value).strip()


def _clean_isbn(value: str) -> str:
    """Remove hyphens, spaces, and trailing qualifiers like (pbk.)."""
    v = re.sub(r"\s*\(.*?\)\s*$", "", value)
    v = re.sub(r"[\s\-]", "", v)
    return v.strip()


def _clean_lccn(value: str) -> str:
    """Normalize LCCN: strip spaces, ensure consistent format."""
    v = re.sub(r"\s+", "", value.strip())
    # Remove any trailing slashes or revision info
    v = re.sub(r"/.*$", "", v)
    return v


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._fed: list[str] = []

    def handle_data(self, d: str) -> None:
        self._fed.append(d)

    def get_data(self) -> str:
        return "".join(self._fed)


def _clean_html(value: str) -> str:
    """Strip HTML tags from a value."""
    s = _HTMLStripper()
    s.feed(value)
    return s.get_data()


def _extract_year(value: str) -> str:
    """Pull first 4-digit year from any format."""
    m = re.search(r"\b(\d{4})\b", value)
    return m.group(1) if m else value


def _extract_isbn13(value: str) -> str:
    """Normalize to ISBN-13 if possible."""
    cleaned = re.sub(r"[\s\-]", "", value)
    cleaned = re.sub(r"\s*\(.*?\)\s*$", "", cleaned)
    # Already 13 digits
    if re.match(r"\d{13}$", cleaned):
        return cleaned
    # Convert ISBN-10 to ISBN-13
    if re.match(r"\d{9}[\dXx]$", cleaned):
        isbn10 = cleaned[:9]
        prefix = "978" + isbn10
        # Calculate check digit
        total = sum(
            int(d) * (1 if i % 2 == 0 else 3)
            for i, d in enumerate(prefix)
        )
        check = (10 - (total % 10)) % 10
        return prefix + str(check)
    return value


def _const_value(value: str) -> str:
    """Placeholder — actual constant value comes from transform_fn field."""
    return value


# ══════════════════════════════════════════════════════════════════════════
# PRESET REGISTRY
# ══════════════════════════════════════════════════════════════════════════

PRESETS: list[dict] = [
    # Date transforms
    {
        "key": "date_mdy_to_dmy",
        "label": "MM/DD/YYYY \u2192 DD/MM/YYYY",
        "category": "date",
        "description": "US to EU date format",
        "fn": _date_mdy_to_dmy,
    },
    {
        "key": "date_dmy_to_iso",
        "label": "DD/MM/YYYY \u2192 YYYY-MM-DD",
        "category": "date",
        "description": "Convert to ISO 8601",
        "fn": _date_dmy_to_iso,
    },
    {
        "key": "date_ymd_to_iso",
        "label": "YYYYMMDD \u2192 YYYY-MM-DD",
        "category": "date",
        "description": "MARC date to ISO",
        "fn": _date_ymd_to_iso,
    },
    {
        "key": "date_marc_to_iso",
        "label": "MARC date \u2192 ISO 8601",
        "category": "date",
        "description": "Handles common MARC date patterns",
        "fn": _date_marc_to_iso,
    },
    # Case transforms
    {
        "key": "case_upper",
        "label": "UPPERCASE",
        "category": "case",
        "description": "Convert to uppercase",
        "fn": _case_upper,
    },
    {
        "key": "case_lower",
        "label": "lowercase",
        "category": "case",
        "description": "Convert to lowercase",
        "fn": _case_lower,
    },
    {
        "key": "case_title",
        "label": "Title Case",
        "category": "case",
        "description": "Capitalize first letter of each word",
        "fn": _case_title,
    },
    {
        "key": "case_sentence",
        "label": "Sentence case",
        "category": "case",
        "description": "Capitalize first letter only",
        "fn": _case_sentence,
    },
    # Text transforms
    {
        "key": "text_trim",
        "label": "Trim whitespace",
        "category": "text",
        "description": "Strip leading/trailing whitespace",
        "fn": _text_trim,
    },
    {
        "key": "text_normalize_spaces",
        "label": "Normalize spaces",
        "category": "text",
        "description": "Collapse multiple spaces to one",
        "fn": _text_normalize_spaces,
    },
    {
        "key": "text_strip_punctuation",
        "label": "Strip trailing punctuation",
        "category": "text",
        "description": "Remove trailing .,:;/",
        "fn": _text_strip_punctuation,
    },
    # Clean transforms
    {
        "key": "clean_isbn",
        "label": "Clean ISBN",
        "category": "clean",
        "description": "Remove hyphens, spaces, qualifiers",
        "fn": _clean_isbn,
    },
    {
        "key": "clean_lccn",
        "label": "Clean LCCN",
        "category": "clean",
        "description": "Normalize LCCN format",
        "fn": _clean_lccn,
    },
    {
        "key": "clean_html",
        "label": "Strip HTML tags",
        "category": "clean",
        "description": "Remove HTML markup",
        "fn": _clean_html,
    },
    # Extract transforms
    {
        "key": "extract_year",
        "label": "Extract year",
        "category": "extract",
        "description": "Pull 4-digit year from any format",
        "fn": _extract_year,
    },
    {
        "key": "extract_isbn13",
        "label": "Extract ISBN-13",
        "category": "extract",
        "description": "Normalize to ISBN-13",
        "fn": _extract_isbn13,
    },
    # Constant
    {
        "key": "const_value",
        "label": "Set constant value",
        "category": "const",
        "description": "Replace with a fixed string (user-provided via transform_fn)",
        "fn": _const_value,
    },
]

# Lookup by key for fast access
_PRESET_MAP: dict[str, dict] = {p["key"]: p for p in PRESETS}


def apply_preset(key: str, value: str) -> str:
    """Apply a preset transform by key. Returns original value if key not found.

    Plugin-contributed presets use the composite key
    ``plugin:<slug>:<ep_key>`` — they're dispatched through the plugin
    runtime rather than the built-in preset map.
    """
    if key.startswith("plugin:"):
        return _apply_plugin_preset(key, value)
    preset = _PRESET_MAP.get(key)
    if not preset:
        return value
    return preset["fn"](value)


def _apply_plugin_preset(key: str, value: str) -> str:
    """``plugin:<slug>:<ep_key>`` → plugin transform registry lookup."""
    # Strip the "plugin:" prefix once; the rest is "<slug>:<ep_key>".
    remainder = key[len("plugin:"):]
    if ":" not in remainder:
        return value
    slug, ep_key = remainder.split(":", 1)
    try:
        from cerulean.core.plugins import get_transform
        entry = get_transform(slug, ep_key)
        if entry is None or entry.callable is None:
            return value
        result = entry.callable(value, {})
        return value if result is None else str(result)
    except Exception:
        # Fall through on any failure — transforms are advisory, a
        # single bad plugin call must never corrupt the pipeline.
        return value


def get_preset_registry() -> list[dict]:
    """Return the preset list without function references (JSON-serializable).

    Includes plugin-contributed transforms (cerulean_ai_spec §5 plugin
    system) alongside built-ins so the Step 5 dropdown shows them in
    the ``Plugin`` category with ``key`` prefixed by ``plugin:<slug>:``.
    """
    registry = [
        {
            "key": p["key"],
            "label": p["label"],
            "category": p["category"],
            "description": p["description"],
        }
        for p in PRESETS
    ]
    # Late import to avoid a circular dep (plugins → extension_points)
    try:
        from cerulean.core.plugins import all_transforms as _all_plugin_transforms
        for t in _all_plugin_transforms():
            registry.append({
                # Composite key — the frontend encodes this as
                # "preset:plugin:<slug>:<key>" in the unified transform
                # dropdown, and the transform pipeline maps it to
                # transform_type="plugin" with transform_fn="<slug>:<key>".
                "key": f"plugin:{t.plugin_slug}:{t.key}",
                "label": f"{t.label}  ({t.plugin_slug})",
                "category": "plugin",
                "description": (t.description or ""),
                "plugin_slug": t.plugin_slug,
                "runtime": t.runtime,
            })
    except Exception:
        # If the plugin system isn't importable yet (e.g. on first-run
        # migrations) we just return the built-ins — never fail the
        # endpoint over an optional extension surface.
        pass
    return registry
