"""
cerulean/core/features.py
─────────────────────────────────────────────────────────────────────────────
Central registry of per-user preference toggles.

The registry is the single source of truth for every toggleable preference
available to an authenticated user — AI features today, other granular
visibility/permission toggles tomorrow. The database table
``user_preferences`` (see models.UserPreference) stores only the user's
chosen value; defaults, descriptions, categories, and UI metadata all live
here so adding a new toggle is a one-line change with no schema churn.

Keys are dot-namespaced:
    ai.*          AI-assisted features (all default OFF — opt-in per spec)
    visibility.*  Per-user visibility / sharing controls

To add a new preference:
    1. Append a Feature entry below.
    2. Reference the key in your router via require_preference("your.key")
       or pref_enabled(user, "your.key").
    3. The settings UI picks it up automatically from the registry.
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ── Categories (grouping in the settings UI) ──────────────────────────────

CATEGORY_AI = "ai"
CATEGORY_VISIBILITY = "visibility"

CATEGORY_LABELS: dict[str, str] = {
    CATEGORY_AI: "AI-Assisted Features",
    CATEGORY_VISIBILITY: "Visibility & Sharing",
}

CATEGORY_DESCRIPTIONS: dict[str, str] = {
    CATEGORY_AI: (
        "AI features send a sample of your library's data to the Anthropic "
        "Claude API to generate suggestions. All features are off by default. "
        "Enable only what you need."
    ),
    CATEGORY_VISIBILITY: (
        "Control who can see your data within Cerulean Next."
    ),
}


# ── Value types ───────────────────────────────────────────────────────────

TYPE_BOOL = "bool"
TYPE_STRING = "string"  # reserved for future multi-option toggles


# ── Feature entry ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Feature:
    key: str
    title: str
    description: str
    category: str
    default: object
    value_type: str = TYPE_BOOL
    # Optional display context (e.g. "Step 1 — Data Ingest"). Used to group
    # features within a category in the UI.
    stage_label: str | None = None
    # Show the "Data shared with Anthropic Claude API" notice on the toggle.
    shares_data_with_anthropic: bool = False
    # Feature is not yet implemented end-to-end — UI shows a "Coming soon"
    # label and the toggle is read-only.
    roadmap: bool = False
    # Valid options when value_type == TYPE_STRING.
    options: list[str] = field(default_factory=list)


# ── The registry ──────────────────────────────────────────────────────────
# NOTE: Every AI feature defaults to False per cerulean_ai_spec.md §11.3.

FEATURES: list[Feature] = [
    # ---- AI features (default OFF) --------------------------------------
    Feature(
        key="ai.data_health_report",
        title="Data Health Report",
        description=(
            "Auto-run an AI analysis after a MARC file finishes indexing. "
            "Produces a plain-English briefing on completeness, encoding issues, "
            "date format anomalies, and outlier records."
        ),
        category=CATEGORY_AI,
        default=False,
        stage_label="Step 1 — Data Ingest",
        shares_data_with_anthropic=True,
    ),
    Feature(
        key="ai.value_aware_mapping",
        title="Value-Aware Field Mapping",
        description=(
            "When generating field mapping suggestions, include a sample of "
            "actual subfield values so the AI can reason about what the data "
            "really contains — not just what the tag name implies."
        ),
        category=CATEGORY_AI,
        default=False,
        stage_label="Step 5 — Field Mapping",
        shares_data_with_anthropic=True,
    ),
    Feature(
        key="ai.transform_rule_gen",
        title="Transform Rule Generation",
        description=(
            "Describe a transform in plain English (\"strip trailing period "
            "and comma\") and have the AI generate a sandboxed rule with a "
            "mandatory before/after preview before it can be approved."
        ),
        category=CATEGORY_AI,
        default=False,
        stage_label="Step 5 — Field Mapping",
        shares_data_with_anthropic=True,
    ),
    Feature(
        key="ai.code_reconciliation",
        title="Branch & Code Reconciliation",
        description=(
            "When a Koha URL and API token are configured, fetch the library's "
            "authorized values and have the AI suggest matches for every source "
            "branch, location, collection, and item-type code found in the data."
        ),
        category=CATEGORY_AI,
        default=False,
        stage_label="Step 7 — Reconciliation",
        shares_data_with_anthropic=True,
    ),
    Feature(
        key="ai.fuzzy_patron_dedup",
        title="Fuzzy Patron Deduplication",
        description=(
            "Detect probable patron duplicates beyond exact-ID matching — "
            "same person entered twice with a slightly different name, birth "
            "date, or address. AI scores each pair with confidence + reason."
        ),
        category=CATEGORY_AI,
        default=False,
        stage_label="Step 8 — Patrons",
        shares_data_with_anthropic=True,
    ),
    Feature(
        key="ai.record_enrichment",
        title="Record Enrichment",
        description=(
            "Flag thin bibliographic records and fetch enrichment data from "
            "Open Library, Google Books, or WorldCat. AI synthesizes the "
            "results into MARC fields presented as suggestions."
        ),
        category=CATEGORY_AI,
        default=False,
        stage_label="Post-Transform",
        shares_data_with_anthropic=True,
        roadmap=True,
    ),
    # ---- Visibility & sharing (seed with a placeholder so the category
    # renders with real content as soon as we wire it up; harmless default). -
    # Intentionally empty for now — will be populated when the log-visibility
    # work lands. The UI gracefully skips empty categories.
]


# ── Lookup helpers ────────────────────────────────────────────────────────

_BY_KEY: dict[str, Feature] = {f.key: f for f in FEATURES}


def get_feature(key: str) -> Feature | None:
    return _BY_KEY.get(key)


def all_feature_keys() -> list[str]:
    return [f.key for f in FEATURES]


def default_value(key: str) -> object:
    f = _BY_KEY.get(key)
    return f.default if f else False


def features_by_category() -> dict[str, list[Feature]]:
    """Group features by category, preserving registry order within each group."""
    grouped: dict[str, list[Feature]] = {}
    for f in FEATURES:
        grouped.setdefault(f.category, []).append(f)
    return grouped


def registry_payload() -> dict:
    """Serialisable representation used by ``GET /users/me/preferences``.

    Exposes the registry so the frontend can auto-render the Preferences
    page without hard-coding feature keys.
    """
    return {
        "categories": [
            {
                "key": cat,
                "label": CATEGORY_LABELS.get(cat, cat.title()),
                "description": CATEGORY_DESCRIPTIONS.get(cat, ""),
                "features": [
                    {
                        "key": f.key,
                        "title": f.title,
                        "description": f.description,
                        "default": f.default,
                        "value_type": f.value_type,
                        "stage_label": f.stage_label,
                        "shares_data_with_anthropic": f.shares_data_with_anthropic,
                        "roadmap": f.roadmap,
                        "options": f.options,
                    }
                    for f in features
                ],
            }
            for cat, features in features_by_category().items()
        ]
    }
