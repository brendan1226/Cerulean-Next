"""Unit tests for cerulean/core/features.py — the user preference registry.

The registry is the single source of truth for every toggleable feature.
These tests exist so a drift in defaults, key names, or value types breaks
CI immediately — the spec (cerulean_ai_spec.md §11.3) is explicit that AI
features must default OFF for every user.
"""

import pytest

from cerulean.core.features import (
    CATEGORY_AI,
    CATEGORY_LABELS,
    FEATURES,
    TYPE_BOOL,
    all_feature_keys,
    default_value,
    features_by_category,
    get_feature,
    registry_payload,
)


class TestFeatureRegistryBasics:
    def test_every_feature_has_non_empty_metadata(self):
        assert FEATURES, "registry should not be empty"
        for f in FEATURES:
            assert f.key, "key must be set"
            assert f.title, f"{f.key}: title must be set"
            assert f.description, f"{f.key}: description must be set"
            assert f.category, f"{f.key}: category must be set"
            assert f.category in CATEGORY_LABELS, f"{f.key}: unknown category {f.category!r}"
            assert f.value_type in (TYPE_BOOL, "string"), (
                f"{f.key}: unexpected value_type {f.value_type!r}"
            )

    def test_keys_are_unique(self):
        keys = [f.key for f in FEATURES]
        assert len(keys) == len(set(keys)), "duplicate feature keys detected"

    def test_keys_are_dot_namespaced(self):
        for f in FEATURES:
            assert "." in f.key, f"{f.key!r}: feature keys should be dot-namespaced"


class TestAIDefaultsOff:
    """Spec §11.3: every AI feature defaults to enabled=False."""

    def test_all_ai_features_default_false(self):
        ai_features = [f for f in FEATURES if f.category == CATEGORY_AI]
        assert ai_features, "no AI features registered"
        for f in ai_features:
            assert f.default is False, (
                f"{f.key} defaults to {f.default!r}; AI features must default OFF"
            )

    def test_all_known_ai_keys_present(self):
        """AI spec keys that must be present in the registry. Patron data
        is PII and never sent to AI, so ai.fuzzy_patron_dedup is
        intentionally absent."""
        required = {
            "ai.data_health_report",
            "ai.value_aware_mapping",
            "ai.transform_rule_gen",
            "ai.code_reconciliation",
            "ai.record_enrichment",
        }
        actual = {f.key for f in FEATURES if f.category == CATEGORY_AI}
        missing = required - actual
        assert not missing, f"AI spec keys missing from registry: {sorted(missing)}"

    def test_no_patron_ai_feature_keys(self):
        """Patron data is PII — no AI feature may target it. Guards against
        accidental reintroduction of fuzzy patron dedup or similar."""
        forbidden_substrings = ("patron", "borrower")
        for f in FEATURES:
            if f.category != CATEGORY_AI:
                continue
            key_lower = f.key.lower()
            for sub in forbidden_substrings:
                assert sub not in key_lower, (
                    f"AI feature {f.key!r} appears to target patron PII; "
                    f"patron data must never be sent to an AI/LLM."
                )

    def test_ai_features_share_data_with_anthropic(self):
        """Every AI feature must carry the data-sharing notice — the
        Preferences page relies on this to show the 'Anthropic API' badge."""
        for f in FEATURES:
            if f.category == CATEGORY_AI:
                assert f.shares_data_with_anthropic, (
                    f"{f.key}: AI features must set shares_data_with_anthropic=True"
                )


class TestLookups:
    def test_get_feature_returns_none_for_unknown_key(self):
        assert get_feature("nope.not.a.real.key") is None

    def test_get_feature_returns_object_for_known_key(self):
        f = get_feature("ai.data_health_report")
        assert f is not None
        assert f.title == "Data Health Report"

    def test_default_value_falls_back_to_false(self):
        assert default_value("nope.not.a.real.key") is False

    def test_default_value_matches_registry(self):
        for f in FEATURES:
            assert default_value(f.key) == f.default

    def test_all_feature_keys_matches_registry(self):
        assert set(all_feature_keys()) == {f.key for f in FEATURES}


class TestRegistryPayload:
    """The payload shape is the contract between the API and the frontend
    Preferences page — breaking these fields breaks the UI auto-render."""

    def test_payload_has_categories_list(self):
        payload = registry_payload()
        assert "categories" in payload
        assert isinstance(payload["categories"], list)

    def test_ai_category_present_and_populated(self):
        payload = registry_payload()
        ai = next((c for c in payload["categories"] if c["key"] == CATEGORY_AI), None)
        assert ai is not None
        assert ai["label"] == CATEGORY_LABELS[CATEGORY_AI]
        assert len(ai["features"]) >= 5

    def test_feature_entries_have_required_fields(self):
        payload = registry_payload()
        required = {
            "key", "title", "description", "default", "value_type",
            "stage_label", "shares_data_with_anthropic", "roadmap", "options",
        }
        for cat in payload["categories"]:
            for f in cat["features"]:
                assert required.issubset(f.keys()), (
                    f"{f.get('key')}: payload missing fields: {required - f.keys()}"
                )

    def test_features_by_category_preserves_registry_order(self):
        grouped = features_by_category()
        assert CATEGORY_AI in grouped
        # Every item in grouped should come from FEATURES, in registry order
        flat = [f.key for group in grouped.values() for f in group]
        registry_keys = [f.key for f in FEATURES]
        assert flat == registry_keys
