"""Unit tests for cerulean/core/preferences.py — sync helpers only.

The async web-layer helpers require a running Postgres, which we don't
provision in the unit suite. These tests cover:

* ``UnknownPreferenceKey`` is raised for anything outside the registry.
* ``pref_enabled_sync`` falls back to registry defaults when user_id is None
  so bare Celery tasks never accidentally send data to the Claude API.
* ``require_preference`` refuses to build a dependency for a key that isn't
  in the registry — caught at import time, not at request time.
"""

import pytest

from cerulean.core.preferences import (
    UnknownPreferenceKey,
    pref_enabled_sync,
    require_preference,
)


class TestPrefEnabledSyncFallbacks:
    """When the calling user isn't known (user_id is None) the helper must
    return the registry default. AI features default False, so the task
    effectively becomes a no-op — that's the safety property we want."""

    def test_unknown_key_raises(self):
        with pytest.raises(UnknownPreferenceKey):
            pref_enabled_sync(None, None, "nope.no.such.key")

    def test_ai_flag_without_user_returns_false(self):
        # No DB session needed because user_id=None short-circuits before
        # the query. Matches production behavior when a task is queued
        # without a user context.
        assert pref_enabled_sync(None, None, "ai.value_aware_mapping") is False
        assert pref_enabled_sync(None, None, "ai.data_health_report") is False
        assert pref_enabled_sync(None, None, "ai.transform_rule_gen") is False


class TestRequirePreferenceValidation:
    def test_unknown_key_rejected_at_dep_construction(self):
        with pytest.raises(UnknownPreferenceKey):
            require_preference("nope.no.such.key")

    def test_known_key_returns_callable(self):
        dep = require_preference("ai.data_health_report")
        assert callable(dep), "require_preference must return a FastAPI dep"
