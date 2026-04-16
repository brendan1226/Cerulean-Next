"""Unit tests for the Phase 5 AI Code Reconciliation helpers in
``cerulean/tasks/reconcile.py``.

These are the pure functions the Celery task relies on — prompt shape,
Koha response normalisation, and JSON parsing. We intentionally DO NOT
exercise the task itself (it talks to Claude, Postgres, and Koha all in
one go); the database-level persistence is covered by a focused test
with the sync SQLAlchemy session mocked.

Covered here:

* ``_koha_endpoint_for_vocab``        — category → REST URL mapping
* ``_summarize_koha_value``           — Koha dict → (code, description)
* ``_build_recon_user_prompt``        — prompt formatting
* ``_parse_recon_suggestions``        — Claude JSON → clean dict list
"""

import pytest

from cerulean.tasks.reconcile import (
    _AI_RECON_MAX_SOURCE_PER_CAT,
    _AI_RECON_SYSTEM_PROMPT,
    _VOCAB_LABELS,
    _build_recon_user_prompt,
    _koha_endpoint_for_vocab,
    _parse_recon_suggestions,
    _summarize_koha_value,
)


# ── _koha_endpoint_for_vocab ─────────────────────────────────────────


class TestKohaEndpointForVocab:
    def test_itype_maps_to_item_types(self):
        url = _koha_endpoint_for_vocab("itype", "https://koha.example.org")
        assert url == "https://koha.example.org/api/v1/item_types"

    @pytest.mark.parametrize("vocab", ["homebranch", "holdingbranch"])
    def test_branch_vocabs_map_to_libraries(self, vocab):
        url = _koha_endpoint_for_vocab(vocab, "https://koha.example.org")
        assert url == "https://koha.example.org/api/v1/libraries"

    @pytest.mark.parametrize("vocab,expected_cat", [
        ("loc", "LOC"),
        ("ccode", "CCODE"),
        ("not_loan", "NOT_LOAN"),
        ("withdrawn", "WITHDRAWN"),
        ("damaged", "DAMAGED"),
    ])
    def test_auth_value_vocabs_use_category_endpoint(self, vocab, expected_cat):
        url = _koha_endpoint_for_vocab(vocab, "https://koha.example.org")
        assert url == (
            f"https://koha.example.org/api/v1/"
            f"authorised_value_categories/{expected_cat}/authorised_values"
        )

    def test_unknown_vocab_returns_none(self):
        assert _koha_endpoint_for_vocab("made_up", "https://x") is None

    def test_base_url_is_used_verbatim(self):
        # Caller is expected to strip the trailing slash — we don't
        # defensively rstrip. The test documents that contract.
        url = _koha_endpoint_for_vocab("itype", "https://koha.example.org")
        assert url.startswith("https://koha.example.org/")


# ── _summarize_koha_value ────────────────────────────────────────────


class TestSummarizeKohaValue:
    def test_itype_standard_shape(self):
        code, desc = _summarize_koha_value(
            "itype", {"itemtype": "BK", "description": "Books"}
        )
        assert (code, desc) == ("BK", "Books")

    def test_itype_accepts_snake_case_key(self):
        # Some older Koha builds return item_type instead of itemtype.
        # The helper falls back so the task isn't tied to a single schema.
        code, desc = _summarize_koha_value(
            "itype", {"item_type": "DVD", "description": "Video"}
        )
        assert (code, desc) == ("DVD", "Video")

    @pytest.mark.parametrize("vocab", ["homebranch", "holdingbranch"])
    def test_branch_uses_branchcode_plus_branchname(self, vocab):
        code, desc = _summarize_koha_value(
            vocab, {"branchcode": "MPL", "branchname": "Main Public Library"}
        )
        assert (code, desc) == ("MPL", "Main Public Library")

    def test_branch_falls_back_to_library_id(self):
        code, desc = _summarize_koha_value(
            "homebranch", {"library_id": "MPL", "name": "Main"}
        )
        assert (code, desc) == ("MPL", "Main")

    def test_authorised_value_uses_value_plus_description(self):
        code, desc = _summarize_koha_value(
            "loc", {"value": "MAINLIB", "description": "Main Library"}
        )
        assert (code, desc) == ("MAINLIB", "Main Library")

    def test_authorised_value_falls_back_to_auth_value_plus_lib(self):
        # Older endpoints exposed the code under 'authorised_value' and
        # the description under 'lib'.
        code, desc = _summarize_koha_value(
            "ccode", {"authorised_value": "FIC", "lib": "Fiction"}
        )
        assert (code, desc) == ("FIC", "Fiction")

    def test_missing_description_returns_empty_string(self):
        code, desc = _summarize_koha_value("itype", {"itemtype": "BK"})
        assert code == "BK"
        assert desc == ""

    def test_completely_empty_dict_returns_empty_strings(self):
        code, desc = _summarize_koha_value("itype", {})
        assert (code, desc) == ("", "")


# ── _build_recon_user_prompt ─────────────────────────────────────────


class TestBuildReconUserPrompt:
    def test_includes_vocab_label(self):
        prompt = _build_recon_user_prompt(
            "loc",
            [("MAIN", 10), ("JUV", 5)],
            [{"value": "MAINLIB", "description": "Main Library"}],
        )
        # Prompt must name the category and its human-readable label so
        # Claude knows what kind of codes it's matching.
        assert "loc" in prompt
        assert _VOCAB_LABELS["loc"] in prompt

    def test_all_source_codes_with_counts_appear(self):
        prompt = _build_recon_user_prompt(
            "itype",
            [("BK", 18234), ("DVD", 421)],
            [{"itemtype": "BK", "description": "Books"}],
        )
        assert "'BK'" in prompt
        assert "'DVD'" in prompt
        assert "18,234" in prompt
        assert "421" in prompt

    def test_record_count_grammar_singular_vs_plural(self):
        prompt = _build_recon_user_prompt(
            "loc", [("X", 1), ("Y", 2)], [{"value": "X", "description": ""}]
        )
        assert "(1 record)" in prompt
        assert "(2 records)" in prompt

    def test_koha_values_list_with_descriptions(self):
        prompt = _build_recon_user_prompt(
            "homebranch",
            [("MAIN", 100)],
            [
                {"branchcode": "MPL", "branchname": "Main Public Library"},
                {"branchcode": "BRL", "branchname": "Branch Library"},
            ],
        )
        assert "'MPL' — Main Public Library" in prompt
        assert "'BRL' — Branch Library" in prompt

    def test_koha_value_without_description_still_renders(self):
        prompt = _build_recon_user_prompt(
            "itype", [("X", 1)], [{"itemtype": "RARE"}]
        )
        # Just the code, no em-dash description
        assert "'RARE'" in prompt
        assert "RARE — " not in prompt

    def test_koha_value_with_missing_code_is_skipped(self):
        prompt = _build_recon_user_prompt(
            "itype", [("X", 1)], [{"description": "orphaned row"}]
        )
        # The orphaned entry must not break the prompt — it's silently
        # dropped so Claude isn't asked to match against an empty code.
        assert "orphaned row" not in prompt

    def test_empty_source_rows_renders_placeholder(self):
        prompt = _build_recon_user_prompt(
            "loc", [], [{"value": "X", "description": "X"}]
        )
        assert "(none)" in prompt

    def test_empty_koha_values_renders_placeholder(self):
        prompt = _build_recon_user_prompt("loc", [("A", 1)], [])
        assert "(none)" in prompt

    def test_prompt_asks_for_json_array_only(self):
        prompt = _build_recon_user_prompt("itype", [("A", 1)], [{"itemtype": "B"}])
        assert "JSON array" in prompt


# ── _parse_recon_suggestions ─────────────────────────────────────────


class TestParseReconSuggestions:
    def test_valid_json_list_parses(self):
        raw = """[
          {"source_value": "MAIN", "koha_value": "MPL", "confidence": 0.95,
           "reasoning": "Exact name match"},
          {"source_value": "JUV",  "koha_value": "JUVENILE", "confidence": 0.8,
           "reasoning": "Abbreviation"}
        ]"""
        out = _parse_recon_suggestions(raw)
        assert len(out) == 2
        assert out[0] == {
            "source_value": "MAIN",
            "koha_value": "MPL",
            "confidence": 0.95,
            "reasoning": "Exact name match",
        }

    def test_strips_markdown_json_fence(self):
        raw = (
            "```json\n"
            '[{"source_value":"A","koha_value":"B","confidence":0.9,"reasoning":"x"}]\n'
            "```"
        )
        out = _parse_recon_suggestions(raw)
        assert len(out) == 1
        assert out[0]["source_value"] == "A"

    def test_strips_unlabeled_markdown_fence(self):
        raw = '```\n[{"source_value":"A","koha_value":"B","confidence":0.5,"reasoning":"x"}]\n```'
        out = _parse_recon_suggestions(raw)
        assert len(out) == 1

    def test_null_koha_value_is_preserved(self):
        raw = '[{"source_value":"BKMO","koha_value":null,"confidence":0.0,"reasoning":"no match"}]'
        out = _parse_recon_suggestions(raw)
        assert len(out) == 1
        assert out[0]["koha_value"] is None

    def test_confidence_clamped_to_zero_one(self):
        raw = """[
          {"source_value":"A","koha_value":"X","confidence":1.5,"reasoning":"high"},
          {"source_value":"B","koha_value":"Y","confidence":-0.2,"reasoning":"low"}
        ]"""
        out = _parse_recon_suggestions(raw)
        assert out[0]["confidence"] == 1.0
        assert out[1]["confidence"] == 0.0

    def test_string_confidence_is_coerced(self):
        # Some models occasionally return confidence as a string.
        raw = '[{"source_value":"A","koha_value":"X","confidence":"0.75","reasoning":"ok"}]'
        out = _parse_recon_suggestions(raw)
        assert out[0]["confidence"] == 0.75

    def test_unparseable_confidence_defaults_to_zero(self):
        raw = '[{"source_value":"A","koha_value":"X","confidence":"hi","reasoning":"ok"}]'
        out = _parse_recon_suggestions(raw)
        assert out[0]["confidence"] == 0.0

    def test_missing_source_value_drops_entry(self):
        raw = '[{"koha_value":"X","confidence":0.5,"reasoning":"no src"}]'
        out = _parse_recon_suggestions(raw)
        assert out == []

    def test_empty_source_value_drops_entry(self):
        raw = '[{"source_value":"","koha_value":"X","confidence":0.5,"reasoning":"empty"}]'
        out = _parse_recon_suggestions(raw)
        assert out == []

    def test_non_string_koha_value_drops_entry(self):
        # A numeric koha_value is a hallucination — must be rejected rather
        # than stored as gibberish in the rule row.
        raw = '[{"source_value":"A","koha_value":42,"confidence":0.5,"reasoning":"wrong type"}]'
        out = _parse_recon_suggestions(raw)
        assert out == []

    def test_non_dict_entries_are_skipped(self):
        raw = '["stringy", 42, {"source_value":"A","koha_value":"B","confidence":0.5,"reasoning":"ok"}]'
        out = _parse_recon_suggestions(raw)
        assert len(out) == 1
        assert out[0]["source_value"] == "A"

    def test_non_list_top_level_returns_empty(self):
        raw = '{"source_value":"A","koha_value":"B","confidence":0.5}'
        out = _parse_recon_suggestions(raw)
        assert out == []

    def test_malformed_json_returns_empty(self):
        assert _parse_recon_suggestions("not json {{") == []
        assert _parse_recon_suggestions("") == []

    def test_reasoning_coerced_to_string(self):
        raw = '[{"source_value":"A","koha_value":"B","confidence":0.5,"reasoning":42}]'
        out = _parse_recon_suggestions(raw)
        assert out[0]["reasoning"] == "42"

    def test_missing_reasoning_tolerated(self):
        raw = '[{"source_value":"A","koha_value":"B","confidence":0.5}]'
        out = _parse_recon_suggestions(raw)
        assert len(out) == 1
        assert out[0]["reasoning"] is None


# ── System prompt invariants ─────────────────────────────────────────


class TestSystemPromptInvariants:
    """The system prompt is the foundation of Phase 5's behaviour. If it
    ever loses one of these anchor phrases Claude starts drifting (we've
    seen this in Phase 2) — so the test locks the contract down."""

    def test_mentions_json_only_output(self):
        assert "JSON array" in _AI_RECON_SYSTEM_PROMPT
        assert "no preamble" in _AI_RECON_SYSTEM_PROMPT.lower()

    def test_names_required_keys(self):
        for k in ("source_value", "koha_value", "confidence", "reasoning"):
            assert k in _AI_RECON_SYSTEM_PROMPT

    def test_allows_null_when_no_match(self):
        # Without this, Claude tends to fabricate a Koha value even when
        # there's nothing reasonable in the supplied list.
        assert "null" in _AI_RECON_SYSTEM_PROMPT.lower()

    def test_warns_against_fabrication(self):
        assert "fabricate" in _AI_RECON_SYSTEM_PROMPT.lower() or \
               "invent" in _AI_RECON_SYSTEM_PROMPT.lower()


# ── Truncation cap ───────────────────────────────────────────────────


class TestSourceCapacityCap:
    def test_cap_is_reasonable(self):
        # Sanity: we never want to send 10k codes in one prompt (cost,
        # context window). But we also don't want it so low that a normal
        # migration hits the cap. 200 is our current breakpoint.
        assert 50 <= _AI_RECON_MAX_SOURCE_PER_CAT <= 500
