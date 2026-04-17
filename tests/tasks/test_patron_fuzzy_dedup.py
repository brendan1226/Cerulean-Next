"""Unit tests for Phase 6 AI Fuzzy Patron Deduplication helpers in
``cerulean/tasks/patrons.py``.

Covers the pure-function helpers the Celery task relies on:

* ``_levenshtein``            — edit distance calculation
* ``_blocking_key``           — surname initial + birth year grouping
* ``_extract_dedup_fields``   — row → minimal dict for Claude
* ``_build_dedup_user_prompt``— prompt formatting
* ``_parse_dedup_response``   — Claude JSON → scored pair list
"""

import pytest

from cerulean.tasks.patrons import (
    _DEDUP_BATCH_SIZE,
    _DEDUP_FIELDS,
    _DEDUP_MAX_PAIRS,
    _DEDUP_SYSTEM_PROMPT,
    _blocking_key,
    _build_dedup_user_prompt,
    _extract_dedup_fields,
    _levenshtein,
    _parse_dedup_response,
)


# ── _levenshtein ──────────────────────────────────────────────────────


class TestLevenshtein:
    def test_identical_strings(self):
        assert _levenshtein("smith", "smith") == 0

    def test_single_insertion(self):
        assert _levenshtein("smith", "smithe") == 1

    def test_single_deletion(self):
        assert _levenshtein("smithe", "smith") == 1

    def test_single_substitution(self):
        assert _levenshtein("smith", "smyth") == 1

    def test_common_misspelling(self):
        # Jon vs John — 1 insertion
        assert _levenshtein("jon", "john") == 1

    def test_completely_different(self):
        assert _levenshtein("abc", "xyz") == 3

    def test_empty_strings(self):
        assert _levenshtein("", "") == 0
        assert _levenshtein("abc", "") == 3
        assert _levenshtein("", "xyz") == 3

    def test_symmetric(self):
        assert _levenshtein("abc", "axc") == _levenshtein("axc", "abc")

    def test_case_sensitive(self):
        assert _levenshtein("Smith", "smith") == 1

    def test_within_threshold_3(self):
        # Smythe vs Smith — 2 edits
        assert _levenshtein("smythe", "smith") <= 3

    def test_beyond_threshold_3(self):
        assert _levenshtein("johnson", "williams") > 3


# ── _blocking_key ─────────────────────────────────────────────────────


class TestBlockingKey:
    def test_surname_initial_plus_birth_year(self):
        row = {"surname": "Smith", "dateofbirth": "1984-03-12"}
        assert _blocking_key(row) == "s:1984"

    def test_surname_only_no_dob(self):
        row = {"surname": "Jones", "dateofbirth": ""}
        assert _blocking_key(row) == "j:"

    def test_missing_surname_returns_none(self):
        assert _blocking_key({"surname": "", "dateofbirth": "1990-01-01"}) is None
        assert _blocking_key({"dateofbirth": "1990-01-01"}) is None
        assert _blocking_key({}) is None

    def test_whitespace_only_surname_returns_none(self):
        assert _blocking_key({"surname": "   "}) is None

    def test_dob_formats(self):
        assert _blocking_key({"surname": "X", "dateofbirth": "03/12/1984"}) == "x:1984"
        assert _blocking_key({"surname": "X", "dateofbirth": "12/03/1984"}) == "x:1984"
        assert _blocking_key({"surname": "X", "dateofbirth": "1984-03-12"}) == "x:1984"

    def test_dob_without_four_digit_year_omits_year(self):
        assert _blocking_key({"surname": "X", "dateofbirth": "03-12-84"}) == "x:"

    def test_lowercases_initial(self):
        assert _blocking_key({"surname": "SMITH"}) == "s:"


# ── _extract_dedup_fields ────────────────────────────────────────────


class TestExtractDedupFields:
    def test_includes_row_index(self):
        out = _extract_dedup_fields({"surname": "Smith"}, 42)
        assert out["row_index"] == 42

    def test_includes_all_dedup_fields(self):
        row = {f: f"val_{f}" for f in _DEDUP_FIELDS}
        out = _extract_dedup_fields(row, 0)
        for f in _DEDUP_FIELDS:
            assert out[f] == f"val_{f}"

    def test_missing_fields_default_to_empty(self):
        out = _extract_dedup_fields({}, 0)
        for f in _DEDUP_FIELDS:
            assert out[f] == ""

    def test_strips_whitespace(self):
        out = _extract_dedup_fields({"surname": "  Smith  "}, 0)
        assert out["surname"] == "Smith"

    def test_none_values_become_empty(self):
        out = _extract_dedup_fields({"surname": None}, 0)
        assert out["surname"] == ""


# ── _build_dedup_user_prompt ─────────────────────────────────────────


class TestBuildDedupUserPrompt:
    def _make_pair(self):
        a = {"row_index": 0, "surname": "Smith", "firstname": "John",
             "dateofbirth": "1984-03-12", "address": "123 Main St",
             "city": "Anytown", "email": "john@example.com",
             "phone": "555-1234", "cardnumber": "P001"}
        b = {"row_index": 1, "surname": "Smith", "firstname": "Jon",
             "dateofbirth": "1984-03-12", "address": "123 Main",
             "city": "Anytown", "email": "", "phone": "555-1234",
             "cardnumber": "P002"}
        return (a, b)

    def test_includes_pair_header(self):
        prompt = _build_dedup_user_prompt([self._make_pair()])
        assert "Pair 0" in prompt

    def test_includes_patron_a_and_b(self):
        prompt = _build_dedup_user_prompt([self._make_pair()])
        assert "Patron A" in prompt
        assert "Patron B" in prompt

    def test_includes_row_indices(self):
        prompt = _build_dedup_user_prompt([self._make_pair()])
        assert "row 0" in prompt
        assert "row 1" in prompt

    def test_includes_field_values(self):
        prompt = _build_dedup_user_prompt([self._make_pair()])
        assert "Smith" in prompt
        assert "John" in prompt
        assert "1984-03-12" in prompt

    def test_empty_fields_omitted(self):
        prompt = _build_dedup_user_prompt([self._make_pair()])
        # Patron B has empty email — should not appear
        lines = [l for l in prompt.splitlines() if "email" in l.lower()]
        # Only patron A's email should show
        assert len(lines) == 1

    def test_asks_for_json(self):
        prompt = _build_dedup_user_prompt([self._make_pair()])
        assert "JSON array" in prompt

    def test_multiple_pairs_numbered(self):
        p = self._make_pair()
        prompt = _build_dedup_user_prompt([p, p])
        assert "Pair 0" in prompt
        assert "Pair 1" in prompt
        assert "2 pairs" in prompt


# ── _parse_dedup_response ────────────────────────────────────────────


class TestParseDedupResponse:
    def test_valid_response(self):
        raw = """[
          {"pair_index": 0, "is_duplicate": true, "confidence": 91,
           "reasoning": "Same person — name + DOB match"},
          {"pair_index": 1, "is_duplicate": false, "confidence": 20,
           "reasoning": "Different people"}
        ]"""
        out = _parse_dedup_response(raw)
        assert len(out) == 2
        assert out[0] == {
            "pair_index": 0,
            "is_duplicate": True,
            "confidence": 91,
            "reasoning": "Same person — name + DOB match",
        }
        assert out[1]["is_duplicate"] is False

    def test_strips_markdown_fence(self):
        raw = '```json\n[{"pair_index":0,"is_duplicate":true,"confidence":80,"reasoning":"ok"}]\n```'
        out = _parse_dedup_response(raw)
        assert len(out) == 1

    def test_confidence_clamped(self):
        raw = '[{"pair_index":0,"is_duplicate":true,"confidence":150,"reasoning":"x"}, {"pair_index":1,"is_duplicate":false,"confidence":-10,"reasoning":"y"}]'
        out = _parse_dedup_response(raw)
        assert out[0]["confidence"] == 100
        assert out[1]["confidence"] == 0

    def test_string_confidence_coerced(self):
        raw = '[{"pair_index":0,"is_duplicate":true,"confidence":"85","reasoning":"ok"}]'
        out = _parse_dedup_response(raw)
        assert out[0]["confidence"] == 85

    def test_missing_pair_index_drops_entry(self):
        raw = '[{"is_duplicate":true,"confidence":80,"reasoning":"ok"}]'
        assert _parse_dedup_response(raw) == []

    def test_non_bool_is_duplicate_drops_entry(self):
        raw = '[{"pair_index":0,"is_duplicate":"yes","confidence":80,"reasoning":"ok"}]'
        assert _parse_dedup_response(raw) == []

    def test_malformed_json(self):
        assert _parse_dedup_response("not json") == []
        assert _parse_dedup_response("") == []

    def test_non_list_returns_empty(self):
        assert _parse_dedup_response('{"pair_index":0}') == []

    def test_non_dict_entries_skipped(self):
        raw = '["string", {"pair_index":0,"is_duplicate":true,"confidence":80,"reasoning":"ok"}]'
        out = _parse_dedup_response(raw)
        assert len(out) == 1

    def test_reasoning_coerced_to_string(self):
        raw = '[{"pair_index":0,"is_duplicate":true,"confidence":50,"reasoning":42}]'
        out = _parse_dedup_response(raw)
        assert out[0]["reasoning"] == "42"

    def test_missing_reasoning_tolerated(self):
        raw = '[{"pair_index":0,"is_duplicate":true,"confidence":50}]'
        out = _parse_dedup_response(raw)
        assert out[0]["reasoning"] is None


# ── System prompt invariants ─────────────────────────────────────────


class TestDedupSystemPromptInvariants:
    def test_mentions_json_only(self):
        assert "JSON array" in _DEDUP_SYSTEM_PROMPT
        assert "no preamble" in _DEDUP_SYSTEM_PROMPT.lower()

    def test_names_required_keys(self):
        for k in ("pair_index", "is_duplicate", "confidence", "reasoning"):
            assert k in _DEDUP_SYSTEM_PROMPT

    def test_warns_against_fabrication(self):
        assert "fabricate" in _DEDUP_SYSTEM_PROMPT.lower() or \
               "invent" in _DEDUP_SYSTEM_PROMPT.lower()

    def test_mentions_empty_fields_handling(self):
        assert "empty" in _DEDUP_SYSTEM_PROMPT.lower()


# ── Config sanity ────────────────────────────────────────────────────


class TestConfigSanity:
    def test_batch_size_reasonable(self):
        assert 10 <= _DEDUP_BATCH_SIZE <= 100

    def test_max_pairs_reasonable(self):
        assert 1000 <= _DEDUP_MAX_PAIRS <= 50000

    def test_dedup_fields_include_essentials(self):
        for f in ("surname", "firstname", "dateofbirth"):
            assert f in _DEDUP_FIELDS
