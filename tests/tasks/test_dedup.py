"""Unit tests for cerulean/tasks/dedup.py helper functions."""

import pymarc
from pymarc import Subfield

from cerulean.tasks.dedup import (
    _extract_match_key,
    _extract_values,
    _filter_oclc,
    _get_001,
    _normalize_isbn,
    _normalize_text,
)


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_record(fields=None):
    """Create a pymarc Record with given fields."""
    record = pymarc.Record()
    if fields:
        for f in fields:
            record.add_field(f)
    return record


def _control(tag, data):
    return pymarc.Field(tag=tag, data=data)


def _data(tag, subfields, indicators=None):
    """Create a data field. subfields is a flat list: [code, val, code, val, ...]"""
    indicators = indicators or [" ", " "]
    sf_objects = [Subfield(code=subfields[i], value=subfields[i+1])
                  for i in range(0, len(subfields), 2)]
    return pymarc.Field(tag=tag, indicators=indicators, subfields=sf_objects)


# ══════════════════════════════════════════════════════════════════════
# _normalize_text
# ══════════════════════════════════════════════════════════════════════


class TestNormalizeText:
    def test_lowercase(self):
        assert _normalize_text("HELLO WORLD") == "hello world"

    def test_strip_diacritics(self):
        result = _normalize_text("café résumé")
        assert "e" in result
        assert "é" not in result

    def test_collapse_whitespace(self):
        assert _normalize_text("hello   world") == "hello world"

    def test_remove_punctuation(self):
        result = _normalize_text("Hello, World!")
        assert "," not in result
        assert "!" not in result

    def test_combined(self):
        result = _normalize_text("  The Title : a Subtitle /  ")
        assert result == "the title a subtitle"


# ══════════════════════════════════════════════════════════════════════
# _normalize_isbn
# ══════════════════════════════════════════════════════════════════════


class TestNormalizeIsbn:
    def test_strip_hyphens(self):
        assert _normalize_isbn("978-0-13-468599-1") == "9780134685991"

    def test_strip_spaces(self):
        assert _normalize_isbn("978 0 13 468599 1") == "9780134685991"

    def test_remove_qualifier(self):
        assert _normalize_isbn("0134685997 (pbk.)") == "0134685997"

    def test_combined(self):
        assert _normalize_isbn("978-0-13-468599-1 (hardcover)") == "9780134685991"

    def test_simple_isbn(self):
        assert _normalize_isbn("0134685997") == "0134685997"


# ══════════════════════════════════════════════════════════════════════
# _filter_oclc
# ══════════════════════════════════════════════════════════════════════


class TestFilterOclc:
    def test_valid_oclc(self):
        assert _filter_oclc("(OCoLC)12345678") == "12345678"

    def test_oclc_with_space(self):
        assert _filter_oclc("(OCoLC) 12345678") == "12345678"

    def test_non_oclc_returns_none(self):
        assert _filter_oclc("(DLC)12345678") is None

    def test_plain_number_returns_none(self):
        assert _filter_oclc("12345678") is None


# ══════════════════════════════════════════════════════════════════════
# _extract_values (dedup version)
# ══════════════════════════════════════════════════════════════════════


class TestExtractValues:
    def test_control_field(self):
        record = _make_record([_control("001", "12345")])
        assert _extract_values(record, "001", None) == ["12345"]

    def test_data_field_with_subfield(self):
        record = _make_record([_data("020", ["a", "978-0-13-468599-1"])])
        assert _extract_values(record, "020", "a") == ["978-0-13-468599-1"]

    def test_missing_field(self):
        record = _make_record()
        assert _extract_values(record, "020", "a") == []

    def test_multiple_values(self):
        record = _make_record([
            _data("035", ["a", "(OCoLC)111"]),
            _data("035", ["a", "(OCoLC)222"]),
        ])
        assert _extract_values(record, "035", "a") == ["(OCoLC)111", "(OCoLC)222"]


# ══════════════════════════════════════════════════════════════════════
# _get_001
# ══════════════════════════════════════════════════════════════════════


class TestGet001:
    def test_present(self):
        record = _make_record([_control("001", "bib12345")])
        assert _get_001(record) == "bib12345"

    def test_missing(self):
        record = _make_record()
        assert _get_001(record) is None


# ══════════════════════════════════════════════════════════════════════
# _extract_match_key
# ══════════════════════════════════════════════════════════════════════


class TestExtractMatchKey:
    def test_001_exact(self):
        record = _make_record([_control("001", "12345")])
        config = [{"tag": "001", "sub": None, "match_type": "exact"}]
        assert _extract_match_key(record, config) == "12345"

    def test_isbn_normalised(self):
        record = _make_record([_data("020", ["a", "978-0-13-468599-1 (pbk.)"])])
        config = [{"tag": "020", "sub": "a", "match_type": "normalised"}]
        assert _extract_match_key(record, config) == "9780134685991"

    def test_title_author_composite(self):
        record = _make_record([
            _data("245", ["a", "The Great Gatsby"]),
            _data("100", ["a", "Fitzgerald, F. Scott"]),
        ])
        config = [
            {"tag": "245", "sub": "a", "match_type": "normalised"},
            {"tag": "100", "sub": "a", "match_type": "normalised"},
        ]
        key = _extract_match_key(record, config)
        assert key is not None
        assert "||" in key
        assert "great gatsby" in key.lower()

    def test_missing_field_returns_none(self):
        record = _make_record([_control("001", "12345")])
        config = [{"tag": "020", "sub": "a", "match_type": "normalised"}]
        assert _extract_match_key(record, config) is None

    def test_oclc_filter(self):
        record = _make_record([_data("035", ["a", "(OCoLC)999888"])])
        config = [{"tag": "035", "sub": "a", "match_type": "exact"}]
        assert _extract_match_key(record, config) == "999888"

    def test_oclc_non_oclc_returns_none(self):
        record = _make_record([_data("035", ["a", "(DLC)12345"])])
        config = [{"tag": "035", "sub": "a", "match_type": "exact"}]
        assert _extract_match_key(record, config) is None

    def test_composite_partial_missing_returns_none(self):
        """If any field in a composite key is missing, return None."""
        record = _make_record([
            _data("245", ["a", "Some Title"]),
            # No 100 field
        ])
        config = [
            {"tag": "245", "sub": "a", "match_type": "normalised"},
            {"tag": "100", "sub": "a", "match_type": "normalised"},
        ]
        assert _extract_match_key(record, config) is None
