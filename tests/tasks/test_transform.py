"""Unit tests for cerulean/tasks/transform.py helper functions."""

import pymarc
from pymarc import Subfield
import pytest

from cerulean.tasks.transform import (
    _add_952_from_csv,
    _apply_fn,
    _apply_lookup,
    _apply_maps_to_record,
    _apply_regex,
    _apply_transform,
    _extract_values,
    _get_001,
    _load_items_csv,
    _set_value,
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
# _extract_values
# ══════════════════════════════════════════════════════════════════════


class TestExtractValues:
    def test_control_field(self):
        record = _make_record([_control("001", "12345")])
        assert _extract_values(record, "001", None) == ["12345"]

    def test_data_field_with_subfield(self):
        record = _make_record([_data("245", ["a", "The Title", "b", "Subtitle"])])
        assert _extract_values(record, "245", "a") == ["The Title"]
        assert _extract_values(record, "245", "b") == ["Subtitle"]

    def test_data_field_with_dollar_prefix(self):
        record = _make_record([_data("245", ["a", "Hello"])])
        assert _extract_values(record, "245", "$a") == ["Hello"]

    def test_data_field_no_subfield(self):
        record = _make_record([_data("245", ["a", "Title", "b", "Sub"])])
        vals = _extract_values(record, "245", None)
        assert len(vals) == 1
        assert "Title" in vals[0]
        assert "Sub" in vals[0]

    def test_missing_field(self):
        record = _make_record([_control("001", "123")])
        assert _extract_values(record, "999", "a") == []

    def test_multiple_fields(self):
        record = _make_record([
            _data("650", ["a", "Science"]),
            _data("650", ["a", "Math"]),
        ])
        assert _extract_values(record, "650", "a") == ["Science", "Math"]


# ══════════════════════════════════════════════════════════════════════
# _apply_regex
# ══════════════════════════════════════════════════════════════════════


class TestApplyRegex:
    def test_basic_substitution(self):
        assert _apply_regex("hello world", "s/world/earth/") == "hello earth"

    def test_case_insensitive(self):
        assert _apply_regex("Hello World", "s/hello/hi/i") == "hi World"

    def test_global_flag(self):
        assert _apply_regex("aaa", "s/a/b/g") == "bbb"

    def test_first_only(self):
        assert _apply_regex("aaa", "s/a/b/") == "baa"

    def test_invalid_pattern_returns_original(self):
        assert _apply_regex("test", "s/[invalid/x/") == "test"

    def test_none_pattern(self):
        assert _apply_regex("test", None) == "test"

    def test_empty_pattern(self):
        assert _apply_regex("test", "") == "test"

    def test_custom_delimiter(self):
        assert _apply_regex("a/b", "s|/|-|") == "a-b"


# ══════════════════════════════════════════════════════════════════════
# _apply_lookup
# ══════════════════════════════════════════════════════════════════════


class TestApplyLookup:
    def test_exact_match(self):
        table = '{"REF": "Reference", "CIR": "Circulating"}'
        assert _apply_lookup("REF", table) == "Reference"

    def test_case_insensitive_fallback(self):
        table = '{"REF": "Reference"}'
        assert _apply_lookup("ref", table) == "Reference"

    def test_no_match_passthrough(self):
        table = '{"REF": "Reference"}'
        assert _apply_lookup("UNKNOWN", table) == "UNKNOWN"

    def test_invalid_json(self):
        assert _apply_lookup("test", "not json") == "test"

    def test_none_lookup(self):
        assert _apply_lookup("test", None) == "test"


# ══════════════════════════════════════════════════════════════════════
# _apply_fn
# ══════════════════════════════════════════════════════════════════════


class TestApplyFn:
    def test_upper(self):
        assert _apply_fn("hello", "value.upper()") == "HELLO"

    def test_slice(self):
        assert _apply_fn("abcdef", "value[:3]") == "abc"

    def test_concat(self):
        assert _apply_fn("42", "'PREFIX-' + value") == "PREFIX-42"

    def test_len(self):
        assert _apply_fn("hello", "str(len(value))") == "5"

    def test_invalid_expression_returns_original(self):
        assert _apply_fn("test", "undefined_var + value") == "test"

    def test_none_expression(self):
        assert _apply_fn("test", None) == "test"

    def test_no_file_access(self):
        # Should fail safely — open is not in _SAFE_BUILTINS
        assert _apply_fn("test", "open('/etc/passwd').read()") == "test"

    def test_no_import(self):
        assert _apply_fn("test", "__import__('os').system('echo pwned')") == "test"


# ══════════════════════════════════════════════════════════════════════
# _apply_transform
# ══════════════════════════════════════════════════════════════════════


class TestApplyTransform:
    def test_copy(self):
        assert _apply_transform("hello", "copy", None) == "hello"

    def test_const(self):
        assert _apply_transform("ignored", "const", "STATIC") == "STATIC"

    def test_unknown_type_passthrough(self):
        assert _apply_transform("val", "unknown_type", None) == "val"


# ══════════════════════════════════════════════════════════════════════
# _set_value
# ══════════════════════════════════════════════════════════════════════


class TestSetValue:
    def test_set_control_field_new(self):
        record = _make_record()
        _set_value(record, "001", None, "NEW001")
        assert record["001"].data == "NEW001"

    def test_set_control_field_replace(self):
        record = _make_record([_control("001", "OLD")])
        _set_value(record, "001", None, "NEW")
        assert record["001"].data == "NEW"

    def test_set_data_field_new(self):
        record = _make_record()
        _set_value(record, "952", "$o", "QA76.5")
        fields = record.get_fields("952")
        assert len(fields) == 1
        assert fields[0]["o"] == "QA76.5"

    def test_set_data_field_add_subfield(self):
        record = _make_record([_data("952", ["a", "MAIN"])])
        _set_value(record, "952", "o", "QA76.5")
        field = record.get_fields("952")[0]
        assert field["a"] == "MAIN"
        assert field["o"] == "QA76.5"

    def test_set_data_field_no_subfield(self):
        record = _make_record()
        _set_value(record, "500", None, "A general note")
        fields = record.get_fields("500")
        assert len(fields) == 1
        assert fields[0]["a"] == "A general note"


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
# _apply_maps_to_record
# ══════════════════════════════════════════════════════════════════════


class TestApplyMapsToRecord:
    def test_copy_map(self):
        record = _make_record([
            _data("852", ["h", "QA76.5 .B45"]),
        ])
        maps = [{
            "source_tag": "852", "source_sub": "h",
            "target_tag": "952", "target_sub": "o",
            "transform_type": "copy", "transform_fn": None,
        }]
        _apply_maps_to_record(record, maps)
        assert record.get_fields("952")[0]["o"] == "QA76.5 .B45"

    def test_const_map(self):
        record = _make_record()
        maps = [{
            "source_tag": "999", "source_sub": None,
            "target_tag": "942", "target_sub": "c",
            "transform_type": "const", "transform_fn": "BK",
        }]
        _apply_maps_to_record(record, maps)
        assert record.get_fields("942")[0]["c"] == "BK"

    def test_regex_map(self):
        record = _make_record([_control("001", "sirsi-12345")])
        maps = [{
            "source_tag": "001", "source_sub": None,
            "target_tag": "001", "target_sub": None,
            "transform_type": "regex", "transform_fn": "s/sirsi-//",
        }]
        _apply_maps_to_record(record, maps)
        assert record["001"].data == "12345"

    def test_lookup_map(self):
        record = _make_record([_data("852", ["b", "MAIN"])])
        maps = [{
            "source_tag": "852", "source_sub": "b",
            "target_tag": "952", "target_sub": "a",
            "transform_type": "lookup",
            "transform_fn": '{"MAIN": "Central Library", "BRANCH": "West Branch"}',
        }]
        _apply_maps_to_record(record, maps)
        assert record.get_fields("952")[0]["a"] == "Central Library"

    def test_skips_missing_source(self):
        record = _make_record()
        maps = [{
            "source_tag": "999", "source_sub": "z",
            "target_tag": "952", "target_sub": "a",
            "transform_type": "copy", "transform_fn": None,
        }]
        _apply_maps_to_record(record, maps)
        assert record.get_fields("952") == []

    def test_multiple_maps_in_order(self):
        record = _make_record([
            _data("852", ["b", "MAIN", "h", "QA76"]),
        ])
        maps = [
            {
                "source_tag": "852", "source_sub": "b",
                "target_tag": "952", "target_sub": "a",
                "transform_type": "copy", "transform_fn": None,
            },
            {
                "source_tag": "852", "source_sub": "h",
                "target_tag": "952", "target_sub": "o",
                "transform_type": "copy", "transform_fn": None,
            },
        ]
        _apply_maps_to_record(record, maps)
        field = record.get_fields("952")[0]
        assert field["a"] == "MAIN"
        assert field["o"] == "QA76"


# ══════════════════════════════════════════════════════════════════════
# _add_952_from_csv
# ══════════════════════════════════════════════════════════════════════


class TestAdd952FromCsv:
    def test_basic_item(self):
        record = _make_record()
        row = {
            "homebranch": "MAIN",
            "holdingbranch": "MAIN",
            "barcode": "123456",
            "itype": "BK",
            "callnumber": "QA76.5",
        }
        _add_952_from_csv(record, row)
        fields = record.get_fields("952")
        assert len(fields) == 1
        f = fields[0]
        assert f["a"] == "MAIN"
        assert f["b"] == "MAIN"
        assert f["p"] == "123456"
        assert f["y"] == "BK"
        assert f["o"] == "QA76.5"

    def test_empty_values_skipped(self):
        record = _make_record()
        row = {"homebranch": "MAIN", "barcode": "", "callnumber": "  "}
        _add_952_from_csv(record, row)
        fields = record.get_fields("952")
        assert len(fields) == 1
        f = fields[0]
        assert f["a"] == "MAIN"
        assert "p" not in f  # barcode was empty, not added

    def test_unknown_columns_ignored(self):
        record = _make_record()
        row = {"homebranch": "MAIN", "biblionumber": "999", "randomcol": "xyz"}
        _add_952_from_csv(record, row)
        fields = record.get_fields("952")
        assert len(fields) == 1
        # Only homebranch mapped
        subs = fields[0].subfields_as_dict()
        assert "a" in subs

    def test_all_empty_no_field_added(self):
        record = _make_record()
        row = {"biblionumber": "999", "randomcol": "xyz"}
        _add_952_from_csv(record, row)
        assert record.get_fields("952") == []


# ══════════════════════════════════════════════════════════════════════
# _load_items_csv
# ══════════════════════════════════════════════════════════════════════


class TestLoadItemsCsv:
    def test_load(self, tmp_path):
        csv_file = tmp_path / "items.csv"
        csv_file.write_text(
            "biblionumber,homebranch,barcode\n"
            "1001,MAIN,BC001\n"
            "1001,BRANCH,BC002\n"
            "1002,MAIN,BC003\n"
        )
        result = _load_items_csv(str(csv_file), "biblionumber")
        assert len(result) == 2
        assert len(result["1001"]) == 2
        assert len(result["1002"]) == 1
        assert result["1001"][0]["barcode"] == "BC001"
