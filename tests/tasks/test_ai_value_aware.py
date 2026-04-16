"""Unit tests for cerulean/tasks/analyze.py value-aware helpers (Phase 2).

Covers ``_build_value_index`` and ``_format_value_index`` — the bits that
turn raw MARC files into the aggregated "top-N distinct values per
subfield" payload that the AI Suggest task feeds to Claude when a user
has ``ai.value_aware_mapping`` enabled.

These are pure I/O + Counter functions, safe to unit-test with a
temporary MARC file on disk.
"""

import pymarc
import pytest
from pymarc import Subfield

from cerulean.tasks.analyze import _build_value_index, _format_value_index


# ── Helpers ──────────────────────────────────────────────────────────

def _make_record(fields):
    r = pymarc.Record()
    for f in fields:
        r.add_field(f)
    return r


def _data(tag, pairs, indicators=None):
    indicators = indicators or [" ", " "]
    subs = [Subfield(code=pairs[i], value=pairs[i + 1]) for i in range(0, len(pairs), 2)]
    return pymarc.Field(tag=tag, indicators=indicators, subfields=subs)


def _write_marc(tmp_path, records, name="sample.mrc"):
    path = tmp_path / name
    with open(path, "wb") as fh:
        for r in records:
            fh.write(r.as_marc())
    return str(path)


# ── _build_value_index ───────────────────────────────────────────────

class TestBuildValueIndex:
    def test_counts_distinct_subfield_values(self, tmp_path):
        records = [
            _make_record([_data("852", ["b", "MAIN", "c", "STACKS"])]),
            _make_record([_data("852", ["b", "MAIN", "c", "JUV"])]),
            _make_record([_data("852", ["b", "BRANCH1", "c", "STACKS"])]),
            _make_record([_data("852", ["b", "MAIN", "c", "STACKS"])]),
        ]
        path = _write_marc(tmp_path, records)
        idx = _build_value_index([path])

        assert "852" in idx
        b_values = dict(idx["852"]["b"])
        assert b_values["MAIN"] == 3
        assert b_values["BRANCH1"] == 1
        c_values = dict(idx["852"]["c"])
        assert c_values["STACKS"] == 3
        assert c_values["JUV"] == 1

    def test_truncates_long_values(self, tmp_path):
        long_val = "X" * 200
        records = [_make_record([_data("245", ["a", long_val])])]
        path = _write_marc(tmp_path, records)
        idx = _build_value_index([path])

        values = dict(idx["245"]["a"])
        assert not any(len(v) > 121 for v in values), "values should be truncated"
        assert any(v.endswith("…") for v in values)

    def test_respects_top_n(self, tmp_path):
        # Produce 5 distinct values with varying frequencies
        records = []
        for val, count in [("AAA", 5), ("BBB", 4), ("CCC", 3), ("DDD", 2), ("EEE", 1)]:
            for _ in range(count):
                records.append(_make_record([_data("852", ["b", val])]))
        path = _write_marc(tmp_path, records)
        idx = _build_value_index([path], top_n=3)

        top = idx["852"]["b"]
        assert len(top) == 3
        # Sorted descending by count
        assert [v for v, _ in top] == ["AAA", "BBB", "CCC"]

    def test_caps_scanned_records_per_file(self, tmp_path):
        records = [_make_record([_data("245", ["a", f"v{i}"])]) for i in range(100)]
        path = _write_marc(tmp_path, records)
        idx = _build_value_index([path], max_records_per_file=10, top_n=50)

        values = idx["245"]["a"]
        # Cap means only 10 records were scanned → at most 10 distinct vals
        assert sum(c for _, c in values) <= 10

    def test_control_field_values_collapsed_under_underscore(self, tmp_path):
        records = [
            _make_record([pymarc.Field(tag="001", data="REC-1")]),
            _make_record([pymarc.Field(tag="001", data="REC-2")]),
            _make_record([pymarc.Field(tag="001", data="REC-1")]),
        ]
        path = _write_marc(tmp_path, records)
        idx = _build_value_index([path])

        # Control fields have no subfields — they live under the synthetic "_"
        assert "001" in idx
        assert "_" in idx["001"]
        counts = dict(idx["001"]["_"])
        assert counts["REC-1"] == 2
        assert counts["REC-2"] == 1

    def test_skips_empty_values(self, tmp_path):
        records = [
            _make_record([_data("852", ["b", "MAIN", "c", ""])]),
            _make_record([_data("852", ["b", "   ", "c", "STACKS"])]),
        ]
        path = _write_marc(tmp_path, records)
        idx = _build_value_index([path])

        # Empty / whitespace-only values should never appear in the index
        b_vals = dict(idx.get("852", {}).get("b", []))
        c_vals = dict(idx.get("852", {}).get("c", []))
        assert "" not in b_vals and "   " not in b_vals
        assert "" not in c_vals

    def test_missing_file_silently_skipped(self, tmp_path):
        # Non-existent path is tolerated — value-aware mapping must never
        # break the AI suggest flow.
        idx = _build_value_index([str(tmp_path / "nope.mrc")])
        assert idx == {}


# ── _format_value_index ─────────────────────────────────────────────

class TestFormatValueIndex:
    def test_empty_index_returns_placeholder(self):
        out = _format_value_index({})
        assert "no value samples" in out.lower()

    def test_rendered_output_contains_tag_and_counts(self):
        idx = {
            "852": {
                "b": [("MAIN", 12483), ("BRANCH1", 4021)],
                "c": [("STACKS", 8000)],
            }
        }
        out = _format_value_index(idx)
        assert "852" in out
        assert "$b" in out
        assert "$c" in out
        assert "MAIN" in out
        assert "12,483" in out   # thousands formatting
        assert "8,000" in out

    def test_control_field_renders_without_subfield_label(self):
        idx = {"001": {"_": [("ABC-1", 10), ("ABC-2", 5)]}}
        out = _format_value_index(idx)
        # Control-field marker "_" should not appear as "$_"
        assert "$_" not in out
        assert "001" in out

    def test_suppresses_one_off_values(self):
        # Single distinct value with only 3 occurrences — below both
        # thresholds; the helper should skip it to save prompt tokens.
        idx = {"900": {"a": [("ONCE", 3)]}}
        out = _format_value_index(idx)
        assert "ONCE" not in out
