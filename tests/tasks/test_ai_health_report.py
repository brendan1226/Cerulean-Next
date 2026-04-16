"""Unit tests for cerulean/tasks/analyze.py Data Health Report helpers (Phase 3).

Covers ``_parse_health_report`` and ``_health_stratified_sample``.
``data_health_report_task`` itself needs a live Anthropic client + DB, so
we test the pure pieces here and rely on manual verification for the
network-bound path.
"""

import json

import pymarc
import pytest
from pymarc import Subfield

from cerulean.tasks.analyze import (
    _health_stratified_sample,
    _parse_health_report,
)


# ── _parse_health_report ─────────────────────────────────────────────

class TestParseHealthReport:
    def test_plain_json_object_roundtrips(self):
        payload = {
            "summary": "Looks OK",
            "findings": [
                {"severity": "info", "category": "completeness",
                 "title": "All 245s present", "detail": ""},
            ],
        }
        out = _parse_health_report(json.dumps(payload))
        assert out is not None
        assert out["summary"] == "Looks OK"
        assert len(out["findings"]) == 1

    def test_strips_markdown_fences(self):
        raw = '```json\n{"summary": "x", "findings": []}\n```'
        out = _parse_health_report(raw)
        assert out is not None
        assert out["summary"] == "x"

    def test_strips_trailing_prose(self):
        raw = 'Sure, here is the report:\n{"summary": "x", "findings": []}\nHope this helps!'
        out = _parse_health_report(raw)
        assert out is not None
        assert out["summary"] == "x"

    def test_invalid_json_returns_none(self):
        assert _parse_health_report("not json at all") is None
        assert _parse_health_report("{broken") is None
        assert _parse_health_report("") is None

    def test_array_payload_rejected(self):
        # The schema is a dict; an array top-level is wrong.
        assert _parse_health_report("[]") is None

    def test_missing_findings_key_tolerated(self):
        out = _parse_health_report('{"summary": "hi"}')
        assert out is not None
        assert out["findings"] == []

    def test_findings_sorted_action_required_first(self):
        raw = json.dumps({
            "findings": [
                {"severity": "info",            "title": "A", "detail": ""},
                {"severity": "action_required", "title": "B", "detail": ""},
                {"severity": "warning",         "title": "C", "detail": ""},
                {"severity": "action_required", "title": "D", "detail": ""},
            ]
        })
        out = _parse_health_report(raw)
        severities = [f["severity"] for f in out["findings"]]
        # action_required first, then warning, then info — stable within tier
        assert severities == [
            "action_required", "action_required", "warning", "info",
        ]

    def test_finding_shape_is_normalised(self):
        raw = json.dumps({"findings": [{"foo": "bar"}]})
        out = _parse_health_report(raw)
        assert out["findings"][0]["severity"] == "info"
        assert out["findings"][0]["category"] == "other"
        assert out["findings"][0]["title"] == "(untitled)"
        assert out["findings"][0]["detail"] == ""
        assert "record_count_estimate" in out["findings"][0]
        assert "tag_hint" in out["findings"][0]

    def test_non_dict_findings_entries_dropped(self):
        raw = json.dumps({"findings": [
            "not a dict",
            42,
            {"severity": "info", "title": "keep"},
        ]})
        out = _parse_health_report(raw)
        assert len(out["findings"]) == 1
        assert out["findings"][0]["title"] == "keep"


# ── _health_stratified_sample ────────────────────────────────────────

def _record_with_title(n):
    r = pymarc.Record()
    r.add_field(pymarc.Field(
        tag="245", indicators=[" ", " "],
        subfields=[Subfield(code="a", value=f"Record {n}")],
    ))
    return r


def _record_no_title():
    r = pymarc.Record()
    r.add_field(pymarc.Field(tag="001", data="NOTITLE"))
    return r


def _write(tmp_path, records):
    path = tmp_path / "sample.mrc"
    with open(path, "wb") as fh:
        for r in records:
            fh.write(r.as_marc())
    return str(path)


class TestHealthStratifiedSample:
    def test_returns_empty_list_for_missing_file(self, tmp_path):
        out = _health_stratified_sample(
            str(tmp_path / "nope.mrc"), first_n=5, random_n=5, scan_cap=100,
        )
        assert out == []

    def test_skips_records_without_245(self, tmp_path):
        records = [_record_no_title(), _record_no_title(), _record_with_title(1)]
        path = _write(tmp_path, records)
        out = _health_stratified_sample(path, first_n=10, random_n=0, scan_cap=100)
        assert len(out) == 1
        assert out[0].get_fields("245")

    def test_first_n_slice_taken_in_order(self, tmp_path):
        records = [_record_with_title(i) for i in range(20)]
        path = _write(tmp_path, records)
        out = _health_stratified_sample(path, first_n=5, random_n=0, scan_cap=100)
        assert len(out) == 5
        titles = [r.get_fields("245")[0].get_subfields("a")[0] for r in out]
        assert titles == ["Record 0", "Record 1", "Record 2", "Record 3", "Record 4"]

    def test_reservoir_samples_without_blowing_memory(self, tmp_path):
        # Build 200 records; ask for first 10 + random 20 from remaining 190.
        records = [_record_with_title(i) for i in range(200)]
        path = _write(tmp_path, records)
        out = _health_stratified_sample(path, first_n=10, random_n=20, scan_cap=500)
        assert len(out) == 30

    def test_scan_cap_limits_random_pool(self, tmp_path):
        # 1000 records, scan_cap=50 → we see the first 10 + however many
        # post-first we hit before the 50-record cap kicks in.
        records = [_record_with_title(i) for i in range(1000)]
        path = _write(tmp_path, records)
        out = _health_stratified_sample(path, first_n=10, random_n=20, scan_cap=50)
        # 10 first + at most 20 reservoir (all filled from records 11..50)
        assert 10 <= len(out) <= 30
