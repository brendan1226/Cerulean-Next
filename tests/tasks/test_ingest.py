"""Unit tests for cerulean/tasks/ingest.py — subfield frequency computation."""

import pymarc
from pymarc import Subfield

from collections import Counter, defaultdict


# ── Reproduce the subfield frequency logic from tag_frequency_task ──
# We test the core algorithm directly rather than calling the Celery task,
# which would require a running database and broker.

def _compute_frequencies(records):
    """Compute tag and subfield frequencies from a list of pymarc Records.

    Returns:
        (tag_freq, subfield_freq) — same structure stored on MARCFile.
    """
    counter = Counter()
    sub_counter = defaultdict(Counter)

    for record in records:
        for field in record.fields:
            counter[field.tag] += 1
            if not field.is_control_field():
                for sf in field.subfields:
                    sub_counter[field.tag][sf.code] += 1

    tag_freq = dict(sorted(counter.items(), key=lambda x: x[1], reverse=True))
    subfield_freq = {tag: dict(subs) for tag, subs in sub_counter.items()}
    return tag_freq, subfield_freq


def _make_record(**kwargs):
    """Create a pymarc Record with specified fields.

    Usage:
        _make_record(
            control={'001': 'test001', '008': 'data'},
            data={'245': [('a', 'Title'), ('b', 'Subtitle')],
                  '100': [('a', 'Author')]},
        )
    """
    record = pymarc.Record()
    for tag, data in (kwargs.get("control") or {}).items():
        record.add_field(pymarc.Field(tag=tag, data=data))
    for tag, subfields in (kwargs.get("data") or {}).items():
        sf_list = [Subfield(code=c, value=v) for c, v in subfields]
        record.add_field(pymarc.Field(tag=tag, indicators=[" ", " "], subfields=sf_list))
    return record


# ══════════════════════════════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════════════════════════════


class TestSubfieldFrequency:
    def test_empty_records(self):
        """No records → empty frequencies."""
        tag_freq, sub_freq = _compute_frequencies([])
        assert tag_freq == {}
        assert sub_freq == {}

    def test_single_record_control_field_only(self):
        """Control fields count toward tag freq but not subfield freq."""
        record = _make_record(control={"001": "12345", "003": "OCoLC"})
        tag_freq, sub_freq = _compute_frequencies([record])
        assert tag_freq == {"001": 1, "003": 1}
        assert sub_freq == {}

    def test_single_record_with_subfields(self):
        """Data fields contribute to both tag and subfield frequency."""
        record = _make_record(
            control={"001": "rec001"},
            data={
                "245": [("a", "Test Title"), ("b", "Subtitle"), ("c", "Author Name")],
                "100": [("a", "Smith, John")],
            },
        )
        tag_freq, sub_freq = _compute_frequencies([record])

        assert tag_freq["001"] == 1
        assert tag_freq["245"] == 1
        assert tag_freq["100"] == 1

        assert sub_freq["245"] == {"a": 1, "b": 1, "c": 1}
        assert sub_freq["100"] == {"a": 1}
        assert "001" not in sub_freq  # control field

    def test_multiple_records_accumulate(self):
        """Frequencies accumulate across records."""
        records = [
            _make_record(
                control={"001": "r1"},
                data={"245": [("a", "Title 1")], "650": [("a", "Subject")]},
            ),
            _make_record(
                control={"001": "r2"},
                data={"245": [("a", "Title 2"), ("b", "Sub")], "650": [("a", "Subject"), ("x", "General")]},
            ),
            _make_record(
                control={"001": "r3"},
                data={"245": [("a", "Title 3")]},
            ),
        ]
        tag_freq, sub_freq = _compute_frequencies(records)

        assert tag_freq["001"] == 3
        assert tag_freq["245"] == 3
        assert tag_freq["650"] == 2

        assert sub_freq["245"]["a"] == 3
        assert sub_freq["245"]["b"] == 1
        assert sub_freq["650"]["a"] == 2
        assert sub_freq["650"]["x"] == 1

    def test_tag_frequency_sorted_descending(self):
        """Tag frequency dict is sorted by count descending."""
        records = [
            _make_record(data={
                "245": [("a", "T")],
                "650": [("a", "S1")],
                "852": [("a", "Loc")],
            }),
            _make_record(data={
                "245": [("a", "T")],
                "650": [("a", "S2")],
            }),
            _make_record(data={
                "245": [("a", "T")],
            }),
        ]
        tag_freq, _ = _compute_frequencies(records)
        counts = list(tag_freq.values())
        assert counts == sorted(counts, reverse=True)
        assert list(tag_freq.keys())[0] == "245"

    def test_repeated_subfields_in_same_field(self):
        """Multiple occurrences of same subfield code in one field are counted."""
        record = pymarc.Record()
        record.add_field(pymarc.Field(
            tag="650", indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="Subject 1"),
                Subfield(code="x", value="General 1"),
                Subfield(code="x", value="General 2"),
            ],
        ))
        _, sub_freq = _compute_frequencies([record])
        assert sub_freq["650"]["a"] == 1
        assert sub_freq["650"]["x"] == 2

    def test_multiple_field_occurrences(self):
        """Multiple instances of same tag in one record are each counted."""
        record = pymarc.Record()
        record.add_field(pymarc.Field(
            tag="650", indicators=["0", " "],
            subfields=[Subfield(code="a", value="Topic A")],
        ))
        record.add_field(pymarc.Field(
            tag="650", indicators=["0", " "],
            subfields=[Subfield(code="a", value="Topic B")],
        ))
        tag_freq, sub_freq = _compute_frequencies([record])
        assert tag_freq["650"] == 2
        assert sub_freq["650"]["a"] == 2

    def test_koha_952_subfield_variety(self):
        """952 (Koha item) fields with many subfields are tracked correctly."""
        record = _make_record(data={
            "952": [
                ("a", "MAIN"), ("b", "MAIN"), ("c", "GEN"),
                ("o", "823.914 SMI"), ("p", "T00012345"),
                ("y", "BK"), ("d", "2024-01-15"),
            ],
        })
        _, sub_freq = _compute_frequencies([record])
        assert set(sub_freq["952"].keys()) == {"a", "b", "c", "o", "p", "y", "d"}
        assert all(v == 1 for v in sub_freq["952"].values())


class TestModelChanges:
    def test_project_has_archived_field(self):
        from cerulean.models import Project
        assert hasattr(Project, "archived")
        col = Project.__table__.columns["archived"]
        assert col.default.arg is False

    def test_marcfile_has_subfield_frequency_field(self):
        from cerulean.models import MARCFile
        assert hasattr(MARCFile, "subfield_frequency")
        col = MARCFile.__table__.columns["subfield_frequency"]
        assert col.nullable is True
