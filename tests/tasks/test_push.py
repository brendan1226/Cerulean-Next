"""Unit tests for cerulean/tasks/push.py helper functions."""

import pytest

from cerulean.tasks.push import _iter_marc


# ══════════════════════════════════════════════════════════════════════
# _iter_marc
# ══════════════════════════════════════════════════════════════════════


class TestIterMarc:
    def test_valid_marc_file(self, tmp_path):
        """Test reading a valid MARC file."""
        import pymarc
        from pymarc import Subfield

        # Create a test MARC file
        record = pymarc.Record()
        record.add_field(pymarc.Field(tag="001", data="test001"))
        record.add_field(pymarc.Field(
            tag="245", indicators=[" ", " "],
            subfields=[Subfield(code="a", value="Test Title")],
        ))

        marc_path = tmp_path / "test.mrc"
        with open(str(marc_path), "wb") as fh:
            fh.write(record.as_marc())

        records = list(_iter_marc(str(marc_path)))
        assert len(records) == 1
        assert records[0]["001"].data == "test001"
        assert records[0]["245"]["a"] == "Test Title"

    def test_multiple_records(self, tmp_path):
        """Test reading multiple MARC records."""
        import pymarc

        marc_path = tmp_path / "multi.mrc"
        with open(str(marc_path), "wb") as fh:
            for i in range(5):
                record = pymarc.Record()
                record.add_field(pymarc.Field(tag="001", data=f"rec{i:03d}"))
                fh.write(record.as_marc())

        records = list(_iter_marc(str(marc_path)))
        assert len(records) == 5
        assert records[0]["001"].data == "rec000"
        assert records[4]["001"].data == "rec004"

    def test_missing_file_raises(self, tmp_path):
        """Test that missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            list(_iter_marc(str(tmp_path / "nonexistent.mrc")))


# ══════════════════════════════════════════════════════════════════════
# Model imports
# ══════════════════════════════════════════════════════════════════════


class TestModelImports:
    def test_push_manifest_importable(self):
        from cerulean.models import PushManifest
        assert PushManifest.__tablename__ == "push_manifests"

    def test_sandbox_instance_importable(self):
        from cerulean.models import SandboxInstance
        assert SandboxInstance.__tablename__ == "sandbox_instances"

    def test_project_has_push_manifests_relationship(self):
        from cerulean.models import Project
        mapper = Project.__mapper__
        assert "push_manifests" in mapper.relationships

    def test_project_has_sandbox_instances_relationship(self):
        from cerulean.models import Project
        mapper = Project.__mapper__
        assert "sandbox_instances" in mapper.relationships
