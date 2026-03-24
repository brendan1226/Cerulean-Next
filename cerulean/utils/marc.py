"""
cerulean/utils/marc.py
─────────────────────────────────────────────────────────────────────────────
Shared MARC file helpers used across multiple task modules.
"""

import re
from typing import Generator

import pymarc

# XML 1.0 only allows #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF].
# Control chars 0x00-0x08, 0x0B-0x0C, 0x0E-0x1F (includes ESC 0x1B) are illegal.
_XML_ILLEGAL_CHARS = re.compile(
    r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]'
)


def is_valid_marc(path: str) -> tuple[bool, str]:
    """Check if a file contains valid MARC data.

    Reads the first few bytes / records to determine if the file
    is valid ISO2709 or MRK format.

    Returns:
        (is_valid, format_or_error_message)
    """
    import os

    if not os.path.isfile(path):
        return False, "File not found"

    file_size = os.path.getsize(path)
    if file_size == 0:
        return False, "File is empty"

    # Check binary ISO2709: first 5 bytes should be ASCII digits (record length)
    try:
        with open(path, "rb") as fh:
            header = fh.read(24)
            if len(header) >= 5 and header[:5].isdigit():
                # Looks like ISO2709 leader — try to parse first record
                fh.seek(0)
                reader = pymarc.MARCReader(
                    fh, to_unicode=True, force_utf8=True, utf8_handling="replace",
                )
                try:
                    record = next(reader, None)
                    if record and record.fields:
                        return True, "iso2709"
                except Exception:
                    pass
    except Exception:
        pass

    # Check MRK text format: first non-blank line starts with = + 3-digit tag
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.startswith("=") and len(stripped) >= 4 and stripped[1:4].isdigit():
                    return True, "mrk"
                break  # first non-blank line didn't match
    except Exception:
        pass

    return False, "File does not appear to contain MARC data"


def sanitize_record(record: pymarc.Record) -> pymarc.Record:
    """Strip XML-illegal control characters from all fields in a MARC record.

    Characters like ESC (0x1B), NUL, and other C0 control chars (except
    TAB, LF, CR) are invalid in XML 1.0 and cause Koha/Zebra indexing
    failures. This strips them from field data and subfield values in-place.
    """
    for field in record.fields:
        if field.is_control_field():
            if field.data:
                field.data = _XML_ILLEGAL_CHARS.sub('', field.data)
        else:
            field.subfields = [
                sf._replace(value=_XML_ILLEGAL_CHARS.sub('', sf.value))
                if sf.value and _XML_ILLEGAL_CHARS.search(sf.value)
                else sf
                for sf in field.subfields
            ]
    return record


def iter_marc(
    path: str, fmt: str = "iso2709"
) -> Generator[pymarc.Record, None, None]:
    """Yield pymarc Record objects from a MARC file.

    Records are sanitized to strip XML-illegal control characters
    (ESC, NUL, etc.) that cause Koha/Zebra indexing failures.

    Args:
        path: Absolute path to the MARC file.
        fmt: Format identifier — "iso2709" (binary) or "mrk" (MARCMaker text).

    Raises:
        FileNotFoundError: If the file does not exist.

    Yields:
        pymarc.Record instances parsed from the file.
    """
    if fmt == "mrk":
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for record in pymarc.MARCReader(fh):
                if record:
                    yield sanitize_record(record)
    else:
        with open(path, "rb") as fh:
            reader = pymarc.MARCReader(
                fh, to_unicode=True, force_utf8=True, utf8_handling="replace",
            )
            for record in reader:
                if record:
                    yield sanitize_record(record)


def get_001(record: pymarc.Record) -> str | None:
    """Extract 001 control number from a record."""
    fields = record.get_fields("001")
    return fields[0].data if fields else None


def record_to_dict(record: pymarc.Record, index: int = 0) -> dict:
    """Serialise a pymarc Record to a JSON-friendly dict.

    Used by files, transform, and push routers for record previews.
    """
    fields = []
    for field in record.fields:
        if field.is_control_field():
            fields.append({"tag": field.tag, "data": field.data})
        else:
            subs = [{"code": sf.code, "value": sf.value} for sf in field.subfields]
            fields.append({
                "tag": field.tag,
                "ind1": field.indicator1,
                "ind2": field.indicator2,
                "subfields": subs,
            })

    return {
        "index": index,
        "leader": record.leader if record.leader else "",
        "title": record.title or "",
        "fields": fields,
    }


def write_marc(records: list[pymarc.Record], output_path: str) -> None:
    """Write a list of pymarc Records to an ISO2709 file."""
    with open(output_path, "wb") as fh:
        for record in records:
            fh.write(record.as_marc())
