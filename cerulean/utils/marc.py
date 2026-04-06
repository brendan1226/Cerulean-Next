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


# ── UTF-8 validation & repair ────────────────────────────────────────────


def scan_utf8_issues(path: str, max_samples: int = 5) -> dict:
    """Scan a MARC file for records that will fail strict UTF-8 decode.

    Even MARC files whose bytes are overall valid UTF-8 can contain records
    where field-level decode boundaries land mid-codepoint (a known quirk
    of Perl's MARC::File::USMARC when parsing byte-length fields containing
    UTF-8 multi-byte chars).  Such records cause downstream Koha imports
    to abort the enclosing transaction.

    Strategy: re-parse each record with pymarc in ``utf8_handling='strict'``
    and ``to_unicode=True``.  Records that fail reveal the same fields that
    Koha's Perl stack will fail on.

    Returns:
        {
            'total_records': int,
            'bad_record_indices': list[int],
            'bad_record_count': int,
            'samples': list[{'index', 'leader', 'error', 'sample_field'}],
        }
    """
    total = 0
    bad_indices: list[int] = []
    samples: list[dict] = []

    # First pass: strict pymarc decode — catches byte-level UTF-8 errors.
    with open(path, "rb") as fh:
        reader = pymarc.MARCReader(
            fh, to_unicode=True, force_utf8=True, utf8_handling="strict"
        )
        while True:
            try:
                rec = next(reader, None)
            except (UnicodeDecodeError, pymarc.exceptions.RecordLengthInvalid,
                    pymarc.exceptions.BaseAddressInvalid,
                    pymarc.exceptions.RecordLeaderInvalid) as exc:
                bad_indices.append(total)
                if len(samples) < max_samples:
                    samples.append({
                        "index": total,
                        "leader": "",
                        "error": f"{type(exc).__name__}: {exc}"[:200],
                        "sample_field": None,
                    })
                total += 1
                continue
            except StopIteration:
                break
            if rec is None:
                break
            total += 1

    # Second pass: count records that contain non-ASCII bytes (informational).
    # These are NOT necessarily bad — Koha handles UTF-8 correctly in most
    # cases — but historically some Perl MARC parsers have had edge-case
    # failures on specific byte-boundary interactions.  We surface the
    # count so users can gauge risk and, if needed, transliterate.
    nonascii_record_count = 0
    with open(path, "rb") as fh:
        data = fh.read()
    starts = [0]
    for i, b in enumerate(data):
        if b == 0x1D and i + 1 < len(data):
            starts.append(i + 1)
    for rec_idx in range(len(starts) - 1):
        rec_bytes = data[starts[rec_idx]:starts[rec_idx + 1]]
        if any(b >= 0x80 for b in rec_bytes):
            nonascii_record_count += 1

    # Second pass: for records in bad_indices, try to describe the fields
    # that contain multi-byte UTF-8 so we know which ones to strip/repair.
    if bad_indices:
        bad_set = set(bad_indices)
        with open(path, "rb") as fh:
            reader = pymarc.MARCReader(
                fh, to_unicode=True, force_utf8=True, utf8_handling="replace"
            )
            for idx, rec in enumerate(reader):
                if idx not in bad_set or rec is None:
                    continue
                # Find a sample replacement-char-bearing field
                sample_field = None
                for f in rec.fields:
                    vals = [f.data] if f.is_control_field() else [sf.value for sf in f.subfields]
                    for v in vals:
                        if "\ufffd" in v or any(ord(c) > 127 for c in v):
                            sample_field = f"{f.tag}: {str(f)[:80]}"
                            break
                    if sample_field:
                        break
                for s in samples:
                    if s["index"] == idx:
                        s["leader"] = rec.leader if rec.leader else ""
                        s["sample_field"] = sample_field
                        break
                if idx > max(bad_indices):
                    break

    return {
        "total_records": total,
        "bad_record_count": len(bad_indices),
        "bad_record_indices": bad_indices,
        "nonascii_record_count": nonascii_record_count,
        "samples": samples,
    }


def repair_utf8(src_path: str, dst_path: str,
                mode: str = "transliterate") -> dict:
    """Rewrite a MARC file to eliminate UTF-8 decode hazards.

    Args:
        src_path: input MARC file
        dst_path: output MARC file
        mode:
          - 'skip'           — drop records that fail strict UTF-8 decode.
          - 'transliterate'  — keep records, but ASCII-fold non-ASCII chars
                               (é→e, ñ→n) in affected fields.  Lossy but
                               preserves record counts.
          - 'replace'        — keep records, replace un-decodable bytes with
                               U+FFFD.  Sometimes leaves the same hazards.

    Returns:
        {'kept', 'skipped', 'repaired_records', 'repaired_fields'}
    """
    if mode not in ("skip", "transliterate", "replace"):
        raise ValueError(f"unknown mode: {mode}")

    # Identify bad records first
    scan = scan_utf8_issues(src_path)
    bad = set(scan["bad_record_indices"])

    # Lazy-import unidecode only when needed
    _unidecode = None
    if mode == "transliterate" and bad:
        try:
            from unidecode import unidecode as _unidecode
        except ImportError:
            raise RuntimeError(
                "transliterate mode requires the 'unidecode' package "
                "(pip install unidecode)"
            )

    kept = 0
    skipped = 0
    repaired_records = 0
    repaired_fields = 0

    with open(src_path, "rb") as fin, open(dst_path, "wb") as fout:
        reader = pymarc.MARCReader(
            fin, to_unicode=True, force_utf8=True, utf8_handling="replace"
        )
        writer = pymarc.MARCWriter(fout)
        for idx, rec in enumerate(reader):
            if rec is None:
                continue
            if idx not in bad:
                writer.write(rec)
                kept += 1
                continue

            # This record was flagged as bad
            if mode == "skip":
                skipped += 1
                continue

            if mode == "replace":
                # pymarc already applied replace during decode; just rewrite
                writer.write(rec)
                kept += 1
                repaired_records += 1
                continue

            # mode == 'transliterate'
            touched = False
            for f in rec.fields:
                if f.is_control_field():
                    old = f.data
                    new = _unidecode(old)
                    if new != old:
                        f.data = new
                        repaired_fields += 1
                        touched = True
                else:
                    new_subfields = []
                    for sf in f.subfields:
                        old = sf.value
                        new = _unidecode(old)
                        if new != old:
                            repaired_fields += 1
                            touched = True
                        new_subfields.append(pymarc.Subfield(code=sf.code, value=new))
                    f.subfields = new_subfields
            if touched:
                repaired_records += 1
            writer.write(rec)
            kept += 1

    return {
        "mode": mode,
        "total_records": scan["total_records"],
        "bad_record_count": scan["bad_record_count"],
        "kept": kept,
        "skipped": skipped,
        "repaired_records": repaired_records,
        "repaired_fields": repaired_fields,
    }
