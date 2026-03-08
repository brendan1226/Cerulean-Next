"""
cerulean/utils/marc.py
─────────────────────────────────────────────────────────────────────────────
Shared MARC file helpers used across multiple task modules.
"""

from typing import Generator

import pymarc


def iter_marc(
    path: str, fmt: str = "iso2709"
) -> Generator[pymarc.Record, None, None]:
    """Yield pymarc Record objects from a MARC file.

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
            yield from pymarc.MARCReader(fh)
    else:
        with open(path, "rb") as fh:
            reader = pymarc.MARCReader(
                fh, to_unicode=True, force_utf8=True, utf8_handling="replace",
            )
            yield from reader


def get_001(record: pymarc.Record) -> str | None:
    """Extract 001 control number from a record."""
    fields = record.get_fields("001")
    return fields[0].data if fields else None


def write_marc(records: list[pymarc.Record], output_path: str) -> None:
    """Write a list of pymarc Records to an ISO2709 file."""
    with open(output_path, "wb") as fh:
        for record in records:
            fh.write(record.as_marc())
