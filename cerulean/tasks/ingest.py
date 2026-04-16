"""
cerulean/tasks/ingest.py
─────────────────────────────────────────────────────────────────────────────
Stage 1 Celery tasks:

    ingest_marc_task     — parse uploaded MARC file, count records, index to DB
    detect_ils_task      — run ILS detection heuristics on an indexed file
    tag_frequency_task   — build full tag frequency histogram for a file

All tasks write AuditEvent rows via AuditLogger.
"""

import csv
import os
from collections import Counter, defaultdict
from collections.abc import Callable, Generator
from pathlib import Path

import pymarc
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.utils.marc import iter_marc as _iter_marc, is_valid_marc as _is_valid_marc

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)


# ══════════════════════════════════════════════════════════════════════════
# INGEST MARC TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.ingest.ingest_marc_task")
def ingest_marc_task(
    self,
    project_id: str,
    file_id: str,
    storage_path: str,
    user_id: str | None = None,
) -> dict:
    """Parse a MARC file, count records, and mark it indexed.

    Args:
        project_id: UUID string of the owning project.
        file_id: UUID string of the MARCFile row.
        storage_path: Absolute path to the uploaded .mrc file.
        user_id: Optional UUID of the user who triggered the upload.
            Forwarded to chained AI tasks so they can check per-user
            feature flags (``ai.data_health_report`` etc.).

    Returns:
        dict with record_count and file_format detected.

    Raises:
        Exception: Re-raised after writing an AuditEvent and marking the file error.
    """
    from cerulean.models import MARCFile
    # Late import to avoid a circular import with analyze → preferences → models
    from cerulean.tasks.analyze import data_health_report_task

    log = AuditLogger(project_id=project_id, stage=1, tag="[ingest]")
    log.info(f"Parsing {Path(storage_path).name}")

    try:
        _set_file_status(file_id, "indexing")

        # Validate file contains MARC data before proceeding
        is_marc, detail = _is_valid_marc(storage_path)
        if not is_marc:
            log.warn(f"File is not valid MARC data: {detail}")
            _set_file_status(file_id, "not_marc", detail)
            return {"record_count": 0, "file_format": "unknown", "error": detail}

        file_format, record_count = _parse_marc_file(storage_path)

        with Session(_engine) as db:
            db.execute(
                update(MARCFile)
                .where(MARCFile.id == file_id)
                .values(
                    file_format=file_format,
                    record_count=record_count,
                    status="indexed",
                )
            )
            db.commit()

        log.info(f"Indexed {record_count:,} records ({file_format})")

        # Chain: kick off ILS detection and tag frequency automatically
        detect_ils_task.apply_async(args=[project_id, file_id, storage_path], queue="ingest")
        tag_frequency_task.apply_async(args=[project_id, file_id, storage_path], queue="analyze")

        # Data Health Report — task is self-gating by user preference so we
        # queue it unconditionally. No-op returns instantly when the user
        # hasn't opted in (per cerulean_ai_spec.md §3 / §11.5).
        data_health_report_task.apply_async(
            args=[project_id, file_id],
            kwargs={"user_id": user_id},
            queue="analyze",
        )

        # Index to Elasticsearch if configured (non-blocking)
        try:
            from cerulean.core.search import get_es_client, ensure_index, index_marc_records
            es = get_es_client()
            if es:
                ensure_index(es)
                indexed = index_marc_records(es, project_id, file_id, _iter_marc(storage_path))
                log.info(f"Indexed {indexed:,} records to Elasticsearch")
        except Exception as es_exc:
            log.warn(f"Elasticsearch indexing skipped: {es_exc}")

        log.complete(f"Ingest complete — {record_count:,} records")
        return {"record_count": record_count, "file_format": file_format}

    except Exception as exc:
        log.error(f"Ingest failed: {exc}")
        _set_file_status(file_id, "error", str(exc))
        raise


# ══════════════════════════════════════════════════════════════════════════
# INGEST ITEMS CSV TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.ingest.ingest_items_csv_task")
def ingest_items_csv_task(self, project_id: str, file_id: str, storage_path: str) -> dict:
    """Parse an items CSV file — detect delimiter, read headers, count rows, collect samples.

    Stores column_headers, record_count, tag_frequency (as populated counts),
    and subfield_frequency (as unique counts + samples) on the MARCFile row.

    Args:
        project_id: UUID of the owning project.
        file_id: UUID of the MARCFile row.
        storage_path: Absolute path to the uploaded CSV file.

    Returns:
        dict with record_count, column_count, columns.
    """
    from cerulean.models import MARCFile

    log = AuditLogger(project_id=project_id, stage=1, tag="[ingest]")
    log.info(f"Parsing items CSV: {Path(storage_path).name}")

    try:
        _set_file_status(file_id, "indexing")

        # Increase CSV field size limit for large fields (e.g. notes, URLs)
        csv.field_size_limit(10 * 1024 * 1024)  # 10 MB

        # Read and detect delimiter
        with open(storage_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            sample = fh.read(8192)

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","

        # Detect whether the file has a header row
        has_header = _detect_csv_header(sample, delimiter)

        # Parse CSV: headers, row count, samples, populated counts, unique counts
        headers: list[str] = []
        row_count = 0
        samples: dict[str, list[str]] = defaultdict(list)
        populated_count: Counter[str] = Counter()
        unique_values: dict[str, set] = defaultdict(set)

        with open(storage_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            if has_header:
                reader = csv.DictReader(fh, delimiter=delimiter)
                headers = list(reader.fieldnames or [])
            else:
                # No header row — generate column names col_1..col_N
                raw_reader = csv.reader(fh, delimiter=delimiter)
                first_row = next(raw_reader, None)
                if first_row:
                    ncols = len(first_row)
                    headers = [f"col_{i+1}" for i in range(ncols)]
                    # Process first row as data
                    row_dict = dict(zip(headers, first_row))
                    row_count = 1
                    for col in headers:
                        val = (row_dict.get(col) or "").strip()
                        if val and val != "NULL":
                            populated_count[col] += 1
                            unique_values[col].add(val)
                            samples[col].append(val)
                # Wrap remaining rows
                reader = csv.DictReader(fh, fieldnames=headers, delimiter=delimiter)

            for row in reader:
                row_count += 1
                for col in headers:
                    val = (row.get(col) or "").strip()
                    if val and val != "NULL":
                        populated_count[col] += 1
                        unique_values[col].add(val)
                        if len(samples[col]) < 20:
                            samples[col].append(val)

        # Build tag_frequency as {column: populated_count}
        tag_freq = {col: populated_count.get(col, 0) for col in headers}

        # Build subfield_frequency as {column: {unique: N, samples: [...]}}
        sub_freq = {
            col: {
                "unique": len(unique_values.get(col, set())),
                "samples": samples.get(col, []),
            }
            for col in headers
        }

        with Session(_engine) as db:
            db.execute(
                update(MARCFile)
                .where(MARCFile.id == file_id)
                .values(
                    file_format="csv",
                    column_headers=headers,
                    record_count=row_count,
                    tag_frequency=tag_freq,
                    subfield_frequency=sub_freq,
                    status="indexed",
                )
            )
            db.commit()

        log.complete(f"Items CSV indexed — {row_count:,} rows, {len(headers)} columns")
        return {"record_count": row_count, "column_count": len(headers), "columns": headers}

    except Exception as exc:
        log.error(f"Items CSV ingest failed: {exc}")
        _set_file_status(file_id, "error", str(exc))
        raise


# ══════════════════════════════════════════════════════════════════════════
# INGEST ITEMS JSON TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.ingest.ingest_items_json_task")
def ingest_items_json_task(self, project_id: str, file_id: str, storage_path: str) -> dict:
    """Parse an items JSON file — detect structure, read keys, count records, collect samples.

    Stores column_headers, record_count, tag_frequency (as populated counts),
    and subfield_frequency (as unique counts + samples) on the MARCFile row.
    """
    import json as _json
    from cerulean.models import MARCFile

    log = AuditLogger(project_id=project_id, stage=1, tag="[ingest]")
    log.info(f"Parsing items JSON: {Path(storage_path).name}")

    try:
        _set_file_status(file_id, "indexing")

        with open(storage_path, "r", encoding="utf-8", errors="replace") as fh:
            data = _json.load(fh)

        # Unwrap if dict with a known collection key
        if isinstance(data, dict):
            for key in ("items", "records", "data", "rows"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                # If still a dict, wrap single object
                if isinstance(data, dict):
                    data = [data]

        if not isinstance(data, list):
            raise ValueError(f"Expected JSON array, got {type(data).__name__}")

        # Extract unique keys as column_headers
        all_keys: list[str] = []
        seen_keys: set[str] = set()
        for record in data:
            if isinstance(record, dict):
                for k in record:
                    if k not in seen_keys:
                        seen_keys.add(k)
                        all_keys.append(k)

        headers = all_keys
        row_count = len(data)

        # Collect stats
        populated_count: Counter[str] = Counter()
        unique_values: dict[str, set] = defaultdict(set)
        samples: dict[str, list[str]] = defaultdict(list)

        for record in data:
            if not isinstance(record, dict):
                continue
            for col in headers:
                val = str(record.get(col, "")).strip()
                if val and val != "None" and val != "NULL":
                    populated_count[col] += 1
                    unique_values[col].add(val)
                    if len(samples[col]) < 20:
                        samples[col].append(val)

        tag_freq = {col: populated_count.get(col, 0) for col in headers}
        sub_freq = {
            col: {
                "unique": len(unique_values.get(col, set())),
                "samples": samples.get(col, []),
            }
            for col in headers
        }

        with Session(_engine) as db:
            db.execute(
                update(MARCFile)
                .where(MARCFile.id == file_id)
                .values(
                    file_format="json",
                    column_headers=headers,
                    record_count=row_count,
                    tag_frequency=tag_freq,
                    subfield_frequency=sub_freq,
                    status="indexed",
                )
            )
            db.commit()

        log.complete(f"Items JSON indexed — {row_count:,} records, {len(headers)} columns")
        return {"record_count": row_count, "column_count": len(headers), "columns": headers}

    except Exception as exc:
        log.error(f"Items JSON ingest failed: {exc}")
        _set_file_status(file_id, "error", str(exc))
        raise


# ══════════════════════════════════════════════════════════════════════════
# INGEST ITEMS MRC TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.ingest.ingest_items_mrc_task")
def ingest_items_mrc_task(self, project_id: str, file_id: str, storage_path: str) -> dict:
    """Parse a MARC file as items data — flatten fields to TAG$subcode keys.

    Each record is flattened: control fields → "TAG" key, data fields → "TAG$subcode" keys.
    All unique keys become column_headers (e.g. ["001", "852$a", "852$b"]).
    """
    from cerulean.models import MARCFile

    log = AuditLogger(project_id=project_id, stage=1, tag="[ingest]")
    log.info(f"Parsing items MRC: {Path(storage_path).name}")

    try:
        _set_file_status(file_id, "indexing")

        all_keys: list[str] = []
        seen_keys: set[str] = set()
        row_count = 0
        populated_count: Counter[str] = Counter()
        unique_values: dict[str, set] = defaultdict(set)
        samples: dict[str, list[str]] = defaultdict(list)

        for record in _iter_marc(storage_path):
            if record is None:
                continue
            row_count += 1

            for field in record.fields:
                if field.is_control_field():
                    key = field.tag
                    if key not in seen_keys:
                        seen_keys.add(key)
                        all_keys.append(key)
                    val = (field.data or "").strip()
                    if val:
                        populated_count[key] += 1
                        unique_values[key].add(val)
                        if len(samples[key]) < 20:
                            samples[key].append(val)
                else:
                    for sf in field.subfields:
                        key = f"{field.tag}${sf.code}"
                        if key not in seen_keys:
                            seen_keys.add(key)
                            all_keys.append(key)
                        val = (sf.value or "").strip()
                        if val:
                            populated_count[key] += 1
                            unique_values[key].add(val)
                            if len(samples[key]) < 20:
                                samples[key].append(val)

        headers = all_keys
        tag_freq = {col: populated_count.get(col, 0) for col in headers}
        sub_freq = {
            col: {
                "unique": len(unique_values.get(col, set())),
                "samples": samples.get(col, []),
            }
            for col in headers
        }

        with Session(_engine) as db:
            db.execute(
                update(MARCFile)
                .where(MARCFile.id == file_id)
                .values(
                    file_format="iso2709",
                    column_headers=headers,
                    record_count=row_count,
                    tag_frequency=tag_freq,
                    subfield_frequency=sub_freq,
                    status="indexed",
                )
            )
            db.commit()

        log.complete(f"Items MRC indexed — {row_count:,} records, {len(headers)} columns")
        return {"record_count": row_count, "column_count": len(headers), "columns": headers}

    except Exception as exc:
        log.error(f"Items MRC ingest failed: {exc}")
        _set_file_status(file_id, "error", str(exc))
        raise


# ══════════════════════════════════════════════════════════════════════════
# ILS DETECTION TASK
# ══════════════════════════════════════════════════════════════════════════

# ILS detection rules: (ils_name, heuristic_fn)
# Each heuristic receives a list of pymarc Record objects (sample).
# Returns (ils_name, confidence) or (None, 0.0).

_ILS_HEURISTICS: list[tuple[str, Callable[[list[pymarc.Record]], tuple[str | None, float]]]] = []


def ils_heuristic(
    name: str,
) -> Callable[[Callable[[list[pymarc.Record]], tuple[str | None, float]]], Callable]:
    """Decorator to register an ILS detection heuristic.

    Args:
        name: Human-readable ILS name, used as the detection result string.

    Returns:
        Decorator that registers and returns the wrapped function unchanged.
    """

    def decorator(
        fn: Callable[[list[pymarc.Record]], tuple[str | None, float]],
    ) -> Callable[[list[pymarc.Record]], tuple[str | None, float]]:
        _ILS_HEURISTICS.append((name, fn))
        return fn

    return decorator


@ils_heuristic("SirsiDynix Symphony")
def _detect_symphony(records: list[pymarc.Record]) -> tuple[str | None, float]:
    # Symphony exports typically have 035$a with "(Sirsi)" or 999$l location codes
    hits = sum(
        1 for r in records
        if any("Sirsi" in (f["a"] or "") for f in r.get_fields("035") if f["a"])
        or any(f.tag == "999" for f in r.fields)
    )
    conf = hits / max(len(records), 1)
    return ("SirsiDynix Symphony", conf) if conf > 0.3 else (None, 0.0)


@ils_heuristic("Innovative Interfaces (III) Millennium / Sierra")
def _detect_iii(records: list[pymarc.Record]) -> tuple[str | None, float]:
    # III exports often have 907 (bib record number) or 945 (item) local fields
    hits = sum(
        1 for r in records
        if any(f.tag in ("907", "945") for f in r.fields)
    )
    conf = hits / max(len(records), 1)
    return ("Innovative Interfaces (III) Millennium / Sierra", conf) if conf > 0.4 else (None, 0.0)


@ils_heuristic("Polaris")
def _detect_polaris(records: list[pymarc.Record]) -> tuple[str | None, float]:
    # Polaris uses 949 for item data with specific subfield patterns
    hits = sum(
        1 for r in records
        if any(f.tag == "949" and f["t"] for f in r.get_fields("949"))
    )
    conf = hits / max(len(records), 1)
    return ("Polaris", conf) if conf > 0.4 else (None, 0.0)


@ils_heuristic("Koha")
def _detect_koha(records: list[pymarc.Record]) -> tuple[str | None, float]:
    # Koha exports use 952 for items and often have 942 for record-level item type
    hits = sum(
        1 for r in records
        if any(f.tag in ("952", "942") for f in r.fields)
    )
    conf = hits / max(len(records), 1)
    return ("Koha", conf) if conf > 0.5 else (None, 0.0)


@celery_app.task(bind=True, name="cerulean.tasks.ingest.detect_ils_task")
def detect_ils_task(self, project_id: str, file_id: str, storage_path: str) -> dict:
    """Run ILS detection heuristics on a MARC file.

    Samples up to 200 records for speed. Updates both MARCFile.ils_signal
    and Project.source_ils (if not already set).

    Args:
        project_id: UUID string of the owning project.
        file_id: UUID string of the MARCFile row.
        storage_path: Absolute path to the uploaded .mrc file.

    Returns:
        dict with detected ils name and confidence score.

    Raises:
        Exception: Re-raised after writing an AuditEvent.
    """
    from cerulean.models import MARCFile, Project

    log = AuditLogger(project_id=project_id, stage=1, tag="[detect]")
    log.info("Running ILS detection heuristics")

    try:
        # Guard: skip if file is not valid MARC
        is_marc, detail = _is_valid_marc(storage_path)
        if not is_marc:
            log.warn(f"Skipping ILS detection — not valid MARC: {detail}")
            return {"ils": None, "confidence": 0.0, "skipped": True}

        sample = _read_sample(storage_path, max_records=200)

        best_ils: str | None = None
        best_conf: float = 0.0
        for ils_name, heuristic in _ILS_HEURISTICS:
            ils, conf = heuristic(sample)
            if conf > best_conf:
                best_ils, best_conf = ils, conf

        if best_ils:
            log.info(f"Detected ILS: {best_ils} (confidence {best_conf:.0%})")
        else:
            log.warn("ILS detection inconclusive — manual identification required")

        with Session(_engine) as db:
            db.execute(
                update(MARCFile)
                .where(MARCFile.id == file_id)
                .values(ils_signal=best_ils)
            )
            # Set on project only if not already identified
            project = db.get(Project, project_id)
            if project and not project.source_ils and best_ils:
                project.source_ils = best_ils
                project.ils_confidence = best_conf
            db.commit()

        log.complete(f"ILS detection complete — {best_ils or 'unknown'}")
        return {"ils": best_ils, "confidence": best_conf}

    except Exception as exc:
        log.error(f"ILS detection failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# TAG FREQUENCY TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(bind=True, name="cerulean.tasks.ingest.tag_frequency_task")
def tag_frequency_task(self, project_id: str, file_id: str, storage_path: str) -> dict:
    """Build a complete tag frequency histogram across all records in a file.

    Stored as JSONB on MARCFile.tag_frequency: {"001": 42118, "245": 42118, ...}
    Sorted descending by count.

    Args:
        project_id: UUID string of the owning project.
        file_id: UUID string of the MARCFile row.
        storage_path: Absolute path to the .mrc file to analyse.

    Returns:
        dict with unique_tags count and total record_count.

    Raises:
        Exception: Re-raised after writing an AuditEvent.
    """
    from cerulean.models import MARCFile

    log = AuditLogger(project_id=project_id, stage=1, tag="[ingest]")
    log.info("Building tag frequency histogram")

    try:
        # Guard: skip if file is not valid MARC
        is_marc, detail = _is_valid_marc(storage_path)
        if not is_marc:
            log.warn(f"Skipping tag frequency — not valid MARC: {detail}")
            return {"unique_tags": 0, "record_count": 0, "skipped": True}

        counter: Counter[str] = Counter()
        sub_counter: dict[str, Counter] = defaultdict(Counter)
        record_count: int = 0
        milestone = 10_000

        for record in _iter_marc(storage_path):
            if record is None:
                continue
            for field in record.fields:
                counter[field.tag] += 1
                if not field.is_control_field():
                    for sf in field.subfields:
                        sub_counter[field.tag][sf.code] += 1
            record_count += 1
            if record_count % milestone == 0:
                self.update_state(
                    state="PROGRESS",
                    meta={"records_processed": record_count},
                )

        # Sort descending by count for display convenience
        tag_freq = dict(sorted(counter.items(), key=lambda x: x[1], reverse=True))
        subfield_freq = {tag: dict(subs) for tag, subs in sub_counter.items()}

        with Session(_engine) as db:
            db.execute(
                update(MARCFile)
                .where(MARCFile.id == file_id)
                .values(tag_frequency=tag_freq, subfield_frequency=subfield_freq)
            )
            db.commit()

        log.complete(
            f"Tag frequency complete — {len(tag_freq)} unique tags across {record_count:,} records"
        )
        return {"unique_tags": len(tag_freq), "record_count": record_count}

    except Exception as exc:
        log.error(f"Tag frequency task failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════


def _parse_marc_file(path: str) -> tuple[str, int]:
    """Detect file format and count records.

    Args:
        path: Absolute path to the MARC file.

    Returns:
        Tuple of (file_format, record_count).
        file_format is "iso2709" or "mrk".
    """
    path_lower = path.lower()
    if path_lower.endswith(".mrk") or path_lower.endswith(".txt"):
        fmt = "mrk"
    else:
        fmt = "iso2709"

    count = sum(1 for r in _iter_marc(path, fmt) if r is not None)
    return fmt, count



# _iter_marc imported from cerulean.utils.marc


def _read_sample(path: str, max_records: int = 200) -> list[pymarc.Record]:
    """Read up to max_records records from a MARC file for heuristic analysis.

    Args:
        path: Absolute path to the MARC file.
        max_records: Maximum number of records to return. Defaults to 200.

    Returns:
        List of up to max_records pymarc.Record objects.
    """
    sample: list[pymarc.Record] = []
    for record in _iter_marc(path):
        if record is None:
            continue
        sample.append(record)
        if len(sample) >= max_records:
            break
    return sample


def _detect_csv_header(sample: str, delimiter: str = ",") -> bool:
    """Heuristic: does the first line of a CSV look like column headers?

    Checks whether the first row contains unique, identifier-like strings
    (not data values like numbers, dates, barcodes, NULLs, or short codes).
    """
    import re

    lines = sample.strip().split("\n")
    if not lines:
        return False

    first_line = lines[0]
    try:
        fields = next(csv.reader([first_line], delimiter=delimiter))
    except StopIteration:
        return False

    if not fields:
        return False

    stripped = [f.strip() for f in fields]

    # Headers must be unique — duplicate values means it's data
    non_empty = [f for f in stripped if f]
    if len(non_empty) != len(set(non_empty)):
        return False

    # Count how many first-row fields look like headers
    header_like = 0
    for f in stripped:
        if not f:
            continue
        # Disqualify: NULL, numbers, dates, barcodes (long digits), single chars
        if f.upper() == "NULL":
            continue
        if re.match(r"^\d+$", f):  # pure numbers
            continue
        if re.match(r"^\d{2}/\d{2}/\d{4}$", f):  # dates
            continue
        if len(f) <= 2:  # single/double char codes are likely data
            continue
        # Headers are typically 3+ char alphabetic words with underscores/spaces/hashes
        if re.match(r"^[A-Za-z_#][A-Za-z0-9_# ]*$", f) and len(f) >= 3:
            header_like += 1

    # Need a strong majority to call it a header row
    return header_like / max(len(fields), 1) > 0.5


def _set_file_status(
    file_id: str, status: str, error_message: str | None = None
) -> None:
    """Update MARCFile.status synchronously.

    Args:
        file_id: UUID string of the MARCFile row.
        status: New status value (e.g. "indexing", "indexed", "error").
        error_message: Optional error detail written when status is "error".
    """
    from cerulean.models import MARCFile

    with Session(_engine) as db:
        db.execute(
            update(MARCFile)
            .where(MARCFile.id == file_id)
            .values(status=status, error_message=error_message)
        )
        db.commit()
