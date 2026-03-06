"""
cerulean/tasks/ingest.py
─────────────────────────────────────────────────────────────────────────────
Stage 1 Celery tasks:

    ingest_marc_task     — parse uploaded MARC file, count records, index to DB
    detect_ils_task      — run ILS detection heuristics on an indexed file
    tag_frequency_task   — build full tag frequency histogram for a file

All tasks write AuditEvent rows via AuditLogger.
"""

import os
from collections import Counter
from pathlib import Path

import pymarc
from celery import shared_task
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)


# ══════════════════════════════════════════════════════════════════════════
# INGEST MARC TASK
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, name="cerulean.tasks.ingest.ingest_marc_task")
def ingest_marc_task(self, project_id: str, file_id: str, storage_path: str) -> dict:
    """
    Parse a MARC file, count records, and mark it indexed.

    Args:
        project_id: UUID string of the owning project.
        file_id: UUID string of the MARCFile row.
        storage_path: Absolute path to the uploaded .mrc file.

    Returns:
        dict with record_count and file_format detected.
    """
    from cerulean.models import MARCFile

    log = AuditLogger(project_id=project_id, stage=1, tag="[ingest]")
    log.info(f"Parsing {Path(storage_path).name}")

    try:
        _set_file_status(file_id, "indexing")

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

        return {"record_count": record_count, "file_format": file_format}

    except Exception as exc:
        log.error(f"Ingest failed: {exc}")
        _set_file_status(file_id, "error", str(exc))
        raise


# ══════════════════════════════════════════════════════════════════════════
# ILS DETECTION TASK
# ══════════════════════════════════════════════════════════════════════════

# ILS detection rules: (ils_name, heuristic_fn)
# Each heuristic receives a list of pymarc Record objects (sample).
# Returns (ils_name, confidence) or (None, 0.0).

_ILS_HEURISTICS = []


def ils_heuristic(name: str):
    """Decorator to register an ILS detection heuristic."""
    def decorator(fn):
        _ILS_HEURISTICS.append((name, fn))
        return fn
    return decorator


@ils_heuristic("SirsiDynix Symphony")
def _detect_symphony(records: list) -> tuple[str | None, float]:
    # Symphony exports typically have 035$a with "(Sirsi)" or 999$l location codes
    hits = sum(
        1 for r in records
        if any("Sirsi" in (f["a"] or "") for f in r.get_fields("035") if f["a"])
        or any(f.tag == "999" for f in r.fields)
    )
    conf = hits / max(len(records), 1)
    return ("SirsiDynix Symphony", conf) if conf > 0.3 else (None, 0.0)


@ils_heuristic("Innovative Interfaces (III) Millennium / Sierra")
def _detect_iii(records: list) -> tuple[str | None, float]:
    # III exports often have 907 (bib record number) or 945 (item) local fields
    hits = sum(
        1 for r in records
        if r.get_fields("907") or r.get_fields("945")
    )
    conf = hits / max(len(records), 1)
    return ("Innovative Interfaces (III)", conf) if conf > 0.4 else (None, 0.0)


@ils_heuristic("Polaris")
def _detect_polaris(records: list) -> tuple[str | None, float]:
    # Polaris uses 949 for items and often 035$a "(PLS)"
    hits = sum(
        1 for r in records
        if r.get_fields("949")
        or any("PLS" in (f["a"] or "") for f in r.get_fields("035") if f["a"])
    )
    conf = hits / max(len(records), 1)
    return ("Polaris", conf) if conf > 0.35 else (None, 0.0)


@ils_heuristic("Koha (already Koha)")
def _detect_koha(records: list) -> tuple[str | None, float]:
    # Data that's already in Koha will have 952 item fields
    hits = sum(1 for r in records if r.get_fields("952"))
    conf = hits / max(len(records), 1)
    return ("Koha", conf) if conf > 0.5 else (None, 0.0)


@celery_app.task(bind=True, name="cerulean.tasks.ingest.detect_ils_task")
def detect_ils_task(self, project_id: str, file_id: str, storage_path: str) -> dict:
    """
    Run ILS detection heuristics on a MARC file.
    Samples up to 200 records for speed.
    Updates both MARCFile.ils_signal and Project.source_ils (if not already set).
    """
    from cerulean.models import MARCFile, Project

    log = AuditLogger(project_id=project_id, stage=1, tag="[detect]")
    log.info("Running ILS detection heuristics")

    try:
        sample = _read_sample(storage_path, max_records=200)

        best_ils, best_conf = None, 0.0
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

        return {"ils": best_ils, "confidence": best_conf}

    except Exception as exc:
        log.error(f"ILS detection failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# TAG FREQUENCY TASK
# ══════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, name="cerulean.tasks.ingest.tag_frequency_task")
def tag_frequency_task(self, project_id: str, file_id: str, storage_path: str) -> dict:
    """
    Build a complete tag frequency histogram across all records in a file.
    Stored as JSONB on MARCFile.tag_frequency: {"001": 42118, "245": 42118, ...}
    Sorted descending by count.
    """
    from cerulean.models import MARCFile

    log = AuditLogger(project_id=project_id, stage=1, tag="[ingest]")
    log.info("Building tag frequency histogram")

    try:
        counter: Counter = Counter()
        record_count = 0
        milestone = 10_000

        for record in _iter_marc(storage_path):
            for field in record.fields:
                counter[field.tag] += 1
            record_count += 1
            if record_count % milestone == 0:
                self.update_state(
                    state="PROGRESS",
                    meta={"records_processed": record_count},
                )
                log.info(f"Tag frequency: {record_count:,} records processed")

        # Sort by count descending
        tag_freq = dict(sorted(counter.items(), key=lambda x: x[1], reverse=True))

        with Session(_engine) as db:
            db.execute(
                update(MARCFile)
                .where(MARCFile.id == file_id)
                .values(tag_frequency=tag_freq)
            )
            db.commit()

        log.complete(f"Tag frequency complete — {len(tag_freq)} unique tags across {record_count:,} records")
        return {"unique_tags": len(tag_freq), "record_count": record_count}

    except Exception as exc:
        log.error(f"Tag frequency task failed: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _parse_marc_file(path: str) -> tuple[str, int]:
    """
    Detect file format and count records.

    Returns:
        (file_format, record_count)
        file_format is "iso2709" or "mrk"
    """
    path_lower = path.lower()
    if path_lower.endswith(".mrk") or path_lower.endswith(".txt"):
        fmt = "mrk"
    else:
        fmt = "iso2709"

    count = sum(1 for _ in _iter_marc(path, fmt))
    return fmt, count


def _iter_marc(path: str, fmt: str = "iso2709"):
    """Yield pymarc Record objects from a file."""
    if fmt == "mrk":
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            yield from pymarc.MARCReader(fh)
    else:
        with open(path, "rb") as fh:
            reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
            yield from reader


def _read_sample(path: str, max_records: int = 200) -> list:
    """Read up to max_records records from a MARC file for heuristic analysis."""
    sample = []
    for record in _iter_marc(path):
        sample.append(record)
        if len(sample) >= max_records:
            break
    return sample


def _set_file_status(file_id: str, status: str, error_message: str | None = None) -> None:
    """Update MARCFile.status synchronously."""
    from cerulean.models import MARCFile
    with Session(_engine) as db:
        db.execute(
            update(MARCFile)
            .where(MARCFile.id == file_id)
            .values(status=status, error_message=error_message)
        )
        db.commit()
