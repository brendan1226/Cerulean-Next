"""
cerulean/tasks/evergreen.py
─────────────────────────────────────────────────────────────────────────────
Stage 13 Celery tasks: Push to Evergreen ILS.

    evergreen_push_bibs_task    — INSERT MARCXML into biblio.record_entry
    evergreen_pingest_task      — trigger parallel ingest (search indexing)
    evergreen_counts_task       — count bibs/items in Evergreen DB

Evergreen uses PostgreSQL. MARC is stored as MARCXML text in
biblio.record_entry.marc. Search indexing is done by pingest.pl which
populates metabib.* tables within the same PostgreSQL database.

Direct database approach is the standard Evergreen migration path.
"""

from datetime import datetime
from pathlib import Path

import pymarc
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.helpers import check_paused as _check_paused

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)

_PROGRESS_INTERVAL = 500


def _update_push_manifest(db: Session, manifest_id: str, **kwargs) -> None:
    from cerulean.models import PushManifest
    db.execute(
        update(PushManifest)
        .where(PushManifest.id == manifest_id)
        .values(**kwargs)
    )
    db.commit()


def _evergreen_conn(project_id: str):
    """Open a psycopg2 connection to the Evergreen PostgreSQL database.

    Reads connection params from the Project record.
    """
    import psycopg2
    from cerulean.models import Project

    with Session(_engine) as db:
        project = db.get(Project, project_id)
        if not project:
            raise ValueError("Project not found")
        if not project.evergreen_db_host:
            raise ValueError("Evergreen DB host not configured")

        return psycopg2.connect(
            host=project.evergreen_db_host,
            port=project.evergreen_db_port or 5432,
            dbname=project.evergreen_db_name or "evergreen",
            user=project.evergreen_db_user or "evergreen",
            password=project.evergreen_db_password or "",
            connect_timeout=15,
        )


def _find_marc_paths(project_id: str) -> list[Path]:
    """Find push-ready MARC files (same logic as push.py)."""
    project_dir = Path(settings.data_root) / project_id
    for name in ["Biblios-mapped-items.mrc", "merged_deduped.mrc", "output.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            return [candidate]
    transformed_dir = project_dir / "transformed"
    if transformed_dir.is_dir():
        transformed = sorted(transformed_dir.glob("*_transformed.mrc"))
        if transformed:
            return transformed
    return []


def _record_to_marcxml(record: pymarc.Record) -> str:
    """Convert a pymarc Record to MARCXML string.

    Evergreen stores MARCXML (not ISO 2709) in biblio.record_entry.marc.
    """
    return pymarc.record_to_xml(record, namespace=True).decode("utf-8")


# ══════════════════════════════════════════════════════════════════════════
# COUNTS TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    name="cerulean.tasks.evergreen.evergreen_counts_task",
    queue="push",
)
def evergreen_counts_task(project_id: str) -> dict:
    """Get record counts from the Evergreen database."""
    try:
        conn = _evergreen_conn(project_id)
        cursor = conn.cursor()

        counts = {}
        queries = {
            "bibs": "SELECT COUNT(*) FROM biblio.record_entry WHERE NOT deleted",
            "deleted_bibs": "SELECT COUNT(*) FROM biblio.record_entry WHERE deleted",
            "call_numbers": "SELECT COUNT(*) FROM asset.call_number WHERE NOT deleted",
            "copies": "SELECT COUNT(*) FROM asset.copy WHERE NOT deleted",
            "patrons": "SELECT COUNT(*) FROM actor.usr WHERE NOT deleted",
            "org_units": "SELECT COUNT(*) FROM actor.org_unit",
        }
        for key, sql in queries.items():
            try:
                cursor.execute(sql)
                counts[key] = cursor.fetchone()[0]
            except Exception:
                counts[key] = None

        cursor.close()
        conn.close()
        return {"success": True, "counts": counts}
    except Exception as exc:
        return {"success": False, "error": str(exc)[:500]}


# ══════════════════════════════════════════════════════════════════════════
# PUSH BIBS TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.evergreen.evergreen_push_bibs_task",
    max_retries=0,
    queue="push",
)
def evergreen_push_bibs_task(
    self, project_id: str, manifest_id: str,
    batch_size: int = 500, disable_triggers: bool = True,
) -> dict:
    """INSERT MARCXML records into Evergreen's biblio.record_entry.

    This is the direct-database migration approach used by the Evergreen
    community for large imports. Records are inserted as MARCXML into
    biblio.record_entry with optional trigger disabling for speed.

    After this completes, run evergreen_pingest_task to build search indexes.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        batch_size: Records per INSERT batch.
        disable_triggers: Disable indexing triggers during import (faster,
                          requires pingest.pl afterward).
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=13, tag="[evergreen-push]")
    log.info(f"Evergreen push starting (batch_size={batch_size}, triggers={'off' if disable_triggers else 'on'})")

    try:
        marc_paths = _find_marc_paths(project_id)
        if not marc_paths:
            error_msg = "No transformed or merged MARC files found"
            log.error(error_msg)
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
            return {"error": error_msg}

        conn = _evergreen_conn(project_id)
        cursor = conn.cursor()

        # Optionally disable triggers for speed
        if disable_triggers:
            log.info("Disabling indexing triggers on biblio.record_entry")
            cursor.execute("ALTER TABLE biblio.record_entry DISABLE TRIGGER ALL")
            conn.commit()

        total = 0
        success = 0
        failed = 0
        batch = []

        self.update_state(state="PROGRESS", meta={
            "step": "importing",
            "records_done": 0,
            "records_total": 0,
        })

        for marc_path in marc_paths:
            log.info(f"Processing {marc_path.name}")
            with open(str(marc_path), "rb") as fh:
                reader = pymarc.MARCReader(
                    fh, to_unicode=True, force_utf8=True, utf8_handling="replace"
                )
                for record in reader:
                    if record is None:
                        continue
                    total += 1

                    try:
                        marcxml = _record_to_marcxml(record)
                        batch.append(marcxml)
                    except Exception as exc:
                        failed += 1
                        if failed <= 10:
                            log.warn(f"Record {total} MARCXML conversion failed: {exc}")
                        continue

                    # Flush batch
                    if len(batch) >= batch_size:
                        inserted = _insert_batch(cursor, batch)
                        success += inserted
                        failed += len(batch) - inserted
                        conn.commit()
                        batch = []

                    if total % _PROGRESS_INTERVAL == 0:
                        _check_paused(project_id, self)
                        self.update_state(state="PROGRESS", meta={
                            "step": "importing",
                            "records_done": success,
                            "records_total": total,
                        })

        # Flush remaining
        if batch:
            inserted = _insert_batch(cursor, batch)
            success += inserted
            failed += len(batch) - inserted
            conn.commit()

        # Re-enable triggers
        if disable_triggers:
            log.info("Re-enabling indexing triggers on biblio.record_entry")
            cursor.execute("ALTER TABLE biblio.record_entry ENABLE TRIGGER ALL")
            conn.commit()

        cursor.close()
        conn.close()

        result_data = {
            "method": "evergreen_direct",
            "records_total": total,
            "records_success": success,
            "records_failed": failed,
            "batch_size": batch_size,
            "triggers_disabled": disable_triggers,
            "files": [p.name for p in marc_paths],
        }

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total,
                                  records_success=success,
                                  records_failed=failed,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())

        log.complete(
            f"Evergreen push complete — {total:,} records, "
            f"{success:,} success, {failed:,} failed"
        )
        return result_data

    except Exception as exc:
        log.error(f"Evergreen push failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


def _insert_batch(cursor, batch: list[str]) -> int:
    """INSERT a batch of MARCXML records into biblio.record_entry.

    Uses executemany for efficiency. Returns count of successfully inserted.

    The INSERT uses Evergreen's standard columns:
        - marc: MARCXML text
        - last_xact_id: transaction identifier (we use 'cerulean-migration')
        - source: set to 1 (default bib source)
    """
    inserted = 0
    sql = """
        INSERT INTO biblio.record_entry (marc, last_xact_id, source)
        VALUES (%s, 'cerulean-migration', 1)
    """
    for marcxml in batch:
        try:
            cursor.execute(sql, (marcxml,))
            inserted += 1
        except Exception:
            # Skip bad records, continue with next
            pass
    return inserted


# ══════════════════════════════════════════════════════════════════════════
# PINGEST (PARALLEL INGEST) TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.evergreen.evergreen_pingest_task",
    max_retries=0,
    queue="push",
)
def evergreen_pingest_task(
    self, project_id: str, manifest_id: str,
    processes: int = 4,
) -> dict:
    """Trigger Evergreen's parallel ingest to build search indexes.

    After bulk loading records into biblio.record_entry with triggers
    disabled, pingest.pl must be run to populate the metabib.* search
    index tables.

    This task connects to the Evergreen server and runs pingest.pl.
    The method depends on deployment:
        - Docker: docker exec into the Evergreen container
        - SSH: ssh to the Evergreen host
        - Local: direct execution

    For now, this is a stub that logs the command to run manually.
    When the Evergreen container is available, this will be implemented
    to execute pingest.pl directly.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        processes: Number of parallel workers for pingest.pl.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=13, tag="[evergreen-pingest]")
    log.info(f"Evergreen pingest starting (processes={processes})")

    try:
        # Get bib count range for pingest
        conn = _evergreen_conn(project_id)
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(id), MAX(id) FROM biblio.record_entry WHERE NOT deleted")
        min_id, max_id = cursor.fetchone()
        cursor.close()
        conn.close()

        pingest_cmd = (
            f"pingest.pl "
            f"--schema biblio --table record_entry "
            f"--start-id {min_id or 1} --end-id {max_id or 1} "
            f"--processes {processes}"
        )

        log.info(f"pingest command: {pingest_cmd}")

        # TODO: Execute pingest.pl when container/SSH access is available.
        # For now, store the command so the user can run it manually or
        # we can implement container exec later (same pattern as Koha's
        # bulkmarcimport.pl execution).
        #
        # When implemented, this will:
        # 1. docker exec / ssh into the Evergreen container
        # 2. Run pingest.pl with the computed ID range
        # 3. Poll for completion (pingest writes to stdout)
        # 4. Update manifest with results

        result_data = {
            "method": "evergreen_pingest",
            "status": "manual_required",
            "pingest_command": pingest_cmd,
            "min_id": min_id,
            "max_id": max_id,
            "processes": processes,
            "message": (
                f"Run this command on the Evergreen server to build search indexes: "
                f"{pingest_cmd}"
            ),
        }

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=max_id - min_id + 1 if min_id and max_id else 0,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())
            project = db.get(Project, project_id)
            if project:
                project.stage_13_complete = True
                db.commit()

        log.complete(f"Evergreen pingest — command generated (manual execution required)")
        return result_data

    except Exception as exc:
        log.error(f"Evergreen pingest failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise
