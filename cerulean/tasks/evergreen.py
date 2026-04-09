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
    batch_size: int = 500, trigger_mode: str = "indexing_only",
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
    log.info(f"Evergreen push starting (batch_size={batch_size}, trigger_mode={trigger_mode})")

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

        # Trigger modes:
        #   "all_on"        — all triggers enabled (slowest, safest)
        #   "indexing_only"  — disable only the heavy indexing trigger (recommended)
        #   "all_off"       — disable all triggers (fastest, requires pingest for everything)
        _HEAVY_TRIGGERS = [
            "aaa_indexing_ingest_or_delete",  # full metabib reingest
            "bbb_simple_rec_trigger",          # simple record cache
        ]
        triggers_disabled = []

        if trigger_mode == "all_off":
            log.info("Disabling ALL triggers on biblio.record_entry")
            cursor.execute("ALTER TABLE biblio.record_entry DISABLE TRIGGER ALL")
            triggers_disabled = ["ALL"]
            conn.commit()
        elif trigger_mode == "indexing_only":
            for trig in _HEAVY_TRIGGERS:
                try:
                    cursor.execute(f"ALTER TABLE biblio.record_entry DISABLE TRIGGER {trig}")
                    triggers_disabled.append(trig)
                    log.info(f"Disabled trigger: {trig}")
                except Exception as exc:
                    log.warn(f"Could not disable trigger {trig}: {exc}")
                    conn.rollback()
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
        if trigger_mode == "all_off":
            log.info("Re-enabling ALL triggers on biblio.record_entry")
            cursor.execute("ALTER TABLE biblio.record_entry ENABLE TRIGGER ALL")
            conn.commit()
        elif trigger_mode == "indexing_only" and triggers_disabled:
            for trig in triggers_disabled:
                try:
                    cursor.execute(f"ALTER TABLE biblio.record_entry ENABLE TRIGGER {trig}")
                    log.info(f"Re-enabled trigger: {trig}")
                except Exception:
                    pass
            conn.commit()

        cursor.close()
        conn.close()

        result_data = {
            "method": "evergreen_direct",
            "records_total": total,
            "records_success": success,
            "records_failed": failed,
            "batch_size": batch_size,
            "trigger_mode": trigger_mode,
            "triggers_disabled": triggers_disabled,
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
    processes: int = 4, batch_size: int = 10000,
    delay_symspell: bool = True,
    skip_browse: bool = False, skip_attrs: bool = False,
    skip_search: bool = False, skip_facets: bool = False,
    skip_display: bool = False,
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
    import subprocess
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=13, tag="[evergreen-pingest]")
    log.info(f"Evergreen pingest starting (processes={processes})")

    # The Evergreen container name — detect or default
    container = "evergreen-test"

    try:
        # Get bib count range for pingest
        conn = _evergreen_conn(project_id)
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(id), MAX(id) FROM biblio.record_entry WHERE NOT deleted")
        min_id, max_id = cursor.fetchone()
        cursor.close()
        conn.close()

        total_records = (max_id - min_id + 1) if min_id and max_id else 0

        # pingest.pl uses the opensrf_core.xml config for DB connection
        # (not command-line DB args), so it gets the password from there.
        pingest_parts = [
            "/openils/bin/pingest.pl",
            "-c /openils/conf/opensrf_core.xml",
            f"--start-id {min_id or 1}",
            f"--end-id {max_id or 1}",
            f"--max-child {processes}",
            f"--batch-size {batch_size}",
        ]
        if delay_symspell:
            pingest_parts.append("--delay-symspell")
        if skip_browse:
            pingest_parts.append("--skip-browse")
        if skip_attrs:
            pingest_parts.append("--skip-attrs")
        if skip_search:
            pingest_parts.append("--skip-search")
        if skip_facets:
            pingest_parts.append("--skip-facets")
        if skip_display:
            pingest_parts.append("--skip-display")
        pingest_cmd = " ".join(pingest_parts)

        log.info(f"Running: docker exec {container} {pingest_cmd}")
        log.info(f"ID range: {min_id} — {max_id} ({total_records:,} records)")

        self.update_state(state="PROGRESS", meta={
            "step": "pingest",
            "records_done": 0,
            "records_total": total_records,
            "container": container,
        })

        # Execute pingest.pl inside the Evergreen container
        # Uses Docker Engine API via Unix socket (same as Koha migration mode)
        import httpx

        transport = httpx.HTTPTransport(uds="/var/run/docker.sock")
        with httpx.Client(transport=transport, timeout=30.0) as docker_client:
            # Create exec
            create_resp = docker_client.post(
                f"http://localhost/containers/{container}/exec",
                json={
                    "Cmd": ["bash", "-c", pingest_cmd],
                    "AttachStdout": True,
                    "AttachStderr": True,
                },
            )
            if create_resp.status_code != 201:
                raise RuntimeError(f"Docker exec create failed: {create_resp.status_code} {create_resp.text[:200]}")
            exec_id = create_resp.json()["Id"]

        # Start exec with a long timeout (pingest can take a while)
        transport2 = httpx.HTTPTransport(uds="/var/run/docker.sock")
        with httpx.Client(transport=transport2, timeout=14400.0) as docker_client:
            start_resp = docker_client.post(
                f"http://localhost/exec/{exec_id}/start",
                json={"Detach": False, "Tty": False},
            )
            raw = start_resp.content

            # Parse Docker multiplexed stream
            stdout_parts = []
            stderr_parts = []
            pos = 0
            while pos + 8 <= len(raw):
                stream_type = raw[pos]
                frame_size = int.from_bytes(raw[pos+4:pos+8], 'big')
                frame_data = raw[pos+8:pos+8+frame_size]
                if stream_type == 1:
                    stdout_parts.append(frame_data)
                elif stream_type == 2:
                    stderr_parts.append(frame_data)
                pos += 8 + frame_size

            stdout = b"".join(stdout_parts).decode("utf-8", errors="replace").strip()
            stderr = b"".join(stderr_parts).decode("utf-8", errors="replace").strip()

        # Get exit code
        transport3 = httpx.HTTPTransport(uds="/var/run/docker.sock")
        with httpx.Client(transport=transport3, timeout=10.0) as docker_client:
            inspect_resp = docker_client.get(f"http://localhost/exec/{exec_id}/json")
            exit_code = inspect_resp.json().get("ExitCode", -1) if inspect_resp.status_code == 200 else -1

        log.info(f"pingest exit code: {exit_code}")
        if stdout:
            log.info(f"pingest stdout (last 500): {stdout[-500:]}")
        if stderr:
            log.warn(f"pingest stderr (last 500): {stderr[-500:]}")

        result_data = {
            "method": "evergreen_pingest",
            "status": "complete" if exit_code == 0 else "error",
            "pingest_command": pingest_cmd,
            "container": container,
            "min_id": min_id,
            "max_id": max_id,
            "total_records": total_records,
            "processes": processes,
            "exit_code": exit_code,
            "stdout": stdout[-2000:] if len(stdout) > 2000 else stdout,
            "stderr": stderr[-1000:] if len(stderr) > 1000 else stderr,
        }

        final_status = "complete" if exit_code == 0 else "error"
        error_msg = f"pingest exited with code {exit_code}" if exit_code != 0 else None

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status=final_status,
                                  records_total=total_records,
                                  records_success=total_records if exit_code == 0 else 0,
                                  result_data=result_data,
                                  error_message=error_msg,
                                  completed_at=datetime.utcnow())
            if exit_code == 0:
                project = db.get(Project, project_id)
                if project:
                    project.stage_13_complete = True
                    db.commit()

        log.complete(f"Evergreen pingest {'complete' if exit_code == 0 else 'failed'} — {total_records:,} records, exit={exit_code}")
        return result_data

    except Exception as exc:
        log.error(f"Evergreen pingest failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise
