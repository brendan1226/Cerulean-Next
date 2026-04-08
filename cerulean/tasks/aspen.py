"""
cerulean/tasks/aspen.py
─────────────────────────────────────────────────────────────────────────────
Stage 12 Celery tasks: Aspen Discovery — Turbo Migration + Turbo Reindex.

    aspen_turbo_migration_task  — bulk extract Koha → Aspen DB
    aspen_turbo_reindex_task    — parallel Solr indexing of Aspen records
"""

import re
from datetime import datetime

import httpx
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.helpers import check_paused as _check_paused

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)


def _aspen_api(aspen_url: str, method: str, **params) -> dict:
    """Call an Aspen SystemAPI endpoint.

    Args:
        aspen_url: Base URL, e.g. http://aspen:85
        method: API method name, e.g. getTurboCounts
        **params: Additional query params

    Returns the "result" dict from the response.
    """
    params["method"] = method
    url = f"{aspen_url.rstrip('/')}/API/SystemAPI"
    # Aspen's Apache vhost may only accept "localhost" as the Host header,
    # even when accessed via container name on a Docker network.
    from urllib.parse import urlparse
    parsed = urlparse(aspen_url)
    headers = {"Host": "localhost"} if parsed.hostname not in ("localhost", "127.0.0.1") else {}
    with httpx.Client(timeout=30.0, verify=False) as client:
        resp = client.get(url, params=params, headers=headers)
        if resp.status_code >= 400:
            raise RuntimeError(f"Aspen API error: HTTP {resp.status_code} — {resp.text[:500]}")
        data = resp.json()
        result = data.get("result", data)
        if isinstance(result, dict) and not result.get("success", True):
            msg = result.get("message", "Unknown error")
            raise RuntimeError(f"Aspen API returned success=false: {msg}")
        return result


def _poll_aspen_job(aspen_url: str, process_id: int, celery_task,
                     project_id: str, step_label: str,
                     poll_interval: float = 10.0,
                     timeout: float = 14400.0) -> dict:
    """Poll Aspen's getTurboStatus until the background process completes.

    Updates Celery task state with progress parsed from the notes field.
    Returns the final status dict.
    """
    import time
    elapsed = 0.0
    while elapsed < timeout:
        _check_paused(project_id, celery_task)
        try:
            status = _aspen_api(aspen_url, "getTurboStatus",
                                backgroundProcessId=process_id)
        except Exception as exc:
            # Transient error — retry on next poll
            time.sleep(poll_interval)
            elapsed += poll_interval
            continue

        is_running = status.get("isRunning", False)
        notes = status.get("notes", "")

        # Parse metrics from notes for progress display
        progress = 0
        total = 0
        rate = 0

        # Migration progress: "Extract: X bibs (Y%)"
        m = re.search(r'Extract: ([\d,]+) bibs \(([\d.]+)%\)', notes)
        if m:
            progress = int(m.group(1).replace(",", ""))
            pct = float(m.group(2))
            if pct > 0:
                total = int(progress / pct * 100)

        # Reindex progress: "Progress: X / Y (Z%)"
        m = re.search(r'Progress: ([\d,]+) / ([\d,]+) \(([\d.]+)%\)', notes)
        if m:
            progress = int(m.group(1).replace(",", ""))
            total = int(m.group(2).replace(",", ""))

        # Rate from any "X rec/sec" or "X works/sec" or "X bibs/sec"
        m = re.search(r'([\d,]+) (?:rec|works|bibs)/sec', notes)
        if m:
            rate = int(m.group(1).replace(",", ""))

        # Last line of notes for display
        last_line = ""
        if notes:
            lines = notes.strip().split("\n")
            last_line = lines[-1] if lines else ""

        celery_task.update_state(state="PROGRESS", meta={
            "step": step_label,
            "process_id": process_id,
            "is_running": is_running,
            "progress": progress,
            "total": total,
            "rate": rate,
            "elapsed_secs": status.get("elapsedSeconds", int(elapsed)),
            "last_line": last_line,
            "status": status.get("status", "running"),
        })

        if not is_running:
            return status

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise RuntimeError(f"Aspen job {process_id} timed out after {timeout}s")


def _update_push_manifest(db: Session, manifest_id: str, **kwargs) -> None:
    from cerulean.models import PushManifest
    db.execute(
        update(PushManifest)
        .where(PushManifest.id == manifest_id)
        .values(**kwargs)
    )
    db.commit()


@celery_app.task(
    bind=True,
    name="cerulean.tasks.aspen.aspen_turbo_migration_task",
    max_retries=0,
    queue="push",
)
def aspen_turbo_migration_task(
    self, project_id: str, manifest_id: str,
    worker_threads: int = 4, batch_size: int = 500,
    profile_name: str = "ils",
) -> dict:
    """Bulk extract from Koha → Aspen DB via Aspen's Turbo Migration API.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        worker_threads: Parallel threads (1-16).
        batch_size: Records per batch (100-5000).
        profile_name: Aspen indexing profile name.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=12, tag="[aspen-migrate]")
    log.info(f"Aspen Turbo Migration starting (threads={worker_threads}, batch={batch_size})")

    try:
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if not project or not project.aspen_url:
                error_msg = "Aspen URL not configured on project"
                log.error(error_msg)
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
                return {"error": error_msg}
            aspen_url = project.aspen_url

        # Pre-flight: check counts
        self.update_state(state="PROGRESS", meta={
            "step": "preflight", "status": "checking",
        })
        counts = _aspen_api(aspen_url, "getTurboCounts")
        koha_bibs = counts.get("koha", {}).get("bibs", 0)
        log.info(f"Preflight: Koha has {koha_bibs:,} bibs")

        # Start migration
        result = _aspen_api(
            aspen_url, "startTurboMigration",
            workerThreads=worker_threads,
            batchSize=batch_size,
            profileName=profile_name,
        )
        process_id = result.get("backgroundProcessId")
        if not process_id:
            raise RuntimeError(f"No backgroundProcessId: {result}")

        log.info(f"Turbo Migration started: processId={process_id}")

        # Persist process ID immediately for recovery
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  result_data={
                                      "method": "aspen_migration",
                                      "process_id": process_id,
                                      "worker_threads": worker_threads,
                                      "batch_size": batch_size,
                                      "koha_bibs_before": koha_bibs,
                                  })

        # Poll until done
        final = _poll_aspen_job(
            aspen_url, process_id, self, project_id,
            step_label="aspen_migration",
        )

        # Parse final notes for summary
        notes = final.get("notes", "")
        elapsed_secs = final.get("elapsedSeconds", 0)

        # Post-flight: verify counts
        post_counts = _aspen_api(aspen_url, "getTurboCounts")
        aspen_records = post_counts.get("aspen", {}).get("ilsRecords", 0)
        grouped_works = post_counts.get("aspen", {}).get("groupedWorks", 0)

        result_data = {
            "method": "aspen_migration",
            "process_id": process_id,
            "status": final.get("status", "completed"),
            "worker_threads": worker_threads,
            "batch_size": batch_size,
            "elapsed_secs": elapsed_secs,
            "koha_bibs": koha_bibs,
            "aspen_ils_records": aspen_records,
            "aspen_grouped_works": grouped_works,
            "notes": notes[-5000:] if len(notes) > 5000 else notes,
        }

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=koha_bibs,
                                  records_success=aspen_records,
                                  records_failed=0,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())

        log.complete(
            f"Aspen Turbo Migration complete — {aspen_records:,} ILS records, "
            f"{grouped_works:,} grouped works, {elapsed_secs}s"
        )
        return result_data

    except Exception as exc:
        log.error(f"Aspen Turbo Migration failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


@celery_app.task(
    bind=True,
    name="cerulean.tasks.aspen.aspen_turbo_reindex_task",
    max_retries=0,
    queue="push",
)
def aspen_turbo_reindex_task(
    self, project_id: str, manifest_id: str,
    worker_threads: int = 8, clear_index: bool = False,
) -> dict:
    """Parallel Solr reindex via Aspen's Turbo Reindex API.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        worker_threads: Parallel threads (1-16).
        clear_index: Clear Solr index before reindexing.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=12, tag="[aspen-reindex]")
    log.info(f"Aspen Turbo Reindex starting (threads={worker_threads}, clear={clear_index})")

    try:
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if not project or not project.aspen_url:
                error_msg = "Aspen URL not configured on project"
                log.error(error_msg)
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
                return {"error": error_msg}
            aspen_url = project.aspen_url

        # Start reindex
        result = _aspen_api(
            aspen_url, "startTurboReindex",
            workerThreads=worker_threads,
            clearIndex=1 if clear_index else 0,
        )
        process_id = result.get("backgroundProcessId")
        if not process_id:
            raise RuntimeError(f"No backgroundProcessId: {result}")

        log.info(f"Turbo Reindex started: processId={process_id}")

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  result_data={
                                      "method": "aspen_reindex",
                                      "process_id": process_id,
                                      "worker_threads": worker_threads,
                                      "clear_index": clear_index,
                                  })

        # Poll until done
        final = _poll_aspen_job(
            aspen_url, process_id, self, project_id,
            step_label="aspen_reindex",
        )

        notes = final.get("notes", "")
        elapsed_secs = final.get("elapsedSeconds", 0)

        # Post-flight
        post_counts = _aspen_api(aspen_url, "getTurboCounts")
        grouped_works = post_counts.get("aspen", {}).get("groupedWorks", 0)

        result_data = {
            "method": "aspen_reindex",
            "process_id": process_id,
            "status": final.get("status", "completed"),
            "worker_threads": worker_threads,
            "clear_index": clear_index,
            "elapsed_secs": elapsed_secs,
            "aspen_grouped_works": grouped_works,
            "notes": notes[-5000:] if len(notes) > 5000 else notes,
        }

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=grouped_works,
                                  records_success=grouped_works,
                                  records_failed=0,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())
            project = db.get(Project, project_id)
            if project:
                project.stage_12_complete = True
                db.commit()

        log.complete(
            f"Aspen Turbo Reindex complete — {grouped_works:,} grouped works, {elapsed_secs}s"
        )
        return result_data

    except Exception as exc:
        log.error(f"Aspen Turbo Reindex failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise
