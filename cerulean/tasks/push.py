"""
cerulean/tasks/push.py
─────────────────────────────────────────────────────────────────────────────
Stage 7 Celery tasks: Push to Koha.

    push_preflight_task  — verify Koha connectivity and version
    push_bulkmarc_task   — POST MARC records to Koha REST API
    push_patrons_task    — POST patron records from CSV
    push_holds_task      — POST holds from CSV
    push_circ_task       — validate circ history CSV (manual SQL import)
    es_reindex_task      — log reindex command, mark stage 7 complete

All tasks write AuditEvent rows via AuditLogger.
"""

import base64
import csv
import os
import subprocess
from datetime import datetime
from pathlib import Path

import httpx
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app
from cerulean.tasks.helpers import check_paused as _check_paused
from cerulean.utils.marc import iter_marc as _iter_marc

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)

_PROGRESS_INTERVAL = 500


# ══════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ══════════════════════════════════════════════════════════════════════════


def _find_marc_paths(project_id: str) -> list[Path]:
    """Find best available MARC output files in priority order.

    Priority: Biblios-mapped-items.mrc > merged_deduped.mrc > output.mrc > merged.mrc > transformed/*.mrc
    """
    project_dir = Path(settings.data_root) / project_id
    reconciled = project_dir / "Biblios-mapped-items.mrc"
    if reconciled.is_file():
        return [reconciled]
    deduped = project_dir / "merged_deduped.mrc"
    if deduped.is_file():
        return [deduped]
    output = project_dir / "output.mrc"
    if output.is_file():
        return [output]
    merged = project_dir / "merged.mrc"
    if merged.is_file():
        return [merged]
    transformed_dir = project_dir / "transformed"
    if transformed_dir.is_dir():
        transformed = sorted(transformed_dir.glob("*_transformed.mrc"))
        if transformed:
            return transformed
    return []


def _decrypt_token(encrypted: str) -> str:
    """Decrypt a Fernet-encrypted Koha API token."""
    key = settings.fernet_key.strip() if settings.fernet_key else ""
    if not key or key.startswith("#"):
        return encrypted  # dev fallback — stored unencrypted
    f = Fernet(key.encode())
    return f.decrypt(encrypted.encode()).decode()


def _rewrite_localhost(url: str) -> str:
    """Rewrite localhost URLs to host.docker.internal for Docker workers."""
    import re
    return re.sub(
        r"^(https?://)localhost(:\d+)?",
        r"\1host.docker.internal\2",
        url,
    )


def _koha_client(project_id: str) -> tuple[str, dict[str, str]]:
    """Load project, decrypt token, return (base_url, auth_headers).

    Supports two auth modes (project.koha_auth_type):
        "basic"  — HTTP Basic Auth. Token stores "user:pass".
        "bearer" — Bearer token. Token stores the raw token string.
    Defaults to "basic" (KTD default: koha/koha).
    """
    from cerulean.models import Project

    with Session(_engine) as db:
        project = db.get(Project, project_id)
        if not project or not project.koha_url:
            raise ValueError("Project or koha_url not configured")
        base_url = _rewrite_localhost(project.koha_url.rstrip("/"))
        token = _decrypt_token(project.koha_token_enc) if project.koha_token_enc else ""
        auth_type = getattr(project, "koha_auth_type", "basic") or "basic"

    if auth_type == "bearer":
        auth_value = f"Bearer {token}"
    else:
        # Basic auth — token is stored as "user:pass"
        b64 = base64.b64encode(token.encode()).decode()
        auth_value = f"Basic {b64}"

    headers = {
        "Authorization": auth_value,
        "Accept": "application/json",
    }
    return base_url, headers


def _update_push_manifest(db: Session, manifest_id: str, **kwargs) -> None:
    """Update a PushManifest row."""
    from cerulean.models import PushManifest
    db.execute(
        update(PushManifest)
        .where(PushManifest.id == manifest_id)
        .values(**kwargs)
    )
    db.commit()



# _iter_marc imported from cerulean.utils.marc


# ══════════════════════════════════════════════════════════════════════════
# PREFLIGHT TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_preflight_task",
    max_retries=0,
    queue="push",
)
def push_preflight_task(self, project_id: str, manifest_id: str) -> dict:
    """Verify Koha API connectivity and detect version/search engine.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.

    Returns:
        dict with reachable, version, search_engine.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[preflight]")
    log.info("Preflight check starting")

    try:
        base_url, headers = _koha_client(project_id)

        with httpx.Client(timeout=30.0, verify=False) as client:
            resp = client.get(f"{base_url}/api/v1/", headers=headers)

        if resp.status_code >= 400:
            error_msg = f"Koha API returned {resp.status_code}: {resp.text[:500]}"
            log.error(error_msg)
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
            return {"reachable": False, "error": error_msg}

        # Try to parse version info
        result_data = {"reachable": True}
        try:
            data = resp.json()
            version = data.get("version", None)
            if version:
                result_data["version"] = version
        except Exception:
            pass

        # Update project with version info if available
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if project and result_data.get("version"):
                project.koha_version = result_data["version"]

            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())

        log.complete(f"Preflight complete — Koha reachable at {base_url}")
        return result_data

    except Exception as exc:
        log.error(f"Preflight failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc),
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# BULK MARC PUSH TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_bulkmarc_task",
    max_retries=0,
    queue="push",
)
def push_bulkmarc_task(self, project_id: str, manifest_id: str, dry_run: bool = True) -> dict:
    """POST MARC records to Koha REST API /api/v1/biblios.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        dry_run: If True, count records only — no HTTP calls.

    Returns:
        dict with records_total, records_success, records_failed.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[push-bibs]")
    log.info(f"Bulk MARC push starting (dry_run={dry_run})")

    try:
        # Locate MARC output files (prefer deduped > merged > transformed)
        marc_paths = _find_marc_paths(project_id)
        if not marc_paths:
            error_msg = "No transformed or merged MARC files found"
            log.error(error_msg)
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
            return {"error": error_msg}

        log.info(f"Pushing from {len(marc_paths)} file(s): {[p.name for p in marc_paths]}")

        total = 0
        success = 0
        failed = 0
        _aborted = False
        _first_error_status = None
        _first_error_body = None

        if dry_run:
            # Count records only
            for marc_path in marc_paths:
                for record in _iter_marc(str(marc_path)):
                    total += 1
                    success += 1
                    if total % _PROGRESS_INTERVAL == 0:
                        _check_paused(project_id, self)
                        self.update_state(state="PROGRESS", meta={
                            "records_done": total, "dry_run": True,
                        })
        else:
            base_url, headers = _koha_client(project_id)
            push_headers = {**headers, "Content-Type": "application/marc"}

            # Early-abort tracking: if first N records all fail with same status, stop
            _EARLY_ABORT_THRESHOLD = 10
            _first_error_status = None
            _first_error_body = None
            _consecutive_same_error = 0
            _aborted = False

            with httpx.Client(timeout=60.0, verify=False) as client:
                for marc_path in marc_paths:
                    if _aborted:
                        break
                    for record in _iter_marc(str(marc_path)):
                        total += 1
                        try:
                            resp = client.post(
                                f"{base_url}/api/v1/biblios",
                                content=record.as_marc(),
                                headers=push_headers,
                            )
                            if resp.status_code < 300:
                                success += 1
                                _consecutive_same_error = 0
                                _first_error_status = None
                            else:
                                failed += 1
                                resp_body = resp.text[:500]
                                if failed <= 10:
                                    log.warn(f"Record {total} failed: HTTP {resp.status_code} — {resp_body}")
                                if _first_error_body is None:
                                    _first_error_body = resp_body
                                # Track consecutive same-status errors for early abort
                                if _first_error_status is None or _first_error_status == resp.status_code:
                                    _first_error_status = resp.status_code
                                    _consecutive_same_error += 1
                                else:
                                    _consecutive_same_error = 1
                                    _first_error_status = resp.status_code
                                if _consecutive_same_error >= _EARLY_ABORT_THRESHOLD and success == 0:
                                    error_msg = (
                                        f"Early abort: first {_EARLY_ABORT_THRESHOLD} records all failed "
                                        f"with HTTP {_first_error_status}. Response: {_first_error_body}"
                                    )
                                    log.error(error_msg)
                                    _aborted = True
                                    break
                        except httpx.HTTPError as exc:
                            failed += 1
                            if failed <= 10:
                                log.warn(f"Record {total} HTTP error: {exc}")

                        if total % _PROGRESS_INTERVAL == 0:
                            _check_paused(project_id, self)
                            self.update_state(state="PROGRESS", meta={
                                "records_done": total,
                                "records_success": success,
                                "records_failed": failed,
                            })

        # Build result data
        result_data_extra = {}
        if _aborted:
            result_data_extra["early_abort"] = True
            result_data_extra["abort_reason"] = error_msg
            result_data_extra["first_error_status"] = _first_error_status
            result_data_extra["first_error_body"] = _first_error_body

        # Update manifest and project
        final_status = "error" if _aborted else "complete"
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status=final_status,
                                  records_total=total,
                                  records_success=success,
                                  records_failed=failed,
                                  error_message=error_msg if _aborted else None,
                                  result_data=result_data_extra or None,
                                  completed_at=datetime.utcnow())

            if not dry_run and not _aborted:
                project = db.get(Project, project_id)
                if project:
                    project.bib_count_pushed = success
                    db.commit()

        if _aborted:
            log.error(
                f"Bulk MARC push aborted — {total:,} records attempted, "
                f"{failed:,} failed (dry_run={dry_run})"
            )
        else:
            log.complete(
                f"Bulk MARC push complete — {total:,} records, "
                f"{success:,} success, {failed:,} failed (dry_run={dry_run})"
            )
        return {
            "records_total": total,
            "records_success": success,
            "records_failed": failed,
            "dry_run": dry_run,
            **({"early_abort": True, "abort_reason": result_data_extra.get("abort_reason")} if _aborted else {}),
        }

    except Exception as exc:
        log.error(f"Bulk MARC push failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc),
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# BULKMARCIMPORT.PL PUSH TASK
# ══════════════════════════════════════════════════════════════════════════


def _detect_container(project_id: str, bib_options: dict | None) -> str:
    """Determine Docker container name for bulkmarcimport.

    Priority: bib_options["container"] > SandboxInstance table > fallback.
    """
    if bib_options and bib_options.get("container"):
        return bib_options["container"]

    from cerulean.models import SandboxInstance

    with Session(_engine) as db:
        instance = db.query(SandboxInstance).filter_by(
            project_id=project_id, status="running"
        ).first()
        if instance and instance.container_name:
            return instance.container_name

    # Fallback naming convention
    return f"koha-kohadev-1"


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_bulkmarcimport_task",
    max_retries=0,
    queue="push",
)
def push_bulkmarcimport_task(
    self, project_id: str, manifest_id: str,
    dry_run: bool = True, bib_options: dict | None = None,
) -> dict:
    """Import MARC records via bulkmarcimport.pl inside a KTD Docker container.

    Steps:
        1. docker cp MARC file(s) into the container at /tmp/
        2. Build bulkmarcimport.pl command with user flags
        3. docker exec to run it
        4. Parse stdout for record counts

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        dry_run: If True, add -t (test mode) flag.
        bib_options: Dict with match_field, insert, update, framework, container.

    Returns:
        dict with records_total, records_success, records_failed, command, output.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[push-bibs-import]")
    bib_options = bib_options or {}
    log.info(f"bulkmarcimport.pl push starting (dry_run={dry_run}, options={bib_options})")

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

        container = _detect_container(project_id, bib_options)
        log.info(f"Using container: {container}")
        log.info(f"Importing from {len(marc_paths)} file(s): {[p.name for p in marc_paths]}")

        self.update_state(state="PROGRESS", meta={
            "step": "copying_files", "container": container,
        })

        # Verify container is running
        try:
            inspect = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Running}}", container],
                capture_output=True, text=True, timeout=10,
            )
            if inspect.returncode != 0 or "true" not in inspect.stdout.lower():
                error_msg = f"Container '{container}' is not running. stdout={inspect.stdout.strip()} stderr={inspect.stderr.strip()}"
                log.error(error_msg)
                with Session(_engine) as db:
                    _update_push_manifest(db, manifest_id,
                                          status="error", error_message=error_msg,
                                          completed_at=datetime.utcnow())
                return {"error": error_msg}
        except subprocess.TimeoutExpired:
            error_msg = f"Timeout checking container '{container}'"
            log.error(error_msg)
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
            return {"error": error_msg}

        all_output = []
        total_records = 0
        total_success = 0
        total_failed = 0

        for i, marc_path in enumerate(marc_paths):
            filename = marc_path.name
            container_path = f"/tmp/{filename}"

            # docker cp the file into the container
            cp_result = subprocess.run(
                ["docker", "cp", str(marc_path), f"{container}:{container_path}"],
                capture_output=True, text=True, timeout=60,
            )
            if cp_result.returncode != 0:
                error_msg = f"docker cp failed for {filename}: {cp_result.stderr.strip()}"
                log.error(error_msg)
                with Session(_engine) as db:
                    _update_push_manifest(db, manifest_id,
                                          status="error", error_message=error_msg,
                                          completed_at=datetime.utcnow())
                return {"error": error_msg}

            log.info(f"Copied {filename} into container at {container_path}")

            # Build bulkmarcimport.pl command
            cmd_parts = ["bulkmarcimport.pl", "-file", container_path]

            if dry_run:
                cmd_parts.append("-t")  # test mode

            match_field = bib_options.get("match_field")
            if match_field:
                cmd_parts.extend(["-match", match_field])

            if bib_options.get("insert", True):
                cmd_parts.append("--insert")

            if bib_options.get("update", False):
                cmd_parts.append("--update")

            framework = bib_options.get("framework")
            if framework:
                cmd_parts.extend(["-framework", framework])

            shell_cmd = " ".join(cmd_parts)
            log.info(f"Running: {shell_cmd}")

            self.update_state(state="PROGRESS", meta={
                "step": "running_import",
                "file": filename,
                "file_index": i + 1,
                "file_count": len(marc_paths),
                "command": shell_cmd,
            })

            # Execute via koha-shell inside the container
            exec_result = subprocess.run(
                ["docker", "exec", container, "koha-shell", "-c", shell_cmd, "kohadev"],
                capture_output=True, text=True, timeout=600,
            )

            stdout = exec_result.stdout
            stderr = exec_result.stderr
            all_output.append({
                "file": filename,
                "command": shell_cmd,
                "returncode": exec_result.returncode,
                "stdout": stdout[-5000:] if len(stdout) > 5000 else stdout,
                "stderr": stderr[-2000:] if len(stderr) > 2000 else stderr,
            })

            if exec_result.returncode != 0:
                log.warn(f"bulkmarcimport.pl returned code {exec_result.returncode} for {filename}")
                log.warn(f"stderr: {stderr[:1000]}")

            # Parse stdout for record counts
            # bulkmarcimport.pl outputs lines like:
            #   "X records added"
            #   "X records updated"
            #   "X records done"
            import re
            added = 0
            updated = 0
            done = 0
            errors = 0
            for line in stdout.splitlines():
                m = re.search(r"(\d+)\s+records?\s+added", line, re.IGNORECASE)
                if m:
                    added = int(m.group(1))
                m = re.search(r"(\d+)\s+records?\s+updated", line, re.IGNORECASE)
                if m:
                    updated = int(m.group(1))
                m = re.search(r"(\d+)\s+records?\s+done", line, re.IGNORECASE)
                if m:
                    done = int(m.group(1))
                m = re.search(r"(\d+)\s+records?\s+(not added|errors?|failed)", line, re.IGNORECASE)
                if m:
                    errors = int(m.group(1))

            file_success = added + updated
            file_total = done if done > 0 else (file_success + errors)
            total_records += file_total
            total_success += file_success
            total_failed += errors

            log.info(f"{filename}: {file_total} processed, {added} added, {updated} updated, {errors} errors")

            # Clean up temp file in container
            subprocess.run(
                ["docker", "exec", container, "rm", "-f", container_path],
                capture_output=True, text=True, timeout=10,
            )

        # Update manifest
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total_records,
                                  records_success=total_success,
                                  records_failed=total_failed,
                                  result_data={"outputs": all_output, "method": "bulkmarcimport"},
                                  completed_at=datetime.utcnow())

            if not dry_run:
                project = db.get(Project, project_id)
                if project:
                    project.bib_count_pushed = total_success
                    db.commit()

        log.complete(
            f"bulkmarcimport.pl complete — {total_records:,} records, "
            f"{total_success:,} success, {total_failed:,} failed (dry_run={dry_run})"
        )
        return {
            "records_total": total_records,
            "records_success": total_success,
            "records_failed": total_failed,
            "dry_run": dry_run,
            "method": "bulkmarcimport",
            "outputs": all_output,
        }

    except subprocess.TimeoutExpired as exc:
        error_msg = f"bulkmarcimport.pl timed out after 600s: {exc}"
        log.error(error_msg)
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=error_msg,
                                  completed_at=datetime.utcnow())
        raise
    except Exception as exc:
        log.error(f"bulkmarcimport.pl push failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc),
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# MIGRATION MODE — stop/start Koha daemons
# ══════════════════════════════════════════════════════════════════════════
#
# During bulk MARC import + reindex, Koha's background daemons compete for
# CPU, RAM, and ES resources.  Migration mode stops non-essential daemons
# before the push and restarts them after.
#
# Daemons stopped:
#   koha-es-indexer      — incremental ES indexer (biggest offender)
#   koha-indexer         — Zebra indexer daemon
#   koha-zebra           — Zebra search server
#   koha-sip             — SIP2 circulation protocol
#   koha-z3950-responder — Z39.50 search responder
#
# Daemons kept running:
#   koha-worker          — processes background jobs (FastMARCImport, TurboIndex)
#   koha-plack           — serves REST API
# ══════════════════════════════════════════════════════════════════════════

# Daemons to stop/start.  Order matters: stop ES indexer first (biggest
# resource hog), start it last.
_MIGRATION_DAEMONS = [
    "koha-es-indexer",
    "koha-indexer",
    "koha-zebra",
    "koha-sip",
    "koha-z3950-responder",
]


def _koha_container_for_project(project_id: str) -> str:
    """Resolve the Koha Docker container name for a project.

    Tries: SandboxInstance table → Docker API container list → fallback.
    """
    from cerulean.models import SandboxInstance

    # Check SandboxInstance first (if user has configured one)
    with Session(_engine) as db:
        instance = db.query(SandboxInstance).filter_by(
            project_id=project_id, status="running"
        ).first()
        if instance and instance.container_name:
            return instance.container_name

    # Auto-detect: query Docker API for containers with "koha" in the name
    # that are on the kohanet network.
    try:
        transport = httpx.HTTPTransport(uds="/var/run/docker.sock")
        with httpx.Client(transport=transport, timeout=5.0) as client:
            r = client.get("http://localhost/containers/json")
            if r.status_code == 200:
                for c in r.json():
                    names = [n.lstrip("/") for n in c.get("Names", [])]
                    for name in names:
                        # Match containers like "kohadev-koha-1", "koha-koha-1", etc.
                        # Exclude db, es, memcached, selenium, cerulean
                        if "koha" in name.lower() and name.endswith("-1"):
                            exclude = ("db-", "es-", "memcached-", "selenium-",
                                       "cerulean-", "redis-", "postgres-")
                            if not any(x in name.lower() for x in exclude):
                                return name
    except Exception:
        pass

    # Last resort fallback
    return "kohadev-koha-1"


def _run_koha_cmd(container: str, cmd: str, timeout: int = 30) -> tuple[int, str, str]:
    """Run a command inside the Koha container via Docker Engine API.

    Uses the mounted Docker socket (/var/run/docker.sock) directly via
    HTTP, so we don't need the Docker CLI binary installed.

    Returns (returncode, stdout, stderr).
    """
    import httpx
    import json
    import time

    socket = "/var/run/docker.sock"
    base = "http://localhost"

    transport = httpx.HTTPTransport(uds=socket)
    with httpx.Client(transport=transport, timeout=float(timeout)) as client:
        # Step 1: Create an exec instance
        create_resp = client.post(
            f"{base}/containers/{container}/exec",
            json={
                "Cmd": ["bash", "-c", cmd],
                "AttachStdout": True,
                "AttachStderr": True,
            },
        )
        if create_resp.status_code != 201:
            return -1, "", f"exec create failed: {create_resp.status_code} {create_resp.text[:200]}"
        exec_id = create_resp.json()["Id"]

        # Step 2: Start the exec and capture output
        start_resp = client.post(
            f"{base}/exec/{exec_id}/start",
            json={"Detach": False, "Tty": False},
        )
        # Output comes as a raw stream with Docker multiplexing headers.
        # For simplicity, grab the raw text and strip control chars.
        raw = start_resp.content
        # Docker multiplexing: each frame has an 8-byte header.
        # Byte 0: stream type (1=stdout, 2=stderr), bytes 4-7: frame size (big-endian).
        stdout_parts = []
        stderr_parts = []
        pos = 0
        while pos + 8 <= len(raw):
            stream_type = raw[pos]
            frame_size = int.from_bytes(raw[pos+4:pos+8], 'big')
            if pos + 8 + frame_size > len(raw):
                # Incomplete frame — grab what's left
                frame_data = raw[pos+8:]
            else:
                frame_data = raw[pos+8:pos+8+frame_size]
            if stream_type == 1:
                stdout_parts.append(frame_data)
            elif stream_type == 2:
                stderr_parts.append(frame_data)
            pos += 8 + frame_size

        # Step 3: Inspect to get exit code
        inspect_resp = client.get(f"{base}/exec/{exec_id}/json")
        exit_code = inspect_resp.json().get("ExitCode", -1) if inspect_resp.status_code == 200 else -1

        stdout = b"".join(stdout_parts).decode("utf-8", errors="replace").strip()
        stderr = b"".join(stderr_parts).decode("utf-8", errors="replace").strip()
        return exit_code, stdout, stderr


def _detect_koha_instance(container: str) -> str:
    """Auto-detect the Koha instance name inside the container.

    Koha containers can have any instance name (kohadev, library, prod, etc.).
    We detect it by listing /etc/koha/sites/ — there's usually exactly one
    instance directory.  Falls back to 'kohadev' if detection fails.
    """
    rc, stdout, stderr = _run_koha_cmd(
        container,
        "ls /etc/koha/sites/ 2>/dev/null | head -1",
    )
    instance = stdout.strip()
    if instance and instance != "." and "/" not in instance:
        return instance
    # Fallback: try koha-list
    rc, stdout, stderr = _run_koha_cmd(container, "koha-list 2>/dev/null | head -1")
    instance = stdout.strip()
    if instance:
        return instance
    return "kohadev"


def migration_mode_status(project_id: str) -> dict:
    """Check which daemons are running/stopped in the Koha container."""
    container = _koha_container_for_project(project_id)
    instance = _detect_koha_instance(container)
    statuses = {}
    for daemon in _MIGRATION_DAEMONS:
        # Check via pidfile (avoids pgrep self-match issues).
        # Koha daemons write pidfiles to /var/run/koha/{instance}/.
        # Verify the PID is actually alive with kill -0.
        rc, stdout, stderr = _run_koha_cmd(
            container,
            (
                f"PIDFILE=/var/run/koha/{instance}/{instance}-{daemon}.pid && "
                f"if [ -f $PIDFILE ] && kill -0 $(cat $PIDFILE 2>/dev/null) 2>/dev/null; then "
                f"echo running; else echo stopped; fi"
            ),
        )
        statuses[daemon] = stdout if stdout in ("running", "stopped") else "unknown"
    return {"container": container, "instance": instance, "daemons": statuses}


def _daemon_cmd(action: str, daemon: str, instance: str, container: str) -> dict:
    """Build and run the appropriate stop/start command for a Koha daemon."""
    # Map daemon names to koha-* commands.  These commands are standard
    # across all Koha installations (packages, KTD, manual installs).
    cmd_map = {
        "koha-es-indexer":       f"koha-es-indexer --{action} {instance}",
        "koha-zebra":            f"koha-zebra --{action} {instance}",
        "koha-sip":              f"koha-sip --{action} {instance}",
        "koha-z3950-responder":  f"koha-z3950-responder --{action} {instance}",
    }

    if daemon in cmd_map:
        cmd = f"{cmd_map[daemon]} 2>&1 || true"
    elif daemon == "koha-indexer":
        # The Zebra indexer daemon doesn't have a koha-* wrapper in all
        # installations.  Use the daemon utility directly.
        if action == "stop":
            cmd = f"daemon --name={instance}-koha-indexer --stop 2>&1 || true"
        else:
            # Restart — find the script path dynamically
            cmd = (
                f"KOHA_HOME=$(xmlstarlet sel -t -v '//config/intranetdir' "
                f"/etc/koha/sites/{instance}/koha-conf.xml 2>/dev/null || echo /usr/share/koha) && "
                f"daemon --name={instance}-koha-indexer "
                f"--errlog=/var/log/koha/{instance}/indexer-error.log "
                f"--output=/var/log/koha/{instance}/indexer-output.log "
                f"--pidfiles=/var/run/koha/{instance}/ "
                f"--verbose=1 --respawn --delay=30 "
                f"--user={instance}-koha.{instance}-koha "
                f"-- $KOHA_HOME/misc/migration_tools/rebuild_zebra.pl -daemon -sleep 5 "
                f"2>&1 || true"
            )
    else:
        return {"action": action, "returncode": -1, "output": f"Unknown daemon: {daemon}"}

    rc, stdout, stderr = _run_koha_cmd(container, cmd, timeout=30)
    return {
        "action": action,
        "returncode": rc,
        "output": (stdout or stderr)[:200],
    }


@celery_app.task(name="cerulean.tasks.push.migration_mode_status_task", queue="push")
def migration_mode_status_task(project_id: str) -> dict:
    """Celery wrapper for migration_mode_status (needs Docker socket)."""
    return migration_mode_status(project_id)


@celery_app.task(name="cerulean.tasks.push.migration_mode_enable_task", queue="push")
def migration_mode_enable_task(project_id: str, buffer_pool_mb: int = 2048) -> dict:
    """Celery wrapper for migration_mode_enable (needs Docker socket)."""
    return migration_mode_enable(project_id, buffer_pool_mb=buffer_pool_mb)


@celery_app.task(name="cerulean.tasks.push.migration_mode_disable_task", queue="push")
def migration_mode_disable_task(project_id: str) -> dict:
    """Celery wrapper for migration_mode_disable (needs Docker socket)."""
    return migration_mode_disable(project_id)


def _tune_koha_db(project_id: str, mode: str = "fast",
                   buffer_pool_mb: int = 2048) -> dict:
    """Tune the Koha MariaDB for migration speed or restore defaults.

    Args:
        project_id: UUID of the project (used to find the DB container).
        mode: "fast" — relax durability + grow buffer pool for import speed.
              "safe" — restore production-safe defaults.
        buffer_pool_mb: InnoDB buffer pool size in MB (only used in "fast" mode).

    Returns dict with original values (for restore) and what was set.
    """
    # Need SUPER privilege for SET GLOBAL — the koha_* user doesn't have
    # it, so we connect as root.  Derive the DB hostname the same way
    # _koha_mysql_conn does (kohadev-koha-1 → kohadev-db-1).
    try:
        import pymysql
        from cerulean.models import SandboxInstance

        db_host = "kohadev-db-1"
        with Session(_engine) as db:
            instance = db.query(SandboxInstance).filter_by(
                project_id=project_id, status="running"
            ).first()
            if instance and instance.container_name:
                parts = instance.container_name.rsplit("-", 2)
                if len(parts) >= 2:
                    db_host = f"{parts[0]}-db-1"

        conn = pymysql.connect(
            host=db_host,
            user="root",
            password="password",
            port=3306,
            connect_timeout=10,
        )
    except Exception as exc:
        return {"error": f"DB connection failed: {exc}", "tuned": False}

    result = {"tuned": True, "mode": mode, "settings": {}}
    try:
        cursor = conn.cursor()
        if mode == "fast":
            # Save originals
            cursor.execute("SELECT @@innodb_flush_log_at_trx_commit")
            orig_flush = cursor.fetchone()[0]
            cursor.execute("SELECT @@innodb_buffer_pool_size")
            orig_pool = cursor.fetchone()[0]

            result["original"] = {
                "innodb_flush_log_at_trx_commit": orig_flush,
                "innodb_buffer_pool_size": orig_pool,
            }

            # Apply fast settings
            cursor.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 2")
            pool_bytes = buffer_pool_mb * 1024 * 1024
            cursor.execute(f"SET GLOBAL innodb_buffer_pool_size = {pool_bytes}")
            conn.commit()

            # Verify
            cursor.execute("SELECT @@innodb_flush_log_at_trx_commit")
            new_flush = cursor.fetchone()[0]
            cursor.execute("SELECT @@innodb_buffer_pool_size")
            new_pool = cursor.fetchone()[0]

            result["settings"] = {
                "innodb_flush_log_at_trx_commit": new_flush,
                "innodb_buffer_pool_size": new_pool,
                "innodb_buffer_pool_size_mb": round(new_pool / (1024 * 1024)),
            }

        elif mode == "safe":
            # Restore production defaults
            cursor.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 1")
            # Don't shrink buffer pool — leave it as-is. Shrinking can be
            # slow and disruptive. The DBA can resize manually if needed.
            conn.commit()

            cursor.execute("SELECT @@innodb_flush_log_at_trx_commit")
            new_flush = cursor.fetchone()[0]
            cursor.execute("SELECT @@innodb_buffer_pool_size")
            current_pool = cursor.fetchone()[0]

            result["settings"] = {
                "innodb_flush_log_at_trx_commit": new_flush,
                "innodb_buffer_pool_size": current_pool,
                "innodb_buffer_pool_size_mb": round(current_pool / (1024 * 1024)),
            }
    except Exception as exc:
        result["error"] = str(exc)[:200]
        result["tuned"] = False
    finally:
        conn.close()

    return result


def migration_mode_enable(project_id: str, buffer_pool_mb: int = 2048) -> dict:
    """Stop non-essential Koha daemons and tune DB for migration."""
    container = _koha_container_for_project(project_id)
    instance = _detect_koha_instance(container)
    daemon_results = {}
    for daemon in _MIGRATION_DAEMONS:
        daemon_results[daemon] = _daemon_cmd("stop", daemon, instance, container)

    db_tuning = _tune_koha_db(project_id, mode="fast", buffer_pool_mb=buffer_pool_mb)

    return {
        "container": container,
        "instance": instance,
        "results": daemon_results,
        "db_tuning": db_tuning,
    }


def migration_mode_disable(project_id: str) -> dict:
    """Restart non-essential Koha daemons and restore DB defaults."""
    container = _koha_container_for_project(project_id)
    instance = _detect_koha_instance(container)
    daemon_results = {}
    for daemon in reversed(_MIGRATION_DAEMONS):
        daemon_results[daemon] = _daemon_cmd("start", daemon, instance, container)

    db_tuning = _tune_koha_db(project_id, mode="safe")

    return {
        "container": container,
        "instance": instance,
        "results": daemon_results,
        "db_tuning": db_tuning,
    }


# ══════════════════════════════════════════════════════════════════════════
# BULK BIBLIOS API PUSH TASK
# ══════════════════════════════════════════════════════════════════════════


def _iter_file_chunks(path: Path, chunk_size: int = 1 << 20):
    """Stream a file in chunks for upload (default 1 MB chunks).

    Keeps peak memory bounded regardless of file size.  Using a generator
    here causes httpx to set a fixed Content-Length (from the caller-supplied
    header) and write the body incrementally rather than buffering.
    """
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            yield chunk


def _poll_interval(elapsed_secs: float) -> float:
    """Exponential-ish backoff for Koha job polling.

    0-60s   → 3s   (responsive for small jobs)
    60-300s → 10s  (medium jobs — cut poll rate ~3x)
    300s+   → 30s  (long-running jobs — don't waste cycles)
    """
    if elapsed_secs < 60:
        return 3.0
    if elapsed_secs < 300:
        return 10.0
    return 30.0


def _poll_koha_job(client: httpx.Client, base_url: str, headers: dict,
                   job_id: int, celery_task, project_id: str,
                   step_label: str, timeout: float = 14400.0,
                   on_progress=None) -> dict:
    """Poll GET /api/v1/jobs/{job_id} until finished/failed.

    Updates Celery task state with progress, optionally calls on_progress(job)
    for custom handling (e.g. persisting to manifest).  Returns the final job
    JSON.  Uses adaptive backoff: 3s -> 10s -> 30s as elapsed time grows.
    Raises RuntimeError on failure or timeout.
    """
    import time
    elapsed = 0.0
    status = ""
    while elapsed < timeout:
        _check_paused(project_id, celery_task)
        resp = client.get(f"{base_url}/api/v1/jobs/{job_id}", headers=headers)
        if resp.status_code >= 400:
            raise RuntimeError(f"Job poll returned HTTP {resp.status_code}: {resp.text[:500]}")
        job = resp.json()
        status = job.get("status", "")
        # Koha returns progress/size as either int or str depending on the
        # job type — coerce defensively.
        try:
            progress = int(job.get("progress") or 0)
        except (TypeError, ValueError):
            progress = 0
        try:
            size = int(job.get("size") or 0)
        except (TypeError, ValueError):
            size = 0

        celery_task.update_state(state="PROGRESS", meta={
            "step": step_label,
            "job_id": job_id,
            "job_status": status,
            "records_done": progress,
            "records_total": size,
            "elapsed_secs": int(elapsed),
        })

        if on_progress is not None:
            try:
                on_progress(job)
            except Exception:
                pass  # progress callbacks must never abort polling

        if status == "finished":
            return job
        if status == "failed":
            # Koha stores errors in various places depending on job type.  Try
            # the common ones and fall back to the raw data blob so the user
            # always sees *something* actionable.
            jdata = job.get("data") or {}
            jreport = jdata.get("report") or {}
            err = (
                jreport.get("errors")
                or jreport.get("error")
                or jdata.get("messages")
                or jdata.get("error")
            )
            if not err:
                # Last resort: dump the whole data blob (truncated)
                import json as _json
                err = _json.dumps(jdata, default=str)[:500]
            raise RuntimeError(
                f"Koha background job {job_id} failed at progress={progress}/{size}: {str(err)[:500]}"
            )

        interval = _poll_interval(elapsed)
        time.sleep(interval)
        elapsed += interval

    raise RuntimeError(f"Koha job {job_id} timed out after {timeout}s (last status: {status})")


def _persist_bulk_state(manifest_id: str, files_state: list[dict],
                         dry_run: bool, current_idx: int) -> None:
    """Write per-file bulk-api state to the PushManifest.result_data field.

    Called after every state transition so a restart can recover Koha job IDs
    and (for post-mortem) a dashboard can show which file is currently active.
    """
    totals = {
        "records_total": sum(f.get("records_total", 0) or 0 for f in files_state),
        "num_added": sum(f.get("num_added", 0) or 0 for f in files_state),
        "num_updated": sum(f.get("num_updated", 0) or 0 for f in files_state),
        "num_ignored": sum(f.get("num_ignored", 0) or 0 for f in files_state),
    }
    result_data = {
        "method": "bulk_api",
        "dry_run": dry_run,
        "current_file_index": current_idx,
        "file_count": len(files_state),
        "files": files_state,
        "totals": totals,
    }
    with Session(_engine) as db:
        _update_push_manifest(db, manifest_id, result_data=result_data)


def _stage_one_file(
    client: httpx.Client, base_url: str, headers: dict,
    marc_path: Path, params: dict, celery_task, project_id: str,
    file_idx: int, file_count: int, log: AuditLogger,
) -> tuple[int, int, dict]:
    """Upload (streaming) + poll staging for one MARC file.

    Returns (staging_job_id, import_batch_id, staging_report).
    Raises RuntimeError on any failure.
    """
    file_size = marc_path.stat().st_size
    log.info(f"[{file_idx + 1}/{file_count}] Staging {marc_path.name} ({file_size:,} bytes)")

    celery_task.update_state(state="PROGRESS", meta={
        "step": "uploading",
        "file": marc_path.name,
        "file_index": file_idx,
        "file_count": file_count,
        "file_size": file_size,
    })

    stage_headers = {
        **headers,
        "Content-Type": "application/marc",
        "Content-Length": str(file_size),
        "x-file-name": marc_path.name,
    }

    # Stream the file in 1 MB chunks — peak memory stays bounded
    resp = client.post(
        f"{base_url}/api/v1/biblios/import",
        content=_iter_file_chunks(marc_path),
        headers=stage_headers,
        params=params,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Staging failed: HTTP {resp.status_code} — {resp.text[:500]}")

    stage_data = resp.json()
    staging_job_id = stage_data.get("job_id")
    if not staging_job_id:
        raise RuntimeError(f"No job_id in staging response: {stage_data}")

    log.info(f"[{file_idx + 1}/{file_count}] Staging job started: job_id={staging_job_id}")

    staging_result = _poll_koha_job(
        client, base_url, headers,
        staging_job_id, celery_task, project_id,
        step_label=f"staging [{file_idx + 1}/{file_count}]",
    )
    report = staging_result.get("data", {}).get("report", {}) or {}
    import_batch_id = report.get("import_batch_id")
    return staging_job_id, import_batch_id, report


def _commit_one_batch(
    client: httpx.Client, base_url: str, headers: dict,
    import_batch_id: int, bib_options: dict, celery_task, project_id: str,
    file_idx: int, file_count: int, log: AuditLogger,
) -> tuple[int, dict]:
    """POST /import_batches/{id}/commit then poll the commit job.

    Returns (commit_job_id, commit_report).  Raises RuntimeError on failure.
    """
    celery_task.update_state(state="PROGRESS", meta={
        "step": f"committing [{file_idx + 1}/{file_count}]",
        "import_batch_id": import_batch_id,
    })

    commit_body: dict = {}
    if bib_options.get("framework"):
        commit_body["framework"] = bib_options["framework"]

    commit_resp = client.post(
        f"{base_url}/api/v1/import_batches/{import_batch_id}/commit",
        json=commit_body,
        headers={**headers, "Content-Type": "application/json"},
    )
    if commit_resp.status_code >= 400:
        raise RuntimeError(
            f"Commit failed: HTTP {commit_resp.status_code} — {commit_resp.text[:500]}"
        )

    commit_data = commit_resp.json()
    commit_job_id = commit_data.get("job_id")
    if not commit_job_id:
        raise RuntimeError(f"No job_id in commit response: {commit_data}")

    log.info(f"[{file_idx + 1}/{file_count}] Commit job started: job_id={commit_job_id}")

    commit_result = _poll_koha_job(
        client, base_url, headers,
        commit_job_id, celery_task, project_id,
        step_label=f"committing [{file_idx + 1}/{file_count}]",
    )
    commit_report = commit_result.get("data", {}).get("report", {}) or {}
    return commit_job_id, commit_report


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_bulk_api_task",
    max_retries=0,
    queue="push",
)
def push_bulk_api_task(
    self, project_id: str, manifest_id: str,
    dry_run: bool = True, bib_options: dict | None = None,
) -> dict:
    """Push MARC records via Koha Bulk Biblios Import API.

    Iterates over every MARC file found in the project (each becomes its own
    import batch).  For each file:
        1. POST /api/v1/biblios/import  — stream MARC file (chunked upload)
        2. Poll GET /api/v1/jobs/{job_id} with exponential backoff
        3. POST /api/v1/import_batches/{batch_id}/commit (skipped if dry_run)
        4. Poll commit job

    Per-file state (staging_job_id, import_batch_id, commit_job_id, status)
    is persisted to PushManifest.result_data on every transition, enabling
    recovery / dashboards after a worker crash.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[push-bibs-bulk]")
    bib_options = bib_options or {}
    log.info(f"Bulk API push starting (dry_run={dry_run}, options={bib_options})")

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

        base_url, headers = _koha_client(project_id)

        # ── Build staging query params (shared across all files) ─────
        params: dict = {
            "record_type": bib_options.get("record_type", "biblio"),
            "encoding": bib_options.get("encoding", "UTF-8"),
            "parse_items": str(bib_options.get("parse_items", True)).lower(),
        }
        if bib_options.get("matcher_id"):
            params["matcher_id"] = bib_options["matcher_id"]
        if bib_options.get("overlay_action"):
            params["overlay_action"] = bib_options["overlay_action"]
        if bib_options.get("nomatch_action"):
            params["nomatch_action"] = bib_options["nomatch_action"]
        if bib_options.get("item_action"):
            params["item_action"] = bib_options["item_action"]
        if bib_options.get("comments"):
            params["comments"] = bib_options["comments"]
        if bib_options.get("framework"):
            params["framework"] = bib_options["framework"]

        # auto_commit + dry_run is nonsense — dry_run wins
        auto_commit = bool(bib_options.get("auto_commit", False)) and not dry_run
        if auto_commit:
            params["auto_commit"] = "true"

        # ── Initialize per-file state ────────────────────────────────
        files_state: list[dict] = [
            {
                "filename": p.name,
                "file_size": p.stat().st_size,
                "status": "pending",  # pending|staging|staged|committing|committed|error
                "staging_job_id": None,
                "import_batch_id": None,
                "commit_job_id": None,
                "records_total": 0,
                "num_added": 0,
                "num_updated": 0,
                "num_ignored": 0,
                "error": None,
            }
            for p in marc_paths
        ]
        _persist_bulk_state(manifest_id, files_state, dry_run, current_idx=0)

        log.info(f"Pushing {len(marc_paths)} file(s): {[p.name for p in marc_paths]}")

        # ── Process each file sequentially ───────────────────────────
        with httpx.Client(timeout=600.0, verify=False) as client:
            for idx, marc_path in enumerate(marc_paths):
                fstate = files_state[idx]
                try:
                    # Stage
                    fstate["status"] = "staging"
                    _persist_bulk_state(manifest_id, files_state, dry_run, current_idx=idx)

                    staging_job_id, import_batch_id, staging_report = _stage_one_file(
                        client, base_url, headers, marc_path, params,
                        self, project_id, idx, len(marc_paths), log,
                    )
                    fstate["staging_job_id"] = staging_job_id
                    fstate["import_batch_id"] = import_batch_id
                    # Number of records staged (not committed yet).  Koha
                    # uses "staged" and "total" in the staging report.
                    records_staged = (
                        staging_report.get("staged")
                        or staging_report.get("total")
                        or staging_report.get("num_records")
                        or 0
                    )
                    fstate["records_total"] = records_staged
                    fstate["status"] = "staged"
                    _persist_bulk_state(manifest_id, files_state, dry_run, current_idx=idx)
                    log.info(f"[{idx + 1}/{len(marc_paths)}] Staged: batch_id={import_batch_id}, records={records_staged}")

                    if dry_run:
                        # Count staged records as "success" for dry-run totals
                        fstate["num_added"] = records_staged
                        continue

                    # Commit
                    fstate["status"] = "committing"
                    _persist_bulk_state(manifest_id, files_state, dry_run, current_idx=idx)

                    if auto_commit:
                        commit_job_id = staging_report.get("commit_job_id")
                        if not commit_job_id:
                            raise RuntimeError(
                                f"auto_commit set but no commit_job_id in staging report: {staging_report}"
                            )
                        # Still poll the commit job to pick up its report
                        commit_result = _poll_koha_job(
                            client, base_url, headers,
                            commit_job_id, self, project_id,
                            step_label=f"committing [{idx + 1}/{len(marc_paths)}]",
                        )
                        commit_report = commit_result.get("data", {}).get("report", {}) or {}
                    else:
                        if not import_batch_id:
                            raise RuntimeError("No import_batch_id from staging — cannot commit")
                        commit_job_id, commit_report = _commit_one_batch(
                            client, base_url, headers, import_batch_id, bib_options,
                            self, project_id, idx, len(marc_paths), log,
                        )

                    fstate["commit_job_id"] = commit_job_id
                    fstate["num_added"] = commit_report.get("num_added", 0) or 0
                    fstate["num_updated"] = commit_report.get("num_updated", 0) or 0
                    fstate["num_ignored"] = commit_report.get("num_ignored", 0) or 0
                    fstate["status"] = "committed"
                    _persist_bulk_state(manifest_id, files_state, dry_run, current_idx=idx)
                    log.info(
                        f"[{idx + 1}/{len(marc_paths)}] Committed: "
                        f"{fstate['num_added']} added, {fstate['num_updated']} updated, "
                        f"{fstate['num_ignored']} ignored"
                    )

                except Exception as file_exc:
                    fstate["status"] = "error"
                    fstate["error"] = str(file_exc)[:500]
                    _persist_bulk_state(manifest_id, files_state, dry_run, current_idx=idx)
                    log.error(f"[{idx + 1}/{len(marc_paths)}] Failed: {file_exc}")
                    raise  # bubble up — stop processing further files

        # ── Aggregate final counts ───────────────────────────────────
        total_records = sum(f["records_total"] for f in files_state)
        total_added = sum(f["num_added"] for f in files_state)
        total_updated = sum(f["num_updated"] for f in files_state)
        total_ignored = sum(f["num_ignored"] for f in files_state)
        records_success = total_added + total_updated
        records_failed = total_ignored

        # Final manifest write with canonical columns + structured result_data
        result_data = {
            "method": "bulk_api",
            "dry_run": dry_run,
            "file_count": len(files_state),
            "files": files_state,
            "totals": {
                "records_total": total_records,
                "num_added": total_added,
                "num_updated": total_updated,
                "num_ignored": total_ignored,
            },
        }
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total_records,
                                  records_success=records_success if not dry_run else total_records,
                                  records_failed=records_failed,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())
            if not dry_run:
                project = db.get(Project, project_id)
                if project:
                    project.bib_count_pushed = records_success
                    db.commit()

        if dry_run:
            log.complete(
                f"Bulk API dry run complete — {len(files_state)} file(s), "
                f"{total_records:,} records staged"
            )
        else:
            log.complete(
                f"Bulk API push complete — {len(files_state)} file(s), {total_records:,} records: "
                f"{total_added:,} added, {total_updated:,} updated, {total_ignored:,} ignored"
            )
        return result_data

    except Exception as exc:
        log.error(f"Bulk API push failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# FAST MARC IMPORT (Koha plugin) PUSH TASK
# ══════════════════════════════════════════════════════════════════════════
#
# Uses the Koha `fastmarcimport` plugin which does staging + commit in a
# single call with N parallel worker processes.  Much faster than the core
# bulk API (which does row-by-row inserts in one transaction).
#
# Plugin endpoint: POST /api/v1/contrib/fastmarcimport/import
#     Body: raw MARC (application/marc) or MARCXML (application/marcxml+xml)
#     Returns: { "job_id": N }
# Poll:  GET /api/v1/jobs/{id}
# Report fields: num_added, num_errors, duration_sec, records_per_sec,
#                worker_count, biblio_ids
# ══════════════════════════════════════════════════════════════════════════

_FASTMARC_PATH = "/api/v1/contrib/fastmarcimport/import"


def _fastmarc_plugin_available(client: httpx.Client, base_url: str, headers: dict) -> bool:
    """Return True if the fastmarcimport plugin endpoint is installed.

    Uses OPTIONS which Mojolicious answers with 200 for any method on an
    existing route, and 404 for missing routes — no side effects, no
    spurious log entries.
    """
    try:
        resp = client.request(
            "OPTIONS",
            f"{base_url}{_FASTMARC_PATH}",
            headers=headers,
            timeout=10.0,
        )
        return resp.status_code != 404
    except httpx.HTTPError:
        return False


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_fastmarc_plugin_task",
    max_retries=0,
    queue="push",
)
def push_fastmarc_plugin_task(
    self, project_id: str, manifest_id: str,
    dry_run: bool = False, bib_options: dict | None = None,
) -> dict:
    """Push MARC records via the Koha fastmarcimport plugin.

    Single POST per file (no separate commit step).  The plugin shards
    records across N parallel workers and does everything in one background
    job, so wall time is roughly (staging + commit) / worker_count.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        dry_run: NOT SUPPORTED by the plugin — the endpoint always commits.
                 We reject dry_run=True to avoid surprising the user.
        bib_options: Dict (unused by plugin; settings are plugin-side config).
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[push-bibs-fast]")
    bib_options = bib_options or {}
    log.info(f"Fast MARC plugin push starting (dry_run={dry_run})")

    if dry_run:
        error_msg = "Fast MARC plugin does not support dry_run — use bulk_api method instead"
        log.error(error_msg)
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=error_msg,
                                  completed_at=datetime.utcnow())
        return {"error": error_msg}

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

        base_url, headers = _koha_client(project_id)

        # Initialize per-file state (same shape as bulk_api for consistency)
        files_state: list[dict] = [
            {
                "filename": p.name,
                "file_size": p.stat().st_size,
                "status": "pending",
                "job_id": None,
                "records_total": 0,
                "num_added": 0,
                "num_errors": 0,
                "num_items_added": 0,
                "num_item_errors": 0,
                "duplicate_barcodes": 0,
                "records_processed": 0,
                "slices_completed": 0,
                "skipped_in_split": 0,
                "chunks_rolled_back": 0,
                "first_error_message": None,
                "failed_records": [],
                # v0.5.0 fields
                "error_mode": None,
                "wide_char_warnings": 0,
                "sidecar_records": 0,
                "sidecar_recovered": 0,
                "duration_sec": None,
                "records_per_sec": None,
                "worker_count": None,
                "error": None,
            }
            for p in marc_paths
        ]
        _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=0)

        log.info(f"Pushing {len(marc_paths)} file(s) via fastmarcimport plugin")

        with httpx.Client(timeout=600.0, verify=False) as client:
            # Preflight check
            if not _fastmarc_plugin_available(client, base_url, headers):
                error_msg = (
                    "fastmarcimport plugin endpoint not found at "
                    f"{base_url}{_FASTMARC_PATH} — is the plugin installed?"
                )
                log.error(error_msg)
                with Session(_engine) as db:
                    _update_push_manifest(db, manifest_id,
                                          status="error", error_message=error_msg,
                                          completed_at=datetime.utcnow())
                return {"error": error_msg}

            for idx, marc_path in enumerate(marc_paths):
                fstate = files_state[idx]
                try:
                    file_size = marc_path.stat().st_size
                    log.info(f"[{idx + 1}/{len(marc_paths)}] Uploading {marc_path.name} ({file_size:,} bytes)")

                    self.update_state(state="PROGRESS", meta={
                        "step": "uploading",
                        "file": marc_path.name,
                        "file_index": idx,
                        "file_count": len(marc_paths),
                        "file_size": file_size,
                    })

                    # Stream the file in 1 MB chunks — bounded memory
                    upload_headers = {
                        **headers,
                        "Content-Type": "application/marc",
                        "Content-Length": str(file_size),
                        "x-file-name": marc_path.name,
                    }
                    resp = client.post(
                        f"{base_url}{_FASTMARC_PATH}",
                        content=_iter_file_chunks(marc_path),
                        headers=upload_headers,
                    )
                    if resp.status_code >= 400:
                        raise RuntimeError(
                            f"Upload failed: HTTP {resp.status_code} — {resp.text[:500]}"
                        )

                    resp_data = resp.json()
                    job_id = resp_data.get("job_id")
                    if not job_id:
                        raise RuntimeError(f"No job_id in response: {resp_data}")

                    fstate["job_id"] = job_id
                    fstate["status"] = "running"
                    _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=idx)
                    log.info(f"[{idx + 1}/{len(marc_paths)}] Plugin job started: job_id={job_id}")

                    # Poll the plugin's job until complete
                    job_result = _poll_koha_job(
                        client, base_url, headers,
                        job_id, self, project_id,
                        step_label=f"importing [{idx + 1}/{len(marc_paths)}]",
                    )
                    report = job_result.get("data", {}).get("report", {}) or {}

                    # Plugin returns numeric fields as strings — coerce.
                    def _as_int(v, default=0):
                        try:
                            return int(v) if v is not None else default
                        except (TypeError, ValueError):
                            return default

                    def _as_float(v):
                        try:
                            return float(v) if v is not None else None
                        except (TypeError, ValueError):
                            return None

                    fstate["records_total"] = _as_int(
                        job_result.get("size") or report.get("num_added"), 0
                    )
                    fstate["num_added"] = _as_int(report.get("num_added"), 0)
                    fstate["num_errors"] = _as_int(report.get("num_errors"), 0)
                    fstate["num_items_added"] = _as_int(report.get("num_items_added"), 0)
                    fstate["num_item_errors"] = _as_int(report.get("num_item_errors"), 0)
                    # v0.4.0+ fields
                    fstate["duplicate_barcodes"] = _as_int(report.get("duplicate_barcodes"), 0)
                    fstate["records_processed"] = _as_int(report.get("records_processed"), 0)
                    fstate["slices_completed"] = _as_int(report.get("slices_completed"), 0)
                    fstate["skipped_in_split"] = _as_int(report.get("skipped_in_split"), 0)
                    fstate["chunks_rolled_back"] = _as_int(report.get("chunks_rolled_back"), 0)
                    fstate["first_error_message"] = report.get("first_error_message")
                    # Cap failed_records list — can be up to 5000 per slice * workers
                    failed = report.get("failed_records") or []
                    fstate["failed_records"] = failed[:2000]
                    # v0.5.0 fields
                    fstate["error_mode"] = report.get("error_mode")
                    fstate["wide_char_warnings"] = _as_int(report.get("wide_char_warnings"), 0)
                    fstate["sidecar_records"] = _as_int(report.get("sidecar_records"), 0)
                    fstate["sidecar_recovered"] = _as_int(report.get("sidecar_recovered"), 0)
                    fstate["duration_sec"] = _as_float(report.get("duration_sec"))
                    fstate["records_per_sec"] = _as_float(report.get("records_per_sec"))
                    fstate["worker_count"] = _as_int(report.get("worker_count"), 0) or None
                    fstate["status"] = "committed"
                    _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=idx)
                    log.info(
                        f"[{idx + 1}/{len(marc_paths)}] Imported: "
                        f"{fstate['num_added']:,} bibs added ({fstate['num_errors']:,} errors), "
                        f"{fstate['num_items_added']:,} items added ({fstate['num_item_errors']:,} errors), "
                        f"{fstate['duration_sec']}s, {fstate['records_per_sec']} rec/s, "
                        f"{fstate['worker_count']} workers"
                    )

                except Exception as file_exc:
                    fstate["status"] = "error"
                    fstate["error"] = str(file_exc)[:500]
                    _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=idx)
                    log.error(f"[{idx + 1}/{len(marc_paths)}] Failed: {file_exc}")
                    raise

        # Aggregate totals
        total_records = sum(f["records_total"] for f in files_state)
        total_added = sum(f["num_added"] for f in files_state)
        total_errors = sum(f["num_errors"] for f in files_state)
        total_items_added = sum(f["num_items_added"] for f in files_state)
        total_item_errors = sum(f["num_item_errors"] for f in files_state)
        total_duplicate_barcodes = sum(f["duplicate_barcodes"] for f in files_state)
        total_records_processed = sum(f["records_processed"] for f in files_state)
        total_skipped_in_split = sum(f["skipped_in_split"] for f in files_state)
        total_chunks_rolled_back = sum(f["chunks_rolled_back"] for f in files_state)
        total_sidecar_records = sum(f["sidecar_records"] for f in files_state)
        total_sidecar_recovered = sum(f["sidecar_recovered"] for f in files_state)
        total_wide_char_warnings = sum(f["wide_char_warnings"] for f in files_state)

        result_data = {
            "method": "plugin_fast",
            "dry_run": False,
            "file_count": len(files_state),
            "files": files_state,
            "totals": {
                "records_total": total_records,
                "num_added": total_added,
                "num_errors": total_errors,
                "num_items_added": total_items_added,
                "num_item_errors": total_item_errors,
                "duplicate_barcodes": total_duplicate_barcodes,
                "records_processed": total_records_processed,
                "skipped_in_split": total_skipped_in_split,
                "chunks_rolled_back": total_chunks_rolled_back,
                "sidecar_records": total_sidecar_records,
                "sidecar_recovered": total_sidecar_recovered,
                "wide_char_warnings": total_wide_char_warnings,
            },
        }
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total_records,
                                  records_success=total_added,
                                  records_failed=total_errors,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())
            project = db.get(Project, project_id)
            if project:
                project.bib_count_pushed = total_added
                db.commit()

        log.complete(
            f"Fast MARC plugin push complete — {len(files_state)} file(s): "
            f"{total_added:,} bibs ({total_errors:,} err), "
            f"{total_items_added:,} items ({total_item_errors:,} err)"
        )
        return result_data

    except Exception as exc:
        log.error(f"Fast MARC plugin push failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# PATRONS PUSH TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_patrons_task",
    max_retries=0,
    queue="push",
)
def push_patrons_task(self, project_id: str, manifest_id: str, dry_run: bool = True) -> dict:
    """POST patron records from CSV to Koha REST API /api/v1/patrons.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        dry_run: If True, validate and count only.

    Returns:
        dict with records_total, records_success, records_failed.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[push-patrons]")
    log.info(f"Patrons push starting (dry_run={dry_run})")

    try:
        # Prefer Stage 6 output, fall back to legacy patrons.csv
        csv_path = Path(settings.data_root) / project_id / "patrons" / "patrons_transformed.csv"
        if not csv_path.is_file():
            csv_path = Path(settings.data_root) / project_id / "patrons.csv"
        if not csv_path.is_file():
            error_msg = "patrons_transformed.csv or patrons.csv not found"
            log.error(error_msg)
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
            return {"error": error_msg}

        # CSV column (Koha DB name) → Koha REST API field name
        # Full mapping from Koha::Patron->to_api_mapping()
        _PATRON_FIELD_MAP = {
            # Required
            "surname": "surname",
            "branchcode": "library_id",
            "categorycode": "category_id",
            # Identity
            "cardnumber": "cardnumber",
            "firstname": "firstname",
            "title": "title",
            "othernames": "other_name",
            "initials": "initials",
            "sex": "gender",
            "userid": "userid",
            "dateofbirth": "date_of_birth",
            # Primary address
            "streetnumber": "street_number",
            "streettype": "street_type",
            "address": "address",
            "address2": "address2",
            "city": "city",
            "state": "state",
            "zipcode": "postal_code",
            "country": "country",
            # Primary contact
            "email": "email",
            "phone": "phone",
            "mobile": "mobile",
            "fax": "fax",
            "emailpro": "secondary_email",
            "phonepro": "secondary_phone",
            # Alternate address
            "B_streetnumber": "altaddress_street_number",
            "B_streettype": "altaddress_street_type",
            "B_address": "altaddress_address",
            "B_address2": "altaddress_address2",
            "B_city": "altaddress_city",
            "B_state": "altaddress_state",
            "B_zipcode": "altaddress_postal_code",
            "B_country": "altaddress_country",
            "B_email": "altaddress_email",
            "B_phone": "altaddress_phone",
            # Alternate contact
            "altcontactfirstname": "altcontact_firstname",
            "altcontactsurname": "altcontact_surname",
            "altcontactaddress1": "altcontact_address",
            "altcontactaddress2": "altcontact_address2",
            "altcontactaddress3": "altcontact_city",
            "altcontactstate": "altcontact_state",
            "altcontactzipcode": "altcontact_postal_code",
            "altcontactcountry": "altcontact_country",
            "altcontactphone": "altcontact_phone",
            # Dates
            "dateenrolled": "date_enrolled",
            "dateexpiry": "expiry_date",
            # Status flags
            "gonenoaddress": "incorrect_address",
            "lost": "patron_card_lost",
            # Notes
            "borrowernotes": "staff_notes",
            "opacnote": "opac_notes",
            "relationship": "relationship_type",
            # SMS
            "smsalertnumber": "sms_number",
            "sms_provider_id": "sms_provider_id",
            # Statistics / sorting
            "sort1": "statistics_1",
            "sort2": "statistics_2",
            # Preferences
            "lang": "lang",
            "checkprevcheckout": "check_previous_checkout",
        }

        total = 0
        success = 0
        failed = 0
        skipped = 0

        with open(str(csv_path), "r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            csv_headers = reader.fieldnames or []
            log.info(f"CSV headers ({len(csv_headers)}): {csv_headers[:20]}")
            mapped_headers = [h for h in csv_headers if h in _PATRON_FIELD_MAP]
            unmapped_headers = [h for h in csv_headers if h not in _PATRON_FIELD_MAP]
            log.info(f"Mapped to Koha fields: {mapped_headers}")
            if unmapped_headers:
                log.info(f"Unmapped CSV columns (ignored): {unmapped_headers}")

            if dry_run:
                for row in reader:
                    total += 1
                    # Validate required fields
                    if row.get("surname") and row.get("branchcode") and row.get("categorycode"):
                        success += 1
                    else:
                        failed += 1
                    if total % _PROGRESS_INTERVAL == 0:
                        _check_paused(project_id, self)
                        self.update_state(state="PROGRESS", meta={
                            "records_done": total, "dry_run": True,
                        })
            else:
                base_url, headers = _koha_client(project_id)
                log.info(f"Posting patrons to {base_url}/api/v1/patrons")
                # Skip Koha's duplicate patron detection during migration
                headers["x-confirm-not-duplicate"] = "1"
                skipped = 0
                with httpx.Client(timeout=30.0, verify=False) as client:
                    for row in reader:
                        total += 1
                        patron_data = {}
                        for csv_col, koha_field in _PATRON_FIELD_MAP.items():
                            val = row.get(csv_col, "").strip()
                            if val:
                                patron_data[koha_field] = val

                        # Skip rows missing required Koha fields
                        if not patron_data.get("surname") or not patron_data.get("library_id") or not patron_data.get("category_id"):
                            skipped += 1
                            if skipped <= 5:
                                log.warn(f"Row {total} skipped — missing required field(s) (surname/library_id/category_id): {patron_data}")
                            continue

                        try:
                            resp = client.post(
                                f"{base_url}/api/v1/patrons",
                                json=patron_data,
                                headers=headers,
                            )
                            if resp.status_code < 300:
                                success += 1
                                if success <= 3:
                                    log.info(f"Patron {total} created: HTTP {resp.status_code} — {resp.text[:300]}")
                                    if success == 1:
                                        log.info(f"Patron {total} payload sent: {patron_data}")
                            else:
                                failed += 1
                                if failed <= 10:
                                    resp_body = resp.text[:500]
                                    log.warn(f"Patron {total} failed: HTTP {resp.status_code} — {resp_body}")
                                    if failed == 1:
                                        log.warn(f"Patron {total} payload: {patron_data}")
                        except httpx.HTTPError as exc:
                            failed += 1
                            if failed <= 10:
                                log.warn(f"Patron {total} HTTP error: {exc}")

                        if total % _PROGRESS_INTERVAL == 0:
                            _check_paused(project_id, self)
                            self.update_state(state="PROGRESS", meta={
                                "records_done": total,
                                "records_success": success,
                                "records_failed": failed,
                            })

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total,
                                  records_success=success,
                                  records_failed=failed,
                                  completed_at=datetime.utcnow())

            if not dry_run:
                project = db.get(Project, project_id)
                if project:
                    project.patron_count = success
                    db.commit()

        if skipped:
            log.warn(f"Skipped {skipped} rows missing required fields")
        log.complete(f"Patrons push complete — {total:,} total, {success:,} success, {failed:,} failed, {skipped} skipped")
        return {
            "records_total": total, "records_success": success,
            "records_failed": failed, "records_skipped": skipped,
            "dry_run": dry_run,
        }

    except Exception as exc:
        log.error(f"Patrons push failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc),
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# HOLDS PUSH TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_holds_task",
    max_retries=0,
    queue="push",
)
def push_holds_task(self, project_id: str, manifest_id: str, dry_run: bool = True) -> dict:
    """POST holds from CSV to Koha REST API /api/v1/holds.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        dry_run: If True, validate and count only.

    Returns:
        dict with records_total, records_success, records_failed.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[push-holds]")
    log.info(f"Holds push starting (dry_run={dry_run})")

    try:
        csv_path = Path(settings.data_root) / project_id / "holds.csv"
        if not csv_path.is_file():
            error_msg = "holds.csv not found"
            log.error(error_msg)
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
            return {"error": error_msg}

        _HOLD_FIELD_MAP = {
            "patron_id": "patron_id",
            "biblio_id": "biblio_id",
            "pickup_library_id": "pickup_library_id",
            "item_id": "item_id",
            "notes": "notes",
            "expiration_date": "expiration_date",
        }

        total = 0
        success = 0
        failed = 0

        with open(str(csv_path), "r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)

            if dry_run:
                for row in reader:
                    total += 1
                    if row.get("patron_id") and row.get("biblio_id"):
                        success += 1
                    else:
                        failed += 1
                    if total % _PROGRESS_INTERVAL == 0:
                        _check_paused(project_id, self)
                        self.update_state(state="PROGRESS", meta={
                            "records_done": total, "dry_run": True,
                        })
            else:
                base_url, headers = _koha_client(project_id)
                with httpx.Client(timeout=30.0, verify=False) as client:
                    for row in reader:
                        total += 1
                        hold_data = {}
                        for csv_col, koha_field in _HOLD_FIELD_MAP.items():
                            val = row.get(csv_col, "").strip()
                            if val:
                                hold_data[koha_field] = val

                        try:
                            resp = client.post(
                                f"{base_url}/api/v1/holds",
                                json=hold_data,
                                headers=headers,
                            )
                            if resp.status_code < 300:
                                success += 1
                            else:
                                failed += 1
                                if failed <= 10:
                                    log.warn(f"Hold {total} failed: HTTP {resp.status_code}")
                        except httpx.HTTPError as exc:
                            failed += 1
                            if failed <= 10:
                                log.warn(f"Hold {total} HTTP error: {exc}")

                        if total % _PROGRESS_INTERVAL == 0:
                            _check_paused(project_id, self)
                            self.update_state(state="PROGRESS", meta={
                                "records_done": total,
                                "records_success": success,
                                "records_failed": failed,
                            })

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total,
                                  records_success=success,
                                  records_failed=failed,
                                  completed_at=datetime.utcnow())

            if not dry_run:
                project = db.get(Project, project_id)
                if project:
                    project.hold_count = success
                    db.commit()

        log.complete(f"Holds push complete — {total:,} total, {success:,} success, {failed:,} failed")
        return {
            "records_total": total, "records_success": success,
            "records_failed": failed, "dry_run": dry_run,
        }

    except Exception as exc:
        log.error(f"Holds push failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc),
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# CIRC HISTORY TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_circ_task",
    max_retries=0,
    queue="push",
)
def push_circ_task(self, project_id: str, manifest_id: str, dry_run: bool = True) -> dict:
    """Validate circulation history CSV. Non-dry-run requires manual SQL import.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        dry_run: If True, validate and count rows.

    Returns:
        dict with records_total, records_success, records_failed.
    """
    log = AuditLogger(project_id=project_id, stage=7, tag="[push-circ]")
    log.info(f"Circ history push starting (dry_run={dry_run})")

    try:
        csv_path = Path(settings.data_root) / project_id / "circ_history.csv"
        if not csv_path.is_file():
            error_msg = "circ_history.csv not found"
            log.error(error_msg)
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id,
                                      status="error", error_message=error_msg,
                                      completed_at=datetime.utcnow())
            return {"error": error_msg}

        total = 0
        success = 0
        failed = 0

        with open(str(csv_path), "r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                total += 1
                # Basic validation — check required fields exist
                if row.get("borrowernumber") and row.get("itemnumber"):
                    success += 1
                else:
                    failed += 1
                if total % _PROGRESS_INTERVAL == 0:
                    self.update_state(state="PROGRESS", meta={
                        "records_done": total, "dry_run": dry_run,
                    })

        if not dry_run:
            log.warn(
                "Circulation history requires manual SQL import into old_issues table. "
                f"Validated {total:,} rows ({success:,} valid, {failed:,} invalid)."
            )

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total,
                                  records_success=success,
                                  records_failed=failed,
                                  result_data={"manual_import_required": not dry_run},
                                  completed_at=datetime.utcnow())

        log.complete(f"Circ history validated — {total:,} rows, {success:,} valid, {failed:,} invalid")
        return {
            "records_total": total, "records_success": success,
            "records_failed": failed, "dry_run": dry_run,
            "manual_import_required": not dry_run,
        }

    except Exception as exc:
        log.error(f"Circ history push failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc),
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# PATRON CATEGORIES SQL PUSH TASK
# ══════════════════════════════════════════════════════════════════════════


def _koha_mysql_conn(project_id: str):
    """Open a pymysql connection to the Koha MariaDB database.

    Detects the KTD database container hostname from SandboxInstance or
    falls back to 'kohadev-db-1'. Uses default KTD credentials.
    """
    import pymysql

    from cerulean.models import SandboxInstance

    db_host = "kohadev-db-1"  # default KTD DB container hostname

    with Session(_engine) as db:
        instance = db.query(SandboxInstance).filter_by(
            project_id=project_id, status="running"
        ).first()
        if instance and instance.container_name:
            # Derive DB container name from app container: kohadev-koha-1 → kohadev-db-1
            parts = instance.container_name.rsplit("-", 2)
            if len(parts) >= 2:
                db_host = f"{parts[0]}-db-1"

    return pymysql.connect(
        host=db_host,
        user="koha_kohadev",
        password="password",
        database="koha_kohadev",
        port=3306,
        connect_timeout=10,
    )


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_patron_categories_sql_task",
    max_retries=0,
    queue="push",
)
def push_patron_categories_sql_task(self, project_id: str, categories: list[dict]) -> dict:
    """Insert patron categories into Koha MariaDB via direct SQL connection.

    Used as fallback when Koha REST API doesn't support POST /patron_categories.
    """
    log = AuditLogger(project_id=project_id, stage=7, tag="[setup]")
    log.info(f"Pushing {len(categories)} patron categories via SQL")

    results = []
    success_count = 0
    failed_count = 0

    try:
        conn = _koha_mysql_conn(project_id)
    except Exception as exc:
        log.error(f"Failed to connect to Koha DB: {exc}")
        return {
            "success_count": 0,
            "failed_count": len(categories),
            "results": [
                {"category_id": c["category_id"], "success": False, "error": f"DB connection failed: {exc}"}
                for c in categories
            ],
        }

    try:
        cursor = conn.cursor()
        for cat in categories:
            cid = cat["category_id"]
            name = cat.get("name", cid)
            ep = cat.get("enrolmentperiod", 99)
            try:
                cursor.execute(
                    "INSERT IGNORE INTO categories "
                    "(categorycode, description, enrolmentperiod, category_type) "
                    "VALUES (%s, %s, %s, %s)",
                    (cid, name, ep, "A"),
                )
                conn.commit()
                success_count += 1
                results.append({"category_id": cid, "success": True})
            except Exception as exc:
                conn.rollback()
                failed_count += 1
                results.append({"category_id": cid, "success": False, "error": str(exc)[:200]})
    finally:
        conn.close()

    log.info(f"Patron categories SQL push: {success_count} created, {failed_count} failed")
    return {
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    }


# ══════════════════════════════════════════════════════════════════════════
# ITEM TYPES (ITYPES) SQL PUSH TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_item_types_sql_task",
    max_retries=0,
    queue="push",
)
def push_item_types_sql_task(self, project_id: str, item_types: list[dict]) -> dict:
    """Insert item types into Koha MariaDB via direct SQL connection.

    Koha REST API does not support POST /item_types, so we use SQL directly.
    """
    log = AuditLogger(project_id=project_id, stage=7, tag="[setup]")
    log.info(f"Pushing {len(item_types)} item types via SQL")

    results = []
    success_count = 0
    failed_count = 0

    try:
        conn = _koha_mysql_conn(project_id)
    except Exception as exc:
        log.error(f"Failed to connect to Koha DB: {exc}")
        return {
            "success_count": 0,
            "failed_count": len(item_types),
            "results": [
                {"item_type_id": it["item_type_id"], "success": False, "error": f"DB connection failed: {exc}"}
                for it in item_types
            ],
        }

    try:
        cursor = conn.cursor()
        for it in item_types:
            itype = it["item_type_id"]
            desc = it.get("description", itype)
            try:
                cursor.execute(
                    "INSERT IGNORE INTO itemtypes "
                    "(itemtype, description) "
                    "VALUES (%s, %s)",
                    (itype, desc),
                )
                conn.commit()
                success_count += 1
                results.append({"item_type_id": itype, "success": True})
            except Exception as exc:
                conn.rollback()
                failed_count += 1
                results.append({"item_type_id": itype, "success": False, "error": str(exc)[:200]})
    finally:
        conn.close()

    log.info(f"Item types SQL push: {success_count} created, {failed_count} failed")
    return {
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    }


# ══════════════════════════════════════════════════════════════════════════
# AUTHORISED VALUES SQL PUSH TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.push_authorised_values_sql_task",
    max_retries=0,
    queue="push",
)
def push_authorised_values_sql_task(
    self, project_id: str, category: str, values: list[dict],
) -> dict:
    """Insert authorised values into Koha MariaDB via direct SQL.

    Koha REST API does not support POST for authorised values, so we
    insert directly.  Each value dict should have:
        {"value": "MAIN", "description": "Main Library"}

    Args:
        project_id: UUID of the project (for DB connection).
        category: Authorised value category (LOC, CCODE, NOT_LOAN, etc.).
        values: List of {"value", "description"} dicts.
    """
    log = AuditLogger(project_id=project_id, stage=7, tag="[setup]")
    log.info(f"Pushing {len(values)} authorised values for {category} via SQL")

    results = []
    success_count = 0
    failed_count = 0

    try:
        conn = _koha_mysql_conn(project_id)
    except Exception as exc:
        log.error(f"Failed to connect to Koha DB: {exc}")
        return {
            "category": category,
            "success_count": 0,
            "failed_count": len(values),
            "results": [
                {"value": v["value"], "success": False, "error": f"DB connection failed: {exc}"}
                for v in values
            ],
        }

    try:
        cursor = conn.cursor()
        for v in values:
            code = v["value"]
            desc = v.get("description", code)
            try:
                cursor.execute(
                    "INSERT IGNORE INTO authorised_values "
                    "(category, authorised_value, lib) "
                    "VALUES (%s, %s, %s)",
                    (category, code, desc),
                )
                conn.commit()
                success_count += 1
                results.append({"value": code, "success": True})
            except Exception as exc:
                conn.rollback()
                failed_count += 1
                results.append({"value": code, "success": False, "error": str(exc)[:200]})
    finally:
        conn.close()

    log.info(f"Authorised values ({category}) SQL push: {success_count} created, {failed_count} failed")
    return {
        "category": category,
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    }


# ══════════════════════════════════════════════════════════════════════════
# TURBOINDEX (Koha plugin) REINDEX TASK
# ══════════════════════════════════════════════════════════════════════════
#
# Companion to FastMARCImport.  fastmarcimport adds biblios with index
# writes deferred; turboindex rebuilds the Elasticsearch index in a single
# parallel pass afterwards.
#
# Plugin endpoint: POST /api/v1/contrib/turboindex/biblios/reindex
#     Body: {"reset":true, "processes":4, "commit":5000, "force_merge":true}
#     All fields optional.
#     Returns: {"job_id": N}
# Poll:  GET /api/v1/jobs/{id}
# Report fields: num_indexed, num_errors, duration_sec, records_per_sec,
#                saved_settings.{refresh_interval, number_of_replicas}
# ══════════════════════════════════════════════════════════════════════════

_TURBOINDEX_PATH = "/api/v1/contrib/turboindex/biblios/reindex"


def _turboindex_plugin_available(client: httpx.Client, base_url: str, headers: dict) -> bool:
    """Return True if the turboindex plugin endpoint is installed.

    Uses OPTIONS which Mojolicious answers with 200 for any method on an
    existing route, and 404 for missing routes — without triggering
    side effects (unlike POST, which would create a real job).
    """
    try:
        resp = client.request(
            "OPTIONS",
            f"{base_url}{_TURBOINDEX_PATH}",
            headers=headers,
            timeout=10.0,
        )
        return resp.status_code != 404
    except httpx.HTTPError:
        return False


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.turboindex_task",
    max_retries=0,
    queue="push",
)
def turboindex_task(
    self, project_id: str, manifest_id: str,
    reset: bool = True, processes: int = 4,
    commit: int = 5000, force_merge: bool = True,
) -> dict:
    """Rebuild the Elasticsearch biblio index via the turboindex plugin.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row (task_type="reindex").
        reset: Drop and recreate the index before reindexing.
        processes: Parallel worker count (1-8 typical).
        commit: ES bulk-commit chunk size.
        force_merge: Run force-merge after indexing (consolidates segments).
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[turboindex]")
    log.info(
        f"TurboIndex reindex starting (reset={reset}, processes={processes}, "
        f"commit={commit}, force_merge={force_merge})"
    )

    def _as_int(v, default=0):
        try:
            return int(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    def _as_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    try:
        base_url, headers = _koha_client(project_id)

        with httpx.Client(timeout=600.0, verify=False) as client:
            if not _turboindex_plugin_available(client, base_url, headers):
                error_msg = (
                    "turboindex plugin endpoint not found at "
                    f"{base_url}{_TURBOINDEX_PATH} — is the plugin installed?"
                )
                log.error(error_msg)
                with Session(_engine) as db:
                    _update_push_manifest(db, manifest_id,
                                          status="error", error_message=error_msg,
                                          completed_at=datetime.utcnow())
                return {"error": error_msg}

            body = {
                "reset": bool(reset),
                "processes": int(processes),
                "commit": int(commit),
                "force_merge": bool(force_merge),
            }
            self.update_state(state="PROGRESS", meta={
                "step": "dispatching",
                "config": body,
            })

            resp = client.post(
                f"{base_url}{_TURBOINDEX_PATH}",
                json=body,
                headers={**headers, "Content-Type": "application/json"},
            )
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Reindex dispatch failed: HTTP {resp.status_code} — {resp.text[:500]}"
                )
            data = resp.json()
            job_id = data.get("job_id")
            if not job_id:
                raise RuntimeError(f"No job_id in response: {data}")

            log.info(f"TurboIndex job started: job_id={job_id}")

            # Mutable live-state dict persisted on every poll so the UI can
            # stream messages + counters as they arrive from Koha.
            live_state = {
                "method": "turboindex",
                "job_id": job_id,
                "config": body,
                "status": "running",
                "progress": 0,
                "size": 0,
                "messages": [],
                "report": {},
            }
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id, result_data=live_state)

            # Track which messages we've already seen — Koha returns the full
            # array every poll, but we only want to append new ones.
            _seen_msg_count = [0]

            def _on_progress(job):
                jdata = job.get("data") or {}
                jmsgs = jdata.get("messages") or []
                jreport = jdata.get("report") or {}
                # Append any new messages
                new_msgs = jmsgs[_seen_msg_count[0]:]
                _seen_msg_count[0] = len(jmsgs)
                # Cap the stored list at 500 entries total to bound memory
                live_state["messages"].extend(new_msgs)
                if len(live_state["messages"]) > 500:
                    live_state["messages"] = live_state["messages"][-500:]
                # Merge latest report snapshot
                if jreport:
                    live_state["report"] = jreport
                live_state["status"] = job.get("status", "running")
                try:
                    live_state["progress"] = int(job.get("progress") or 0)
                    live_state["size"] = int(job.get("size") or 0)
                except (TypeError, ValueError):
                    pass
                # Persist to manifest
                with Session(_engine) as db:
                    _update_push_manifest(db, manifest_id, result_data=dict(live_state))
                # Log any new error/warning messages to audit log too
                for m in new_msgs:
                    mtype = (m.get("type") or "").lower()
                    mtxt = (m.get("message") or "")[:500]
                    if mtype == "error":
                        log.error(f"[koha job] {mtxt}")
                    elif mtype == "warning":
                        log.warn(f"[koha job] {mtxt}")
                    else:
                        log.info(f"[koha job] {mtxt}")

            job_result = _poll_koha_job(
                client, base_url, headers,
                job_id, self, project_id,
                step_label="turboindex",
                on_progress=_on_progress,
            )
            report = job_result.get("data", {}).get("report", {}) or {}

        num_indexed = _as_int(report.get("num_indexed"), 0)
        num_errors = _as_int(report.get("num_errors"), 0)
        duration_sec = _as_float(report.get("duration_sec"))
        records_per_sec = _as_float(report.get("records_per_sec"))
        saved_settings = report.get("saved_settings") or {}

        result_data = {
            "method": "turboindex",
            "job_id": job_id,
            "config": body,
            "status": "finished",
            "num_indexed": num_indexed,
            "num_errors": num_errors,
            "duration_sec": duration_sec,
            "records_per_sec": records_per_sec,
            "saved_settings": saved_settings,
            "report": report,
            "messages": live_state.get("messages") or [],
            "progress": num_indexed + num_errors,
            "size": num_indexed + num_errors,
        }

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=num_indexed + num_errors,
                                  records_success=num_indexed,
                                  records_failed=num_errors,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())
            project = db.get(Project, project_id)
            if project:
                project.stage_7_complete = True
                project.current_stage = 8
                db.commit()

        log.complete(
            f"TurboIndex complete — {num_indexed:,} indexed, {num_errors:,} errors, "
            f"{duration_sec}s ({records_per_sec} rec/s)"
        )
        return result_data

    except Exception as exc:
        log.error(f"TurboIndex reindex failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# MIGRATION TOOLKIT PLUGIN — unified Koha migration plugin
# ══════════════════════════════════════════════════════════════════════════
#
# Replaces the separate FastMARCImport + TurboIndex plugins with a single
# combined plugin:  Koha::Plugin::BWS::MigrationToolkit
#
# Base path: /api/v1/contrib/migrationtoolkit
# Endpoints: /preflight, /db-tuning, /import, /stage, /batches/{id}/commit, /reindex
# ══════════════════════════════════════════════════════════════════════════

_TOOLKIT_BASE = "/api/v1/contrib/migrationtoolkit"


def _toolkit_available(client: httpx.Client, base_url: str, headers: dict) -> bool:
    """Check if the Migration Toolkit plugin is installed via GET /preflight."""
    try:
        resp = client.get(
            f"{base_url}{_TOOLKIT_BASE}/preflight",
            headers=headers,
            timeout=10.0,
        )
        return resp.status_code == 200
    except httpx.HTTPError:
        return False


def _toolkit_preflight(client: httpx.Client, base_url: str, headers: dict) -> dict:
    """Fetch Koha reference data counts from the toolkit preflight endpoint."""
    resp = client.get(
        f"{base_url}{_TOOLKIT_BASE}/preflight",
        headers=headers,
        timeout=10.0,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Preflight failed: HTTP {resp.status_code} — {resp.text[:300]}")
    return resp.json()


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.toolkit_import_task",
    max_retries=0,
    queue="push",
)
def toolkit_import_task(
    self, project_id: str, manifest_id: str,
    dry_run: bool = False, bib_options: dict | None = None,
) -> dict:
    """Push MARC records via the Migration Toolkit plugin's fast import.

    Flow:
        1. (Optional) POST /db-tuning {action: "apply"}
        2. POST /import with raw MARC body (streaming upload)
        3. Poll GET /jobs/{id} until finished
        4. (Optional) POST /db-tuning {action: "restore"}

    The plugin handles daemon stop/start internally (daemons_stopped=1).
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[toolkit-import]")
    bib_options = bib_options or {}
    log.info(f"Migration Toolkit import starting (dry_run={dry_run})")

    if dry_run:
        error_msg = "Migration Toolkit fast import does not support dry_run"
        log.error(error_msg)
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=error_msg,
                                  completed_at=datetime.utcnow())
        return {"error": error_msg}

    def _as_int(v, default=0):
        try:
            return int(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    def _as_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

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

        base_url, headers = _koha_client(project_id)

        # Per-file state
        files_state: list[dict] = [
            {
                "filename": p.name,
                "file_size": p.stat().st_size,
                "status": "pending",
                "job_id": None,
                "num_added": 0,
                "num_errors": 0,
                "num_items_added": 0,
                "num_item_errors": 0,
                "duplicate_barcodes": 0,
                "records_processed": 0,
                "slices_completed": 0,
                "skipped_in_split": 0,
                "sidecar_records": 0,
                "sidecar_recovered": 0,
                "chunks_rolled_back": 0,
                "first_error_message": None,
                "failed_records": [],
                "error_mode": None,
                "daemons_stopped": 0,
                "wide_char_warnings": 0,
                "worker_count": None,
                "duration_sec": None,
                "records_per_sec": None,
                "error": None,
            }
            for p in marc_paths
        ]
        _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=0)

        with httpx.Client(timeout=600.0, verify=False) as client:
            # Verify plugin
            if not _toolkit_available(client, base_url, headers):
                error_msg = f"Migration Toolkit plugin not found at {base_url}{_TOOLKIT_BASE}"
                log.error(error_msg)
                with Session(_engine) as db:
                    _update_push_manifest(db, manifest_id,
                                          status="error", error_message=error_msg,
                                          completed_at=datetime.utcnow())
                return {"error": error_msg}

            # Optional: apply DB tuning
            db_tuning_result = None
            if bib_options.get("db_tuning", True):
                try:
                    resp = client.post(
                        f"{base_url}{_TOOLKIT_BASE}/db-tuning",
                        json={"action": "apply"},
                        headers={**headers, "Content-Type": "application/json"},
                    )
                    if resp.status_code == 200:
                        db_tuning_result = resp.json()
                        log.info(f"DB tuning applied: {db_tuning_result.get('messages', [])}")
                except Exception as exc:
                    log.warn(f"DB tuning failed (non-fatal): {exc}")

            for idx, marc_path in enumerate(marc_paths):
                fstate = files_state[idx]
                try:
                    file_size = marc_path.stat().st_size
                    log.info(f"[{idx + 1}/{len(marc_paths)}] Uploading {marc_path.name} ({file_size:,} bytes)")

                    self.update_state(state="PROGRESS", meta={
                        "step": "uploading",
                        "file": marc_path.name,
                        "file_index": idx,
                        "file_count": len(marc_paths),
                        "file_size": file_size,
                    })

                    upload_headers = {
                        **headers,
                        "Content-Type": "application/marc",
                        "Content-Length": str(file_size),
                        "x-file-name": marc_path.name,
                    }
                    resp = client.post(
                        f"{base_url}{_TOOLKIT_BASE}/import",
                        content=_iter_file_chunks(marc_path),
                        headers=upload_headers,
                    )
                    if resp.status_code >= 400:
                        raise RuntimeError(f"Upload failed: HTTP {resp.status_code} — {resp.text[:500]}")

                    job_id = resp.json().get("job_id")
                    if not job_id:
                        raise RuntimeError(f"No job_id in response: {resp.json()}")

                    fstate["job_id"] = job_id
                    fstate["status"] = "running"
                    _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=idx)
                    log.info(f"[{idx + 1}/{len(marc_paths)}] Import job started: job_id={job_id}")

                    # Poll with live state persistence
                    live_state = {"messages": []}
                    _seen_msgs = [0]

                    def _on_progress(job):
                        jdata = job.get("data") or {}
                        jmsgs = jdata.get("messages") or []
                        new_msgs = jmsgs[_seen_msgs[0]:]
                        _seen_msgs[0] = len(jmsgs)
                        live_state["messages"].extend(new_msgs)
                        if len(live_state["messages"]) > 500:
                            live_state["messages"] = live_state["messages"][-500:]
                        _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=idx)

                    job_result = _poll_koha_job(
                        client, base_url, headers,
                        job_id, self, project_id,
                        step_label=f"importing [{idx + 1}/{len(marc_paths)}]",
                        on_progress=_on_progress,
                    )
                    report = job_result.get("data", {}).get("report", {}) or {}

                    # Extract all report fields (coerce strings to ints)
                    fstate["records_total"] = _as_int(job_result.get("size") or report.get("num_added"), 0)
                    fstate["num_added"] = _as_int(report.get("num_added"), 0)
                    fstate["num_errors"] = _as_int(report.get("num_errors"), 0)
                    fstate["num_items_added"] = _as_int(report.get("num_items_added"), 0)
                    fstate["num_item_errors"] = _as_int(report.get("num_item_errors"), 0)
                    fstate["duplicate_barcodes"] = _as_int(report.get("duplicate_barcodes"), 0)
                    fstate["records_processed"] = _as_int(report.get("records_processed"), 0)
                    fstate["slices_completed"] = _as_int(report.get("slices_completed"), 0)
                    fstate["skipped_in_split"] = _as_int(report.get("skipped_in_split"), 0)
                    fstate["sidecar_records"] = _as_int(report.get("sidecar_records"), 0)
                    fstate["sidecar_recovered"] = _as_int(report.get("sidecar_recovered"), 0)
                    fstate["chunks_rolled_back"] = _as_int(report.get("chunks_rolled_back"), 0)
                    fstate["first_error_message"] = report.get("first_error_message")
                    fstate["failed_records"] = (report.get("failed_records") or [])[:2000]
                    fstate["error_mode"] = report.get("error_mode")
                    fstate["daemons_stopped"] = _as_int(report.get("daemons_stopped"), 0)
                    fstate["wide_char_warnings"] = _as_int(report.get("wide_char_warnings"), 0)
                    fstate["worker_count"] = _as_int(report.get("worker_count"), 0) or None
                    fstate["duration_sec"] = _as_float(report.get("duration_sec"))
                    fstate["records_per_sec"] = _as_float(report.get("records_per_sec"))
                    fstate["messages"] = live_state["messages"]
                    fstate["status"] = "committed"
                    _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=idx)
                    log.info(
                        f"[{idx + 1}/{len(marc_paths)}] Imported: "
                        f"{fstate['num_added']:,} bibs ({fstate['num_errors']:,} err), "
                        f"{fstate['num_items_added']:,} items ({fstate['num_item_errors']:,} err), "
                        f"{fstate['duration_sec']}s, {fstate['records_per_sec']} rec/s, "
                        f"{fstate['worker_count']} workers"
                    )

                except Exception as file_exc:
                    fstate["status"] = "error"
                    fstate["error"] = str(file_exc)[:500]
                    _persist_bulk_state(manifest_id, files_state, dry_run=False, current_idx=idx)
                    log.error(f"[{idx + 1}/{len(marc_paths)}] Failed: {file_exc}")
                    raise

            # Restore DB tuning
            if db_tuning_result and db_tuning_result.get("saved_flush") is not None:
                try:
                    resp = client.post(
                        f"{base_url}{_TOOLKIT_BASE}/db-tuning",
                        json={
                            "action": "restore",
                            "saved_flush": db_tuning_result["saved_flush"],
                            "saved_pool": db_tuning_result.get("saved_pool"),
                        },
                        headers={**headers, "Content-Type": "application/json"},
                    )
                    if resp.status_code == 200:
                        log.info(f"DB tuning restored: {resp.json().get('messages', [])}")
                except Exception as exc:
                    log.warn(f"DB tuning restore failed (non-fatal): {exc}")

        # Aggregate
        total_records = sum(f.get("records_total", 0) or f.get("num_added", 0) for f in files_state)
        total_added = sum(f["num_added"] for f in files_state)
        total_errors = sum(f["num_errors"] for f in files_state)
        total_items_added = sum(f["num_items_added"] for f in files_state)
        total_item_errors = sum(f["num_item_errors"] for f in files_state)

        result_data = {
            "method": "toolkit",
            "dry_run": False,
            "file_count": len(files_state),
            "files": files_state,
            "db_tuning": db_tuning_result,
            "totals": {
                "records_total": total_records,
                "num_added": total_added,
                "num_errors": total_errors,
                "num_items_added": total_items_added,
                "num_item_errors": total_item_errors,
                "duplicate_barcodes": sum(f["duplicate_barcodes"] for f in files_state),
                "records_processed": sum(f["records_processed"] for f in files_state),
                "skipped_in_split": sum(f["skipped_in_split"] for f in files_state),
            },
        }
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=total_records,
                                  records_success=total_added,
                                  records_failed=total_errors,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())
            project = db.get(Project, project_id)
            if project:
                project.bib_count_pushed = total_added
                db.commit()

        log.complete(
            f"Toolkit import complete — {len(files_state)} file(s): "
            f"{total_added:,} bibs ({total_errors:,} err), "
            f"{total_items_added:,} items ({total_item_errors:,} err)"
        )
        return result_data

    except Exception as exc:
        log.error(f"Toolkit import failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.toolkit_reindex_task",
    max_retries=0,
    queue="push",
)
def toolkit_reindex_task(
    self, project_id: str, manifest_id: str,
    reset: bool = True, processes: int = 4,
    commit: int = 5000, force_merge: bool = True,
) -> dict:
    """Rebuild ES index via the Migration Toolkit plugin's /reindex endpoint."""
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[toolkit-reindex]")
    log.info(f"Toolkit reindex starting (reset={reset}, processes={processes})")

    def _as_int(v, default=0):
        try:
            return int(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    def _as_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    try:
        base_url, headers = _koha_client(project_id)

        with httpx.Client(timeout=600.0, verify=False) as client:
            if not _toolkit_available(client, base_url, headers):
                error_msg = f"Migration Toolkit plugin not found at {base_url}{_TOOLKIT_BASE}"
                log.error(error_msg)
                with Session(_engine) as db:
                    _update_push_manifest(db, manifest_id,
                                          status="error", error_message=error_msg,
                                          completed_at=datetime.utcnow())
                return {"error": error_msg}

            body = {
                "reset": bool(reset),
                "processes": int(processes),
                "commit": int(commit),
                "force_merge": bool(force_merge),
            }

            resp = client.post(
                f"{base_url}{_TOOLKIT_BASE}/reindex",
                json=body,
                headers={**headers, "Content-Type": "application/json"},
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"Reindex dispatch failed: HTTP {resp.status_code} — {resp.text[:500]}")

            job_id = resp.json().get("job_id")
            if not job_id:
                raise RuntimeError(f"No job_id in response: {resp.json()}")

            log.info(f"Toolkit reindex job started: job_id={job_id}")

            # Live state for UI
            live_state = {
                "method": "toolkit_reindex",
                "job_id": job_id,
                "config": body,
                "status": "running",
                "progress": 0,
                "size": 0,
                "messages": [],
                "report": {},
            }
            with Session(_engine) as db:
                _update_push_manifest(db, manifest_id, result_data=live_state)

            _seen_msgs = [0]

            def _on_progress(job):
                jdata = job.get("data") or {}
                jmsgs = jdata.get("messages") or []
                jreport = jdata.get("report") or {}
                new_msgs = jmsgs[_seen_msgs[0]:]
                _seen_msgs[0] = len(jmsgs)
                live_state["messages"].extend(new_msgs)
                if len(live_state["messages"]) > 500:
                    live_state["messages"] = live_state["messages"][-500:]
                if jreport:
                    live_state["report"] = jreport
                live_state["status"] = job.get("status", "running")
                try:
                    live_state["progress"] = int(job.get("progress") or 0)
                    live_state["size"] = int(job.get("size") or 0)
                except (TypeError, ValueError):
                    pass
                with Session(_engine) as db:
                    _update_push_manifest(db, manifest_id, result_data=dict(live_state))

            job_result = _poll_koha_job(
                client, base_url, headers,
                job_id, self, project_id,
                step_label="toolkit_reindex",
                on_progress=_on_progress,
            )
            report = job_result.get("data", {}).get("report", {}) or {}

        num_indexed = _as_int(report.get("num_indexed"), 0)
        num_errors = _as_int(report.get("num_errors"), 0)
        duration_sec = _as_float(report.get("duration_sec"))
        records_per_sec = _as_float(report.get("records_per_sec"))

        result_data = {
            "method": "toolkit_reindex",
            "job_id": job_id,
            "config": body,
            "status": "finished",
            "num_indexed": num_indexed,
            "num_errors": num_errors,
            "duration_sec": duration_sec,
            "records_per_sec": records_per_sec,
            "saved_settings": report.get("saved_settings") or {},
            "report": report,
            "messages": live_state.get("messages") or [],
            "progress": num_indexed + num_errors,
            "size": num_indexed + num_errors,
        }

        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  records_total=num_indexed + num_errors,
                                  records_success=num_indexed,
                                  records_failed=num_errors,
                                  result_data=result_data,
                                  completed_at=datetime.utcnow())
            project = db.get(Project, project_id)
            if project:
                project.stage_7_complete = True
                project.current_stage = 8
                db.commit()

        log.complete(
            f"Toolkit reindex complete — {num_indexed:,} indexed, {num_errors:,} errors, "
            f"{duration_sec}s ({records_per_sec} rec/s)"
        )
        return result_data

    except Exception as exc:
        log.error(f"Toolkit reindex failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc)[:1000],
                                  completed_at=datetime.utcnow())
        raise


# ══════════════════════════════════════════════════════════════════════════
# ELASTICSEARCH REINDEX TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.push.es_reindex_task",
    max_retries=0,
    queue="push",
)
def es_reindex_task(self, project_id: str, manifest_id: str, reindex_engine: str | None = None) -> dict:
    """Log appropriate reindex command and mark Stage 7 complete.

    Args:
        project_id: UUID of the project.
        manifest_id: UUID of the PushManifest row.
        reindex_engine: "elasticsearch" or "zebra". Auto-detects from project if None.

    Returns:
        dict with search_engine and reindex_command.
    """
    from cerulean.models import Project

    log = AuditLogger(project_id=project_id, stage=7, tag="[reindex]")
    log.info("Reindex task starting")

    try:
        with Session(_engine) as db:
            project = db.get(Project, project_id)
            search_engine = project.search_engine if project else None

        # User-selected engine overrides auto-detection
        if reindex_engine:
            search_engine = reindex_engine

        if search_engine in ("es8", "elasticsearch"):
            reindex_cmd = "koha-elasticsearch --rebuild -d -b -a kohadev"
            log.info(f"Elasticsearch — run: {reindex_cmd}")
        else:
            reindex_cmd = "koha-rebuild-zebra -f -b -a --run-as-root kohadev"
            log.info(f"Zebra — run: {reindex_cmd}")

        with Session(_engine) as db:
            project = db.get(Project, project_id)
            if project:
                project.stage_7_complete = True
                project.current_stage = 8

            _update_push_manifest(db, manifest_id,
                                  status="complete",
                                  result_data={"search_engine": search_engine, "reindex_command": reindex_cmd},
                                  completed_at=datetime.utcnow())

        log.complete(f"Reindex task complete — Stage 7 done, advancing to Stage 8")
        return {
            "search_engine": search_engine,
            "reindex_command": reindex_cmd,
        }

    except Exception as exc:
        log.error(f"Reindex task failed: {exc}")
        with Session(_engine) as db:
            _update_push_manifest(db, manifest_id,
                                  status="error", error_message=str(exc),
                                  completed_at=datetime.utcnow())
        raise
