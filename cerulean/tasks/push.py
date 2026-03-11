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

    Priority: Biblios-mapped-items.mrc > merged_deduped.mrc > merged.mrc > transformed/*.mrc
    """
    project_dir = Path(settings.data_root) / project_id
    reconciled = project_dir / "Biblios-mapped-items.mrc"
    if reconciled.is_file():
        return [reconciled]
    deduped = project_dir / "merged_deduped.mrc"
    if deduped.is_file():
        return [deduped]
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

        with httpx.Client(timeout=30.0) as client:
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

            with httpx.Client(timeout=60.0) as client:
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
                with httpx.Client(timeout=30.0) as client:
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
                with httpx.Client(timeout=30.0) as client:
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
