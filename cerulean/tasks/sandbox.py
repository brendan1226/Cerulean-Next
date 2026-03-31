"""
cerulean/tasks/sandbox.py
─────────────────────────────────────────────────────────────────────────────
Stage 11 Celery tasks: KTD Sandbox.

    ktd_provision_task  — start a KTD Docker container
    ktd_teardown_task   — stop and remove a KTD container

All tasks write AuditEvent rows via AuditLogger.
"""

import subprocess
import time
from datetime import datetime

from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session

from cerulean.core.config import get_settings
from cerulean.tasks.audit import AuditLogger
from cerulean.tasks.celery_app import celery_app

settings = get_settings()
_sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
_engine = create_engine(_sync_url, pool_pre_ping=True)


# ══════════════════════════════════════════════════════════════════════════
# KTD PROVISION TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.sandbox.ktd_provision_task",
    max_retries=0,
    queue="sandbox",
)
def ktd_provision_task(
    self, project_id: str, instance_id: str, koha_version: str | None = None,
) -> dict:
    """Start a KTD Docker container for sandbox testing.

    Args:
        project_id: UUID of the project.
        instance_id: UUID of the SandboxInstance row.
        koha_version: Optional Koha version tag for the container.

    Returns:
        dict with container_id, container_name, koha_url, port.
    """
    from cerulean.models import Project, SandboxInstance

    log = AuditLogger(project_id=project_id, stage=11, tag="[sandbox]")
    log.info("KTD sandbox provisioning starting")

    container_name = f"cerulean-ktd-{project_id[:8]}"

    try:
        self.update_state(state="PROGRESS", meta={"step": "starting_container"})

        # Build docker run command
        image = f"koha/koha-testing:{koha_version}" if koha_version else "koha/koha-testing:master"
        cmd = [
            "docker", "run", "-d",
            "--name", container_name,
            "-p", "0:8080",   # auto-assign host port
            image,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            error_msg = f"docker run failed: {result.stderr.strip()}"
            log.error(error_msg)
            with Session(_engine) as db:
                db.execute(
                    update(SandboxInstance)
                    .where(SandboxInstance.id == instance_id)
                    .values(status="error", error_message=error_msg, updated_at=datetime.utcnow())
                )
                db.commit()
            return {"error": error_msg}

        container_id = result.stdout.strip()[:12]

        # Get assigned port
        self.update_state(state="PROGRESS", meta={"step": "detecting_port"})
        port_result = subprocess.run(
            ["docker", "port", container_name, "8080"],
            capture_output=True, text=True, timeout=10,
        )
        exposed_port = 8080
        if port_result.returncode == 0 and port_result.stdout.strip():
            # Output like "0.0.0.0:32768"
            port_str = port_result.stdout.strip().split(":")[-1]
            try:
                exposed_port = int(port_str)
            except ValueError:
                pass

        koha_url = f"http://localhost:{exposed_port}"

        # Wait for container health check (up to 60s)
        self.update_state(state="PROGRESS", meta={"step": "waiting_for_health"})
        healthy = False
        for attempt in range(12):
            inspect_result = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Running}}", container_name],
                capture_output=True, text=True, timeout=10,
            )
            if inspect_result.stdout.strip() == "true":
                healthy = True
                break
            time.sleep(5)

        status = "running" if healthy else "error"
        error_msg = None if healthy else "Container failed health check"

        # Update SandboxInstance
        with Session(_engine) as db:
            db.execute(
                update(SandboxInstance)
                .where(SandboxInstance.id == instance_id)
                .values(
                    container_id=container_id,
                    container_name=container_name,
                    status=status,
                    koha_url=koha_url,
                    exposed_port=exposed_port,
                    error_message=error_msg,
                    updated_at=datetime.utcnow(),
                )
            )

            if healthy:
                project = db.get(Project, project_id)
                if project:
                    project.stage_11_complete = True

            db.commit()

        if healthy:
            log.complete(f"KTD sandbox running — {koha_url}")
        else:
            log.error(f"KTD sandbox failed health check — {container_name}")

        return {
            "container_id": container_id,
            "container_name": container_name,
            "koha_url": koha_url,
            "port": exposed_port,
            "status": status,
        }

    except Exception as exc:
        log.error(f"KTD provision failed: {exc}")
        with Session(_engine) as db:
            db.execute(
                update(SandboxInstance)
                .where(SandboxInstance.id == instance_id)
                .values(status="error", error_message=str(exc), updated_at=datetime.utcnow())
            )
            db.commit()
        raise


# ══════════════════════════════════════════════════════════════════════════
# KTD TEARDOWN TASK
# ══════════════════════════════════════════════════════════════════════════


@celery_app.task(
    bind=True,
    name="cerulean.tasks.sandbox.ktd_teardown_task",
    max_retries=0,
    queue="sandbox",
)
def ktd_teardown_task(self, project_id: str, instance_id: str) -> dict:
    """Stop and remove a KTD Docker container.

    Args:
        project_id: UUID of the project.
        instance_id: UUID of the SandboxInstance row.

    Returns:
        dict with status.
    """
    from cerulean.models import SandboxInstance

    log = AuditLogger(project_id=project_id, stage=11, tag="[sandbox]")
    log.info("KTD sandbox teardown starting")

    try:
        with Session(_engine) as db:
            instance = db.get(SandboxInstance, instance_id)
            if not instance:
                log.error(f"SandboxInstance {instance_id} not found")
                return {"error": "instance_not_found"}
            container_id = instance.container_id
            container_name = instance.container_name

            # Mark as stopping
            instance.status = "stopping"
            instance.updated_at = datetime.utcnow()
            db.commit()

        if not container_id and not container_name:
            log.warn("No container_id or name — nothing to stop")
            with Session(_engine) as db:
                db.execute(
                    update(SandboxInstance)
                    .where(SandboxInstance.id == instance_id)
                    .values(status="stopped", updated_at=datetime.utcnow())
                )
                db.commit()
            return {"status": "stopped"}

        target = container_id or container_name

        # Stop container
        self.update_state(state="PROGRESS", meta={"step": "stopping"})
        subprocess.run(["docker", "stop", target], capture_output=True, text=True, timeout=30)

        # Remove container
        self.update_state(state="PROGRESS", meta={"step": "removing"})
        subprocess.run(["docker", "rm", target], capture_output=True, text=True, timeout=30)

        with Session(_engine) as db:
            db.execute(
                update(SandboxInstance)
                .where(SandboxInstance.id == instance_id)
                .values(status="stopped", updated_at=datetime.utcnow())
            )
            db.commit()

        log.complete(f"KTD sandbox stopped — {target}")
        return {"status": "stopped", "container": target}

    except Exception as exc:
        log.error(f"KTD teardown failed: {exc}")
        with Session(_engine) as db:
            db.execute(
                update(SandboxInstance)
                .where(SandboxInstance.id == instance_id)
                .values(status="error", error_message=str(exc), updated_at=datetime.utcnow())
            )
            db.commit()
        raise
