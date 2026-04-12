"""
cerulean/api/routers/plugins.py
---------------------------------------------------------------------
Plugin Manager — upload, download, and auto-install Koha plugins.

GET    /plugins                              — list uploaded plugins
POST   /plugins/upload                       — upload a .kpz file
GET    /plugins/{id}/download                — download the .kpz file
DELETE /plugins/{id}                         — delete plugin
POST   /projects/{pid}/plugins/{id}/install  — auto-install to project's Koha
"""

import shutil
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import Plugin

router = APIRouter(tags=["plugins"])
settings = get_settings()

_PLUGIN_DIR = Path(settings.data_root) / "_plugins"


class PluginOut(BaseModel):
    id: str
    filename: str
    display_name: str
    description: str | None
    version: str | None
    file_size_bytes: int | None
    uploaded_by: str | None
    created_at: str


class InstallResult(BaseModel):
    success: bool
    message: str
    details: str | None = None


# ── List ─────────────────────────────────────────────────────────────────

@router.get("/plugins", response_model=list[PluginOut])
async def list_plugins(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Plugin).order_by(Plugin.display_name)
    )
    plugins = result.scalars().all()
    return [
        PluginOut(
            id=p.id,
            filename=p.filename,
            display_name=p.display_name,
            description=p.description,
            version=p.version,
            file_size_bytes=p.file_size_bytes,
            uploaded_by=p.uploaded_by,
            created_at=p.created_at.isoformat(),
        )
        for p in plugins
    ]


# ── Upload ───────────────────────────────────────────────────────────────

@router.post("/plugins/upload", response_model=PluginOut, status_code=201)
async def upload_plugin(
    file: UploadFile,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not file.filename or not file.filename.endswith(".kpz"):
        raise HTTPException(400, detail="Only .kpz files are accepted.")

    _PLUGIN_DIR.mkdir(parents=True, exist_ok=True)

    # Read file
    content = await file.read()
    file_size = len(content)

    # Derive display name from filename: "koha-plugin-migration-toolkit-1.2.kpz" → "Migration Toolkit"
    stem = Path(file.filename).stem
    display = stem.replace("koha-plugin-", "").replace("Koha-Plugin-", "")
    display = display.replace("-", " ").replace("_", " ").title()

    # Save to disk
    import uuid
    plugin_id = str(uuid.uuid4())
    dest = _PLUGIN_DIR / f"{plugin_id}.kpz"
    dest.write_bytes(content)

    # Get uploader info
    user_id = getattr(request.state, "user_id", None)
    uploaded_by = user_id  # could resolve to email later

    plugin = Plugin(
        id=plugin_id,
        filename=file.filename,
        display_name=display,
        description=None,
        version=None,
        storage_path=str(dest),
        file_size_bytes=file_size,
        uploaded_by=uploaded_by,
    )
    db.add(plugin)
    await db.flush()
    await db.refresh(plugin)

    return PluginOut(
        id=plugin.id,
        filename=plugin.filename,
        display_name=plugin.display_name,
        description=plugin.description,
        version=plugin.version,
        file_size_bytes=plugin.file_size_bytes,
        uploaded_by=plugin.uploaded_by,
        created_at=plugin.created_at.isoformat(),
    )


# ── Download ─────────────────────────────────────────────────────────────

@router.get("/plugins/{plugin_id}/download")
async def download_plugin(
    plugin_id: str,
    db: AsyncSession = Depends(get_db),
):
    plugin = await db.get(Plugin, plugin_id)
    if not plugin:
        raise HTTPException(404, detail="Plugin not found.")
    path = Path(plugin.storage_path)
    if not path.is_file():
        raise HTTPException(404, detail="Plugin file missing from storage.")
    return FileResponse(
        path=str(path),
        filename=plugin.filename,
        media_type="application/octet-stream",
    )


# ── Delete ───────────────────────────────────────────────────────────────

@router.delete("/plugins/{plugin_id}", status_code=204)
async def delete_plugin(
    plugin_id: str,
    db: AsyncSession = Depends(get_db),
):
    plugin = await db.get(Plugin, plugin_id)
    if not plugin:
        raise HTTPException(404, detail="Plugin not found.")
    # Remove file
    path = Path(plugin.storage_path)
    if path.is_file():
        path.unlink()
    await db.delete(plugin)
    await db.flush()


# ── Auto-install to Koha ─────────────────────────────────────────────────

@router.post(
    "/projects/{project_id}/plugins/{plugin_id}/install",
    response_model=InstallResult,
)
async def install_plugin_to_koha(
    project_id: str,
    plugin_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Upload a .kpz plugin to Koha via the REST API, then restart Plack.

    Uses POST /api/v1/contrib/cerulean/plugins/upload if the Cerulean
    Endpoints plugin is available, otherwise falls back to:
    1. Copy file into the Koha container
    2. Run koha-shell to install it
    3. Restart plack workers
    """
    project = await require_project(project_id, db, request)
    plugin = await db.get(Plugin, plugin_id)
    if not plugin:
        raise HTTPException(404, detail="Plugin not found.")
    if not project.koha_url:
        raise HTTPException(409, detail="No Koha URL configured for this project.")

    plugin_path = Path(plugin.storage_path)
    if not plugin_path.is_file():
        raise HTTPException(404, detail="Plugin file missing from storage.")

    # Try REST API upload first, then Docker fallback
    result = await _install_via_rest(project, plugin_path, plugin.filename)
    if not result["success"]:
        result = _install_via_docker(project, plugin_path, plugin.filename)

    if result["success"]:
        await audit_log(db, project_id, stage=7, level="info", tag="[plugin]",
                        message=f"Installed plugin {plugin.display_name} ({plugin.filename})")

    return InstallResult(**result)


async def _install_via_rest(project, plugin_path: Path, filename: str) -> dict:
    """Try to upload the plugin via Koha REST API."""
    import httpx

    base_url = project.koha_url.rstrip("/")
    # Rewrite localhost for Docker networking
    if "localhost" in base_url or "127.0.0.1" in base_url:
        base_url = base_url.replace("localhost", "host.docker.internal").replace("127.0.0.1", "host.docker.internal")

    headers = _build_koha_headers(project)

    try:
        with httpx.Client(timeout=60.0, verify=False) as client:
            # Upload via Koha's plugin upload endpoint
            with open(plugin_path, "rb") as f:
                resp = client.post(
                    f"{base_url}/api/v1/contrib/cerulean/plugins/upload",
                    files={"file": (filename, f, "application/octet-stream")},
                    headers=headers,
                )

            if resp.status_code in (200, 201):
                # Restart Plack via the plugin endpoint
                restart_resp = client.post(
                    f"{base_url}/api/v1/contrib/cerulean/system/restart-plack",
                    headers=headers,
                )
                return {
                    "success": True,
                    "message": f"Installed via REST API. Plack restart: {restart_resp.status_code}",
                    "details": resp.text[:500],
                }

            # REST upload not available — fall through to Docker
            return {"success": False, "message": f"REST upload returned {resp.status_code}", "details": None}

    except Exception as exc:
        return {"success": False, "message": f"REST upload failed: {str(exc)[:300]}", "details": None}


def _install_via_docker(project, plugin_path: Path, filename: str) -> dict:
    """Install plugin by copying into the Koha container and running the installer."""
    import httpx

    # Detect container name from koha_url or use default
    container = "koha-koha-1"  # common docker-compose name

    try:
        # Step 1: Copy the .kpz file into the container using docker cp equivalent
        # (Docker API tar upload to /tmp/)
        import io
        import tarfile

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            tar.add(str(plugin_path), arcname=filename)
        tar_buffer.seek(0)

        transport = httpx.HTTPTransport(uds="/var/run/docker.sock")
        with httpx.Client(transport=transport, timeout=30.0) as client:
            # Upload tar archive to /tmp/ in the container
            put_resp = client.put(
                f"http://localhost/containers/{container}/archive",
                params={"path": "/tmp/"},
                content=tar_buffer.read(),
                headers={"Content-Type": "application/x-tar"},
            )
            if put_resp.status_code != 200:
                return {
                    "success": False,
                    "message": f"Failed to copy file into container: {put_resp.status_code}",
                    "details": put_resp.text[:300],
                }

        # Step 2: Install the plugin via koha-shell
        install_cmd = (
            f"koha-shell -c '"
            f"perl /usr/share/koha/bin/admin/install_plugins.pl "
            f"--input /tmp/{filename}' kohadev"
        )
        exit_code, stdout, stderr = _docker_exec_sync(container, install_cmd)

        # Step 3: Restart Plack workers
        restart_code, _, restart_err = _docker_exec_sync(container, "koha-plack --restart kohadev")

        if exit_code == 0:
            return {
                "success": True,
                "message": f"Installed via Docker. Plack restart exit={restart_code}",
                "details": (stdout or "")[:500],
            }
        else:
            return {
                "success": False,
                "message": f"Plugin install exited with code {exit_code}",
                "details": (stderr or stdout or "")[:500],
            }

    except Exception as exc:
        return {"success": False, "message": f"Docker install failed: {str(exc)[:300]}", "details": None}


def _docker_exec_sync(container: str, cmd: str, timeout: float = 120.0) -> tuple[int, str, str]:
    """Run a command inside a Docker container. Returns (exit_code, stdout, stderr)."""
    import httpx

    transport = httpx.HTTPTransport(uds="/var/run/docker.sock")
    with httpx.Client(transport=transport, timeout=30.0) as client:
        create_resp = client.post(
            f"http://localhost/containers/{container}/exec",
            json={"Cmd": ["bash", "-c", cmd], "AttachStdout": True, "AttachStderr": True},
        )
        if create_resp.status_code != 201:
            raise RuntimeError(f"Exec create failed: {create_resp.status_code}")
        exec_id = create_resp.json()["Id"]

    transport2 = httpx.HTTPTransport(uds="/var/run/docker.sock")
    with httpx.Client(transport=transport2, timeout=timeout) as client:
        start_resp = client.post(
            f"http://localhost/exec/{exec_id}/start",
            json={"Detach": False, "Tty": False},
        )
        raw = start_resp.content
        stdout_parts, stderr_parts = [], []
        pos = 0
        while pos + 8 <= len(raw):
            stream_type = raw[pos]
            frame_size = int.from_bytes(raw[pos + 4:pos + 8], "big")
            frame_data = raw[pos + 8:pos + 8 + frame_size]
            if stream_type == 1:
                stdout_parts.append(frame_data)
            elif stream_type == 2:
                stderr_parts.append(frame_data)
            pos += 8 + frame_size

        stdout = b"".join(stdout_parts).decode("utf-8", errors="replace").strip()
        stderr = b"".join(stderr_parts).decode("utf-8", errors="replace").strip()

    transport3 = httpx.HTTPTransport(uds="/var/run/docker.sock")
    with httpx.Client(transport=transport3, timeout=10.0) as client:
        inspect = client.get(f"http://localhost/exec/{exec_id}/json")
        exit_code = inspect.json().get("ExitCode", -1) if inspect.status_code == 200 else -1

    return exit_code, stdout, stderr


def _build_koha_headers(project) -> dict:
    """Build auth headers for Koha REST API calls."""
    import base64
    from cryptography.fernet import Fernet

    headers = {}
    token = project.koha_token_enc
    if token:
        # Try to decrypt
        try:
            fernet_key = settings.fernet_key.strip()
            if fernet_key and not fernet_key.startswith("#"):
                f = Fernet(fernet_key.encode())
                token = f.decrypt(token.encode()).decode()
        except Exception:
            pass  # token stored unencrypted (dev mode)

        if project.koha_auth_type == "bearer":
            headers["Authorization"] = f"Bearer {token}"
        else:
            encoded = base64.b64encode(token.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

    return headers
