"""
cerulean/api/routers/macros.py
─────────────────────────────────────────────────────────────────────────────
Macro system — save and replay sequences of batch edit operations.

GET    /macros                         — list macros
POST   /macros                         — create macro
GET    /macros/{id}                    — get macro detail
PATCH  /macros/{id}                    — update macro
DELETE /macros/{id}                    — delete macro
POST   /projects/{pid}/macros/{id}/run — run macro on project data
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import audit_log, require_project
from cerulean.core.database import get_db
from cerulean.models import Macro

router = APIRouter(tags=["macros"])


class MacroCreate(BaseModel):
    name: str
    description: str | None = None
    operations: list[dict]      # array of batch-edit operation dicts
    scope: str = "global"

class MacroUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    operations: list[dict] | None = None

class MacroOut(BaseModel):
    id: str
    name: str
    description: str | None
    operations: list[dict]
    scope: str
    created_by: str | None
    use_count: int
    created_at: str
    updated_at: str


@router.get("/macros")
async def list_macros(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Macro).order_by(Macro.use_count.desc(), Macro.name)
    )
    macros = result.scalars().all()
    return [
        {
            "id": m.id, "name": m.name, "description": m.description,
            "operations": m.operations, "scope": m.scope,
            "created_by": m.created_by, "use_count": m.use_count,
            "created_at": m.created_at.isoformat() if m.created_at else "",
            "updated_at": m.updated_at.isoformat() if m.updated_at else "",
            "operation_count": len(m.operations) if m.operations else 0,
        }
        for m in macros
    ]


@router.post("/macros", status_code=201)
async def create_macro(
    body: MacroCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not body.operations:
        raise HTTPException(400, detail="At least one operation is required.")

    user_email = None
    user_id = getattr(request.state, "user_id", None)
    if user_id:
        from cerulean.models import User
        user = await db.get(User, user_id)
        if user:
            user_email = user.email

    macro = Macro(
        id=str(uuid.uuid4()),
        name=body.name,
        description=body.description,
        operations=body.operations,
        scope=body.scope,
        created_by=user_email,
    )
    db.add(macro)
    await db.flush()
    await db.refresh(macro)

    return {
        "id": macro.id, "name": macro.name,
        "operation_count": len(macro.operations),
        "message": f"Macro '{macro.name}' created with {len(macro.operations)} operations.",
    }


@router.get("/macros/{macro_id}")
async def get_macro(macro_id: str, db: AsyncSession = Depends(get_db)):
    macro = await db.get(Macro, macro_id)
    if not macro:
        raise HTTPException(404, detail="Macro not found.")
    return {
        "id": macro.id, "name": macro.name, "description": macro.description,
        "operations": macro.operations, "scope": macro.scope,
        "created_by": macro.created_by, "use_count": macro.use_count,
        "created_at": macro.created_at.isoformat() if macro.created_at else "",
    }


@router.patch("/macros/{macro_id}")
async def update_macro(
    macro_id: str,
    body: MacroUpdate,
    db: AsyncSession = Depends(get_db),
):
    macro = await db.get(Macro, macro_id)
    if not macro:
        raise HTTPException(404, detail="Macro not found.")
    if body.name is not None:
        macro.name = body.name
    if body.description is not None:
        macro.description = body.description
    if body.operations is not None:
        macro.operations = body.operations
    await db.flush()
    return {"id": macro.id, "name": macro.name, "message": "Macro updated."}


@router.delete("/macros/{macro_id}", status_code=204)
async def delete_macro(macro_id: str, db: AsyncSession = Depends(get_db)):
    macro = await db.get(Macro, macro_id)
    if not macro:
        raise HTTPException(404, detail="Macro not found.")
    await db.delete(macro)
    await db.flush()


@router.post("/projects/{project_id}/macros/{macro_id}/run")
async def run_macro(
    project_id: str,
    macro_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Run a macro (sequence of batch edit operations) on the project's MARC data."""
    await require_project(project_id, db)
    macro = await db.get(Macro, macro_id)
    if not macro:
        raise HTTPException(404, detail="Macro not found.")

    import httpx
    from cerulean.core.config import get_settings
    settings = get_settings()

    results = []
    total_mods = 0

    for i, op in enumerate(macro.operations):
        op_type = op.get("type", "")
        op_body = {k: v for k, v in op.items() if k != "type"}

        # Map operation type to batch-edit endpoint
        endpoint_map = {
            "find_replace": f"/api/v1/projects/{project_id}/batch-edit/find-replace",
            "regex": f"/api/v1/projects/{project_id}/batch-edit/regex",
            "add_field": f"/api/v1/projects/{project_id}/batch-edit/add-field",
            "delete_field": f"/api/v1/projects/{project_id}/batch-edit/delete-field",
        }

        endpoint = endpoint_map.get(op_type)
        if not endpoint:
            results.append({"step": i + 1, "type": op_type, "error": f"Unknown operation type: {op_type}"})
            continue

        # Call the batch-edit endpoint internally
        try:
            # Use internal function calls instead of HTTP
            from cerulean.api.routers.batch_edit import (
                find_replace, add_field, delete_field, regex_replace,
                FindReplaceRequest, AddFieldRequest, DeleteFieldRequest, RegexRequest,
            )
            from sqlalchemy.orm import Session
            from cerulean.core.database import get_db as _get_db

            if op_type == "find_replace":
                r = await find_replace(project_id, FindReplaceRequest(**op_body), db)
            elif op_type == "regex":
                r = await regex_replace(project_id, RegexRequest(**op_body), db)
            elif op_type == "add_field":
                r = await add_field(project_id, AddFieldRequest(**op_body), db)
            elif op_type == "delete_field":
                r = await delete_field(project_id, DeleteFieldRequest(**op_body), db)
            else:
                r = None

            if r:
                total_mods += r.modifications
                results.append({
                    "step": i + 1, "type": op_type,
                    "records_modified": r.records_modified,
                    "modifications": r.modifications,
                })
            else:
                results.append({"step": i + 1, "type": op_type, "error": "No result"})
        except Exception as exc:
            results.append({"step": i + 1, "type": op_type, "error": str(exc)[:200]})

    # Increment use count
    macro.use_count += 1
    await db.flush()

    await audit_log(db, project_id, stage=3, level="info", tag="[macro]",
                    message=f"Ran macro '{macro.name}' ({len(macro.operations)} ops) — {total_mods} total modifications")

    return {
        "macro_name": macro.name,
        "operations_run": len(results),
        "total_modifications": total_mods,
        "results": results,
    }
