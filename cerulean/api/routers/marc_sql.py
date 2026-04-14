"""
cerulean/api/routers/marc_sql.py
─────────────────────────────────────────────────────────────────────────────
MARC SQL Explorer — query MARC records using SQL-like syntax.

POST /projects/{id}/marc-sql/query   — run a query
GET  /projects/{id}/marc-sql/schema  — describe available "columns" (tags)

Query syntax (simplified SQL):
    SELECT 001, 245$a, 852$a WHERE 942$c = 'DVD' AND 852$a CONTAINS 'MAIN'
    SELECT 001, 245$a WHERE 020$a EXISTS LIMIT 100
    SELECT * WHERE 245$a LIKE '%journal%'
    SELECT 001, 100$a, 245$a ORDER BY 100$a LIMIT 50
"""

import re
from pathlib import Path

import pymarc
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.api.deps import require_project
from cerulean.core.config import get_settings
from cerulean.core.database import get_db

router = APIRouter(prefix="/projects", tags=["marc-sql"])
settings = get_settings()


class SqlQueryRequest(BaseModel):
    query: str
    file_path: str | None = None
    max_results: int = 500


def _find_marc_file(project_id: str, file_path: str | None = None) -> Path:
    project_dir = Path(settings.data_root) / project_id
    if file_path:
        for sub in ["", "raw", "transformed"]:
            candidate = (project_dir / sub / file_path) if sub else (project_dir / file_path)
            if candidate.is_file():
                return candidate
        raise HTTPException(404, detail=f"File not found: {file_path}")
    for name in ["output.mrc", "Biblios-mapped-items.mrc", "merged_deduped.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            return candidate
    raw = project_dir / "raw"
    if raw.is_dir():
        files = sorted(raw.glob("*.mrc"))
        if files:
            return files[0]
    raise HTTPException(404, detail="No MARC files found in project.")


def _extract_value(record: pymarc.Record, field_spec: str) -> str:
    """Extract value from record. Supports '001', '245$a', '245' (all subfields)."""
    if "$" in field_spec:
        tag, sub = field_spec.split("$", 1)
    else:
        tag, sub = field_spec, None

    fields = record.get_fields(tag)
    if not fields:
        return ""

    values = []
    for f in fields:
        if sub and hasattr(f, "subfields"):
            for sf in f.subfields:
                if sf.code == sub:
                    values.append(sf.value)
        elif hasattr(f, "data"):
            values.append(f.data)
        elif not sub and hasattr(f, "subfields"):
            values.append(" ".join(sf.value for sf in f.subfields))
    return "; ".join(values)


def _parse_query(query_str: str) -> dict:
    """Parse a simplified SQL query into components.

    SELECT 001, 245$a, 852$a WHERE 942$c = 'DVD' AND 852$a CONTAINS 'MAIN' LIMIT 100
    """
    q = query_str.strip().rstrip(";")

    # Extract LIMIT
    limit = 500
    limit_match = re.search(r'\bLIMIT\s+(\d+)\s*$', q, re.IGNORECASE)
    if limit_match:
        limit = int(limit_match.group(1))
        q = q[:limit_match.start()].strip()

    # Extract ORDER BY
    order_by = None
    order_dir = "asc"
    order_match = re.search(r'\bORDER\s+BY\s+([\w$]+)(?:\s+(ASC|DESC))?\s*$', q, re.IGNORECASE)
    if order_match:
        order_by = order_match.group(1)
        if order_match.group(2):
            order_dir = order_match.group(2).lower()
        q = q[:order_match.start()].strip()

    # Split SELECT ... WHERE ...
    where_match = re.search(r'\bWHERE\s+(.+)$', q, re.IGNORECASE)
    conditions = []
    if where_match:
        where_clause = where_match.group(1).strip()
        q = q[:where_match.start()].strip()
        conditions = _parse_conditions(where_clause)

    # Parse SELECT fields
    select_match = re.match(r'^SELECT\s+(.+)$', q, re.IGNORECASE)
    if not select_match:
        raise HTTPException(400, detail="Query must start with SELECT. Example: SELECT 001, 245$a WHERE 942$c = 'DVD'")

    fields_str = select_match.group(1).strip()
    if fields_str == "*":
        fields = ["*"]
    else:
        fields = [f.strip() for f in fields_str.split(",") if f.strip()]

    return {
        "fields": fields,
        "conditions": conditions,
        "order_by": order_by,
        "order_dir": order_dir,
        "limit": limit,
    }


def _parse_conditions(where_str: str) -> list[dict]:
    """Parse WHERE clause into condition dicts.

    Supports: =, !=, CONTAINS, NOT CONTAINS, LIKE, EXISTS, MISSING
    Joined by AND/OR
    """
    conditions = []
    # Split on AND/OR (keeping the operator)
    parts = re.split(r'\s+(AND|OR)\s+', where_str, flags=re.IGNORECASE)

    logic = "and"
    for i, part in enumerate(parts):
        part = part.strip()
        if part.upper() in ("AND", "OR"):
            logic = part.lower()
            continue

        # EXISTS / MISSING (no value)
        exists_match = re.match(r'^([\w$]+)\s+(EXISTS|MISSING|NOT\s+EXISTS)$', part, re.IGNORECASE)
        if exists_match:
            field = exists_match.group(1)
            op = "exists" if "NOT" not in exists_match.group(2).upper() and "MISSING" not in exists_match.group(2).upper() else "missing"
            conditions.append({"field": field, "operator": op, "value": "", "logic": logic})
            continue

        # Field OPERATOR 'value' or Field OPERATOR value
        match = re.match(
            r"^([\w$]+)\s+(=|!=|<>|CONTAINS|NOT\s+CONTAINS|LIKE|NOT\s+LIKE|STARTS\s+WITH|>|<|>=|<=)\s+['\"]?(.+?)['\"]?\s*$",
            part, re.IGNORECASE
        )
        if match:
            field = match.group(1)
            op_raw = match.group(2).strip().upper()
            value = match.group(3).strip().strip("'\"")

            op_map = {
                "=": "equals", "!=": "not_equals", "<>": "not_equals",
                "CONTAINS": "contains", "NOT CONTAINS": "not_contains",
                "LIKE": "like", "NOT LIKE": "not_like",
                "STARTS WITH": "starts_with",
            }
            op = op_map.get(op_raw, "equals")
            conditions.append({"field": field, "operator": op, "value": value, "logic": logic})
            continue

        raise HTTPException(400, detail=f"Cannot parse condition: '{part}'. Use: field = 'value', field CONTAINS 'text', field EXISTS")

    return conditions


def _evaluate_condition(record: pymarc.Record, cond: dict) -> bool:
    """Evaluate a single condition against a record."""
    actual = _extract_value(record, cond["field"]).strip()
    value = cond["value"]
    op = cond["operator"]

    if op == "equals":
        return actual.lower() == value.lower()
    elif op == "not_equals":
        return actual.lower() != value.lower()
    elif op == "contains":
        return value.lower() in actual.lower()
    elif op == "not_contains":
        return value.lower() not in actual.lower()
    elif op == "like":
        # SQL LIKE: % = any, _ = single char
        pattern = re.escape(value).replace(r"\%", ".*").replace(r"\_", ".")
        return bool(re.match(f"^{pattern}$", actual, re.IGNORECASE))
    elif op == "not_like":
        pattern = re.escape(value).replace(r"\%", ".*").replace(r"\_", ".")
        return not bool(re.match(f"^{pattern}$", actual, re.IGNORECASE))
    elif op == "starts_with":
        return actual.lower().startswith(value.lower())
    elif op == "exists":
        return bool(actual)
    elif op == "missing":
        return not actual
    return False


def _evaluate_all_conditions(record: pymarc.Record, conditions: list[dict]) -> bool:
    """Evaluate all conditions with AND/OR logic."""
    if not conditions:
        return True

    result = _evaluate_condition(record, conditions[0])
    for cond in conditions[1:]:
        val = _evaluate_condition(record, cond)
        if cond.get("logic", "and") == "or":
            result = result or val
        else:
            result = result and val
    return result


# ── Query Endpoint ───────────────────────────────────────────────────

@router.post("/{project_id}/marc-sql/query")
async def run_query(
    project_id: str,
    body: SqlQueryRequest,
    db: AsyncSession = Depends(get_db),
):
    """Run a SQL-like query against the project's MARC data."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id, body.file_path)

    parsed = _parse_query(body.query)
    fields = parsed["fields"]
    conditions = parsed["conditions"]
    limit = min(parsed["limit"], body.max_results)

    # If SELECT *, we'll discover fields from first matching record
    discover_fields = fields == ["*"]
    if discover_fields:
        fields = []

    rows = []
    total_scanned = 0
    total_matched = 0

    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for record in reader:
            if record is None:
                continue
            total_scanned += 1

            if not _evaluate_all_conditions(record, conditions):
                continue

            total_matched += 1

            if discover_fields and not fields:
                # Build field list from first record's tags
                seen = set()
                for f in record.get_fields():
                    if f.tag not in seen:
                        if hasattr(f, "subfields") and f.subfields:
                            for sf in f.subfields:
                                fields.append(f"{f.tag}${sf.code}")
                        else:
                            fields.append(f.tag)
                        seen.add(f.tag)
                # Keep it reasonable
                fields = fields[:50]

            if len(rows) < limit:
                row = {"_index": total_scanned - 1}
                for field_spec in fields:
                    row[field_spec] = _extract_value(record, field_spec)
                rows.append(row)

    # Sort if requested
    if parsed["order_by"] and rows:
        sort_field = parsed["order_by"]
        reverse = parsed["order_dir"] == "desc"
        rows.sort(key=lambda r: r.get(sort_field, "").lower(), reverse=reverse)

    return {
        "query": body.query,
        "fields": fields,
        "total_scanned": total_scanned,
        "total_matched": total_matched,
        "rows_returned": len(rows),
        "rows": rows,
    }


# ── Schema Endpoint ─────────────────────────────────────────────────

@router.get("/{project_id}/marc-sql/schema")
async def get_schema(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return the available 'columns' (MARC tags/subfields) for the project's data."""
    await require_project(project_id, db)
    source = _find_marc_file(project_id)

    # Sample first 100 records to build schema
    tags = {}
    with open(str(source), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for i, record in enumerate(reader):
            if record is None:
                continue
            if i >= 100:
                break
            for f in record.get_fields():
                if f.tag not in tags:
                    tags[f.tag] = {"tag": f.tag, "subfields": set(), "count": 0}
                tags[f.tag]["count"] += 1
                if hasattr(f, "subfields"):
                    for sf in f.subfields:
                        tags[f.tag]["subfields"].add(sf.code)

    schema = []
    for tag in sorted(tags.keys()):
        t = tags[tag]
        subs = sorted(t["subfields"])
        schema.append({
            "tag": tag,
            "subfields": subs,
            "columns": [tag] + [f"{tag}${s}" for s in subs],
            "sample_count": t["count"],
        })

    return {"schema": schema, "sample_records": min(100, i + 1) if 'i' in dir() else 0}
