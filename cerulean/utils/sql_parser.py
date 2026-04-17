"""
cerulean/utils/sql_parser.py
─────────────────────────────────────────────────────────────────────────────
Lightweight parser for INSERT INTO statements from control value SQL files.

Extracts code/description pairs from Koha-shaped INSERT statements:
  INSERT INTO branches (branchcode, branchname) VALUES ('MAIN', 'Main Library');
  INSERT INTO authorised_values (category, authorised_value, lib) VALUES ('LOC', 'REF', 'Reference');

Does NOT attempt to be a full SQL parser — just handles the specific
patterns that Koha migration tools produce.
"""

import re
from typing import Iterator


# Match: INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);
_INSERT_RE = re.compile(
    r"INSERT\s+(?:IGNORE\s+)?INTO\s+"
    r"[`\"']?(\w+)[`\"']?"                    # table name
    r"\s*\(([^)]+)\)"                          # column list
    r"\s*VALUES\s*\(([^)]+)\)",                # value list
    re.IGNORECASE,
)

# Koha table → cv_type mapping
_TABLE_CV_TYPE: dict[str, str] = {
    "branches": "branches",
    "itemtypes": "item_types",
    "categories": "patron_categories",
    "authorised_values": "_authorised_values",  # needs category-based dispatch
}

# authorised_values.category → cv_type
_AV_CATEGORY_CV_TYPE: dict[str, str] = {
    "LOC": "shelving_locations",
    "CCODE": "collection_codes",
    "ITYPE": "item_types",
    "NOT_LOAN": "item_types",
    "LOST": "other",
    "WITHDRAWN": "other",
    "DAMAGED": "other",
    "BOR_TITLES": "other",
}


def _unquote(val: str) -> str:
    """Strip surrounding quotes from a SQL value."""
    val = val.strip()
    if (val.startswith("'") and val.endswith("'")) or \
       (val.startswith('"') and val.endswith('"')):
        return val[1:-1].replace("\\'", "'").replace("\\\\", "\\")
    return val


def _split_csv(s: str) -> list[str]:
    """Split a comma-separated list respecting quoted strings."""
    parts = []
    current = []
    in_quote = False
    quote_char = None
    for ch in s:
        if in_quote:
            current.append(ch)
            if ch == quote_char:
                in_quote = False
        elif ch in ("'", '"'):
            in_quote = True
            quote_char = ch
            current.append(ch)
        elif ch == ",":
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return parts


def parse_insert_statements(sql_text: str) -> list[dict]:
    """Parse INSERT statements and return a list of {cv_type, code, description} dicts.

    Skips lines that don't match the expected pattern. Does not raise on
    malformed SQL — unparseable lines are silently ignored (logged by caller).
    """
    results: list[dict] = []
    for match in _INSERT_RE.finditer(sql_text):
        table = match.group(1).lower()
        cols = [c.strip().strip("`\"'").lower() for c in _split_csv(match.group(2))]
        vals = [_unquote(v) for v in _split_csv(match.group(3))]

        if len(cols) != len(vals):
            continue

        row = dict(zip(cols, vals))

        cv_type = _TABLE_CV_TYPE.get(table)
        if not cv_type:
            continue

        if cv_type == "_authorised_values":
            category = row.get("category", "").upper()
            cv_type = _AV_CATEGORY_CV_TYPE.get(category, f"av:{category.lower()}" if category else "other")

        code, description = _extract_code_desc(table, row)
        if not code:
            continue

        results.append({
            "cv_type": cv_type,
            "code": code,
            "description": description or "",
        })

    return results


def _extract_code_desc(table: str, row: dict) -> tuple[str, str]:
    """Extract the code + description columns based on table type."""
    if table == "branches":
        return row.get("branchcode", ""), row.get("branchname", "")
    elif table == "itemtypes":
        return row.get("itemtype", ""), row.get("description", "")
    elif table == "categories":
        return row.get("categorycode", ""), row.get("description", "")
    elif table == "authorised_values":
        code = row.get("authorised_value", "") or row.get("value", "")
        desc = row.get("lib", "") or row.get("description", "")
        return code, desc
    return "", ""
