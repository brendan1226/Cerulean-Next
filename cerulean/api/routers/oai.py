"""
cerulean/api/routers/oai.py
─────────────────────────────────────────────────────────────────────────────
OAI-PMH 2.0 endpoint for harvesting MARC records from Cerulean projects.

Supports the subset of OAI-PMH verbs needed by MarcEdit:
    Identify, ListRecords, GetRecord, ListMetadataFormats

Usage from MarcEdit:
    Base URL: https://cerulean-next.gallagher-family-hub.com/oai/{project_id}
    Metadata format: marcxml

This is NOT behind JWT auth — it uses the project's visibility setting.
Shared projects are harvestable; private projects require a token query param.
"""

from datetime import datetime
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

import pymarc
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from cerulean.core.config import get_settings
from cerulean.core.database import get_db
from cerulean.models import Project

router = APIRouter(tags=["oai-pmh"])
settings = get_settings()

_NS = "http://www.openarchives.org/OAI/2.0/"
_MARCXML_NS = "http://www.loc.gov/MARC21/slim"


def _oai_response(verb: str, project_id: str) -> Element:
    """Build the OAI-PMH response wrapper."""
    root = Element("OAI-PMH", xmlns=_NS)
    SubElement(root, "responseDate").text = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    req = SubElement(root, "request", verb=verb)
    req.text = f"{settings.data_root}/oai/{project_id}"
    return root


def _oai_error(code: str, message: str) -> Response:
    root = Element("OAI-PMH", xmlns=_NS)
    SubElement(root, "responseDate").text = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    SubElement(root, "request")
    err = SubElement(root, "error", code=code)
    err.text = message
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode")
    return Response(content=xml, media_type="text/xml")


def _find_marc_file(project_id: str) -> Path | None:
    project_dir = Path(settings.data_root) / project_id
    for name in ["output.mrc", "Biblios-mapped-items.mrc", "merged_deduped.mrc", "merged.mrc"]:
        candidate = project_dir / name
        if candidate.is_file():
            return candidate
    transformed = project_dir / "transformed"
    if transformed.is_dir():
        files = sorted(transformed.glob("*_transformed.mrc"))
        if files:
            return files[0]
    raw = project_dir / "raw"
    if raw.is_dir():
        files = sorted(raw.glob("*.mrc"))
        if files:
            return files[0]
    return None


@router.get("/oai/{project_id}")
async def oai_endpoint(
    project_id: str,
    verb: str = Query(...),
    identifier: str | None = Query(None),
    metadataPrefix: str | None = Query(None),
    resumptionToken: str | None = Query(None),
    _from: str | None = Query(None, alias="from"),
    until: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """OAI-PMH 2.0 endpoint. Supports Identify, ListMetadataFormats, ListRecords, GetRecord."""
    project = await db.get(Project, project_id)
    if not project:
        return _oai_error("badArgument", "Project not found.")

    # Access control: only shared/legacy projects are harvestable without auth
    if project.visibility == "private" and project.owner_id:
        return _oai_error("badArgument", "This project is private. Set visibility to 'shared' to enable OAI harvesting.")

    if verb == "Identify":
        return _identify(project)
    elif verb == "ListMetadataFormats":
        return _list_formats(project)
    elif verb == "ListRecords":
        return _list_records(project, metadataPrefix or "marcxml", resumptionToken)
    elif verb == "GetRecord":
        if not identifier:
            return _oai_error("badArgument", "identifier is required for GetRecord.")
        return _get_record(project, identifier, metadataPrefix or "marcxml")
    else:
        return _oai_error("badVerb", f"Unsupported verb: {verb}")


def _identify(project: Project) -> Response:
    root = _oai_response("Identify", project.id)
    ident = SubElement(root, "Identify")
    SubElement(ident, "repositoryName").text = f"Cerulean Next — {project.library_name}"
    SubElement(ident, "baseURL").text = f"/oai/{project.id}"
    SubElement(ident, "protocolVersion").text = "2.0"
    SubElement(ident, "earliestDatestamp").text = "2020-01-01T00:00:00Z"
    SubElement(ident, "deletedRecord").text = "no"
    SubElement(ident, "granularity").text = "YYYY-MM-DDThh:mm:ssZ"
    SubElement(ident, "adminEmail").text = "admin@bywatersolutions.com"
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode")
    return Response(content=xml, media_type="text/xml")


def _list_formats(project: Project) -> Response:
    root = _oai_response("ListMetadataFormats", project.id)
    formats = SubElement(root, "ListMetadataFormats")
    fmt = SubElement(formats, "metadataFormat")
    SubElement(fmt, "metadataPrefix").text = "marcxml"
    SubElement(fmt, "schema").text = "http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"
    SubElement(fmt, "metadataNamespace").text = _MARCXML_NS
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode")
    return Response(content=xml, media_type="text/xml")


def _list_records(project: Project, prefix: str, token: str | None) -> Response:
    if prefix != "marcxml":
        return _oai_error("cannotDisseminateFormat", "Only marcxml is supported.")

    marc_file = _find_marc_file(project.id)
    if not marc_file:
        return _oai_error("noRecordsMatch", "No MARC files found in project.")

    # Pagination via resumptionToken (simple offset-based)
    offset = 0
    batch_size = 100
    if token:
        try:
            offset = int(token)
        except ValueError:
            return _oai_error("badResumptionToken", "Invalid resumptionToken.")

    root = _oai_response("ListRecords", project.id)
    list_el = SubElement(root, "ListRecords")

    count = 0
    total = 0
    with open(str(marc_file), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for i, record in enumerate(reader):
            if record is None:
                continue
            total = i + 1
            if i < offset:
                continue
            if count >= batch_size:
                # Add resumption token for next page
                rt = SubElement(list_el, "resumptionToken")
                rt.text = str(offset + batch_size)
                break

            rec_el = SubElement(list_el, "record")
            header = SubElement(rec_el, "header")
            # Use 001 as identifier, fall back to index
            ctrl = record.get_fields("001")
            oai_id = f"oai:cerulean:{project.id}:{ctrl[0].data if ctrl else str(i)}"
            SubElement(header, "identifier").text = oai_id
            SubElement(header, "datestamp").text = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            metadata = SubElement(rec_el, "metadata")
            marcxml_str = pymarc.record_to_xml(record, namespace=True).decode("utf-8")
            # Parse and attach the MARCXML
            from xml.etree.ElementTree import fromstring
            try:
                marc_el = fromstring(marcxml_str)
                metadata.append(marc_el)
            except Exception:
                SubElement(metadata, "error").text = "Failed to serialize record"

            count += 1

    if count == 0 and offset == 0:
        return _oai_error("noRecordsMatch", "No records found.")

    xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode")
    return Response(content=xml, media_type="text/xml")


def _get_record(project: Project, identifier: str, prefix: str) -> Response:
    if prefix != "marcxml":
        return _oai_error("cannotDisseminateFormat", "Only marcxml is supported.")

    marc_file = _find_marc_file(project.id)
    if not marc_file:
        return _oai_error("idDoesNotExist", "No MARC files found.")

    # Parse identifier — format: oai:cerulean:{project_id}:{001_value_or_index}
    parts = identifier.split(":")
    target = parts[-1] if parts else identifier

    with open(str(marc_file), "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="replace")
        for i, record in enumerate(reader):
            if record is None:
                continue
            ctrl = record.get_fields("001")
            ctrl_val = ctrl[0].data if ctrl else str(i)
            if ctrl_val == target or str(i) == target:
                root = _oai_response("GetRecord", project.id)
                gr = SubElement(root, "GetRecord")
                rec_el = SubElement(gr, "record")
                header = SubElement(rec_el, "header")
                SubElement(header, "identifier").text = identifier
                SubElement(header, "datestamp").text = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                metadata = SubElement(rec_el, "metadata")
                marcxml_str = pymarc.record_to_xml(record, namespace=True).decode("utf-8")
                from xml.etree.ElementTree import fromstring
                try:
                    metadata.append(fromstring(marcxml_str))
                except Exception:
                    SubElement(metadata, "error").text = "Failed to serialize"
                xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode")
                return Response(content=xml, media_type="text/xml")

    return _oai_error("idDoesNotExist", f"Record '{target}' not found.")
