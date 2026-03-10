"""
cerulean/core/search.py
─────────────────────────────────────────────────────────────────────────────
Elasticsearch client wrapper with graceful degradation.
All functions return None / empty results when ES is not configured or
unreachable — never raises on connection failure.
"""

import logging

from cerulean.core.config import get_settings

logger = logging.getLogger(__name__)

INDEX_NAME = "cerulean-marc"


def get_es_client():
    """Return an Elasticsearch client or None if not configured/reachable."""
    settings = get_settings()
    if not settings.elasticsearch_url:
        return None
    try:
        from elasticsearch import Elasticsearch

        es = Elasticsearch(settings.elasticsearch_url)
        if not es.ping():
            logger.debug("Elasticsearch ping failed at %s", settings.elasticsearch_url)
            return None
        return es
    except Exception:
        logger.debug("Elasticsearch unavailable", exc_info=True)
        return None


def ensure_index(es) -> None:
    """Create the MARC records index if it doesn't exist."""
    if es is None:
        return
    try:
        if not es.indices.exists(index=INDEX_NAME):
            es.indices.create(
                index=INDEX_NAME,
                body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                    },
                    "mappings": {
                        "properties": {
                            "project_id": {"type": "keyword"},
                            "file_id": {"type": "keyword"},
                            "record_index": {"type": "integer"},
                            "marc_001": {"type": "keyword"},
                            "title": {"type": "text"},
                            "author": {"type": "text"},
                            "isbn": {"type": "keyword"},
                            "all_text": {"type": "text"},
                        }
                    },
                },
            )
    except Exception:
        logger.warning("Failed to create ES index", exc_info=True)


def index_marc_records(es, project_id: str, file_id: str, records) -> int:
    """Bulk-index MARC records into Elasticsearch.

    Args:
        es: Elasticsearch client (or None).
        project_id: Project UUID.
        file_id: MARCFile UUID.
        records: Iterable of pymarc.Record objects.

    Returns:
        Number of records indexed, or 0 if ES unavailable.
    """
    if es is None:
        return 0

    from elasticsearch.helpers import bulk

    def _gen_actions():
        for i, record in enumerate(records):
            # Extract key fields
            marc_001 = ""
            if record["001"]:
                marc_001 = record["001"].data or ""

            title = record.title() or ""

            author = ""
            if record["100"] and record["100"]["a"]:
                author = record["100"]["a"]
            elif record["110"] and record["110"]["a"]:
                author = record["110"]["a"]

            isbn = ""
            if record["020"] and record["020"]["a"]:
                isbn = record["020"]["a"]

            # Build all_text from all fields
            parts = []
            for field in record.fields:
                if field.is_control_field():
                    parts.append(field.data or "")
                else:
                    for sf in field.subfields:
                        parts.append(sf.value or "")
            all_text = " ".join(parts)

            yield {
                "_index": INDEX_NAME,
                "_id": f"{file_id}_{i}",
                "_source": {
                    "project_id": project_id,
                    "file_id": file_id,
                    "record_index": i,
                    "marc_001": marc_001,
                    "title": title,
                    "author": author,
                    "isbn": isbn,
                    "all_text": all_text,
                },
            }

    try:
        success, errors = bulk(es, _gen_actions(), raise_on_error=False)
        if errors:
            logger.warning("ES bulk indexing had %d errors", len(errors))
        return success
    except Exception:
        logger.warning("ES bulk indexing failed", exc_info=True)
        return 0


def search_records(es, project_id: str, query: str, offset: int = 0, limit: int = 20) -> dict:
    """Full-text search across indexed MARC records for a project.

    Returns:
        dict with total, offset, limit, and hits list.
    """
    if es is None:
        return {"total": 0, "offset": offset, "limit": limit, "hits": []}

    try:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"multi_match": {"query": query, "fields": ["title^3", "author^2", "isbn", "all_text"]}},
                    ],
                    "filter": [
                        {"term": {"project_id": project_id}},
                    ],
                }
            },
            "from": offset,
            "size": limit,
            "_source": ["project_id", "file_id", "record_index", "marc_001", "title", "author", "isbn"],
        }
        result = es.search(index=INDEX_NAME, body=body)
        total = result["hits"]["total"]["value"] if isinstance(result["hits"]["total"], dict) else result["hits"]["total"]
        hits = [
            {**h["_source"], "score": h["_score"]}
            for h in result["hits"]["hits"]
        ]
        return {"total": total, "offset": offset, "limit": limit, "hits": hits}
    except Exception:
        logger.warning("ES search failed", exc_info=True)
        return {"total": 0, "offset": offset, "limit": limit, "hits": []}
