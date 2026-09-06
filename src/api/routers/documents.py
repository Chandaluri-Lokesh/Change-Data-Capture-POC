"""
POST /api/documents/{doc_type}

Inserts or replaces a single P2P document directly into MongoDB.
This triggers a real Debezium CDC event which flows through Kafka
into Postgres and Neo4j — the same path as the simulator.

Supported doc_type values: rfq, po, asn, grn, invoice
"""

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

COLLECTION_MAP = {
    'rfq':     'rfqs',
    'po':      'purchase_orders',
    'asn':     'asns',
    'grn':     'grns',
    'invoice': 'invoices',
}


class DocumentPayload(BaseModel):
    fields: dict[str, Any]


@router.post('/{doc_type}')
async def insert_document(doc_type: str, payload: DocumentPayload, request: Request):
    """
    Insert or replace a P2P document.

    The `fields` dict must contain the document's primary key field
    (e.g. `po_number` for a PO), which will be used as `_id`.
    `created_at` and `updated_at` are auto-filled if absent.
    """
    collection_name = COLLECTION_MAP.get(doc_type.lower())
    if not collection_name:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown doc_type {doc_type!r}. Valid: {list(COLLECTION_MAP)}",
        )

    doc = dict(payload.fields)
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    doc.setdefault('created_at', now)
    doc['updated_at'] = now
    doc['document_type'] = doc_type.upper()

    # Derive _id from the natural key if present, else require explicit _id
    key_map = {
        'rfq': 'rfq_number', 'po': 'po_number', 'asn': 'asn_number',
        'grn': 'grn_number',  'invoice': 'invoice_number',
    }
    key_field = key_map[doc_type.lower()]
    if key_field not in doc and '_id' not in doc:
        raise HTTPException(
            status_code=422,
            detail=f"Document must include '{key_field}' or '_id'.",
        )
    doc.setdefault('_id', doc.get(key_field) or doc['_id'])
    doc.setdefault(key_field, doc['_id'])

    db = request.app.state.mongo_db
    try:
        db[collection_name].replace_one({'_id': doc['_id']}, doc, upsert=True)
        logger.info(f"[documents] Inserted {doc_type.upper()} _id={doc['_id']!r}")
        return {'status': 'ok', 'collection': collection_name, '_id': doc['_id']}
    except Exception as exc:
        logger.error(f"[documents] MongoDB write failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.delete('/{doc_type}/{doc_id}')
async def delete_document(doc_type: str, doc_id: str, request: Request):
    """Delete a document by its natural key — triggers CDC delete event."""
    collection_name = COLLECTION_MAP.get(doc_type.lower())
    if not collection_name:
        raise HTTPException(status_code=400, detail=f"Unknown doc_type {doc_type!r}")

    db = request.app.state.mongo_db
    result = db[collection_name].delete_one({'_id': doc_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"{doc_id!r} not found")
    return {'status': 'deleted', 'collection': collection_name, '_id': doc_id}
