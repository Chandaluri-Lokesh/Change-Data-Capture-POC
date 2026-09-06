"""
Schema drift detection and payload validation for the P2P CDC pipeline.

Validates that required fields are present and non-None before writing.
Logs unknown top-level fields (schema drift) but does NOT reject events for
them — unknown fields are stored in ParsedEvent.extra_fields for audit.

Edge cases
──────────
- op=d / tombstone: no document body needed — always returns True.
- required field present but None: treated as missing (same as absent).
- collection not in REQUIRED_FIELDS: no required fields → always True.
- extra/unknown fields: logged as INFO; stored in extra_fields.
"""

import logging
from transformer.debezium_parser import ParsedEvent

logger = logging.getLogger(__name__)

# Minimum non-null fields for a write-eligible event per collection
REQUIRED_FIELDS: dict[str, list] = {
    'rfqs':            ['rfq_number', 'status'],
    'purchase_orders': ['po_number', 'vendor_id', 'status'],
    'asns':            ['asn_number', 'po_number', 'status'],
    'grns':            ['grn_number', 'po_number', 'status'],
    'invoices':        ['invoice_number', 'po_number', 'status'],
}

# Top-level known fields per collection (for drift detection)
KNOWN_FIELDS: dict[str, set] = {
    'rfqs': {
        '_id', 'document_type', 'rfq_number', 'requested_date', 'requested_by',
        'plant', 'status', 'response_due_date', 'invited_vendors', 'line_items',
        'created_at', 'updated_at',
    },
    'purchase_orders': {
        '_id', 'document_type', 'po_number', 'rfq_number', 'vendor_id', 'vendor_name',
        'order_date', 'delivery_date', 'plant', 'currency', 'status', 'payment_terms',
        'line_items', 'order_total', 'created_at', 'updated_at',
    },
    'asns': {
        '_id', 'document_type', 'asn_number', 'po_number', 'vendor_id',
        'ship_date', 'carrier', 'tracking_number', 'expected_arrival', 'status',
        'line_items', 'created_at', 'updated_at',
    },
    'grns': {
        '_id', 'document_type', 'grn_number', 'po_number', 'asn_number',
        'receipt_date', 'received_by', 'plant', 'status', 'line_items',
        'created_at', 'updated_at',
    },
    'invoices': {
        '_id', 'document_type', 'invoice_number', 'po_number', 'grn_number',
        'vendor_id', 'invoice_date', 'due_date', 'currency', 'status', 'line_items',
        'subtotal', 'tax_rate', 'tax_amount', 'total_amount', 'created_at', 'updated_at',
    },
}


def validate(event: ParsedEvent) -> bool:
    """
    Returns True if the event is safe to write.

    - Delete and tombstone events always pass.
    - Checks required fields are present and non-None.
    - Logs unknown fields (schema drift) into event.extra_fields.
    """
    if event.op in ('d', 'tombstone'):
        return True

    if not event.document:
        logger.warning(
            f"[schema_guard] op={event.op!r} on {event.collection!r} "
            f"has no document body — routing to DLQ. doc_id={event.doc_id!r}"
        )
        return False

    required = REQUIRED_FIELDS.get(event.collection, [])
    missing  = [f for f in required if event.document.get(f) is None]
    if missing:
        logger.warning(
            f"[schema_guard] Missing required fields {missing} in {event.collection!r} "
            f"doc_id={event.doc_id!r} — routing to DLQ"
        )
        return False

    known   = KNOWN_FIELDS.get(event.collection, set())
    unknown = {k: v for k, v in event.document.items() if k not in known}
    if unknown:
        logger.info(
            f"[schema_guard] Drift in {event.collection!r} doc_id={event.doc_id!r}: "
            f"unknown fields {sorted(unknown.keys())} — captured in extra_fields"
        )
        event.extra_fields.update(unknown)

    return True
