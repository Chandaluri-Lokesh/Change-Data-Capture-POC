"""
Schema drift detection and payload validation.

Validates that required fields are present and non-None before writing to Postgres.
Logs unknown fields (schema drift) but does NOT reject events for them —
unknown fields are captured in ParsedEvent.extra_fields for audit purposes.

Why row-level validation (not just DDL constraints)?
  Postgres NOT NULL constraints raise at write time, which would push the
  entire batch to DLQ.  Validating here lets us DLQ individual bad rows
  and still write the rest of the batch.

Edge cases
──────────
- op=d / tombstone: no document body needed — always returns True.
- required field present but None: treated as missing (same as absent).
- collection not in REQUIRED_FIELDS: no required fields → always True.
- extra/unknown fields: logged as INFO (schema drift); stored in extra_fields.
- Collection rename / drop: Debezium stops producing events silently.
  schema_guard cannot detect this — use the staleness check in offset_manager.
"""

import logging
from transformer.debezium_parser import ParsedEvent

logger = logging.getLogger(__name__)

# Fields that must be non-None for a write-eligible event
REQUIRED_FIELDS: dict[str, list] = {
    'orders':   ['customer_id', 'status'],
    'products': ['sku', 'price'],
}

# Known fields per collection (for drift detection)
KNOWN_FIELDS: dict[str, set] = {
    'orders':   {'_id', 'customer_id', 'status', 'city', 'total', 'items'},
    'products': {'_id', 'sku', 'name', 'price', 'qty_on_hand'},
}


def validate(event: ParsedEvent) -> bool:
    """
    Returns True if the event is safe to write to Postgres.

    - Delete and tombstone events always pass (no document body to validate).
    - Checks required fields are present and non-None; returns False if any missing.
    - Logs unknown fields (schema drift) and stores them in event.extra_fields.
      Unknown fields do NOT cause a False return — they are captured for audit.

    Side-effect: may populate event.extra_fields with unknown field values.
    """
    if event.op in ('d', 'tombstone'):
        return True

    if not event.document:
        logger.warning(
            f"[schema_guard] op={event.op!r} on {event.collection!r} "
            f"has no document body — routing to DLQ. doc_id={event.doc_id!r}"
        )
        return False

    # ── Required field check ─────────────────────────────────────────────────
    required = REQUIRED_FIELDS.get(event.collection, [])
    missing  = [f for f in required if event.document.get(f) is None]

    if missing:
        logger.warning(
            f"[schema_guard] Missing required fields {missing} in {event.collection!r} "
            f"doc_id={event.doc_id!r} — routing to DLQ"
        )
        return False

    # ── Schema drift detection ───────────────────────────────────────────────
    known   = KNOWN_FIELDS.get(event.collection, set())
    unknown = {k: v for k, v in event.document.items() if k not in known}

    if unknown:
        logger.info(
            f"[schema_guard] Drift in {event.collection!r} doc_id={event.doc_id!r}: "
            f"unknown fields {sorted(unknown.keys())} — captured in extra_fields"
        )
        event.extra_fields.update(unknown)

    return True
